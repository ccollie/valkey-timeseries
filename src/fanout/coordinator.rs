use super::cluster::{fanout_cluster_message, is_cluster_mode, register_message_receiver, send_cluster_message};
use crate::common::db::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::ids::flake_id;
use crate::common::pool::get_pooled_buffer;
use crate::fanout::request::{
    deserialize_error_response,
    serialize_error_response,
    CardinalityRequest,
    CardinalityResponse,
    ErrorResponse,
    IndexQueryRequest,
    IndexQueryResponse,
    LabelNamesRequest,
    LabelNamesResponse,
    LabelValuesRequest,
    LabelValuesResponse,
    MGetRequest,
    MessageHeader,
    MultiGetResponse,
    MultiRangeRequest,
    MultiRangeResponse,
    RangeRequest,
    RangeResponse,
    Request,
    Response
};
use crate::fanout::results_tracker::ResponseCallback;
use crate::fanout::types::{ClusterMessageType, InFlightRequest};
use papaya::HashMap;
use std::os::raw::{c_char, c_uchar};
use std::sync::LazyLock;
use valkey_module::RedisModuleCtx;
use valkey_module::{BlockedClient, Context, Status, ThreadSafeContext, ValkeyModuleCtx};

const CLUSTER_REQUEST_MESSAGE: u8 = 0x01;
const CLUSTER_RESPONSE_MESSAGE: u8 = 0x02;


pub(super) type InFlightRequestMap = HashMap<u64, InFlightRequest, BuildNoHashHasher<u64>>;

pub(super) static INFLIGHT_REQUEST_MAP: LazyLock<InFlightRequestMap> =
    LazyLock::new(InFlightRequestMap::default);

pub(super) fn add_inflight_request(
    request_id: u64,
    request: InFlightRequest,
) {
    let guard = INFLIGHT_REQUEST_MAP.guard();
    INFLIGHT_REQUEST_MAP.insert(request_id, request, &guard);
}

pub fn send_request<T: Request<R>, R: Response, F>(
    ctx: &Context,
    request: &T,
    callback: F
) -> u64
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, &[R]) + Send + 'static,
{
    let id = flake_id();
    let db = get_current_db(ctx);

    let msg_type = <T as Request<R>>::request_type();
    let mut buf = get_pooled_buffer(512);
    let header = MessageHeader {
        request_id: id,
        msg_type,
        db,
    };
    header.serialize(&mut buf);
    request.serialize(&mut buf);

    let node_count = fanout_cluster_message(ctx, CLUSTER_REQUEST_MESSAGE, buf.as_slice());

    let tracker = T::create_tracker(ctx, id, node_count, callback);

    let inflight_request = InFlightRequest::new(
        db,
        tracker,
    );

    if node_count > 0 {
        add_inflight_request(id, inflight_request);
        id
    } else {
        // return error !
        // log error
        0
    }
}

fn send_response_message(ctx: &Context, sender_id: *const c_char, buf: &[u8]) -> Status {
    // Send the response back to the original sender
    send_cluster_message(
        ctx,
        sender_id,
        CLUSTER_RESPONSE_MESSAGE,
        buf,
    )
}

fn send_error_response(
    ctx: &Context,
    request_id: u64,
    target_node: *const c_char,
    error_response: &ErrorResponse,
) -> Status {
    let mut buf = get_pooled_buffer(512);
    // Write request_id to buf
    buf.extend_from_slice(&request_id.to_le_bytes());
    serialize_error_response(&mut buf, &error_response.error_message);
    send_response_message(ctx, target_node, &buf)
}

pub(super) fn create_response_done_callback<T, F>(ctx: &Context, request_id: u64, callback: F) -> ResponseCallback<T>
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[T]) + Send + 'static
{
    let thread_ctx = ThreadSafeContext::with_blocked_client(ctx.block_client());

    let tracker_callback: ResponseCallback<T> = Box::new(move |res| {
        let map = INFLIGHT_REQUEST_MAP.pin();
        match map.remove(&request_id) {
            Some(inflight_request) => {
                let db = inflight_request.db;
                let save_db = {
                    let ctx_locked = thread_ctx.lock();
                    let save_db = get_current_db(&ctx_locked);
                    if save_db != db {
                        set_current_db(&ctx_locked, inflight_request.db);
                    }
                    save_db
                };

                match res {
                    Ok(res) => {
                        callback(&thread_ctx, &res);
                    }
                    Err(_e) => {
                        // Handle no response
                        let ctx_locked = thread_ctx.lock();
                        ctx_locked.log_warning("No response received");
                    }
                }

                {
                    let ctx_locked = thread_ctx.lock();
                    if save_db != db {
                        set_current_db(&ctx_locked, save_db);
                    }
                }
            }
            None => {
                let ctx_locked = thread_ctx.lock();
                // todo: trigger error
                ctx_locked.log_warning("missing inflight request:");
            }
        }
    });

    tracker_callback
}

fn parse_header(ctx: &Context, payload: *const c_uchar, len: u32) -> Option<(MessageHeader, &[u8])> {
    if payload.is_null() || len < MessageHeader::serialized_size() as u32 {
        ctx.log_warning("Invalid payload");
        return None;
    }
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    // Attempt to deserialize the header
    MessageHeader::deserialize(buffer).map_or_else(
        || {
            ctx.log_warning("Failed to deserialize message header");
            None
        },
        |(header, consumed_bytes)| Some((header, &buffer[consumed_bytes..]))
    )
}


pub fn process_request<T: Request<R>, R: Response>(
    ctx: &Context,
    header: &MessageHeader,
    sender_id: *const c_char,
    buf: &[u8]
) {
    // Deserialize the index query request
    let request = match T::deserialize(buf) {
        Ok(request) => request,
        Err(e) => {
            let msg = format!("Failed to deserialize request: {:?}", e);
            ctx.log_warning(&msg);
            return;
        }
    };

    let request_id = header.request_id;
    let db = header.db;
    let save_db = {
        let save_db = get_current_db(&ctx);
        if save_db != db {
            set_current_db(ctx, db);
        }
        save_db
    };

    match request.exec(ctx) {
        Ok(response) => {
            if save_db != db {
                set_current_db(ctx, save_db);
            }
            let mut buf = get_pooled_buffer(1024);
            // Write request_id (u64) as little-endian
            buf.extend_from_slice(&request_id.to_le_bytes());
            response.serialize(&mut buf);
            if send_response_message(ctx, sender_id, &buf) == Status::Err {
                let msg = format!("Failed to send response message to node {:?}", sender_id);
                ctx.log_warning(&msg);
            }
        }
        Err(e) => {
            if save_db != db {
                set_current_db(ctx, save_db);
            }
            let msg = format!("Failed to execute request: {:?}", e);
            // should we send error response?
            ctx.log_warning(&msg);
        }
    }
}

extern "C" fn on_request_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    // Deserialize the header
    let Some((header, buf)) = parse_header(&ctx, payload, len) else {
        return;
    };

    if buf.is_empty() {
        ctx.log_warning(&format!(
            "BUG: payload for request type {:?} is empty",
            header.msg_type
        ));
        return;
    }

    // Handle the request based on the message type
    match header.msg_type {
        ClusterMessageType::IndexQuery =>
            process_request::<IndexQueryRequest, IndexQueryResponse>(&ctx, &header, sender_id, buf),
        ClusterMessageType::MGetQuery =>
            process_request::<MGetRequest, MultiGetResponse>(&ctx, &header, sender_id, buf),
        ClusterMessageType::MultiRangeQuery =>
            process_request::<MultiRangeRequest, MultiRangeResponse>(&ctx, &header, sender_id, buf),
        ClusterMessageType::LabelNames =>
            process_request::<LabelNamesRequest, LabelNamesResponse>(&ctx, &header, sender_id, buf),
        ClusterMessageType::LabelValues =>
            process_request::<LabelValuesRequest, LabelValuesResponse>(&ctx, &header, sender_id, buf),
        ClusterMessageType::RangeQuery =>
            process_request::<RangeRequest, RangeResponse>(&ctx, &header, sender_id, buf),
        ClusterMessageType::Cardinality =>
            process_request::<CardinalityRequest, CardinalityResponse>(&ctx, &header, sender_id, buf),
        _ => ctx.log_warning(&format!("Unknown message type: {:?}", header.msg_type)),
    }
}

const U64_SIZE: usize = size_of::<u64>();

fn parse_response_header<'a>(ctx: &'a Context, buffer: &'a [u8]) -> Option<(u64, &'a [u8])> {
    if buffer.len() >= U64_SIZE {
        match buffer[..U64_SIZE].try_into() {
            Ok(request_id_bytes) => {
                let request_id = u64::from_le_bytes(request_id_bytes);
                Some((request_id, &buffer[U64_SIZE..]))
            }
            Err(_) => {
                ctx.log_warning("Failed to parse request ID");
                None
            }
        }
    } else {
        ctx.log_warning("Invalid response payload");
        None
    }
}

/// Processes a valid error response by notifying the corresponding in-flight request.
fn process_error_response(ctx: &Context, request: &InFlightRequest, buf: &[u8]) {
    match deserialize_error_response(buf) {
        Ok(error_response) => {
            request.raise_error(error_response.error_message)
        },
        Err(_) => ctx.log_warning("Failed to deserialize error response"),
    }
}

fn process_response<R: Response>(
    ctx: &Context,
    request: &InFlightRequest,
    buf: &[u8],
)
{
    let response = match R::deserialize(buf) {
        Ok(response) => response,
        Err(_) => {
            // Handle deserialization error
            let msg_type = request.request_type();
            let msg = format!("BUG: Failed to deserialize response for type {msg_type}");
            ctx.log_warning(&msg);
            return;
        }
    };

    R::update_tracker(&request.responses, response);
}

/// Handles responses from other nodes in the cluster.
extern "C" fn on_cluster_response_received(
    ctx: *mut ValkeyModuleCtx,
    _sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    if payload.is_null() ||  len < U64_SIZE as u32 {
        ctx.log_warning("Invalid response payload");
        return;
    }
    let buf = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    let Some((request_id, buf)) = parse_response_header(&ctx, buf) else {
        return;
    };

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUEST_MAP.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!("Failed to find inflight request for id {}", request_id));
        return;
    };

    let msg_type = request.request_type();

    if buf.is_empty() {
        let msg = format!("BUG: empty response payload for request type {msg_type}({request_id})");
        ctx.log_warning(&msg);
        return;
    }

    match msg_type {
        ClusterMessageType::IndexQueryResponse =>
            process_response::<IndexQueryResponse>(&ctx, request, buf),
        ClusterMessageType::MultiGetResponse =>
            process_response::<MultiGetResponse>(&ctx, request, buf),
        ClusterMessageType::MultiRangeQueryResponse =>
            process_response::<MultiRangeResponse>(&ctx, request, buf),
        ClusterMessageType::LabelNamesResponse =>
            process_response::<LabelNamesResponse>(&ctx, request, buf),
        ClusterMessageType::LabelValuesResponse =>
            process_response::<LabelValuesResponse>(&ctx, request, buf),
        ClusterMessageType::RangeQueryResponse =>
            process_response::<RangeResponse>(&ctx, request, buf),
        ClusterMessageType::CardinalityResponse =>
            process_response::<CardinalityResponse>(&ctx, request, buf),
        ClusterMessageType::Error => {
            process_error_response(&ctx, request, buf);
        }
        _ => ctx.log_warning(&format!("Unknown message type: {:?}", msg_type)),
    }
}

pub fn register_cluster_message_handlers(ctx: &Context) {
    if !is_cluster_mode(ctx) {
        return;
    }
    // Register the cluster message handlers
    register_message_receiver(
        ctx,
        CLUSTER_REQUEST_MESSAGE,
        Some(on_request_received),
    );
    register_message_receiver(
        ctx,
        CLUSTER_RESPONSE_MESSAGE,
        Some(on_cluster_response_received),
    );
    //
    // register_message_receiver(
    //     ctx,
    //     CLUSTER_ERROR_MESSAGE,
    //     on_cluster_request_received,
    // );
}