use super::cluster::{
    fanout_cluster_message, is_cluster_mode, is_multi_or_lua, register_message_receiver,
    send_cluster_message,
};
use crate::common::db::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::ids::flake_id;
use crate::common::pool::get_pooled_buffer;
use crate::fanout::error::Error;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::request::{
    CardinalityCommand, IndexQueryCommand, LabelNamesCommand, LabelValuesCommand, MessageHeader,
};
use crate::fanout::results_tracker::ResponseCallback;
use crate::fanout::{
    CommandMessageType, MGetShardedCommand, MultiRangeCommand, MultiShardCommand, RangeCommand,
    ResultsTracker, StatsCommand, TrackerEnum,
};
use crate::{config, error_consts};
use core::time::Duration;
use papaya::HashMap;
use std::os::raw::{c_char, c_uchar};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;
use valkey_module::{
    BlockedClient, Context, RedisModuleTimerID, Status, ThreadSafeContext, ValkeyModuleCtx,
};
use valkey_module::{RedisModuleCtx, ValkeyError, ValkeyResult};

const CLUSTER_REQUEST_MESSAGE: u8 = 0x01;
const CLUSTER_RESPONSE_MESSAGE: u8 = 0x02;
const CLUSTER_ERROR_MESSAGE: u8 = 0x03;

struct InFlightRequest {
    pub command_type: CommandMessageType,
    pub db: i32,
    pub responses: TrackerEnum,
    pub timer_id: RedisModuleTimerID,
    pub timed_out: AtomicBool,
}

impl InFlightRequest {
    pub fn new(db: i32, request_type: CommandMessageType, tracker: TrackerEnum) -> Self {
        Self {
            db,
            responses: tracker,
            timer_id: 0,
            timed_out: AtomicBool::new(false),
            command_type: request_type,
        }
    }

    pub fn is_completed(&self) -> bool {
        self.responses.is_completed()
    }

    pub fn call_done(&self) {
        self.responses.call_done();
    }

    pub fn is_timed_out(&self) -> bool {
        self.timed_out.load(Ordering::SeqCst)
    }

    pub fn raise_error(&self, error: Error) {
        self.responses.raise_error(error);
        if !self.is_completed() {
            self.call_done();
        }
    }

    pub fn time_out(&self) {
        self.timed_out.store(true, Ordering::SeqCst);
        if !self.is_completed() {
            self.call_done();
        }
    }
}

type InFlightRequestMap = HashMap<u64, InFlightRequest, BuildNoHashHasher<u64>>;

static INFLIGHT_REQUESTS: LazyLock<InFlightRequestMap> = LazyLock::new(InFlightRequestMap::default);

fn validate_cluster_exec(ctx: &Context) -> ValkeyResult<()> {
    if !is_cluster_mode(ctx) {
        return Err(ValkeyError::Str("Cluster mode is not enabled"));
    }
    if is_multi_or_lua(ctx) {
        return Err(ValkeyError::Str("Cannot execute in MULTI or Lua context"));
    }
    Ok(())
}

fn on_command_timeout(ctx: &Context, id: u64) {
    let map = INFLIGHT_REQUESTS.pin();
    if let Some(request) = map.get(&id) {
        let _ = ctx.stop_timer::<u64>(request.timer_id);
        if !request.is_completed() {
            request.time_out();
        }
    }
}

fn get_multi_shard_command_timeout() -> Duration {
    let timeout = config::MULTI_SHARD_COMMAND_TIMEOUT.load(Ordering::Relaxed);
    Duration::from_millis(timeout)
}

pub fn send_multi_shard_request<T: MultiShardCommand, F>(
    ctx: &Context,
    request: &T::REQ,
    callback: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<T::RES>) + Send + 'static,
    TrackerEnum: From<ResultsTracker<T::RES>>,
{
    validate_cluster_exec(ctx)?;

    let id = flake_id();
    let db = get_current_db(ctx);

    let mut buf = get_pooled_buffer(512);
    let msg_type = T::request_type();
    let header = MessageHeader {
        request_id: id,
        msg_type,
        db,
        reserved: 0,
    };
    header.serialize(&mut buf);
    request.serialize(&mut buf);

    let node_count = fanout_cluster_message(ctx, CLUSTER_REQUEST_MESSAGE, buf.as_slice());

    let tracker = create_tracker::<T, F>(ctx, id, node_count, callback);

    let mut inflight_request = InFlightRequest::new(db, msg_type, tracker);

    if node_count > 0 {
        let timeout = get_multi_shard_command_timeout();
        let timer_id = ctx.create_timer(timeout, on_command_timeout, id);
        inflight_request.timer_id = timer_id;
        let map = INFLIGHT_REQUESTS.pin();
        map.insert(id, inflight_request);
        Ok(id)
    } else {
        // return error !
        Err(ValkeyError::Str(error_consts::NO_CLUSTER_NODES_AVAILABLE))
    }
}

fn create_tracker<T: MultiShardCommand, F>(
    ctx: &Context,
    request_id: u64,
    expected_results: usize,
    callback: F,
) -> TrackerEnum
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<T::RES>) + Send + 'static,
    TrackerEnum: From<ResultsTracker<T::RES>>,
{
    let cbk = create_response_done_callback(ctx, request_id, callback);

    let tracker: ResultsTracker<T::RES> = ResultsTracker::new(expected_results, cbk);

    TrackerEnum::from(tracker)
}

fn send_response_message(ctx: &Context, sender_id: *const c_char, buf: &[u8]) -> Status {
    // Send the response back to the original sender
    send_cluster_message(ctx, sender_id, CLUSTER_RESPONSE_MESSAGE, buf)
}

fn send_error_response(
    ctx: &Context,
    request_id: u64,
    target_node: *const c_char,
    error: Error,
) -> Status {
    let mut buf = get_pooled_buffer(512);
    // Write request_id to buf
    buf.extend_from_slice(&request_id.to_le_bytes());
    // Serialize the error response
    error.serialize(&mut buf);
    send_cluster_message(ctx, target_node, CLUSTER_ERROR_MESSAGE, &buf)
}

fn create_response_done_callback<T, F>(
    ctx: &Context,
    request_id: u64,
    callback: F,
) -> ResponseCallback<T>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<T>) + Send + 'static,
{
    let thread_ctx = ThreadSafeContext::with_blocked_client(ctx.block_client());

    let tracker_callback: ResponseCallback<T> = Box::new(move |res, errors| {
        let map = INFLIGHT_REQUESTS.pin();
        match map.remove(&request_id) {
            Some(inflight_request) => {
                {
                    let ctx_locked = thread_ctx.lock();
                    let _ = ctx_locked.stop_timer::<u64>(inflight_request.timer_id);
                };

                if errors.is_empty() {
                    let db = inflight_request.db;
                    let save_db = {
                        let ctx_locked = thread_ctx.lock();
                        let save_db = get_current_db(&ctx_locked);
                        if save_db != db {
                            set_current_db(&ctx_locked, inflight_request.db);
                        }
                        save_db
                    };
                    callback(&thread_ctx, res);
                    if save_db != db {
                        let ctx_locked = thread_ctx.lock();
                        set_current_db(&ctx_locked, save_db);
                    }
                } else {
                    let ctx_locked = thread_ctx.lock();
                    if inflight_request.is_timed_out() {
                        let msg = format!(
                            "Multi-shard command {} timed out after {} ms",
                            inflight_request.command_type,
                            get_multi_shard_command_timeout().as_millis()
                        );
                        ctx_locked.reply_error_string(&msg);
                        ctx_locked.log_warning(&msg);
                    } else {
                        // get the first error and send it back
                        let msg = errors[0].to_string();
                        ctx_locked.reply_error_string(&msg);
                        for error in errors.iter() {
                            let msg = format!("Error: {}", error);
                            ctx_locked.log_warning(&msg);
                        }
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

fn parse_header(
    ctx: &Context,
    payload: *const c_uchar,
    len: u32,
) -> Option<(MessageHeader, &[u8])> {
    if payload.is_null() || len < MessageHeader::min_serialized_size() as u32 {
        ctx.log_warning(error_consts::COMMAND_DESERIALIZATION_ERROR);
        return None;
    }
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    // Attempt to deserialize the header
    MessageHeader::deserialize(buffer).map_or_else(
        || {
            ctx.log_warning(error_consts::COMMAND_DESERIALIZATION_ERROR);
            None
        },
        |(header, consumed_bytes)| Some((header, &buffer[consumed_bytes..])),
    )
}

// Processes a valid request by executing the command and sending back the response.
fn process_request<T: MultiShardCommand>(
    ctx: &Context,
    header: &MessageHeader,
    sender_id: *const c_char,
    buf: &[u8],
) {
    let request_id = header.request_id;

    // Deserialize the request
    let request = match T::REQ::deserialize(buf) {
        Ok(request) => request,
        Err(e) => {
            let msg = e.to_string();
            send_error_response(ctx, request_id, sender_id, e.into());
            ctx.log_warning(&msg);
            return;
        }
    };

    let db = header.db;
    let save_db = {
        let save_db = get_current_db(ctx);
        if save_db != db {
            set_current_db(ctx, db);
        }
        save_db
    };

    match T::exec(ctx, request) {
        Ok(response) => {
            let mut buf = get_pooled_buffer(1024);
            // Write request_id (u64) as little-endian
            buf.extend_from_slice(&request_id.to_le_bytes());
            response.serialize(&mut buf);
            if send_response_message(ctx, sender_id, &buf) == Status::Err {
                let msg = format!(
                    "Failed to send {} response message to node {:?}",
                    T::request_type(),
                    sender_id
                );
                ctx.log_warning(&msg);
            }
        }
        Err(err) => {
            send_error_response(ctx, request_id, sender_id, err.into());
        }
    }

    if save_db != db {
        set_current_db(ctx, save_db);
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
        CommandMessageType::IndexQuery => {
            process_request::<IndexQueryCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::MGetQuery => {
            process_request::<MGetShardedCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::MultiRangeQuery => {
            process_request::<MultiRangeCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::LabelNames => {
            process_request::<LabelNamesCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::LabelValues => {
            process_request::<LabelValuesCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::RangeQuery => {
            process_request::<RangeCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::Cardinality => {
            process_request::<CardinalityCommand>(&ctx, &header, sender_id, buf)
        }
        CommandMessageType::Stats => process_request::<StatsCommand>(&ctx, &header, sender_id, buf),
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
    match Error::deserialize(buf) {
        Ok(error) => request.raise_error(error),
        Err(_) => ctx.log_warning("Failed to deserialize error response"),
    }
}

fn process_response<T: MultiShardCommand>(
    ctx: &Context,
    request: &InFlightRequest,
    request_id: u64,
    sender_id: *const c_char,
    buf: &[u8],
) {
    let response = match T::RES::deserialize(buf) {
        Ok(response) => response,
        Err(e) => {
            let msg = e.to_string();
            send_error_response(ctx, request_id, sender_id, e.into());
            ctx.log_warning(&msg);
            return;
        }
    };

    T::update_tracker(&request.responses, response);
}

/// Handles responses from other nodes in the cluster. The receiver is the original sender of
/// the request.
extern "C" fn on_response_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    if payload.is_null() || len < U64_SIZE as u32 {
        ctx.log_warning("Invalid response payload");
        return;
    }
    let buf = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    let Some((request_id, buf)) = parse_response_header(&ctx, buf) else {
        return;
    };

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {}",
            request_id
        ));
        return;
    };

    let msg_type = request.command_type;

    if buf.is_empty() {
        let msg = format!("BUG: empty response payload for request type {msg_type}({request_id})");
        ctx.log_warning(&msg);
        return;
    }

    match msg_type {
        CommandMessageType::IndexQuery => {
            process_response::<IndexQueryCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::MGetQuery => {
            process_response::<MGetShardedCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::MultiRangeQuery => {
            process_response::<MultiRangeCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::LabelNames => {
            process_response::<LabelNamesCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::LabelValues => {
            process_response::<LabelValuesCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::RangeQuery => {
            process_response::<RangeCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::Cardinality => {
            process_response::<CardinalityCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::Stats => {
            process_response::<CardinalityCommand>(&ctx, request, request_id, sender_id, buf)
        }
        CommandMessageType::Error => {
            process_error_response(&ctx, request, buf);
        }
    }
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    _sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    if payload.is_null() || len < U64_SIZE as u32 {
        ctx.log_warning("Invalid response payload");
        return;
    }
    let buf = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    let Some((request_id, buf)) = parse_response_header(&ctx, buf) else {
        return;
    };

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {}",
            request_id
        ));
        return;
    };

    process_error_response(&ctx, request, buf);
}

pub fn register_cluster_message_handlers(ctx: &Context) {
    // Register the cluster message handlers
    register_message_receiver(ctx, CLUSTER_REQUEST_MESSAGE, Some(on_request_received));
    register_message_receiver(ctx, CLUSTER_RESPONSE_MESSAGE, Some(on_response_received));
    register_message_receiver(ctx, CLUSTER_ERROR_MESSAGE, Some(on_error_received));
}
