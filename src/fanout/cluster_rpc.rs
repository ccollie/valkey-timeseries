use super::cluster_message::{RequestMessage, serialize_request_message};
use super::fanout_error::{FanoutError, NO_CLUSTER_NODES_AVAILABLE};
use super::utils::{generate_id, is_clustered, is_multi_or_lua};
use crate::common::db::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::fanout::{FanoutResult, NodeInfo};
use crate::fanout::cluster_map::NodeLocation;
use crate::fanout::registry::get_fanout_request_handler;
use core::time::Duration;
use papaya::HashMap;
use std::os::raw::{c_char, c_int, c_uchar};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use valkey_module::{
    Context, RedisModuleCtx, Status, VALKEYMODULE_OK, ValkeyError, ValkeyModule_GetMyClusterID,
    ValkeyModule_RegisterClusterMessageReceiver, ValkeyModule_SendClusterMessage,
    ValkeyModuleClusterMessageReceiver, ValkeyModuleCtx, ValkeyResult,
};

pub(super) const CLUSTER_REQUEST_MESSAGE: u8 = 0x01;
pub(super) const CLUSTER_RESPONSE_MESSAGE: u8 = 0x02;
pub(super) const CLUSTER_ERROR_MESSAGE: u8 = 0x03;

pub static DEFAULT_CLUSTER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) type ResponseCallback =
    Box<dyn Fn(FanoutResult<&[u8]>, &NodeInfo) + Send + Sync>;

struct InFlightRequest {
    id: u64,
    targets: Arc<Vec<NodeInfo>>,
    response_handler: ResponseCallback,
    outstanding: AtomicU64,
    timer_id: u64,
    timed_out: AtomicBool,
}

impl InFlightRequest {
    fn rpc_done(&self, ctx: &Context) {
        if self.outstanding.fetch_sub(1, Ordering::Relaxed) == 1 {
            // Last response received, clean up
            self.cancel_timer(ctx);
            let map = INFLIGHT_REQUESTS.pin();
            map.remove(&self.id);
        }
    }

    fn cancel_timer(&self, ctx: &Context) {
        if self.timer_id > 0 {
            let _ = ctx.stop_timer::<u64>(self.timer_id);
        }
    }

    fn handle_response(&self, ctx: &Context, resp: FanoutResult<&[u8]>, sender_id: *const c_char) {
        let handler = &self.response_handler;
        // Binary search to find the NodeInfo associated with the target
        let target_node = self.targets
            .binary_search_by(|node| node.raw_id_ptr().cmp(&sender_id))
            .ok()
            .and_then(|idx| self.targets.get(idx));

        let Some(node) = target_node else {
            ctx.log_warning(&format!(
                "Received response from unknown node {:?} for request {}",
                sender_id, self.id
            ));
            return;
        };

        handler(resp, node);
    }

}

type InFlightRequestMap = HashMap<u64, InFlightRequest, BuildNoHashHasher<u64>>;

static INFLIGHT_REQUESTS: LazyLock<InFlightRequestMap> = LazyLock::new(InFlightRequestMap::default);

fn on_request_timeout(ctx: &Context, id: u64) {
    // We only mark the request as timed out the first time through. The actual removal from the map
    // happens when the last response arrives or when we hit the timeout again.
    let map = INFLIGHT_REQUESTS.pin();
    if let Some(request) = map.get(&id) {
        if request.timed_out.load(Ordering::Relaxed) {
            let _ = ctx.stop_timer::<u64>(request.timer_id);
            // Already timed out, remove from the map
            map.remove(&id);
            return;
        }
        request.timed_out.store(true, Ordering::Relaxed);

        let local_node_id = get_current_node_id();

        request.handle_response(ctx, Err(FanoutError::timeout()), local_node_id);
        // Reset the timer to give some extra time for late responses
    }
}

fn get_current_node_id() -> *const c_char {
    unsafe {
        let node_id = ValkeyModule_GetMyClusterID
            .expect("ValkeyModule_GetMyClusterID function is unavailable")();
        node_id as *const c_char
    }
}

fn validate_cluster_exec(ctx: &Context) -> ValkeyResult<()> {
    if !is_clustered(ctx) {
        return Err(ValkeyError::Str("Cluster mode is not enabled"));
    }
    if is_multi_or_lua(ctx) {
        return Err(ValkeyError::Str("Cannot execute in MULTI or Lua context"));
    }
    Ok(())
}

pub fn get_cluster_command_timeout() -> Duration {
    DEFAULT_CLUSTER_REQUEST_TIMEOUT
}

pub(super) fn send_cluster_request<F>(
    ctx: &Context,
    request_buf: &[u8],
    targets: Arc<Vec<NodeInfo>>,
    handler: &str,
    response_handler: F,
    timeout: Option<Duration>,
) -> ValkeyResult<()>
where
    F: Fn(Result<&[u8], FanoutError>, &NodeInfo) + Send + Sync + 'static,
{
    validate_cluster_exec(ctx)?;

    let id = generate_id();
    let db = get_current_db(ctx);

    let mut buf = Vec::with_capacity(512);
    serialize_request_message(&mut buf, id, db, handler, request_buf);

    let mut node_count = 0;
    for node in targets.iter() {
        if node.location == NodeLocation::Remote {
            let target_id = node.id.as_ptr().cast::<c_char>();
            let status =
                send_cluster_message(ctx, target_id, CLUSTER_REQUEST_MESSAGE, buf.as_slice());
            if status == Status::Err {
                let msg = format!("Failed to send message to node {}", node.address());
                ctx.log_warning(&msg);
                continue;
            }
            node_count += 1;
        }
    }

    if node_count == 0 {
        return Err(ValkeyError::Str(NO_CLUSTER_NODES_AVAILABLE));
    }

    let timeout = timeout.unwrap_or_else(get_cluster_command_timeout);
    let timer_id = ctx.create_timer(timeout, on_request_timeout, id);

    let request = InFlightRequest {
        id,
        response_handler: Box::new(response_handler),
        timer_id,
        outstanding: AtomicU64::new(node_count as u64),
        timed_out: AtomicBool::new(false),
        targets: targets.clone(),
    };

    let map = INFLIGHT_REQUESTS.pin();
    map.insert(id, request);
    Ok(())
}

/// Send the response back to the original sender
fn send_message_internal(
    ctx: &Context,
    msg_type: u8,
    request_id: u64,
    sender_id: *const c_char,
    handler: &str,
    buf: &[u8],
) -> Status {
    let mut dest = Vec::with_capacity(1024);
    let db = get_current_db(ctx);
    serialize_request_message(&mut dest, request_id, db, handler, buf);
    send_cluster_message(ctx, sender_id, msg_type, buf)
}

// Send the response back to the original sender
fn send_response_message(
    ctx: &Context,
    request_id: u64,
    sender_id: *const c_char,
    handler: &str,
    buf: &[u8],
) -> Status {
    send_message_internal(
        ctx,
        CLUSTER_RESPONSE_MESSAGE,
        request_id,
        sender_id,
        handler,
        buf,
    )
}

fn send_error_response(
    ctx: &Context,
    request_id: u64,
    target_node: *const c_char,
    error: FanoutError,
) -> Status {
    let mut buf: Vec<u8> = Vec::with_capacity(512);
    error.serialize(&mut buf);
    send_message_internal(
        ctx,
        CLUSTER_ERROR_MESSAGE,
        request_id,
        target_node,
        "",
        &buf,
    )
}

fn parse_cluster_message(
    ctx: &'_ Context,
    sender_id: Option<*const c_char>,
    payload: *const c_uchar,
    len: u32,
) -> Option<RequestMessage<'_>> {
    let buffer = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    match RequestMessage::new(buffer) {
        Ok(msg) => {
            let buf = msg.buf;
            if buf.is_empty() {
                let request_id = msg.request_id;
                let msg = format!("BUG: empty response payload for request ({request_id})");
                ctx.log_warning(&msg);
                if let Some(sender_id) = sender_id {
                    let error = FanoutError::invalid_message();
                    let _ = send_error_response(ctx, 0, sender_id, error);
                }
                return None;
            }
            Some(msg)
        }
        Err(e) => {
            let msg = format!("Failed to parse cluster message: {e}");
            ctx.log_warning(&msg);
            if let Some(sender_id) = sender_id {
                let _ = send_error_response(ctx, 0, sender_id, e.into());
            }
            None
        }
    }
}

/// Processes a valid request by executing the command and sending back the response.
fn process_request<'a>(ctx: &'a Context, message: RequestMessage<'a>, sender_id: *const c_char) {
    let request_id = message.request_id;

    let handler = get_fanout_request_handler(&message.handler, true);

    let save_db = get_current_db(ctx);
    set_current_db(ctx, message.db);

    let mut dest = Vec::with_capacity(1024);
    let buf = message.buf;

    // todo: run in thread pool?
    let res = handler(ctx, buf, &mut dest);

    // I'm not sure if we need to restore the db here, but just in case
    set_current_db(ctx, save_db);
    if let Err(e) = res {
        let msg = e.to_string();
        send_error_response(ctx, request_id, sender_id, e.into());
        ctx.log_warning(&msg);
        return;
    };

    if send_response_message(ctx, request_id, sender_id, &message.handler, &dest) == Status::Err {
        let msg = format!("Failed to send response message to node {sender_id:?}");
        ctx.log_warning(&msg);
    }
}

/// Sends a message to a specific cluster node.
///
/// # Arguments
///
/// * `target_node_id` - The 40-byte hex ID of the target node.
/// * `msg_type` - The type of the message to send.
/// * `message_body` - The raw byte payload of the message.
///
/// # Returns
///
/// `Status::Ok` on success, `Status::Err` with a message on failure.
pub fn send_cluster_message(
    ctx: &Context,
    target_node_id: *const c_char,
    msg_type: u8,
    message_body: &[u8],
) -> Status {
    unsafe {
        if ValkeyModule_SendClusterMessage
            .expect("ValkeyModule_SendClusterMessage is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            target_node_id,
            msg_type,
            message_body.as_ptr().cast::<c_char>(),
            message_body.len() as u32,
        ) == VALKEYMODULE_OK as c_int
        {
            Status::Ok
        } else {
            Status::Err
        }
    }
}

/// Handles incoming requests from requester nodes in the cluster.
extern "C" fn on_request_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    let Some(message) = parse_cluster_message(&ctx, Some(sender_id), payload, len) else {
        return;
    };

    process_request(&ctx, message, sender_id);
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

    let Some(message) = parse_cluster_message(&ctx, None, payload, len) else {
        return;
    };

    let request_id = message.request_id;

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}. Possible timeout.",
        ));
        return;
    };

    request.rpc_done(&ctx);

    request.handle_response(&ctx, Ok(message.buf), sender_id);
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let Some(message) = parse_cluster_message(&ctx, None, payload, len) else {
        return;
    };

    let request_id = message.request_id;

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}. Possible timeout.",
        ));
        return;
    };

    request.rpc_done(&ctx);

    match FanoutError::deserialize(message.buf) {
        Ok((error, _)) => request.handle_response(&ctx, Err(error), sender_id),
        Err(_) => {
            ctx.log_warning("Failed to deserialize error response");
            let err = FanoutError::invalid_message();
            request.handle_response(&ctx, Err(err), sender_id);
        }
    }
}

/// Registers a callback function to handle incoming cluster messages.
/// This should typically be called during module initialization (e.g., ValkeyModule_OnLoad).
///
/// ## Arguments
///
/// * `receiver_func` - The function pointer matching the expected signature
///   `ValkeyModuleClusterMessageReceiverFunc`.
///
/// ## Safety
///
/// The provided `receiver_func` must be valid for the lifetime of the module
/// and correctly handle the arguments passed by Valkey.
fn register_message_receiver(
    ctx: &Context, // Static method as it's usually done once at load time
    type_: u8,
    receiver_func: ValkeyModuleClusterMessageReceiver,
) {
    unsafe {
        ValkeyModule_RegisterClusterMessageReceiver
            .expect("ValkeyModule_RegisterClusterMessageReceiver is not available")(
            ctx.ctx as *mut ValkeyModuleCtx,
            type_,
            receiver_func,
        );
    }
}

/// Registers the cluster message handlers for request, response, and error messages.
/// This function should be called during module initialization
pub fn register_cluster_message_handlers(ctx: &Context) {
    // Register the cluster message handlers
    register_message_receiver(ctx, CLUSTER_REQUEST_MESSAGE, Some(on_request_received));
    register_message_receiver(ctx, CLUSTER_RESPONSE_MESSAGE, Some(on_response_received));
    register_message_receiver(ctx, CLUSTER_ERROR_MESSAGE, Some(on_error_received));
}
