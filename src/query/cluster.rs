use crate::common::db::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::humanize::humanize_duration;
use crate::error_consts;
use crate::fanout::cluster::{
    fanout_cluster_message, get_current_node, register_message_receiver, send_cluster_message,
};
use crate::fanout::error::Error;
use crate::fanout::serialization::{Deserialized, Serialized};
use crate::fanout::{
    generate_request_id, get_cluster_command_timeout, parse_message_header, parse_response_header,
    CommandMessageType, MessageHeader, QuerySeriesData, SearchQueryRequest, SearchQueryResponse,
};
use crate::query::get_query_series_data;
use futures::channel::oneshot;
use metricsql_common::async_runtime::timeout;
use metricsql_common::pool::get_pooled_buffer;
use metricsql_runtime::{QueryResult, QueryResults, RuntimeError, RuntimeResult, SearchQuery};
use papaya::HashMap;
use std::os::raw::{c_char, c_uchar};
use std::sync::{LazyLock, Mutex, MutexGuard};
use valkey_module::{Context, RedisModuleCtx, Status, ValkeyModuleCtx, ValkeyResult};

const CLUSTER_REQUEST_MESSAGE: u8 = 16;
const CLUSTER_RESPONSE_MESSAGE: u8 = 17;
const CLUSTER_ERROR_MESSAGE: u8 = 18;

type SenderChannel = oneshot::Sender<RuntimeResult<QueryResults>>;

struct Inner {
    results: Vec<QueryResult>,
    outstanding_requests: u32,
    response_tx: Option<SenderChannel>,
    completed: bool,
    errored: bool,
}

struct InFlightRequest {
    inner: Mutex<Inner>,
    db: i32,
}

impl InFlightRequest {
    pub fn new(db: i32, outstanding_requests: u32, channel: SenderChannel) -> Self {
        let inner = Inner {
            results: Vec::with_capacity((outstanding_requests * 2) as usize),
            outstanding_requests,
            response_tx: Some(channel),
            completed: false,
            errored: false,
        };
        InFlightRequest {
            inner: Mutex::new(inner),
            db,
        }
    }
}

impl InFlightRequest {
    pub fn update(&self, result: SearchQueryResponse) {
        let mut results: Vec<QueryResult> =
            result.series.into_iter().map(QueryResult::from).collect();
        let mut inner = self.inner.lock().unwrap();
        inner.results.append(&mut results);
        self.decrement_internal(&mut inner);
    }

    fn return_results(
        &self,
        inner: &mut MutexGuard<Inner>,
        res: RuntimeResult<QueryResults>,
    ) -> bool {
        if inner.completed {
            // If the request is already completed, we cannot call the callback
            return false;
        }
        inner.completed = true;
        let Some(response_tx) = inner.response_tx.take() else {
            // If the response channel is not set, we cannot send the results
            // todo: log
            return false;
        };
        inner.errored = res.is_err();
        match response_tx.send(res) {
            Ok(_) => true,
            Err(_e) => {
                // If the response channel is closed, we cannot send the results}
                // todo: log
                false
            }
        }
    }

    fn callback_if_needed(&self, inner: &mut MutexGuard<Inner>) -> bool {
        if inner.outstanding_requests != 0 {
            return false;
        }
        let final_results = std::mem::take(&mut inner.results);
        let results = QueryResults::new(final_results);
        self.return_results(inner, Ok(results))
    }

    fn decrement_internal(&self, inner: &mut MutexGuard<Inner>) -> bool {
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        // If no outstanding request, execute the callback
        self.callback_if_needed(inner)
    }

    pub fn completed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.completed
    }

    pub fn is_errored(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.errored
    }

    pub fn raise_error(&self, error: Error) {
        let err = RuntimeError::General(error.to_string());
        let mut inner = self.inner.lock().unwrap();
        self.return_results(&mut inner, Err(err));
    }

    pub fn call_done(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.outstanding_requests = 0;
        self.callback_if_needed(&mut inner);
    }
}

type InFlightRequestMap = HashMap<u64, InFlightRequest, BuildNoHashHasher<u64>>;
static INFLIGHT_REQUESTS: LazyLock<InFlightRequestMap> = LazyLock::new(InFlightRequestMap::default);

/// Handles a search query request in the cluster.
pub(super) async fn cluster_search_query(
    request: SearchQuery,
    db: i32,
) -> RuntimeResult<QueryResults> {
    let request: SearchQueryRequest = request.into();

    let id = generate_request_id();
    let header = MessageHeader {
        request_id: id,
        msg_type: CommandMessageType::SearchQuery,
        db,
        reserved: 0,
    };
    let mut buf = get_pooled_buffer(256);
    header.serialize(&mut buf);
    request.serialize(&mut buf);

    let node_count = {
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        let node_count =
            fanout_cluster_message(&ctx_guard, CLUSTER_REQUEST_MESSAGE, buf.as_slice());
        drop(ctx_guard);
        if node_count == 0 {
            // If no nodes are available, we cannot proceed with the request
            return Err(RuntimeError::ProviderError(
                error_consts::NO_CLUSTER_NODES_AVAILABLE.to_string(),
            ));
        }
        node_count as u32
    };

    let (sender, receiver) = oneshot::channel::<RuntimeResult<QueryResults>>();

    // Create a new scope and drop the guard after inserting the request. Prevents holding the guard
    // across the await point.
    {
        let inflight_request = InFlightRequest::new(db, node_count, sender);
        let map = INFLIGHT_REQUESTS.pin();
        map.insert(id, inflight_request);
        drop(map)
    }

    let command_timeout = get_cluster_command_timeout();
    match timeout(command_timeout, receiver).await {
        Ok(Ok(res)) => {
            // If the request is successful, we need to clean up the in-flight request
            remove_inflight_request(id);
            res
        }
        Ok(Err(_res)) => {
            // If the request fails, we need to clean up the in-flight request
            remove_inflight_request(id);
            log_warning("search_query: oneshot channel failed to receive response");
            Err(RuntimeError::General(
                "Request canceled or timed out".to_string(),
            ))
        }
        Err(_elapsed) => {
            remove_inflight_request(id);
            let msg = format!(
                "search_query: request timed out after {}",
                humanize_duration(&command_timeout)
            );
            log_warning(&msg);
            Err(RuntimeError::deadline_exceeded(""))
        }
    }
}

fn log_warning(msg: &str) {
    let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
    let ctx = &ctx_guard;
    ctx.log_warning(msg);
}

fn remove_inflight_request(req_id: u64) -> bool {
    let in_flight_requests = INFLIGHT_REQUESTS.pin();
    in_flight_requests.remove(&req_id).is_some()
}

fn send_response_message(ctx: &Context, sender_id: *const c_char, buf: &[u8]) -> Status {
    // Send the response back to the original sender
    send_cluster_message(ctx, sender_id, CLUSTER_RESPONSE_MESSAGE, buf)
}

fn send_error_response(ctx: &Context, request_id: u64, target_node: *const c_char, error: Error) {
    let mut buf = get_pooled_buffer(256);
    // Write request_id to buf
    buf.extend_from_slice(&request_id.to_le_bytes());
    // Serialize the error response
    error.serialize(&mut buf);
    if send_cluster_message(ctx, target_node, CLUSTER_ERROR_MESSAGE, &buf) == Status::Err {
        let node_id = get_current_node();
        let msg = format!(
            "Failed to send error response for request ID {request_id}: node_id: {node_id:?}"
        );
        ctx.log_warning(&msg);
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
    let Some((header, buf)) = parse_message_header(&ctx, payload, len) else {
        return;
    };

    if buf.is_empty() {
        ctx.log_warning("BUG: payload for search query request is empty");
        send_error_response(
            &ctx,
            header.request_id,
            sender_id,
            Error::from(error_consts::COMMAND_DESERIALIZATION_ERROR),
        );
        return;
    }

    let db = header.db;
    let request_id = header.request_id;

    let Ok(request) = SearchQueryRequest::deserialize(buf) else {
        // todo: return an error response to the sender
        ctx.log_warning("Failed to deserialize request payload");
        send_error_response(
            &ctx,
            request_id,
            sender_id,
            Error::from(error_consts::COMMAND_DESERIALIZATION_ERROR),
        );
        return;
    };

    let save_db = get_current_db(&ctx);
    set_current_db(&ctx, db);

    match process_request(&ctx, request) {
        Ok(response) => {
            set_current_db(&ctx, save_db);
            // Serialize the response and send it back to the sender
            let mut response_buf = get_pooled_buffer(2048);
            response.serialize(&mut response_buf);
            if send_cluster_message(&ctx, sender_id, CLUSTER_RESPONSE_MESSAGE, &response_buf)
                == Status::Err
            {
                let node_id = get_current_node();
                let msg = format!(
                    "Failed to send response for request ID {request_id}: node_id: {node_id:?}",
                );
                ctx.log_warning(&msg);
            }
        }
        Err(e) => {
            set_current_db(&ctx, save_db);
            send_error_response(&ctx, request_id, sender_id, Error::from(e));
        }
    }
}

fn process_request(ctx: &Context, req: SearchQueryRequest) -> ValkeyResult<SearchQueryResponse> {
    let search_query: SearchQuery = SearchQuery {
        start: req.start,
        end: req.end,
        matchers: req.matchers,
        max_metrics: req.max_metrics,
    };

    get_query_series_data(ctx, search_query, |pairs| {
        let series = pairs
            .into_iter()
            .map(|pair| pair.into())
            .collect::<Vec<QuerySeriesData>>();
        Ok(SearchQueryResponse { series })
    })
}

/// Handles responses from other nodes in the cluster. The receiver is the original sender of
/// the request.
extern "C" fn on_response_received(
    ctx: *mut ValkeyModuleCtx,
    _sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);

    let buf = unsafe { std::slice::from_raw_parts(payload, len as usize) };
    let Some((request_id, buf)) = parse_response_header(&ctx, buf) else {
        return;
    };

    // fetch corresponding inflight request by request_id
    let map = INFLIGHT_REQUESTS.pin();
    let Some(request) = map.get(&request_id) else {
        ctx.log_warning(&format!(
            "Failed to find inflight request for id {request_id}"
        ));
        return;
    };

    let Ok(response) = SearchQueryResponse::deserialize(buf) else {
        // todo: return an error response to the sender
        ctx.log_warning("Failed to deserialize response payload");
        request.raise_error(Error::from(error_consts::COMMAND_DESERIALIZATION_ERROR));
        return;
    };

    request.update(response);
}

extern "C" fn on_error_received(
    ctx: *mut ValkeyModuleCtx,
    _sender_id: *const c_char,
    _type: u8,
    payload: *const c_uchar,
    len: u32,
) {
    let ctx = Context::new(ctx as *mut RedisModuleCtx);
    if payload.is_null() || len < size_of::<u64>() as u32 {
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
            "Failed to find inflight request for id {request_id}"
        ));
        return;
    };

    handle_error_response(&ctx, request, buf);
}

fn handle_error_response(ctx: &Context, request: &InFlightRequest, buf: &[u8]) {
    match Error::deserialize(buf) {
        Ok(error) => request.raise_error(error),
        Err(_) => ctx.log_warning("Failed to deserialize error response"),
    }
}

pub fn register_cluster_message_handlers(ctx: &Context) {
    // Register the cluster message handlers
    register_message_receiver(ctx, CLUSTER_REQUEST_MESSAGE, Some(on_request_received));
    register_message_receiver(ctx, CLUSTER_RESPONSE_MESSAGE, Some(on_response_received));
    register_message_receiver(ctx, CLUSTER_ERROR_MESSAGE, Some(on_error_received));
}
