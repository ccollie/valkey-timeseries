use super::cluster_rpc::{get_cluster_command_timeout, send_cluster_request};
use super::fanout_error::{ErrorKind, FanoutError};
use crate::fanout::cluster_map::NodeLocation;
use crate::fanout::serialization::{Deserialized, Serializable, Serialized};
use crate::fanout::{FanoutTargetMode, NodeInfo, get_fanout_targets};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyResult, ValkeyValue};

/// A trait representing a fanout operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
pub trait FanoutOperation: Default + Send {
    /// The request type must be serializable and sendable across threads.
    type Request: Send;
    /// The response type must be sendable across threads.
    type Response: Send;

    /// Return the name of the fanout operation.
    fn name() -> &'static str;

    /// Handle a local request on the current node, returning the response or an error.
    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response>;

    /// Return the timeout duration for the entire fanout operation.
    /// This timeout applies to the overall operation, not individual RPC calls.
    fn get_timeout(&self) -> Duration {
        get_cluster_command_timeout()
    }

    /// Return the target mode for the fanout operation.
    /// Override this method to change the target mode as needed.
    fn get_target_mode(&self, _ctx: &Context) -> FanoutTargetMode {
        FanoutTargetMode::Primary
    }

    /// Generate the request to be sent to each target node.
    fn generate_request(&mut self) -> Self::Request;

    /// Called once per successful response from a target node.
    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo);

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // Log the error with context
        log::error!(
            "Fanout operation {}, failed for target {}: {error}",
            Self::name(),
            target.socket_address,
        );
    }

    /// Called once all responses have been received, or an error has occurred.
    /// This is where the final reply to the client should be generated.
    /// If there were any errors, the default implementation will generate an error reply.
    fn generate_reply(&mut self, ctx: &Context);
}

struct FanoutStateInner<OP>
where
    OP: FanoutOperation,
{
    handler: OP,
    outstanding: usize,
    timed_out: bool,
    already_responded: bool,
    errors: Vec<FanoutError>,
    thread_ctx: ThreadSafeContext<BlockedClient>,
}

impl<OP> FanoutStateInner<OP>
where
    OP: FanoutOperation,
{
    fn generate_request(&mut self) -> OP::Request {
        self.handler.generate_request()
    }

    fn rpc_done(&mut self) -> bool {
        assert!(
            self.outstanding > 0,
            "Cluster Fanout: Outstanding RPCs is already zero in rpc_done"
        );
        self.outstanding = self.outstanding.saturating_sub(1);
        if self.outstanding == 0 {
            self.on_completion();
        }
        self.outstanding == 0
    }

    fn on_error(&mut self, error: FanoutError, _target: &NodeInfo) {
        if error.kind == ErrorKind::Timeout {
            // Record the first timeout error for logging/debugging purposes
            if !self.timed_out {
                self.errors.push(error);
                self.timed_out = true;
            }
        } else {
            self.errors.push(error);
        }
        self.rpc_done();
    }

    fn on_response(&mut self, resp: OP::Response, target: &NodeInfo) -> bool {
        if !self.timed_out {
            self.handler.on_response(resp, target);
        }
        self.rpc_done()
    }

    fn on_completion(&mut self) {
        // No errors, generate a successful reply
        let thread_ctx = &self.thread_ctx;
        let ctx = thread_ctx.lock(); // ????? do we need to lock to reply?
        self.generate_reply(&ctx);
    }

    fn generate_reply(&mut self, ctx: &Context) {
        if self.already_responded {
            return;
        }
        self.already_responded = true;
        if !self.timed_out && self.errors.is_empty() {
            self.handler.generate_reply(ctx);
        } else {
            self.generate_error_reply(ctx);
        }
    }

    fn generate_error_reply(&self, ctx: &Context) {
        let internal_error_log_prefix: String = format!(
            "Failure(fanout) in operation {}: Internal error on node with address ",
            OP::name()
        );

        let mut error_message = String::new();

        if self.timed_out {
            error_message.push_str("Operation timed out.");
        } else if !self.errors.is_empty() {
            error_message = "Internal error found.".to_string();
            for err in &self.errors {
                ctx.log_warning(&format!("{internal_error_log_prefix}{err:?}"));
            }
        }

        if error_message.is_empty() {
            error_message = "Unknown error".to_string();
        }

        // Reply to a client with an error
        ctx.reply_error_string(&error_message);
    }
}

/// Internal structure to manage the state of an ongoing fanout operation.
/// It tracks outstanding RPCs, errors, and coordinates the final reply generation.
pub(crate) struct FanoutState<OP>
where
    OP: FanoutOperation,
{
    inner: Mutex<FanoutStateInner<OP>>,
}

static MUTEX_POISONED_MSG: &str = "FanoutState mutex poisoned";

impl<OP> FanoutState<OP>
where
    OP: FanoutOperation,
{
    pub fn new(context: &Context, handler: OP) -> Self {
        let blocked_client = context.block_client();
        Self {
            inner: Mutex::new(FanoutStateInner {
                handler,
                outstanding: 0,
                errors: Vec::new(),
                timed_out: false,
                already_responded: false,
                thread_ctx: ThreadSafeContext::with_blocked_client(blocked_client),
            }),
        }
    }

    fn set_outstanding(&mut self, count: usize) {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.outstanding = count;
    }

    pub fn generate_request(&mut self) -> OP::Request {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.generate_request()
    }

    pub fn on_error(&self, error: FanoutError, target: &NodeInfo) {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.on_error(error, target);
    }

    pub fn on_response(&self, resp: OP::Response, target: &NodeInfo) {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.on_response(resp, target);
    }

    pub fn handle_local_request(&mut self, ctx: &Context, request: OP::Request, target: &NodeInfo) {
        match OP::get_local_response(ctx, request) {
            Ok(response) => self.on_response(response, target),
            Err(err) => self.on_error(err.into(), target),
        }
    }
}

/// A trait for invoking fanout operations across cluster nodes.
/// It manages the sending of requests, handling responses, and coordinating the overall operation.
/// # Type Parameters
/// - `OP`: The type implementing the `FanoutOperation` trait.
pub trait RpcInvoker<OP>: Send
where
    OP: FanoutOperation,
{
    fn invoke_rpc(
        &self,
        context: &Context,
        req: OP::Request,
        targets: Arc<Vec<NodeInfo>>,
        state: FanoutState<OP>,
        timeout: Duration,
    ) -> ValkeyResult<()>;
}

pub struct BaseFanoutInvoker<OP>
where
    OP: FanoutOperation,
{
    __phantom: PhantomData<OP>,
}

impl<OP> Default for BaseFanoutInvoker<OP>
where
    OP: FanoutOperation,
{
    fn default() -> Self {
        Self {
            __phantom: PhantomData::<OP> {},
        }
    }
}

impl<OP> RpcInvoker<OP> for BaseFanoutInvoker<OP>
where
    OP: FanoutOperation + 'static,
    OP::Request: Serializable + Send + 'static,
    OP::Response: Serializable + Send + 'static,
{
    fn invoke_rpc(
        &self,
        ctx: &Context,
        req: OP::Request,
        targets: Arc<Vec<NodeInfo>>,
        state: FanoutState<OP>,
        timeout: Duration,
    ) -> ValkeyResult<()> {
        let response_handler = move |res: Result<&[u8], FanoutError>, target: &NodeInfo| match res {
            Ok(buf) => match OP::Response::deserialize(buf) {
                Ok(resp) => {
                    state.on_response(resp, target);
                }
                Err(e) => {
                    let err =
                        FanoutError::serialization(format!("Failed to deserialize response: {e}"));
                    state.on_error(err, target);
                }
            },
            Err(err) => {
                state.on_error(err, target);
            }
        };

        // Consider using a byte pool here if serialization size is predictable
        let mut buf = Vec::with_capacity(512);
        req.serialize(&mut buf);

        send_cluster_request(
            ctx,
            &buf,
            targets,
            OP::name(),
            response_handler,
            Some(timeout),
        )
    }
}

pub fn exec_fanout_request<OP>(
    ctx: &Context,
    rpc_invoker: impl RpcInvoker<OP>,
    operation: OP,
) -> ValkeyResult<ValkeyValue>
where
    OP: FanoutOperation + 'static,
    OP::Request: Serializable + Send + 'static,
    OP::Response: Serializable + Send + 'static,
{
    let mut op = operation;
    let timeout = op.get_timeout();

    let req = op.generate_request();
    let target_mode = op.get_target_mode(ctx);
    let mut state = FanoutState::new(ctx, op);

    let targets = get_fanout_targets(ctx, target_mode);
    let outstanding = targets.len();
    state.set_outstanding(outstanding);

    let local_pos = targets
        .iter()
        .position(|x| x.location == NodeLocation::Local);

    if let Some(idx) = local_pos {
        let local = targets.get(idx).expect("Local node info not found");
        if outstanding > 1 {
            let req_local = state.generate_request();
            state.handle_local_request(ctx, req_local, local);
        } else {
            state.handle_local_request(ctx, req, local);
            return Ok(ValkeyValue::NoReply);
        }
    }

    rpc_invoker.invoke_rpc(ctx, req, targets, state, timeout)?;

    // We will reply later, from the callbacks
    Ok(ValkeyValue::NoReply)
}

#[inline]
pub fn exec_fanout_request_base<OP>(ctx: &Context, op: OP) -> ValkeyResult<ValkeyValue>
where
    OP: FanoutOperation + 'static,
    OP::Request: Serializable + Send + 'static,
    OP::Response: Serializable + Send + 'static,
{
    let invoker = BaseFanoutInvoker::<OP>::default();
    exec_fanout_request(ctx, invoker, op)
}
