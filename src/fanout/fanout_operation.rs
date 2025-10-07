use super::cluster_rpc::{get_cluster_command_timeout, send_cluster_request};
use super::fanout_error::ErrorKind;
use super::fanout_error::{FanoutError, FanoutResult};
use super::fanout_targets::{
    FanoutTarget, FanoutTargetMode, compute_query_fanout_mode, get_fanout_targets,
};
use crate::fanout::serialization::{Deserialized, Serializable, Serialized};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyResult, ValkeyValue};

/// A trait representing a fan-out operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
///
/// # Type Parameters
/// - `Request`: The type representing the request to be sent to the target nodes.
/// - `Response`: The type representing the response expected from the target nodes.
///
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
    fn get_target_mode(&self, ctx: &Context) -> FanoutTargetMode {
        compute_query_fanout_mode(ctx)
    }

    /// Generate the request to be sent to each target node.
    fn generate_request(&mut self) -> Self::Request;

    /// Called once per successful response from a target node.
    fn on_response(&mut self, resp: Self::Response, target: FanoutTarget);

    fn on_error(&mut self, error: FanoutError, target: FanoutTarget) {
        // Log the error with context
        log::error!(
            "Fanout operation {}, failed for target {}: {}",
            Self::name(),
            target,
            error,
        );
    }

    /// Called once all responses have been received, or an error has occurred.
    /// This is where the final reply to the client should be generated.
    /// If there were any errors, the default implementation will generate an error reply.
    fn generate_reply(&mut self, ctx: &Context);
}

/// A trait for invoking fanout operations across cluster nodes.
/// It manages the sending of requests, handling responses, and coordinating the overall operation.
/// # Type Parameters
/// - `H`: The type implementing the `FanoutOperation` trait.
/// - `Request`: The type representing the request to be sent to the target nodes.
/// - `Response`: The type representing the response expected from the target nodes.
///
pub trait RpcInvoker<OP>: Send
where
    OP: FanoutOperation,
{
    fn invoke_rpc(
        &self,
        context: &Context,
        req: OP::Request,
        targets: &[FanoutTarget],
        callback: Box<dyn Fn(FanoutResult<OP::Response>, FanoutTarget) + Send + Sync>,
        timeout: Duration,
    ) -> ValkeyResult<()>;
}

/// Internal structure to manage the state of an ongoing fanout operation.
/// It tracks outstanding RPCs, errors, and coordinates the final reply generation.
struct FanoutState<OP>
where
    OP: FanoutOperation,
{
    handler: OP,
    outstanding: usize,
    timed_out: bool,
    already_responded: bool,
    thread_ctx: ThreadSafeContext<BlockedClient>,
    errors: Vec<FanoutError>,
    __phantom: std::marker::PhantomData<OP>,
}

impl<OP> FanoutState<OP>
where
    OP: FanoutOperation,
{
    pub fn new(context: &Context, handler: OP) -> Self {
        let blocked_client = context.block_client();
        Self {
            handler,
            outstanding: 0,
            thread_ctx: ThreadSafeContext::with_blocked_client(blocked_client),
            errors: Vec::new(),
            timed_out: false,
            already_responded: false,
            __phantom: Default::default(),
        }
    }

    fn generate_request(&mut self) -> OP::Request {
        self.handler.generate_request()
    }

    fn rpc_done(&mut self) -> bool {
        if self.outstanding > 0 {
            self.outstanding = self.outstanding.saturating_sub(1);
            if self.outstanding == 0 {
                self.on_completion();
                return true;
            }
        } else {
            // Equivalent to C++ CHECK(outstanding_ > 0);
            // panic!("Outstanding RPCs is already zero in rpc_done");
        }
        false
    }

    fn on_error(&mut self, error: FanoutError, _target: FanoutTarget) {
        if error.kind == ErrorKind::Timeout {
            self.timed_out = true;
            // Only record the first timeout error
            return;
        }
        self.errors.push(error);
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

    fn handle_rpc_callback(
        &mut self,
        resp: FanoutResult<OP::Response>,
        target: FanoutTarget,
    ) -> bool {
        if !self.timed_out {
            match resp {
                Ok(response) => {
                    // Handle successful response
                    self.handler.on_response(response, target);
                }
                Err(err) => {
                    self.on_error(err, target);
                }
            }
        }
        self.rpc_done()
    }

    fn handle_local_request(&mut self, ctx: &Context, request: OP::Request) {
        let target = FanoutTarget::Local;
        let resp = match OP::get_local_response(ctx, request) {
            Ok(response) => Ok(response),
            Err(err) => Err(err.into()),
        };
        self.handle_rpc_callback(resp, target);
    }

    fn on_completion(&mut self) {
        // No errors, generate a successful reply
        let thread_ctx = &self.thread_ctx;
        let ctx = thread_ctx.lock(); // ????? do we need to lock to reply?
        self.generate_reply(&ctx);
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
        targets: &[FanoutTarget],
        callback: Box<dyn Fn(FanoutResult<OP::Response>, FanoutTarget) + Send + Sync>,
        timeout: Duration,
    ) -> ValkeyResult<()> {
        let response_handler = Arc::new(
            move |res: Result<&[u8], FanoutError>, target: FanoutTarget| match res {
                Ok(buf) => match OP::Response::deserialize(buf) {
                    Ok(resp) => callback(Ok(resp), target),
                    Err(_e) => {
                        let err = FanoutError::serialization("");
                        callback(Err(err), target);
                    }
                },
                Err(err) => callback(Err(err), target),
            },
        );

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

    let mut targets = get_fanout_targets(ctx, target_mode);
    let outstanding = targets.len();

    state.outstanding = outstanding;

    let local_pos = targets.iter().position(|x| x.is_local());
    if let Some(idx) = local_pos {
        targets.swap_remove(idx);
        state.outstanding = state.outstanding.saturating_sub(1);
        if state.outstanding > 1 {
            let req_local = state.generate_request();
            state.handle_local_request(ctx, req_local);
        } else {
            state.handle_local_request(ctx, req);
            return Ok(ValkeyValue::NoReply);
        }
    }

    let state_mutex = Mutex::new(state);
    rpc_invoker.invoke_rpc(
        ctx,
        req,
        targets.as_slice(),
        Box::new(move |res, target| {
            let mut state = state_mutex.lock().expect("mutex poisoned");
            state.handle_rpc_callback(res, target);
        }),
        timeout,
    )?;

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
