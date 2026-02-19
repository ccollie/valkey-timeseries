use super::cluster_rpc::{get_cluster_command_timeout, invoke_rpc};
use super::fanout_error::{ErrorKind, FanoutError};
use crate::common::threads::spawn_with_context;
use crate::fanout::serialization::{Deserialized, Serializable};
use crate::fanout::{FanoutResult, FanoutTargetMode, NodeInfo, get_fanout_targets};
use ahash::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use valkey_module::{Context, ValkeyResult};

pub(super) type FanoutResponseCallback = Box<dyn Fn(FanoutResult<&[u8]>, &NodeInfo) + Send + Sync>;

pub type FanoutCommandResult<T = ()> = Result<T, FanoutError>;

/// A trait representing a fanout operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
pub trait FanoutCommand: Default + Send + 'static {
    /// The request type.
    type Request: Serializable + Send + 'static;
    /// The response type.
    type Response: Serializable;

    /// Return the name of the fanout operation.
    fn name() -> &'static str;

    /// Handle a local request on the current node, returning the response or an error.
    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response>;

    /// Return the timeout duration for the entire fanout operation.
    /// This timeout applies to the overall operation, not individual RPC calls.
    fn get_timeout(&self) -> Duration {
        get_cluster_command_timeout()
    }

    /// Get the list of target nodes for the fanout operation.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> Arc<HashSet<NodeInfo>> {
        get_fanout_targets(ctx, FanoutTargetMode::Random)
    }

    /// Execute the fanout operation across cluster nodes.
    fn exec_command<F>(self, ctx: &Context, f: F) -> FanoutResult
    where
        F: FnOnce(&mut Self, FanoutCommandResult) + Send + 'static,
    {
        let timeout = self.get_timeout();
        let targets = self.get_targets(ctx);
        exec_command(ctx, self, targets, timeout, f)
    }

    /// Generate the request to be sent to each target node.
    fn generate_request(&self) -> Self::Request;

    /// Called once per successful response from a target node.
    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo);

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // Log the error with context
        let msg = format!(
            "Fanout operation {}, failed for target {}: {error}",
            Self::name(),
            target.socket_address,
        );
        crate::common::logging::log_warning(&msg)
    }

    /// Called once all responses have been received, without errors.
    fn on_completion(&mut self) {}

    fn generate_error_reply(&self) -> FanoutError {
        let message = "Internal error found.";
        FanoutError::custom(message)
    }
}

/// Execute the fanout operation across cluster nodes.
/// todo: pass in nodes to target instead of letting the command decide, for better separation of concerns.
pub fn exec_command<OP: FanoutCommand, F>(
    ctx: &Context,
    command: OP,
    targets: Arc<HashSet<NodeInfo>>,
    timeout: Duration,
    f: F,
) -> FanoutResult
where
    F: FnOnce(&mut OP, FanoutCommandResult) + Send + 'static,
{
    let op = command;

    let req = op.generate_request();
    let outstanding = targets.len();

    let local_node = targets.iter().find(|x| x.is_local());

    let state = Arc::new(FanoutState::new(op, outstanding, f));

    if let Some(local) = local_node {
        // when there are multiple outstanding requests, push the local request to the thread pool to avoid blocking.
        if outstanding > 1 {
            // push to the thread pool
            let req_local = state
                .inner
                .lock()
                .expect(MUTEX_POISONED_MSG)
                .operation
                .generate_request();
            let local_state = state.clone();
            spawn_local_request(local_state, req_local, *local);
        } else {
            state.handle_local_request(ctx, req, local);
            return Ok(());
        }
    }

    let response_handler = move |res: Result<&[u8], FanoutError>, target: &NodeInfo| {
        let Ok(buf) = res else {
            state.on_error(res.err().unwrap(), target);
            return;
        };
        match OP::Response::deserialize(buf) {
            Ok(resp) => state.on_response(resp, target),
            Err(e) => {
                let err =
                    FanoutError::serialization(format!("Failed to deserialize response: {e}"));
                state.on_error(err, target);
            }
        }
    };

    invoke_rpc(
        ctx,
        OP::name(),
        req,
        targets,
        Box::new(response_handler),
        timeout,
    )?;

    Ok(())
}

/// Internal structure to manage the state of an ongoing fanout operation.
struct FanoutStateInner<OP, F>
where
    OP: FanoutCommand,
    F: FnOnce(&mut OP, FanoutCommandResult),
{
    operation: OP,
    outstanding: usize,
    timed_out: bool,
    error_count: usize,
    responded: bool,
    callback: Option<F>,
}

impl<OP, F> FanoutStateInner<OP, F>
where
    OP: FanoutCommand,
    F: FnOnce(&mut OP, FanoutCommandResult),
{
    fn rpc_done(&mut self) {
        self.outstanding = self.outstanding.saturating_sub(1);
        if self.outstanding == 0 {
            self.on_completion();
        }
    }

    fn on_error(&mut self, error: FanoutError, target: &NodeInfo) {
        // Invoke the handler's error callback for custom error handling
        self.operation.on_error(error.clone(), target);
        self.error_count += 1;
        self.timed_out |= error.kind == ErrorKind::Timeout;
        self.rpc_done();
    }

    fn on_response(&mut self, resp: OP::Response, target: &NodeInfo) {
        if !self.timed_out {
            self.operation.on_response(resp, target);
        }
        self.rpc_done()
    }

    fn on_completion(&mut self) {
        if self.responded {
            return;
        }
        self.responded = true;

        let result = if self.timed_out {
            Err(FanoutError::timeout())
        } else if self.error_count > 0 {
            Err(self.operation.generate_error_reply())
        } else {
            self.operation.on_completion();
            Ok(())
        };

        let callback = self.callback.take().unwrap();
        callback(&mut self.operation, result);
    }
}

impl<OP, F> Drop for FanoutStateInner<OP, F>
where
    OP: FanoutCommand,
    F: FnOnce(&mut OP, FanoutCommandResult),
{
    fn drop(&mut self) {
        self.on_completion();
    }
}

/// Internal structure to manage the state of an ongoing fanout operation.
/// It tracks outstanding RPCs, errors, and coordinates the final reply generation.
struct FanoutState<OP, F>
where
    OP: FanoutCommand,
    F: FnOnce(&mut OP, FanoutCommandResult),
{
    inner: Mutex<FanoutStateInner<OP, F>>,
}

static MUTEX_POISONED_MSG: &str = "FanoutState mutex poisoned";

impl<OP, F> FanoutState<OP, F>
where
    OP: FanoutCommand,
    F: FnOnce(&mut OP, FanoutCommandResult),
{
    fn new(operation: OP, outstanding: usize, f: F) -> Self {
        Self {
            inner: Mutex::new(FanoutStateInner {
                operation,
                outstanding,
                error_count: 0,
                timed_out: false,
                responded: false,
                callback: Some(f),
            }),
        }
    }

    fn on_error(&self, error: FanoutError, target: &NodeInfo) {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.on_error(error, target);
    }

    fn on_response(&self, resp: OP::Response, target: &NodeInfo) {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.on_response(resp, target);
    }

    fn handle_local_request(&self, ctx: &Context, request: OP::Request, target: &NodeInfo) {
        match OP::get_local_response(ctx, request) {
            Ok(response) => self.on_response(response, target),
            Err(err) => self.on_error(err.into(), target),
        }
    }
}

/// Spawn a local request handler in a separate thread.
fn spawn_local_request<OP, F>(state: Arc<FanoutState<OP, F>>, req: OP::Request, target: NodeInfo)
where
    OP: FanoutCommand,
    OP::Request: Send + 'static,
    F: FnOnce(&mut OP, FanoutCommandResult) + Send + 'static,
{
    spawn_with_context(move |ctx| {
        state.handle_local_request(ctx, req, &target);
    });
}
