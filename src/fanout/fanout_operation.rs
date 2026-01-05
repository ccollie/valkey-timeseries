use super::blocked_client::{BlockedClientPrivateData, FanoutBlockedClient};
use super::cluster_rpc::{get_cluster_command_timeout, send_cluster_request};
use super::fanout_error::{ErrorKind, FanoutError};
use crate::common::threads::spawn_with_context;
use crate::fanout::serialization::{Deserialized, Serializable, Serialized};
use crate::fanout::{FanoutResult, FanoutTargetMode, NodeInfo, get_fanout_targets};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use valkey_module::{Context, Status, ValkeyResult, ValkeyValue};

pub type FanoutResponseCallback = Box<dyn Fn(FanoutResult<&[u8]>, &NodeInfo) + Send + Sync>;

/// A trait representing a fanout operation that can be performed across cluster nodes.
/// It handles processing node-specific requests, managing responses, and generating the
/// final reply to the client.
pub trait FanoutOperation: Default + Send + 'static {
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
    fn get_targets(&self, ctx: &Context) -> Arc<BTreeSet<NodeInfo>> {
        get_fanout_targets(ctx, FanoutTargetMode::Random)
    }

    /// Execute the fanout operation across cluster nodes.
    fn exec(self, ctx: &Context) -> ValkeyResult<ValkeyValue> {
        let timeout = self.get_timeout();
        let op = self;

        let req = op.generate_request();
        let targets = op.get_targets(ctx);
        let outstanding = targets.len();

        let local_node = targets.iter().find(|x| x.is_local());

        let state = Arc::new(FanoutState::new(ctx, op, outstanding));

        if let Some(local) = local_node {
            if outstanding > 1 {
                // push to the thread pool
                let req_local = state
                    .inner
                    .lock()
                    .expect(MUTEX_POISONED_MSG)
                    .generate_request();
                let local_state = state.clone();
                spawn_local_request(local_state, req_local, *local);
            } else {
                state.handle_local_request(ctx, req, local);
                return Ok(ValkeyValue::NoReply);
            }
        }

        let response_handler = move |res: Result<&[u8], FanoutError>, target: &NodeInfo| {
            let Ok(buf) = res else {
                state.on_error(res.err().unwrap(), target);
                return;
            };
            match Self::Response::deserialize(buf) {
                Ok(resp) => state.on_response(resp, target),
                Err(e) => {
                    let err =
                        FanoutError::serialization(format!("Failed to deserialize response: {e}"));
                    state.on_error(err, target);
                }
            }
        };

        Self::invoke_rpc(ctx, req, targets, Box::new(response_handler), timeout)?;

        // We will reply later, from the callbacks
        Ok(ValkeyValue::NoReply)
    }

    fn invoke_rpc(
        ctx: &Context,
        req: Self::Request,
        targets: Arc<BTreeSet<NodeInfo>>,
        response_handler: FanoutResponseCallback,
        timeout: Duration,
    ) -> ValkeyResult<()> {
        // Consider using a byte-pool buffer here if serialization size is predictable
        let mut buf = Vec::with_capacity(512);
        req.serialize(&mut buf);

        send_cluster_request(
            ctx,
            &buf,
            targets,
            Self::name(),
            response_handler,
            Some(timeout),
        )
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

    /// Called once all responses have been received, or an error has occurred.
    /// This is where the final reply to the client should be generated.
    /// If there were any errors, the default implementation will generate an error reply.
    fn generate_reply(&mut self, ctx: &Context) -> Status;

    fn generate_timeout_reply(&self, ctx: &Context) -> Status {
        let msg = "A multi-shard command failed because at least one shard did not reply within the given timeframe.";
        ctx.reply_error_string(msg)
    }

    fn generate_error_reply(&self, ctx: &Context) -> Status {
        let message = "Internal error found.";
        ctx.reply_error_string(message)
    }
}

/// Internal structure to manage the state of an ongoing fanout operation.
struct FanoutStateInner<OP>
where
    OP: FanoutOperation,
{
    operation: OP,
    outstanding: usize,
    timed_out: bool,
    error_count: usize,
    blocked_client: Option<FanoutBlockedClient<OP>>,
}

impl<OP> FanoutStateInner<OP>
where
    OP: FanoutOperation,
{
    fn generate_request(&mut self) -> OP::Request {
        self.operation.generate_request()
    }

    fn rpc_done(&mut self) {
        assert!(
            self.outstanding > 0,
            "Cluster Fanout: Outstanding RPCs is already zero in rpc_done"
        );
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
        if let Some(mut bc) = self.blocked_client.take() {
            let response_ctx = BlockedClientPrivateData::new(
                std::mem::take(&mut self.operation),
                self.timed_out,
                self.error_count,
            );

            bc.set_reply_private_data(response_ctx);
            // will unblock on drop()
        }
    }
}

impl<OP> Drop for FanoutStateInner<OP>
where
    OP: FanoutOperation,
{
    fn drop(&mut self) {
        if self.blocked_client.is_some() {
            self.on_completion();
        }
    }
}

/// Internal structure to manage the state of an ongoing fanout operation.
/// It tracks outstanding RPCs, errors, and coordinates the final reply generation.
struct FanoutState<OP>
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
    fn new(context: &Context, operation: OP, outstanding: usize) -> Self {
        let mut blocked_client = FanoutBlockedClient::new(context);
        blocked_client.measure_time_start();
        Self {
            inner: Mutex::new(FanoutStateInner {
                operation,
                outstanding,
                error_count: 0,
                timed_out: false,
                blocked_client: Some(blocked_client),
            }),
        }
    }

    fn generate_request(&mut self) -> OP::Request {
        let mut inner = self.inner.lock().expect(MUTEX_POISONED_MSG);
        inner.generate_request()
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

fn spawn_local_request<OP>(state: Arc<FanoutState<OP>>, req: OP::Request, target: NodeInfo)
where
    OP: FanoutOperation,
    OP::Request: Send + 'static,
{
    spawn_with_context(move |ctx| {
        state.handle_local_request(ctx, req, &target);
    });
}
