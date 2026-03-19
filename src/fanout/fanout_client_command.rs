use crate::fanout::FanoutContext;
use crate::fanout::blocked_client::FanoutBlockedClient;
use crate::fanout::serialization::Serializable;
use crate::fanout::{FanoutCommand, FanoutResult, FanoutTargetMode, NodeInfo, get_fanout_targets};
use ahash::HashSet;
use std::sync::Arc;
use valkey_module::{Context, Status, ValkeyResult, ValkeyValue};

/// A trait for cluster-mode commands which send results back to clients after receiving responses from other nodes.
/// This is a higher-level abstraction over `FanoutCommand` that includes client response handling logic.
pub trait FanoutClientCommand: Default + Send + 'static {
    type Request: Serializable + Send + 'static;
    type Response: Serializable + Send;

    fn name() -> &'static str;

    /// Get the list of target nodes for the fanout operation.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> Arc<HashSet<NodeInfo>> {
        get_fanout_targets(ctx, FanoutTargetMode::Random)
    }

    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response>;

    fn generate_request(&self) -> Self::Request;

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo);

    /// NOTE: Use the provided `FanoutContext` reply helpers. This is already
    /// running on the main thread and does not require locking.
    fn reply(&mut self, ctx: &FanoutContext) -> Status;

    /// Execute the fanout operation across cluster nodes.
    /// The `where Self: FanoutCommand` bound is always satisfied via the blanket impl below.
    fn exec(self, ctx: &Context) -> ValkeyResult<ValkeyValue>
    where
        Self: FanoutCommand,
    {
        let mut blocked_client = FanoutBlockedClient::<Self>::new(ctx);
        let handle_response = move |op: Self, result: FanoutResult| {
            blocked_client.unblock(op, result);
        };
        Self::exec_command(self, ctx, handle_response)?;
        Ok(ValkeyValue::NoReply)
    }
}

// ── Blanket impl of FanoutCommand ────────────────────────────────────────────
impl<T: FanoutClientCommand> FanoutCommand for T {
    type Request = T::Request;
    type Response = T::Response;

    fn name() -> &'static str {
        T::name()
    }

    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response> {
        T::get_local_response(ctx, req)
    }

    /// Get the list of target nodes for the fanout operation.
    /// By default, it retrieves a random replica per shard.
    fn get_targets(&self, ctx: &Context) -> Arc<HashSet<NodeInfo>> {
        FanoutClientCommand::get_targets(self, ctx)
    }

    fn generate_request(&self) -> Self::Request {
        // Fully-qualified to avoid infinite recursion
        FanoutClientCommand::generate_request(self)
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) {
        FanoutClientCommand::on_response(self, resp, target)
    }
}
