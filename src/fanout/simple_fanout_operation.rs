use crate::fanout::FanoutContext;
use crate::fanout::serialization::Serializable;
use crate::fanout::{
    FanoutClientCommand, FanoutCommand, FanoutTargetMode, NodeInfo, get_fanout_targets,
};
use ahash::HashSet;
use std::sync::Arc;
use valkey_module::{Context, Status, ValkeyResult};

/// A single trait that satisfies both FanoutCommand and FanoutOperation.
/// Implement only this trait; the blanket impls below handle the rest.
pub trait SimpleFanoutClientCommand: Default + Send + 'static {
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
}

// ── Blanket impl of FanoutCommand ────────────────────────────────────────────
impl<T: SimpleFanoutClientCommand> FanoutCommand for T {
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
        SimpleFanoutClientCommand::get_targets(self, ctx)
    }

    fn generate_request(&self) -> Self::Request {
        // Fully-qualified to avoid infinite recursion
        SimpleFanoutClientCommand::generate_request(self)
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) {
        SimpleFanoutClientCommand::on_response(self, resp, target)
    }
}

// ── Blanket impl of FanoutOperation ──────────────────────────────────────────
impl<T: SimpleFanoutClientCommand> FanoutClientCommand for T {
    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        SimpleFanoutClientCommand::reply(self, ctx)
    }
}
