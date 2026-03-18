use crate::fanout::serialization::Serializable;
use crate::fanout::{
    FanoutCommand, FanoutOperation, FanoutTargetMode, NodeInfo, get_fanout_targets,
};
use ahash::HashSet;
use std::sync::Arc;
use valkey_module::{BlockedClient, Context, Status, ThreadSafeContext, ValkeyResult};

/// A single trait that satisfies both FanoutCommand and FanoutOperation.
/// Implement only this trait; the blanket impls below handle the rest.
pub trait SimpleFanoutOperation: Default + Send + 'static {
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

    /// NOTE! For the moment, use ONLY thread_ctx.repy(), since calling lock() will deadlock
    fn reply(&mut self, thread_ctx: &ThreadSafeContext<BlockedClient>) -> Status;
}

// ── Blanket impl of FanoutCommand ────────────────────────────────────────────
impl<T: SimpleFanoutOperation> FanoutCommand for T {
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
        SimpleFanoutOperation::get_targets(self, ctx)
    }

    fn generate_request(&self) -> Self::Request {
        // Fully-qualified to avoid infinite recursion
        SimpleFanoutOperation::generate_request(self)
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) {
        SimpleFanoutOperation::on_response(self, resp, target)
    }
}

// ── Blanket impl of FanoutOperation ──────────────────────────────────────────
impl<T: SimpleFanoutOperation> FanoutOperation for T {
    fn reply(&mut self, thread_ctx: &ThreadSafeContext<BlockedClient>) -> Status {
        SimpleFanoutOperation::reply(self, thread_ctx)
    }
}
