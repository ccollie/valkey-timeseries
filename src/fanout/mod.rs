pub mod cluster;
mod coordinator;
mod results_tracker;
mod types;
mod error;
pub mod request;

use crate::fanout::coordinator::send_request;
pub use coordinator::register_cluster_message_handlers;
pub use request::*;
pub(super) use results_tracker::*;
use valkey_module::{BlockedClient, Context, ThreadSafeContext};

pub fn perform_mget<F>(
    ctx: &Context,
    request: &MGetRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[MultiGetResponse]) + Send + 'static,
{
    send_request::<MGetRequest, MultiGetResponse, F>(ctx, request, on_done)
}

pub fn perform_card<F> (
    ctx: &Context,
    request: &CardinalityRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[CardinalityResponse]) + Send + 'static,
{
    send_request::<CardinalityRequest, CardinalityResponse, F>(ctx, request, on_done)
}