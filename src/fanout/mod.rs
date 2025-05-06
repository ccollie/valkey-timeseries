pub mod cluster;
mod coordinator;
mod results_tracker;
mod types;
mod error;
pub mod request;

use crate::fanout::coordinator::send_request;
use crate::series::request_types::{MGetRequest, MatchFilterOptions};
pub use coordinator::register_cluster_message_handlers;
pub use request::*;
pub(super) use results_tracker::*;
use valkey_module::{BlockedClient, Context, ThreadSafeContext};


pub fn perform_remote_mget_request<F>(
    ctx: &Context,
    request: &MGetRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[MultiGetResponse]) + Send + 'static,
{
    send_request::<MGetShardedCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_card_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[CardinalityResponse]) + Send + 'static,
{
    send_request::<CardinalityCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_label_names_request<F> (
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[LabelNamesResponse]) + Send + 'static,
{
    send_request::<LabelNamesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_label_values_request<F> (
    ctx: &Context,
    request: &LabelValuesRequest,
    on_done: F,
) -> u64 
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[LabelValuesResponse]) + Send + 'static,
{
    send_request::<LabelValuesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_index_query_request<F> (
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> u64
where F: FnOnce(&ThreadSafeContext<BlockedClient>, &[IndexQueryResponse]) + Send + 'static,
{
    send_request::<IndexQueryCommand, F>(ctx, request, on_done)
}