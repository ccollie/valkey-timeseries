pub mod cluster;
mod coordinator;
mod error;
pub mod request;
mod results_tracker;
mod types;

use crate::fanout::coordinator::send_request;
use crate::series::request_types::{MGetRequest, MatchFilterOptions};
pub use coordinator::register_cluster_message_handlers;
pub use request::*;
pub(super) use results_tracker::*;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyResult};
use crate::series::index::PostingsStats;

pub fn perform_remote_mget_request<F>(ctx: &Context, request: &MGetRequest, on_done: F) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<MultiGetResponse>) + Send + 'static,
{
    send_request::<MGetShardedCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_card_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<CardinalityResponse>) + Send + 'static,
{
    send_request::<CardinalityCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_label_names_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<LabelNamesResponse>) + Send + 'static,
{
    send_request::<LabelNamesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_label_values_request<F>(
    ctx: &Context,
    request: &LabelValuesRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<LabelValuesResponse>) + Send + 'static,
{
    send_request::<LabelValuesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_index_query_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<IndexQueryResponse>) + Send + 'static,
{
    send_request::<IndexQueryCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_stats_request<F>(
    ctx: &Context,
    limit: usize,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<PostingsStats>) + Send + 'static,
{
    let request = StatsRequest { limit };
    send_request::<StatsCommand, F>(ctx, &request, on_done)
}