pub mod cluster;
mod coordinator;
pub mod request;
mod results_tracker;

use crate::series::index::PostingsStats;
use crate::series::request_types::{MGetRequest, MatchFilterOptions, RangeGroupingOptions, RangeOptions};
pub use coordinator::{register_cluster_message_handlers, send_multi_shard_request};
pub use request::*;
use results_tracker::*;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyResult};

pub fn perform_remote_mget_request<F>(
    ctx: &Context,
    request: &MGetRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<MultiGetResponse>, ()) + Send + 'static,
{
    send_multi_shard_request::<MGetShardedCommand, _>(ctx, request, (), on_done)
}

pub fn perform_remote_card_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<CardinalityResponse>, ()) + Send + 'static,
{
    send_multi_shard_request::<CardinalityCommand, _>(ctx, request, (), on_done)
}

pub fn perform_remote_label_names_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<LabelNamesResponse>, ()) + Send + 'static,
{
    send_multi_shard_request::<LabelNamesCommand, F>(ctx, request, (), on_done)
}

pub fn perform_remote_label_values_request<F>(
    ctx: &Context,
    request: &LabelValuesRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<LabelValuesResponse>, ()) + Send + 'static,
{
    send_multi_shard_request::<LabelValuesCommand, F>(ctx, request, (), on_done)
}

pub fn perform_remote_index_query_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<IndexQueryResponse>, ()) + Send + 'static,
{
    send_multi_shard_request::<IndexQueryCommand, F>(ctx, request, (), on_done)
}

pub fn perform_remote_stats_request<F>(ctx: &Context, limit: usize, on_done: F) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<PostingsStats>, u64) + Send + 'static,
{
    let request = StatsRequest { limit };
    send_multi_shard_request::<StatsCommand, F>(ctx, &request, limit as u64 , on_done)
}

pub fn perform_remote_mrange_request<F>(
    ctx: &Context,
    request: RangeOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<MultiRangeResponse>, Option<RangeGroupingOptions>) + Send + 'static,
{
    let mut request = request;
    let grouping = request.grouping.take();
    send_multi_shard_request::<MultiRangeCommand, F>(ctx, &request, grouping, on_done)
}
