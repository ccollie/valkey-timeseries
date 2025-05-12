pub mod cluster;
mod coordinator;
pub mod request;
mod results_tracker;

use crate::series::index::PostingsStats;
use crate::series::request_types::{MGetRequest, MatchFilterOptions, RangeOptions};
pub use coordinator::send_multi_shard_request;
pub use request::*;
use results_tracker::*;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyResult};

pub fn perform_remote_command<T: MultiShardCommand, F>(
    ctx: &Context,
    request: T::REQ,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, T::REQ, Vec<T::RES>) + Send + 'static,
    TrackerEnum: From<ResultsTracker<T>>,
{
    send_multi_shard_request::<T, _>(ctx, request, on_done)
}

pub fn perform_remote_mget_request<F>(
    ctx: &Context,
    request: MGetRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, MGetRequest, Vec<MultiGetResponse>)
        + Send
        + 'static,
{
    perform_remote_command::<MGetShardedCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_card_request<F>(
    ctx: &Context,
    request: MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, MatchFilterOptions, Vec<CardinalityResponse>)
        + Send
        + 'static,
{
    send_multi_shard_request::<CardinalityCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_label_names_request<F>(
    ctx: &Context,
    request: MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, MatchFilterOptions, Vec<LabelNamesResponse>)
        + Send
        + 'static,
{
    send_multi_shard_request::<LabelNamesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_label_values_request<F>(
    ctx: &Context,
    request: LabelValuesRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, LabelValuesRequest, Vec<LabelValuesResponse>)
        + Send
        + 'static,
{
    send_multi_shard_request::<LabelValuesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_index_query_request<F>(
    ctx: &Context,
    request: MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, MatchFilterOptions, Vec<IndexQueryResponse>)
        + Send
        + 'static,
{
    send_multi_shard_request::<IndexQueryCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_stats_request<F>(ctx: &Context, limit: usize, on_done: F) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, StatsRequest, Vec<PostingsStats>) + Send + 'static,
{
    let request = StatsRequest { limit };
    send_multi_shard_request::<StatsCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_mrange_request<F>(
    ctx: &Context,
    request: RangeOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, RangeOptions, Vec<MultiRangeResponse>)
        + Send
        + 'static,
{
    send_multi_shard_request::<MultiRangeCommand, F>(ctx, request, on_done)
}
