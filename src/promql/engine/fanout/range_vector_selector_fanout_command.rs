use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::{handle_range_query, shard_read};
use crate::promql::generated::{
    RangeQuery, RangeQueryResponse, SeriesSelector as ProtoSeriesSelector,
};
use promql_parser::label::Matchers;
use valkey_module::ValkeyResult;

pub struct RangeVectorSelectorFanoutCommand {
    matchers: Matchers,
    start_time: i64,
    end_time: i64,
    max_series: u64,
    max_points_per_series: u64,
    /// One entry per shard, each with its own symbol table. Label resolution
    /// and the duplicate-series check wait for the requester's pool (see
    /// `check_unique_series`): this callback runs on the main thread.
    responses: Vec<RangeQueryResponse>,
}

impl RangeVectorSelectorFanoutCommand {
    pub fn new(
        matchers: Matchers,
        start_time: Timestamp,
        end_time: Timestamp,
        max_series: u64,
        max_points_per_series: u64,
    ) -> Self {
        Self {
            matchers,
            start_time,
            end_time,
            max_series,
            max_points_per_series,
            responses: Vec::new(),
        }
    }

    /// Consume the accumulated selector results: every shard's response.
    pub fn get_responses(self) -> Vec<RangeQueryResponse> {
        self.responses
    }
}

impl FanoutCommand for RangeVectorSelectorFanoutCommand {
    type Request = RangeQuery;
    type Response = RangeQueryResponse;

    fn name() -> &'static str {
        "query-range"
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: RangeQuery,
    ) -> ValkeyResult<RangeQueryResponse> {
        let selector = super::required(req.selector, "selector")?;
        let series_selector: SeriesSelector = (&selector).try_into()?;
        let read = shard_read(ctx)?;
        // Locks per batch of matched series; decodes with the lock released.
        handle_range_query(
            ctx,
            series_selector,
            req.start_time,
            req.end_time,
            req.max_series,
            req.max_points_per_series,
            &read,
        )
    }

    fn generate_request(&self) -> RangeQuery {
        let selector = ProtoSeriesSelector::from(&self.matchers);
        RangeQuery {
            selector: Some(selector),
            start_time: self.start_time,
            end_time: self.end_time,
            max_series: self.max_series,
            max_points_per_series: self.max_points_per_series,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        self.responses.push(resp);
        Ok(())
    }
}
