use crate::commands::fanout_codec::symbol_table;
use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::labels::HasFingerprint;
use crate::labels::filters::SeriesSelector;
use crate::promql::engine::fanout::query_utils::{handle_instant_query, shard_read};
use crate::promql::generated::{
    InstantQuery, InstantQueryResponse, SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::hashers::FingerprintHashSet;
use crate::promql::{EvalLabels, InstantSample};
use promql_parser::label::Matchers;
use std::sync::Arc;
use valkey_module::ValkeyResult;

pub struct InstantVectorSelectorFanoutCommand {
    matchers: Arc<Matchers>,
    timestamp: i64,
    lookback_delta: u64,
    max_series: u64,
    /// Samples already in evaluator form: each response is resolved against
    /// its symbol table as it arrives (see `on_response`).
    results: Vec<InstantSample<EvalLabels>>,
    seen: FingerprintHashSet,
}

impl InstantVectorSelectorFanoutCommand {
    pub fn new(
        matchers: Arc<Matchers>,
        timestamp: Timestamp,
        lookback_delta: u64,
        max_series: u64,
    ) -> Self {
        Self {
            matchers,
            timestamp,
            lookback_delta,
            max_series,
            results: vec![],
            seen: FingerprintHashSet::default(),
        }
    }

    /// Consume the accumulated selector results.
    pub fn into_samples(self) -> Vec<InstantSample<EvalLabels>> {
        self.results
    }
}

impl FanoutCommand for InstantVectorSelectorFanoutCommand {
    type Request = InstantQuery;
    type Response = InstantQueryResponse;

    fn name() -> &'static str {
        "query"
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: InstantQuery,
    ) -> ValkeyResult<InstantQueryResponse> {
        let selector = super::required(req.selector, "selector")?;
        let series_selector: SeriesSelector = (&selector).try_into()?;
        // The response is the samples read, so the budget is held until it is returned.
        let read = shard_read(ctx)?;
        // Locks per batch of matched series, not for the whole read.
        handle_instant_query(
            ctx,
            series_selector,
            req.timestamp,
            req.lookback_delta,
            req.max_series,
            &read,
        )
    }

    fn generate_request(&self) -> InstantQuery {
        let selector = ProtoSeriesSelector::from(&*self.matchers);
        InstantQuery {
            selector: Some(selector),
            timestamp: self.timestamp,
            lookback_delta: self.lookback_delta,
            max_series: self.max_series,
        }
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        let symbol_table = resp.labels.unwrap_or_default();
        let mut resolver = symbol_table::EvalLabelResolver::new(&symbol_table);
        self.results.reserve(resp.samples.len());
        for mut s in resp.samples {
            let labels = resolver
                .resolve(&mut s)
                .map_err(|why| super::peer_error(target, "returned malformed labels", why))?;
            let fingerprint = labels.fingerprint();
            if !self.seen.insert(fingerprint) {
                return Err(super::duplicate_series_error(&labels).into());
            }
            self.results.push(InstantSample {
                labels,
                timestamp_ms: s.timestamp,
                value: s.value,
            });
        }
        Ok(())
    }
}
