use crate::commands::fanout_codec::symbol_table;
use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::promql::engine::QueryOptions;
use crate::promql::engine::fanout::query_utils::{handle_instant_query, shard_read};
use crate::promql::engine::fanout::type_conversions::UniqueSeries;
use crate::promql::generated::{
    InstantQuery, InstantQueryResponse, SeriesSelector as ProtoSeriesSelector,
};
use crate::promql::{EvalLabels, InstantSample};
use promql_parser::label::Matchers;
use std::sync::Arc;
use valkey_module::ValkeyResult;

/// An instant-vector read: what to select, and when.
#[derive(Debug, Clone)]
pub(in crate::promql) struct InstantVectorParams {
    pub matchers: Arc<Matchers>,
    pub timestamp: Timestamp,
    pub lookback_delta: u64,
    pub max_series: u64,
}

impl InstantVectorParams {
    /// `matchers` at `timestamp`, with the lookback and series limit of `options`.
    pub(in crate::promql) fn of(
        matchers: Arc<Matchers>,
        timestamp: Timestamp,
        options: &QueryOptions,
    ) -> Self {
        Self {
            matchers,
            timestamp,
            lookback_delta: options.lookback_delta.as_millis() as u64,
            max_series: options.max_series as u64,
        }
    }
}

impl From<&InstantVectorParams> for InstantQuery {
    fn from(params: &InstantVectorParams) -> Self {
        InstantQuery {
            selector: Some(ProtoSeriesSelector::from(&*params.matchers)),
            timestamp: params.timestamp,
            lookback_delta: params.lookback_delta,
            max_series: params.max_series,
        }
    }
}

pub struct InstantVectorSelectorFanoutCommand {
    params: InstantVectorParams,
    /// Samples already in evaluator form: each response is resolved against
    /// its symbol table as it arrives (see `on_response`).
    results: Vec<InstantSample<EvalLabels>>,
    seen: UniqueSeries,
}

impl InstantVectorSelectorFanoutCommand {
    pub fn new(params: InstantVectorParams) -> Self {
        Self {
            params,
            results: vec![],
            seen: UniqueSeries::default(),
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
        let series_selector = super::shard_selector(req.selector.as_ref())?;
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
        InstantQuery::from(&self.params)
    }

    fn on_response(&mut self, resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        let symbol_table = resp.labels.unwrap_or_default();
        let mut resolver = symbol_table::EvalLabelResolver::new(&symbol_table);
        self.results.reserve(resp.samples.len());
        for mut s in resp.samples {
            let labels = resolver
                .resolve(&mut s)
                .map_err(|why| super::peer_error(target, "returned malformed labels", why))?;
            self.seen.insert(&labels)?;
            self.results.push(InstantSample {
                labels,
                timestamp_ms: s.timestamp,
                value: s.value,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::fanout_codec::symbol_table::SymbolTableBuilder;
    use crate::labels::filters::SeriesSelector;
    use crate::promql::generated::InstantSample as ProtoInstantSample;
    use promql_parser::label::{MatchOp, Matcher};

    fn node(port: u16) -> NodeInfo {
        NodeInfo::for_test(port)
    }

    fn command() -> InstantVectorSelectorFanoutCommand {
        let matchers = Matchers::new(vec![
            Matcher::new(MatchOp::Equal, "__name__", "cpu"),
            Matcher::new(MatchOp::NotEqual, "host", "b"),
        ]);
        InstantVectorSelectorFanoutCommand::new(InstantVectorParams {
            matchers: Arc::new(matchers),
            timestamp: 9_000,
            lookback_delta: 300_000,
            max_series: 50,
        })
    }

    /// A sample a shard answers with: its labels, timestamp and value.
    type ShardSample<'a> = (&'a [(&'a str, &'a str)], i64, f64);

    /// One shard's answer: its own symbol table, and one sample per label set.
    fn response(series: &[ShardSample<'_>]) -> InstantQueryResponse {
        let mut table = SymbolTableBuilder::default();
        let samples = series
            .iter()
            .map(|&(pairs, timestamp, value)| {
                let (label_name_refs, label_value_refs) =
                    table.intern_eval(&EvalLabels::from_pairs(pairs));
                ProtoInstantSample {
                    labels: Vec::new(),
                    value,
                    timestamp,
                    label_name_refs,
                    label_value_refs,
                }
            })
            .collect();
        InstantQueryResponse {
            samples,
            labels: Some(table.finish()),
        }
    }

    #[test]
    fn the_request_carries_the_selector_and_its_bounds() {
        let req = command().generate_request();
        assert_eq!(req.timestamp, 9_000);
        assert_eq!(req.lookback_delta, 300_000);
        assert_eq!(req.max_series, 50);
        let selector: SeriesSelector = (&req.selector.expect("a selector")).try_into().unwrap();
        assert_eq!(
            selector.to_string(),
            SeriesSelector::from(&*command().params.matchers).to_string()
        );
    }

    /// Each shard's labels resolve against that shard's own table: the same
    /// ref means a different label on each.
    #[test]
    fn responses_from_every_shard_are_resolved_and_kept() {
        let mut cmd = command();
        cmd.on_response(
            response(&[(&[("__name__", "cpu"), ("host", "a")], 8_000, 1.0)]),
            &node(7000),
        )
        .unwrap();
        cmd.on_response(
            response(&[
                (&[("__name__", "cpu"), ("host", "c")], 8_500, 2.0),
                (&[("__name__", "cpu"), ("host", "d")], 9_000, 3.0),
            ]),
            &node(7001),
        )
        .unwrap();
        let mut got: Vec<(String, i64, f64)> = cmd
            .into_samples()
            .into_iter()
            .map(|s| {
                (
                    s.labels.get("host").unwrap().to_string(),
                    s.timestamp_ms,
                    s.value,
                )
            })
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            got,
            vec![
                ("a".to_string(), 8_000, 1.0),
                ("c".to_string(), 8_500, 2.0),
                ("d".to_string(), 9_000, 3.0),
            ]
        );
    }

    /// A PromQL series is its label set: two with the same labels are refused,
    /// whether two shards sent them or one did.
    #[test]
    fn a_label_set_seen_twice_is_refused() {
        let labels: &[(&str, &str)] = &[("__name__", "cpu"), ("host", "a")];
        let mut across = command();
        across
            .on_response(response(&[(labels, 1, 1.0)]), &node(7000))
            .unwrap();
        let err = across
            .on_response(response(&[(labels, 2, 2.0)]), &node(7001))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("more than one series has the labels"),
            "{err}"
        );

        let mut within = command();
        let err = within
            .on_response(response(&[(labels, 1, 1.0), (labels, 2, 2.0)]), &node(7000))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("more than one series has the labels"),
            "{err}"
        );
    }

    /// A ref past the end of the table is the peer's fault, named as such,
    /// not a panic or a silently mislabelled series.
    #[test]
    fn a_label_ref_outside_the_table_is_a_peer_error() {
        let mut bad = response(&[(&[("__name__", "cpu"), ("host", "a")], 1, 1.0)]);
        bad.samples[0].label_value_refs[1] = 99;
        let err = command().on_response(bad, &node(7002)).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("malformed labels"), "{text}");
        assert!(text.contains("7002"), "the peer is named: {text}");
    }

    #[test]
    fn an_empty_answer_adds_nothing() {
        let mut cmd = command();
        cmd.on_response(InstantQueryResponse::default(), &node(7000))
            .unwrap();
        assert!(cmd.into_samples().is_empty());
    }
}
