//! Shard-side PromQL aggregation push-down.
//!
//! Without this, evaluating `sum by (job) (http_requests_total)` in a cluster
//! ships every matching series to the coordinator and reduces there. This
//! command pushes the whole aggregation — the instant-vector parameters *and*
//! the operator — to each shard, so what crosses the wire is one partial state
//! (or one candidate sample) per group per shard rather than the raw input.
//!
//! It is the PromQL counterpart of the GROUPBY/REDUCE push-down in
//! `MRangeFanoutCommand`, and follows the same three rules:
//!
//! * The coordinator decides whether to push down; shards obey the request.
//! * A shard refuses an operator it cannot push down rather than answer
//!   something else: what comes back is always the reduced or selected output.
//! * The reduction the coordinator performs on the shards' output equals the
//!   single-node aggregation over the concatenated input; see
//!   [`crate::promql::exec::partial_aggregation`] for the per-operator argument.

use crate::commands::fanout_codec::symbol_table;
use crate::common::Timestamp;
use crate::fanout::{FanoutCommand, FanoutCommandResult, FanoutContext, FanoutError, NodeInfo};
use crate::promql::deadline::{Deadline, SERIES_WORK};
use crate::promql::engine::fanout::InstantVectorParams;
use crate::promql::engine::fanout::query_utils::{local_instant_eval_samples, shard_read};
use crate::promql::engine::fanout::type_conversions::{decode_peer_partial, wrong_payload};
use crate::promql::engine::query_reader::{
    AggregationOperation, AggregationParam, AggregationRequest,
};
use crate::promql::exec::aggregations::{AggregationKind, PushdownStrategy, apply_aggregation};
use crate::promql::exec::partial_aggregation::{PartialGroups, merge_count_values};
use crate::promql::generated::{
    AggregationGroupPartial, AggregationQuery, AggregationQueryResponse, InstantQuery,
    InstantSample as ProtoInstantSample,
};
use crate::promql::{EvalResult, EvalSample, QueryError};
use valkey_module::ValkeyResult;

/// What the shards' output accumulates into, decided by the operator's
/// push-down strategy — the same shape [`crate::promql::engine::query_reader::GridSink`]
/// gives a grid request.
enum AggregationSink {
    /// [`PushdownStrategy::Reduce`]: the shards' partial states, merged per
    /// group as they arrive. Boxed: the map's inline storage is a few KiB,
    /// against a `Vec` header in the other variant.
    Partials(Box<PartialGroups>),
    /// [`PushdownStrategy::Select`]/[`PushdownStrategy::CountValues`]: the
    /// shards' locally selected samples or per-value counts, re-selected or
    /// summed once every shard has answered.
    Candidates(Vec<EvalSample>),
}

impl AggregationSink {
    /// # Panics
    /// If `kind` has no push-down strategy. Callers must check
    /// [`AggregationKind::pushdown_strategy`] first — that check is also how
    /// they decide to push down at all.
    fn for_kind(kind: AggregationKind) -> Self {
        match kind
            .pushdown_strategy()
            .expect("BUG: aggregation operator cannot be pushed down")
        {
            PushdownStrategy::Reduce => Self::Partials(Box::new(PartialGroups::new(kind))),
            PushdownStrategy::Select | PushdownStrategy::CountValues => {
                Self::Candidates(Vec::new())
            }
        }
    }
}

pub(in crate::promql) struct AggregationFanoutCommand {
    vector: InstantVectorParams,
    aggregation: AggregationRequest,
    /// Where the shards' output goes, by the operator's strategy.
    sink: AggregationSink,
    /// Latched when a peer rejected the operation outright — an older node with
    /// no aggregation push-down at all. The caller retries without push-down
    /// rather than failing the query.
    unsupported_peer: bool,
    /// The query's: checked as the shards' output is merged, and as it is
    /// reduced to the answer ([`Self::into_result`]).
    deadline: Deadline,
    /// Why merging stopped, when `deadline` passed during it: every later
    /// response is dropped unread, and [`Self::into_result`] reports this, in
    /// its own words, rather than a fan-out failure.
    stopped: Option<QueryError>,
}

impl AggregationFanoutCommand {
    /// # Panics
    /// If `aggregation` has no push-down strategy; see
    /// [`AggregationSink::for_kind`].
    pub fn new(
        vector: InstantVectorParams,
        aggregation: AggregationRequest,
        deadline: Deadline,
    ) -> Self {
        Self {
            vector,
            sink: AggregationSink::for_kind(aggregation.operation.kind),
            aggregation,
            unsupported_peer: false,
            deadline,
            stopped: None,
        }
    }

    /// True when at least one peer could not handle the operation: the caller
    /// should re-run the query without push-down.
    pub fn peer_unsupported(&self) -> bool {
        self.unsupported_peer
    }

    /// Reduce the shards' output to the final instant vector.
    pub fn into_result(mut self) -> EvalResult<Vec<EvalSample>> {
        if let Some(err) = self.stopped.take() {
            return Err(err.into());
        }
        let deadline = &self.deadline;
        let AggregationRequest {
            operation:
                AggregationOperation {
                    kind,
                    ref modifier,
                    ref param,
                },
            eval_timestamp: eval_time,
        } = self.aggregation;

        match self.sink {
            AggregationSink::Partials(partials) => Ok(partials.finalize(eval_time)),
            AggregationSink::Candidates(candidates) => {
                if kind.pushdown_strategy() == Some(PushdownStrategy::CountValues) {
                    return merge_count_values(candidates, deadline);
                }
                // The selection is idempotent: re-selecting over every shard's
                // picks is the answer.
                let param = param.as_ref().map(AggregationParam::to_expr_result);
                apply_aggregation(
                    kind,
                    modifier.as_ref(),
                    param,
                    candidates,
                    eval_time,
                    deadline,
                )
            }
        }
    }

    fn accumulate_partials(
        &mut self,
        partials: Vec<AggregationGroupPartial>,
        resolver: &mut symbol_table::EvalLabelResolver<'_>,
        target: &NodeInfo,
    ) -> FanoutCommandResult {
        let AggregationSink::Partials(groups) = &mut self.sink else {
            unreachable!("BUG: partial states arrive only for a reduction");
        };
        for mut partial in partials {
            let labels = resolver
                .resolve(&mut partial)
                .map_err(|why| super::peer_error(target, "returned malformed labels", why))?;
            let state = decode_peer_partial(
                partial.state,
                target,
                format_args!(
                    "a pushed-down {:?} reduction",
                    self.aggregation.operation.kind
                ),
            )?;
            groups.merge(labels, state);
            if let Err(err) = self.deadline.checkpoint(SERIES_WORK) {
                self.stopped = Some(err);
                return Ok(());
            }
        }
        Ok(())
    }
}

impl FanoutCommand for AggregationFanoutCommand {
    type Request = AggregationQuery;
    type Response = AggregationQueryResponse;

    fn name() -> &'static str {
        "query-aggregation"
    }

    fn get_local_response(
        ctx: &FanoutContext,
        req: AggregationQuery,
    ) -> ValkeyResult<AggregationQueryResponse> {
        let query = super::required(req.query, "instant-vector parameters")?;
        let operation =
            AggregationOperation::try_from(super::required(req.operation, "operator")?)?;
        let series_selector = super::shard_selector(query.selector.as_ref())?;
        // Held to the end, through the aggregation: the coordinator counts only the groups
        // that come back, so this is all that counts the input while it is held here.
        let read = shard_read(ctx)?;
        // Locks per batch of matched series, not for the whole read.
        let samples = local_instant_eval_samples(
            ctx,
            series_selector,
            query.timestamp,
            query.lookback_delta,
            query.max_series,
            &read,
        )?;
        // This shard's half of the duplicate-label rule: the coordinator sees only
        // groups, so only here can two series with the same labels be caught (see
        // `execute_cluster_aggregation` for the cross-shard case).
        super::check_unique_series(samples.iter().map(|s| &s.labels))
            .map_err(|e| super::shard_error(QueryError::Execution(e)))?;

        aggregate_locally(operation, samples, req.eval_timestamp, &read.deadline)
            .map_err(super::shard_error)
    }

    fn generate_request(&self) -> AggregationQuery {
        let query = InstantQuery::from(&self.vector);
        AggregationQuery {
            query: Some(query),
            operation: Some((&self.aggregation.operation).into()),
            eval_timestamp: self.aggregation.eval_timestamp,
        }
    }

    fn on_response(&mut self, mut resp: Self::Response, target: &NodeInfo) -> FanoutCommandResult {
        if self.stopped.is_some() {
            return Ok(());
        }
        let labels = resp.labels.take().unwrap_or_default();
        let mut resolver = symbol_table::EvalLabelResolver::new(&labels);
        let samples = std::mem::take(&mut resp.samples)
            .into_iter()
            .map(|s| {
                resolver
                    .resolve_sample(s)
                    .map_err(|why| super::peer_error(target, "returned malformed labels", why))
            })
            .collect::<Result<Vec<EvalSample>, FanoutError>>()?;
        match &mut self.sink {
            AggregationSink::Partials(_) => {
                // Corrupt-peer defense: a reduction response carries partials
                // only. Aggregating stray samples as if they were reduced
                // values would silently double-count.
                if !samples.is_empty() {
                    return Err(wrong_payload(
                        target,
                        samples.len(),
                        "samples",
                        format_args!(
                            "a pushed-down {:?} reduction",
                            self.aggregation.operation.kind
                        ),
                        "partial states",
                    ));
                }
                self.accumulate_partials(resp.partials, &mut resolver, target)?;
            }
            AggregationSink::Candidates(candidates) => {
                if !resp.partials.is_empty() {
                    return Err(wrong_payload(
                        target,
                        resp.partials.len(),
                        "partial states",
                        format_args!("a pushed-down {:?}", self.aggregation.operation.kind),
                        "samples",
                    ));
                }
                candidates.extend(samples);
            }
        }
        Ok(())
    }

    fn on_error(&mut self, error: FanoutError, _target: &NodeInfo) {
        // A peer that does not know this operation at all (rolling upgrade)
        // rejects the envelope rather than answering it. Latch that so the
        // caller can fall back to selecting the raw instant vector.
        self.unsupported_peer |= error.is_unsupported_operation();
    }
}

/// This shard's half of `operation` over the instant vector it read: the
/// partial states of a reduction, or the samples a selection keeps (or
/// `count_values` counts), stamped with `eval_timestamp`. Stops once
/// `deadline` has passed: the coordinator would no longer wait for the answer.
fn aggregate_locally(
    operation: AggregationOperation,
    samples: Vec<EvalSample>,
    eval_timestamp: Timestamp,
    deadline: &Deadline,
) -> EvalResult<AggregationQueryResponse> {
    let AggregationOperation {
        kind,
        modifier,
        param,
    } = operation;
    let Some(strategy) = kind.pushdown_strategy() else {
        unreachable!("decoding refuses an operator with no push-down form");
    };
    match strategy {
        PushdownStrategy::Reduce => {
            let mut groups = PartialGroups::new(kind);
            groups.accumulate(modifier.as_ref(), samples, deadline)?;
            Ok(partials_response(groups))
        }
        PushdownStrategy::Select | PushdownStrategy::CountValues => {
            let param = param.as_ref().map(AggregationParam::to_expr_result);
            let aggregated = apply_aggregation(
                kind,
                modifier.as_ref(),
                param,
                samples,
                eval_timestamp,
                deadline,
            )?;
            Ok(samples_response(aggregated))
        }
    }
}

/// A reduction's answer: one partial per group, its labels as refs into the
/// response's symbol table.
fn partials_response(groups: PartialGroups) -> AggregationQueryResponse {
    let mut symbols = symbol_table::SymbolTableBuilder::default();
    let partials = groups
        .into_partials()
        .map(|(labels, state)| {
            let (label_name_refs, label_value_refs) = symbols.intern_eval(&labels);
            AggregationGroupPartial {
                labels: Vec::new(),
                state: Some(state.into()),
                label_name_refs,
                label_value_refs,
            }
        })
        .collect();
    AggregationQueryResponse {
        partials,
        samples: Vec::new(),
        labels: Some(symbols.finish()),
    }
}

/// Labels go into the response's symbol table straight from the evaluator's
/// label sets, by identity when they are interned: no owned label strings.
fn samples_response(samples: Vec<EvalSample>) -> AggregationQueryResponse {
    let mut symbols = symbol_table::SymbolTableBuilder::default();
    let samples = samples
        .into_iter()
        .map(|s| {
            let (label_name_refs, label_value_refs) = symbols.intern_eval(&s.labels);
            ProtoInstantSample {
                labels: Vec::new(),
                value: s.value,
                timestamp: s.timestamp_ms,
                label_name_refs,
                label_value_refs,
            }
        })
        .collect();
    AggregationQueryResponse {
        partials: Vec::new(),
        samples,
        labels: Some(symbols.finish()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::fanout::type_conversions::decode_pushdown_kind;
    use crate::promql::exec::types::EvalLabels;
    use crate::promql::generated::AggregationKind as ProtoAggregationKind;
    use promql_parser::label::Labels as ModifierLabels;
    use promql_parser::label::Matchers;
    use promql_parser::parser::LabelModifier;
    use std::sync::Arc;

    const EVAL_TS: Timestamp = 1_700_000_000_000;

    fn sample(labels: &[(&str, &str)], value: f64) -> EvalSample {
        EvalSample {
            labels: EvalLabels::from_pairs(labels),
            value,
            timestamp_ms: EVAL_TS,
            drop_name: false,
        }
    }

    fn node(port: u16) -> NodeInfo {
        NodeInfo::for_test(port)
    }

    fn params(kind: AggregationKind, modifier: Option<LabelModifier>) -> AggregationRequest {
        request(kind, modifier, None)
    }

    fn request(
        kind: AggregationKind,
        modifier: Option<LabelModifier>,
        param: Option<AggregationParam>,
    ) -> AggregationRequest {
        AggregationRequest {
            operation: AggregationOperation {
                kind,
                modifier,
                param,
            },
            eval_timestamp: EVAL_TS,
        }
    }

    fn command(aggregation: AggregationRequest) -> AggregationFanoutCommand {
        command_until(aggregation, Deadline::default())
    }

    fn command_until(
        aggregation: AggregationRequest,
        deadline: Deadline,
    ) -> AggregationFanoutCommand {
        let vector = InstantVectorParams {
            matchers: Arc::new(Matchers::empty()),
            timestamp: EVAL_TS,
            lookback_delta: 300_000,
            max_series: 0,
        };
        AggregationFanoutCommand::new(vector, aggregation, deadline)
    }

    /// One shard's response, produced as `get_local_response` produces it.
    fn shard_response(
        aggregation: &AggregationRequest,
        samples: Vec<EvalSample>,
    ) -> AggregationQueryResponse {
        aggregate_locally(
            aggregation.operation.clone(),
            samples,
            EVAL_TS,
            &Deadline::default(),
        )
        .unwrap()
    }

    /// The full coordinator side: each shard aggregates its slice, the
    /// responses cross the wire (proto in, proto out), the coordinator reduces.
    fn fanout(aggregation: AggregationRequest, shards: &[Vec<EvalSample>]) -> Vec<(String, f64)> {
        let mut cmd = command(aggregation.clone());
        for (index, shard) in shards.iter().enumerate() {
            let response = shard_response(&aggregation, shard.clone());
            cmd.on_response(response, &node(7000 + index as u16))
                .expect("shard response accepted");
        }
        rendered(cmd.into_result().unwrap())
    }

    fn single_node(
        aggregation: &AggregationRequest,
        samples: Vec<EvalSample>,
    ) -> Vec<(String, f64)> {
        let operation = &aggregation.operation;
        let param = operation
            .param
            .as_ref()
            .map(AggregationParam::to_expr_result);
        rendered(
            apply_aggregation(
                operation.kind,
                operation.modifier.as_ref(),
                param,
                samples,
                EVAL_TS,
                &Deadline::default(),
            )
            .unwrap(),
        )
    }

    /// Results as sorted `(labels, value)` pairs so they can be compared
    /// irrespective of grouping/iteration order.
    fn rendered(samples: Vec<EvalSample>) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = samples
            .into_iter()
            .map(|s| (s.labels.to_string(), s.value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        out
    }

    fn test_shards() -> Vec<Vec<EvalSample>> {
        vec![
            vec![
                sample(&[("__name__", "m"), ("job", "a"), ("i", "1")], 1.0),
                sample(&[("__name__", "m"), ("job", "a"), ("i", "2")], 7.0),
                sample(&[("__name__", "m"), ("job", "b"), ("i", "3")], 3.0),
            ],
            vec![
                sample(&[("__name__", "m"), ("job", "a"), ("i", "4")], 5.0),
                sample(&[("__name__", "m"), ("job", "b"), ("i", "5")], 9.0),
            ],
            vec![sample(&[("__name__", "m"), ("job", "b"), ("i", "6")], 2.0)],
        ]
    }

    /// The push-down contract, end to end: for every operator with a strategy,
    /// the fanout result equals the single-node result over the same input.
    #[test]
    fn test_fanout_matches_single_node() {
        let shards = test_shards();
        let all: Vec<EvalSample> = shards.iter().flatten().cloned().collect();
        let by_job = LabelModifier::Include(ModifierLabels::new(vec!["job"]));

        let cases = [
            params(AggregationKind::Sum, None),
            params(AggregationKind::Sum, Some(by_job.clone())),
            params(AggregationKind::Avg, Some(by_job.clone())),
            params(AggregationKind::Min, Some(by_job.clone())),
            params(AggregationKind::Max, Some(by_job.clone())),
            params(AggregationKind::Count, Some(by_job.clone())),
            params(AggregationKind::Group, Some(by_job.clone())),
            params(AggregationKind::Stddev, Some(by_job.clone())),
            params(AggregationKind::Stdvar, Some(by_job.clone())),
            request(
                AggregationKind::Topk,
                Some(by_job.clone()),
                Some(AggregationParam::Scalar(2.0)),
            ),
            request(
                AggregationKind::Bottomk,
                Some(by_job.clone()),
                Some(AggregationParam::Scalar(1.0)),
            ),
            request(
                AggregationKind::Topk,
                None,
                Some(AggregationParam::Scalar(3.0)),
            ),
            request(
                AggregationKind::Limitk,
                Some(by_job.clone()),
                Some(AggregationParam::Scalar(2.0)),
            ),
            request(
                AggregationKind::LimitRatio,
                Some(by_job.clone()),
                Some(AggregationParam::Scalar(0.5)),
            ),
            request(
                AggregationKind::CountValues,
                Some(by_job.clone()),
                Some(AggregationParam::Label("value".into())),
            ),
        ];

        for aggregation in cases {
            let operation = &aggregation.operation;
            let what = format!("{:?} by {:?}", operation.kind, operation.modifier);
            assert_eq!(
                single_node(&aggregation, all.clone()),
                fanout(aggregation.clone(), &shards),
                "{what}"
            );
        }
    }

    /// The request carries the operator, its modifier and its parameter, plus
    /// the instant-vector parameters, and survives a proto round trip.
    #[test]
    fn test_request_round_trip() {
        use crate::fanout::serialization::{Deserialized, Serialized};

        let vector = InstantVectorParams {
            timestamp: EVAL_TS,
            lookback_delta: 300_000,
            max_series: 1_000,
            matchers: Arc::new(Matchers::empty()),
        };
        let aggregation = request(
            AggregationKind::Topk,
            Some(LabelModifier::Exclude(ModifierLabels::new(vec!["pod"]))),
            Some(AggregationParam::Scalar(5.0)),
        );
        let cmd = AggregationFanoutCommand::new(vector, aggregation, Deadline::default());

        let request = cmd.generate_request();
        let mut buf = Vec::new();
        request.serialize(&mut buf);
        let decoded = AggregationQuery::deserialize(&buf).unwrap();

        assert_eq!(decoded, request);
        let operation = decoded.operation.clone().expect("operator present");
        assert_eq!(operation.kind, ProtoAggregationKind::Topk as i32);
        assert_eq!(operation.scalar_param, Some(5.0));
        assert_eq!(operation.label_param, None);
        let grouping = operation.grouping.clone().expect("grouping present");
        assert!(grouping.without);
        assert_eq!(grouping.labels, vec!["pod".to_string()]);
        assert_eq!(
            LabelModifier::from(grouping),
            LabelModifier::Exclude(ModifierLabels::new(vec!["pod"]))
        );

        let query = decoded.query.expect("instant-vector parameters present");
        assert_eq!(query.timestamp, EVAL_TS);
        assert_eq!(query.lookback_delta, 300_000);
        assert_eq!(query.max_series, 1_000);
        assert!(query.selector.is_some());
    }

    /// Only the decomposable operators are pushed down, and an operator this
    /// node does not know is never mistaken for a real one: a shard refuses it.
    #[test]
    fn test_decode_pushdown_kind() {
        use crate::promql::generated::AggregationOperation as ProtoAggregationOperation;
        for kind in [0, 99, -1] {
            let operation = ProtoAggregationOperation {
                kind,
                ..Default::default()
            };
            assert!(AggregationOperation::try_from(operation).is_err(), "{kind}");
        }

        assert_eq!(
            decode_pushdown_kind(ProtoAggregationKind::Sum as i32),
            Some((AggregationKind::Sum, PushdownStrategy::Reduce))
        );
        assert_eq!(
            decode_pushdown_kind(ProtoAggregationKind::LimitRatio as i32),
            Some((AggregationKind::LimitRatio, PushdownStrategy::Select))
        );
        // Zero is `AGGREGATION_KIND_UNSPECIFIED` — what a peer sends when it
        // omits the field — not an operator. Reading it as one would silently
        // apply whichever operator happened to be listed first.
        assert_eq!(decode_pushdown_kind(0), None);
        // Beyond the enum: a newer coordinator's operator.
        assert_eq!(decode_pushdown_kind(99), None);
        assert_eq!(decode_pushdown_kind(-1), None);
    }

    /// A response that carries the wrong payload shape for the request's
    /// strategy is rejected instead of being counted.
    #[test]
    fn test_mismatched_payload_is_rejected() {
        let aggregation = params(AggregationKind::Sum, None);
        let mut cmd = command(aggregation.clone());
        let stray = AggregationQueryResponse {
            partials: Vec::new(),
            samples: vec![ProtoInstantSample {
                value: 1.0,
                ..Default::default()
            }],
            labels: None,
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());

        let topk = request(
            AggregationKind::Topk,
            None,
            Some(AggregationParam::Scalar(1.0)),
        );
        let mut cmd = command(topk);
        let stray = AggregationQueryResponse {
            partials: vec![AggregationGroupPartial::default()],
            samples: Vec::new(),
            labels: None,
        };
        assert!(cmd.on_response(stray, &node(7000)).is_err());
    }

    /// A reduction's partials carry their group labels as refs into the
    /// response's symbol table, each distinct name and value once; the
    /// coordinator merges them into interned group labels, and a ref outside
    /// the table is a corrupt response.
    #[test]
    fn test_partials_travel_by_symbol_table() {
        let by_job = LabelModifier::Include(ModifierLabels::new(vec!["job"]));
        let aggregation = params(AggregationKind::Sum, Some(by_job));
        let response = shard_response(&aggregation, test_shards()[0].clone());
        assert_eq!(response.partials.len(), 2);
        assert!(response.partials.iter().all(|p| p.labels.is_empty()));
        let table = response.labels.clone().unwrap();
        assert_eq!(table.names, vec!["job"]);
        assert_eq!(table.values.len(), 2);

        let mut cmd = command(aggregation.clone());
        cmd.on_response(response.clone(), &node(7000)).unwrap();
        let result = cmd.into_result().unwrap();
        assert!(
            result
                .iter()
                .all(|s| matches!(s.labels, EvalLabels::Interned(_))),
            "{result:?}"
        );
        assert_eq!(
            rendered(result),
            single_node(&aggregation, test_shards()[0].clone())
        );

        let mut corrupt = response;
        corrupt.partials[0].label_value_refs[0] = 99;
        let mut cmd = command(aggregation);
        let err = cmd.on_response(corrupt, &node(7000)).unwrap_err();
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    /// A partial that covers no samples cannot come from a shard; merged, it
    /// conjured a group of its own (`sum` 0 for `job="z"`, which no shard saw).
    #[test]
    fn test_partial_without_samples_is_rejected() {
        use crate::promql::generated::{AggregationPartialState, Label as ProtoLabel};
        let response = |state: Option<AggregationPartialState>| AggregationQueryResponse {
            partials: vec![AggregationGroupPartial {
                labels: vec![ProtoLabel {
                    name: "job".to_string(),
                    value: "z".to_string(),
                }],
                state,
                ..Default::default()
            }],
            samples: Vec::new(),
            labels: None,
        };
        for (what, state) in [
            ("no state", None),
            ("zero count", Some(AggregationPartialState::default())),
        ] {
            let mut cmd = command(params(AggregationKind::Sum, None));
            let err = cmd
                .on_response(response(state), &node(7000))
                .expect_err(what);
            assert!(err.to_string().contains("partial"), "{what}: {err}");
        }

        let mut cmd = command(params(AggregationKind::Sum, None));
        let real = AggregationPartialState {
            count: 2,
            acc1: 5.0,
            ..Default::default()
        };
        assert!(cmd.on_response(response(Some(real)), &node(7000)).is_ok());
    }

    /// A peer that rejects the operation outright latches the fallback flag.
    #[test]
    fn test_unsupported_peer_latches_fallback() {
        let mut cmd = command(params(AggregationKind::Sum, None));
        assert!(!cmd.peer_unsupported());
        cmd.on_error(FanoutError::invalid_message(), &node(7000));
        assert!(cmd.peer_unsupported());

        // An ordinary failure is not a version problem.
        let mut cmd = command(params(AggregationKind::Sum, None));
        cmd.on_error(FanoutError::timeout(), &node(7000));
        assert!(!cmd.peer_unsupported());
    }

    /// The coordinator's merge walks every partial or candidate sample
    /// the shards sent, so it stops once the query's deadline has passed: a
    /// stop while merging partials is latched and later responses are dropped
    /// unread, and the query is refused with its own reason.
    #[test]
    fn test_merging_stops_at_a_passed_deadline() {
        // Enough groups for several deadline checkpoints, each its own value
        // so `count_values` keeps a count per sample too.
        let samples: Vec<EvalSample> = (0..5_000)
            .map(|i| sample(&[("instance", &i.to_string())], i as f64))
            .collect();
        let by_instance = || {
            Some(LabelModifier::Include(ModifierLabels::new(vec![
                "instance",
            ])))
        };
        let topk = request(
            AggregationKind::Topk,
            by_instance(),
            Some(AggregationParam::Scalar(1.0)),
        );
        let count_values = request(
            AggregationKind::CountValues,
            None,
            Some(AggregationParam::Label("v".to_string())),
        );
        let cases = [
            ("sum", params(AggregationKind::Sum, by_instance())),
            ("topk", topk),
            ("count_values", count_values),
        ];
        for (what, aggregation) in cases {
            let arrived = shard_response(&aggregation, samples.clone());

            let mut live = command(aggregation.clone());
            live.on_response(arrived.clone(), &node(7000)).unwrap();
            assert!(live.into_result().is_ok(), "{what}");

            let passed = Deadline::until(Some(std::time::Instant::now()));
            let mut late = command_until(aggregation, passed);
            late.on_response(arrived.clone(), &node(7000)).unwrap();
            late.on_response(arrived, &node(7001))
                .expect("dropped unread once stopped");
            let err = late
                .into_result()
                .err()
                .unwrap_or_else(|| panic!("{what}: past its deadline"));
            assert!(
                matches!(QueryError::from(err), QueryError::Timeout),
                "{what}"
            );
        }
    }
}
