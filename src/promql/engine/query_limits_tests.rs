//! Query limits and cancellation, end to end over an in-memory reader: the
//! sample budget, materialization and reply caps, the series bound and a
//! client's disconnect. These assert what a query is refused or charged, not
//! how many reader calls it makes (see `counting_query_reader`).

use crate::common::Sample;
use crate::labels::Labels;
use crate::promql::EvalLabels;
use crate::promql::engine::QueryReader;
use crate::promql::engine::label_profile::{LabelProfile, ProfileWindow};
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, GridOutcome, GridRequest,
};
use crate::promql::engine::{evaluate_instant, evaluate_range};
use crate::promql::model::{InstantSample, RangeSample};
use crate::promql::{QueryOptions, QueryResult};
use promql_parser::parser::EvalStmt;
use promql_parser::parser::VectorSelector;
use std::sync::Arc;
use std::time::Duration;

use crate::promql::engine::test_reader::*;

/// A range query with an explicit sample budget, returning the result
/// rather than expecting success.
fn try_range(
    reader: Arc<dyn QueryReader>,
    query: &str,
    max_samples: usize,
) -> Result<(), crate::promql::QueryError> {
    let expr = promql_parser::parser::parse(query).expect("valid test query");
    let stmt = EvalStmt {
        expr,
        start: ms(RANGE_START_MS),
        end: ms(RANGE_END_MS),
        interval: STEP,
        lookback_delta: options().lookback_delta,
    };
    let options = QueryOptions {
        max_samples,
        ..options()
    };
    evaluate_range(reader, stmt, options).map(|_| ())
}

/// The samples one selector's preload loads for the range window: the
/// window plus the 5m lookback, at 10s cadence, over three series.
const SAMPLES_PER_SELECTOR: usize =
    3 * ((RANGE_END_MS - (RANGE_START_MS - 300_000)) / INTERVAL_MS) as usize;

/// Refuses, as a shard over its own per-read limit would, any grid
/// request spanning more than `max_span_ms`: the union of a subquery's
/// windows is refused while each outer step's own window is answered.
struct RefusesWideGrids {
    inner: Arc<dyn QueryReader>,
    max_span_ms: i64,
}

impl QueryReader for RefusesWideGrids {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<InstantSample<EvalLabels>>> {
        self.inner.query(selector, timestamp, options)
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
        self.inner.query_range(selector, start_ms, end_ms, options)
    }

    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> QueryResult<AggregationOutcome> {
        self.inner
            .query_aggregation(selector, timestamp, aggregation, options)
    }

    fn query_grid(
        &self,
        selector: &VectorSelector,
        request: &GridRequest,
        options: QueryOptions,
    ) -> QueryResult<GridOutcome> {
        if request.query_end - request.query_start > self.max_span_ms {
            return Err(crate::promql::engine::sample_budget::too_many_samples(1, 1));
        }
        self.inner.query_grid(selector, request, options)
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        window: Option<ProfileWindow>,
        options: QueryOptions,
    ) -> QueryResult<Option<LabelProfile>> {
        self.inner.label_profile(selector, window, options)
    }
}

#[test]
fn sample_budget_is_query_wide() {
    let (_, reader) = build_reader();
    // One selector fits; two do not: the budget sums every read the query makes,
    // so `a + b` is refused where `a` alone was accepted.
    let one = SAMPLES_PER_SELECTOR + 10;
    try_range(reader.clone(), "a", one).expect("a single selector fits the budget");
    let err = try_range(reader.clone(), "a + b", one).expect_err("two selectors exceed it");
    assert!(
        err.to_string().contains("too many samples"),
        "budget error should name the cause: {err}"
    );
    assert!(
        err.to_string().contains("ts-promql-max-samples-per-query"),
        "budget error should name the parameter: {err}"
    );
    try_range(reader.clone(), "a + b", 2 * one).expect("doubling the budget admits both");
    try_range(reader, "a + b", 0).expect("0 is unlimited");
}

#[test]
fn sample_budget_covers_a_subquerys_reads() {
    let (_, reader) = build_reader();
    // The subquery's union grid is preloaded by a sub-evaluator that shares the
    // outer query's budget, so its read counts against the same total.
    let err = try_range(reader.clone(), "max_over_time(a[2m:1m])", 10)
        .expect_err("the subquery's preload is charged to the query");
    assert!(err.to_string().contains("too many samples"), "{err}");
    try_range(reader, "max_over_time(a[2m:1m])", 0).expect("unlimited");
}

#[test]
fn oversized_subquery_is_rejected() {
    // `a[100y:1ms]` is ~3.15e12 steps; no configuration limits a subquery's grid.
    let query = "sum_over_time(a[100y:1ms])";
    let (_, reader) = build_reader();
    let err = try_range(reader, query, 0).expect_err("range query must be rejected");
    assert!(err.to_string().contains("too many steps"), "{err}");

    let (_, reader) = build_reader();
    let expr = promql_parser::parser::parse(query).expect("valid test query");
    let stmt = EvalStmt {
        expr,
        start: ms(RANGE_END_MS),
        end: ms(RANGE_END_MS),
        interval: Duration::ZERO,
        lookback_delta: options().lookback_delta,
    };
    let err = evaluate_instant(reader, stmt, ms(RANGE_END_MS), options())
        .expect_err("instant query must be rejected");
    assert!(err.to_string().contains("too many steps"), "{err}");

    // An ordinary subquery still evaluates.
    let (_, reader) = build_reader();
    run_range(reader, "max_over_time(a[30d:1m])");
}

#[test]
fn a_refused_subquery_union_fails_the_query() {
    // Too many samples ends the query. It used to be treated like any
    // declined preload: each outer step then read its own narrower window,
    // which the source accepted, and the query succeeded after all.
    let reader: Arc<dyn QueryReader> = Arc::new(RefusesWideGrids {
        inner: build_data(),
        max_span_ms: 300_000,
    });
    let err = try_range(reader, "max_over_time((a)[4m:1m])", 0)
        .expect_err("the union read was refused for too many samples");
    assert!(err.to_string().contains("too many samples"), "{err}");
}

#[test]
fn a_raw_rollup_answer_is_charged_once() {
    let (_, reader) = build_reader();
    // `rate(a[1m])` over the 5 steps reads (2_940_000, 3_240_000] raw: 30
    // samples per series, 90 in all. That span is what was loaded; the 15
    // rolled points computed from it used to be charged on top.
    const RAW: usize = 3 * 30;
    try_range(reader.clone(), "rate(a[1m])", RAW).expect("the raw span fits the budget");
    try_range(reader, "rate(a[1m])", RAW - 1).expect_err("one sample short of the span");
}

/// One sample per series under a lookback that covers every step fills a
/// grid slot at every step. The read decoded three samples; the grids hold
/// 30,000 points, and those are what the query is charged: each series
/// costs the larger of its span and its grid.
#[test]
fn a_sparse_series_is_charged_for_the_grid_it_fills() {
    const STEPS: i64 = 10_000;
    let querier = MemorySeriesQuerier::new();
    for l in 0..3 {
        let labels = Labels::from_pairs(&[("__name__", "sparse"), ("l", &l.to_string())]);
        querier.add_sample(&labels, Sample::new(0, 1.0));
    }
    let reader: Arc<dyn QueryReader> = Arc::new(querier);
    let run = |max_samples: usize| {
        let start = 1_000;
        let end = start + (STEPS - 1) * 1_000;
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("sparse").unwrap(),
            start: ms(start),
            end: ms(end),
            interval: Duration::from_secs(1),
            // Reaches back to the one sample from the last step.
            lookback_delta: Duration::from_millis(end as u64 + 1),
        };
        let options = QueryOptions {
            max_samples,
            ..options()
        };
        evaluate_range(reader.clone(), stmt, options)
    };

    let grids = 3 * STEPS as usize;
    let result = run(grids).expect("exactly the grids fits");
    assert_eq!(result.iter().map(|s| s.samples.len()).sum::<usize>(), grids);
    let err = run(grids - 1).expect_err("the grids, not the three raw samples, are charged");
    assert!(err.to_string().contains("too many samples"), "{err}");
    assert!(run(0).is_ok(), "0 is unlimited");
}

/// The same for a rollup: one sample per series in a window that spans
/// every step answers at every step. The expansion is charged as the
/// rollup builds it, and the grid only for what it adds past that, so the
/// query costs its grids exactly once.
#[test]
fn a_sparse_rollup_is_charged_once_for_the_grid_it_fills() {
    const STEPS: i64 = 10_000;
    let querier = MemorySeriesQuerier::new();
    for l in 0..3 {
        let labels = Labels::from_pairs(&[("__name__", "sparse"), ("l", &l.to_string())]);
        querier.add_sample(&labels, Sample::new(0, 1.0));
    }
    let reader: Arc<dyn QueryReader> = Arc::new(querier);
    let run = |max_samples: usize| {
        let start = 1_000;
        let end = start + (STEPS - 1) * 1_000;
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("count_over_time(sparse[1d])").unwrap(),
            start: ms(start),
            end: ms(end),
            interval: Duration::from_secs(1),
            lookback_delta: options().lookback_delta,
        };
        let options = QueryOptions {
            max_samples,
            ..options()
        };
        evaluate_range(reader.clone(), stmt, options)
    };

    let grids = 3 * STEPS as usize;
    let result = run(grids).expect("exactly the grids fits");
    assert_eq!(result.iter().map(|s| s.samples.len()).sum::<usize>(), grids);
    let err = run(grids - 1).expect_err("the expansion, not the three raw samples, is charged");
    assert!(err.to_string().contains("too many samples"), "{err}");
    assert!(run(0).is_ok(), "0 is unlimited");
}

/// An expression that reads nothing still returns samples, and a range
/// query keeps every step's: the result is charged for what it holds past
/// what the query loaded, so a generator cannot get past the budget.
#[test]
fn generated_range_results_are_charged() {
    let (_, reader) = build_reader();
    for query in [
        "1",
        "vector(1)",
        "absent(nonexistent)",
        // Builds 3 points per evaluation: under the per-evaluation subquery
        // cap at every limit here, so the result is what is charged.
        "sum_over_time(vector(1)[3s:1s])",
    ] {
        let err = try_range(reader.clone(), query, 1).expect_err(query);
        assert!(
            err.to_string().contains("too many samples"),
            "{query}: {err}"
        );
        try_range(reader.clone(), query, RANGE_STEPS)
            .unwrap_or_else(|err| panic!("{query}: one sample per step fits: {err}"));
        try_range(reader.clone(), query, RANGE_STEPS - 1).expect_err(query);
    }
    try_range(reader, "vector(1)", 0).expect("0 is unlimited");
}

/// An instant query returning a subquery returns its whole grid.
#[test]
fn a_generated_instant_result_is_charged() {
    let (_, reader) = build_reader();
    let run = |max_samples: usize| {
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("vector(1)[1m:1s]").unwrap(),
            start: ms(RANGE_END_MS),
            end: ms(RANGE_END_MS),
            interval: Duration::ZERO,
            lookback_delta: options().lookback_delta,
        };
        let options = QueryOptions {
            max_samples,
            ..options()
        };
        evaluate_instant(reader.clone(), stmt, ms(RANGE_END_MS), options)
    };
    let Ok(crate::promql::QueryValue::Matrix(series)) = run(0) else {
        panic!("a subquery answers a matrix");
    };
    let points: usize = series.iter().map(|s| s.samples.len()).sum();
    assert!(points >= 60, "{points}");
    run(points).expect("exactly the grid fits");
    let err = run(points - 1).expect_err("one sample short of the grid");
    assert!(err.to_string().contains("too many samples"), "{err}");
}

/// A subquery's result is built whole for each outer step, then reduced by
/// the function around it, so the query's own result never sees it. It is
/// capped per evaluation — at most what one outer step builds — not summed
/// over the outer steps.
#[test]
fn a_subquerys_materialization_is_capped_per_evaluation() {
    let (_, reader) = build_reader();
    let instant = |query: &str, max_samples: usize| {
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse(query).unwrap(),
            start: ms(RANGE_END_MS),
            end: ms(RANGE_END_MS),
            interval: Duration::ZERO,
            lookback_delta: options().lookback_delta,
        };
        let options = QueryOptions {
            max_samples,
            ..options()
        };
        evaluate_instant(reader.clone(), stmt, ms(RANGE_END_MS), options)
    };
    // What one evaluation of the subquery builds.
    let Ok(crate::promql::QueryValue::Matrix(grid)) = instant("vector(1)[1m:1s]", 0) else {
        panic!("a subquery answers a matrix");
    };
    let built: usize = grid.iter().map(|s| s.samples.len()).sum();

    // One point returned, `built` built: the cap is what refuses it.
    const SUMMED: &str = "sum_over_time(vector(1)[1m:1s])";
    instant(SUMMED, built).expect("one evaluation's worth fits");
    let err = instant(SUMMED, built - 1).expect_err("one sample short of the subquery");
    assert!(err.to_string().contains("too many samples"), "{err}");

    // Every outer step builds it again; only one evaluation's worth counts.
    try_range(reader.clone(), SUMMED, built).expect("per evaluation, not per query");
    try_range(reader.clone(), SUMMED, built - 1).expect_err("each evaluation is capped");

    try_range(reader, SUMMED, 0).expect("0 is unlimited");
}

/// The reply's size is checked as the range result grows, from a floor on
/// the final estimate: a result that will be refused stops early, and one
/// the final check admits is never refused by the floor.
#[test]
fn a_range_reply_too_large_stops_while_it_is_built() {
    use crate::promql::engine::admission::{estimate_range_reply_bytes, range_reply_bytes_floor};
    let (_, reader) = build_reader();
    let run = |end_ms: i64, max_reply_bytes: usize| {
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("vector(1)").unwrap(),
            start: ms(0),
            end: ms(end_ms),
            interval: Duration::from_secs(1),
            lookback_delta: options().lookback_delta,
        };
        let options = QueryOptions {
            max_reply_bytes,
            ..options()
        };
        evaluate_range(reader.clone(), stmt, options)
    };

    const STEPS: usize = 100_000;
    let err = run((STEPS as i64 - 1) * 1_000, 64 * 1024).expect_err("past the limit");
    let crate::promql::QueryError::ReplyTooLarge { bytes, limit } = err else {
        panic!("refused for its reply size: {err}");
    };
    assert_eq!(limit, 64 * 1024);
    assert!(
        bytes < range_reply_bytes_floor(1, STEPS) / 10,
        "stopped near the limit, not after the whole result: {bytes}"
    );

    let small = run(9_000, 0).expect("unlimited");
    let exact = estimate_range_reply_bytes(&small);
    run(9_000, exact).expect("the floor never refuses what the final estimate admits");
}

/// `ts-promql-max-response-series` bounds what the query returns, not only
/// each read: an `or` of two three-series reads returns six.
#[test]
fn the_result_is_bounded_by_max_series_not_only_each_read() {
    // `or` matches label sets without `__name__`, so `a or b` would keep only
    // `a`'s three: the right side gets a label of its own.
    const OR6: &str = r#"a or label_replace(b, "src", "b", "", "")"#;
    let (_, reader) = build_reader();
    let parse = |q: &str| promql_parser::parser::parse(q).unwrap();
    let with_max_series = |max_series| QueryOptions {
        max_series,
        ..options()
    };
    let range = |query: &str, max_series| {
        let stmt = EvalStmt {
            expr: parse(query),
            start: ms(RANGE_START_MS),
            end: ms(RANGE_END_MS),
            interval: STEP,
            lookback_delta: options().lookback_delta,
        };
        evaluate_range(reader.clone(), stmt, with_max_series(max_series))
    };
    let instant = |query: &str, max_series| {
        let stmt = EvalStmt {
            expr: parse(query),
            start: ms(RANGE_END_MS),
            end: ms(RANGE_END_MS),
            interval: Duration::ZERO,
            lookback_delta: options().lookback_delta,
        };
        evaluate_instant(
            reader.clone(),
            stmt,
            ms(RANGE_END_MS),
            with_max_series(max_series),
        )
    };

    assert_eq!(range(OR6, 6).unwrap().len(), 6);
    let err = range(OR6, 4).expect_err("six series returned, each read three");
    assert!(
        err.to_string().ends_with("max series limit: 6 > 4"),
        "{err}"
    );
    assert!(instant(OR6, 6).is_ok());
    let err = instant(OR6, 4).expect_err("the instant result too");
    assert!(
        err.to_string().ends_with("max series limit: 6 > 4"),
        "{err}"
    );
    assert!(range(OR6, 0).is_ok(), "0 is unlimited");

    // Series a range query accumulates across steps: every series holds the
    // same value at a step and a new one at the next, so `count_values` makes
    // one series per step, five in all, from a single series per step.
    assert_eq!(
        range("count_values(\"v\", a)", 5).unwrap().len(),
        RANGE_STEPS
    );
    let err = range("count_values(\"v\", a)", 4).expect_err("one series per step");
    assert!(err.to_string().contains("max series limit"), "{err}");
}

/// A query whose client disconnected stops at its next deadline check
/// with `Cancelled`, instead of running on for a reply that goes nowhere.
#[test]
fn a_disconnected_clients_query_is_cancelled() {
    use crate::common::replies::TestClient;
    use crate::promql::QueryError;
    let (_, reader) = build_reader();
    let client = TestClient::new();
    let with_token = QueryOptions {
        cancel: client.token(),
        ..options()
    };
    let stmt = |query: &str, start: i64, end: i64, step: Duration| EvalStmt {
        expr: promql_parser::parser::parse(query).unwrap(),
        start: ms(start),
        end: ms(end),
        interval: step,
        lookback_delta: options().lookback_delta,
    };
    // A subquery checks the deadline at its own steps too.
    let query = "max_over_time(rate(a[1m])[2m:10s])";
    let range = || {
        evaluate_range(
            reader.clone(),
            stmt(query, RANGE_START_MS, RANGE_END_MS, STEP),
            with_token,
        )
    };
    let instant = || {
        let at = RANGE_END_MS;
        evaluate_instant(
            reader.clone(),
            stmt(query, at, at, Duration::ZERO),
            ms(at),
            with_token,
        )
    };

    assert!(range().is_ok() && instant().is_ok(), "connected: evaluates");
    client.disconnect();
    assert!(
        matches!(range(), Err(QueryError::Cancelled)),
        "{:?}",
        range().err()
    );
    assert!(
        matches!(instant(), Err(QueryError::Cancelled)),
        "{:?}",
        instant().err()
    );
}
