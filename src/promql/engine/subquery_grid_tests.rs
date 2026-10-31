//! A range query evaluates a subquery's inner expression once per step of the
//! subquery's union grid and slices each outer step's window out of it. These
//! pin that the answer is the one per-step evaluation gives, for every shape
//! the slicing has to get right.
use crate::common::Sample;
use crate::labels::Labels;
use crate::promql::QueryOptions;
use crate::promql::engine::admission::Admission;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::{QueryReader, evaluate_range};
use crate::promql::exec::{
    grid_points_evaluated_by, subquery_grid_counts, subquery_grids_built_by, without_subquery_grids,
};
use promql_parser::parser::{EvalStmt, parse};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// `m` on two series every 10 s; `n` on one series with a gap; both over the
/// whole query window plus its lookback.
fn reader() -> Arc<dyn QueryReader> {
    let querier = MemorySeriesQuerier::new();
    for job in ["a", "b"] {
        let labels = Labels::from_pairs(&[("__name__", "m"), ("job", job)]);
        for t in 0..400 {
            let value = if job == "a" {
                t as f64
            } else {
                (t % 7) as f64 - 3.0
            };
            querier.add_sample(&labels, Sample::new(t * 10_000, value));
        }
    }
    let labels = Labels::from_pairs(&[("__name__", "n"), ("job", "a")]);
    for t in (0..400).filter(|t| !(150..210).contains(t)) {
        querier.add_sample(&labels, Sample::new(t * 10_000, -(t as f64)));
    }
    Arc::new(querier)
}

fn options() -> QueryOptions {
    QueryOptions {
        deadline: None,
        ..QueryOptions::default()
    }
}

/// The range query `query` from 1500 s to 3000 s at `step`, as sorted
/// `(labels, samples)`.
fn run(query: &str, step: Duration) -> Vec<(Labels, Vec<Sample>)> {
    let stmt = EvalStmt {
        expr: parse(query).unwrap_or_else(|e| panic!("{query}: {e}")),
        start: UNIX_EPOCH + Duration::from_secs(1_500),
        end: UNIX_EPOCH + Duration::from_secs(3_000),
        interval: step,
        lookback_delta: Duration::from_secs(300),
    };
    evaluate_range(reader(), stmt, options())
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .into_iter()
        .map(|s| (s.labels, s.samples))
        .collect()
}

fn same_as_per_step(query: &str, step: Duration) -> bool {
    let cached = run(query, step);
    let per_step = without_subquery_grids(|| run(query, step));
    assert_eq!(cached, per_step, "{query} at {step:?}");
    !cached.is_empty()
}

const QUERIES: &[&str] = &[
    "max_over_time(m[5m:1m])",
    "sum_over_time(rate(m[2m])[10m:30s])",
    "max_over_time(m[5m:1m]) - min_over_time(m[5m:1m])",
    "max_over_time(max_over_time(m[2m:30s])[10m:1m])",
    "max_over_time(m[5m:1m] @ 2000)",
    "max_over_time(m[5m:1m] @ end())",
    "avg_over_time(m[5m:1m] offset 2m)",
    "max_over_time(timestamp(m)[5m:1m])",
    "max_over_time(vector(time())[5m:1m])",
    "last_over_time(abs(m)[5m:1m])",
    "last_over_time(m[5m:1m])",
    "count_over_time((m or abs(n))[10m:1m])",
    // One label set whose pending drop changes from step to step: `n` itself
    // where it reports, `abs` of its value ten minutes earlier in its gap.
    "last_over_time((n or abs(n offset 10m))[10m:1m])",
    "last_over_time(n[10m:30s])",
    "sum by (job) (count_over_time(n[10m:1m]))",
    // Labels that change from step to step: a union series per value, each
    // window holding a few of them.
    "count_over_time(count_values(\"v\", m)[5m:1m])",
    "sum(last_over_time(count_values(\"v\", vector(time()))[1m:1m]))",
];

/// Every query answers what per-step evaluation answers, at an outer step that
/// lets the windows overlap (the grid is used) and at one that does not.
#[test]
fn evaluated_subquery_grids_answer_as_per_step_evaluation() {
    for query in QUERIES {
        for step in [Duration::from_secs(60), Duration::from_secs(30)] {
            assert!(
                same_as_per_step(query, step),
                "{query}: no result to compare"
            );
        }
        same_as_per_step(query, Duration::from_secs(900));
    }
}

/// A subquery whose windows overlap is evaluated from its union grid; one
/// whose outer step is wider than its range is not — and neither is a
/// subquery nested in it, whose union would span the gaps between the outer
/// windows that per-step evaluation never visits.
#[test]
fn only_gap_free_windows_build_a_grid() {
    let nested = "max_over_time(max_over_time(m[2m:30s])[5m:1m])";
    let built = |query: &str, step: u64| {
        subquery_grids_built_by(|| run(query, Duration::from_secs(step))).1
    };
    assert_eq!(built("max_over_time(m[5m:1m])", 60), 1);
    assert_eq!(built("max_over_time(m[5m:1m])", 900), 0);
    assert_eq!(built(nested, 60), 2, "both levels overlap");
    assert_eq!(
        built(nested, 900),
        0,
        "the nested union spans the outer gaps"
    );
}

fn succeeds(query: &str, opts: QueryOptions) -> bool {
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: UNIX_EPOCH + Duration::from_secs(1_500),
        end: UNIX_EPOCH + Duration::from_secs(3_000),
        interval: Duration::from_secs(60),
        lookback_delta: Duration::from_secs(300),
    };
    evaluate_range(reader(), stmt, opts).is_ok()
}

/// The smallest limit (by bisection; success is monotone in it) at which
/// `query` succeeds per step under the limit `opts` sets.
fn least_per_step_limit(query: &str, opts: impl Fn(usize) -> QueryOptions) -> usize {
    let (mut fails, mut passes) = (1usize, 1usize << 20);
    assert!(without_subquery_grids(|| succeeds(query, opts(passes))));
    while passes - fails > 1 {
        let mid = (fails + passes) / 2;
        if without_subquery_grids(|| succeeds(query, opts(mid))) {
            passes = mid;
        } else {
            fails = mid;
        }
    }
    passes
}

/// A grid is optional work: it must never make a query fail that per-step
/// evaluation answers, at the tightest sample limit or process-wide in-flight
/// limit that per-step evaluation fits in.
#[test]
fn a_grid_never_fails_a_query_per_step_evaluation_answers() {
    let query = "max_over_time(vector(1)[5m:1m]) + max_over_time(sum(m)[5m:1m])";
    let by_samples = |limit| QueryOptions {
        max_samples: limit,
        ..options()
    };
    // Its own count: on the process-wide one, other tests' queries running
    // alongside move the tightest limit between the probe and the check.
    static IN_FLIGHT: Admission = Admission::new();
    let by_in_flight = |limit| QueryOptions {
        max_samples_in_flight: limit,
        admission: Some(&IN_FLIGHT),
        ..options()
    };
    let limit = least_per_step_limit(query, by_samples);
    assert!(
        succeeds(query, by_samples(limit)),
        "max_samples {limit}: per-step evaluation succeeds, the grid fails it"
    );
    let limit = least_per_step_limit(query, by_in_flight);
    assert!(
        succeeds(query, by_in_flight(limit)),
        "in-flight limit {limit}: per-step evaluation succeeds, the grid fails it"
    );
}

/// A grid over a long union under a small sample limit is abandoned as soon as
/// it crosses the limit — within a batch of steps, where the per-step path
/// stops too — rather than built whole and discarded.
#[test]
fn a_grid_over_the_limit_is_abandoned_while_it_is_built() {
    // A one-day subquery at a one-second step: about 88,000 union steps.
    let query = "max_over_time(vector(1)[1d:1s])";
    let opts = QueryOptions {
        max_samples: 100,
        ..options()
    };
    let (_, evaluated) = grid_points_evaluated_by(|| succeeds(query, opts));
    assert!(
        evaluated <= 2 * 64,
        "built {evaluated} points of a grid capped at 100 before abandoning it"
    );
}

/// `INFO ts_promql` counts the grids built and the ones declined for going over
/// the sample limit, so a subquery left to evaluate per step leaves a trail.
/// The counters are process-wide and other tests move them too, hence `>`.
#[test]
fn built_and_declined_grids_are_counted() {
    let before = subquery_grid_counts();
    run("max_over_time(m[5m:1m])", Duration::from_secs(60));
    assert!(subquery_grid_counts().built > before.built, "a built grid");

    let before = subquery_grid_counts();
    let opts = QueryOptions {
        max_samples: 100,
        ..options()
    };
    succeeds("max_over_time(vector(1)[1d:1s])", opts);
    assert!(
        subquery_grid_counts().declined > before.declined,
        "a grid over the limit"
    );
}

/// A grid is held for the whole query, so the points it keeps count toward the
/// process-wide in-flight limit while it lives. Without room for them the
/// grid is declined and the subquery evaluates per step; once the query ends
/// they are given back.
#[test]
fn a_grid_reserves_its_points_in_flight() {
    static COUNTS: Admission = Admission::new();
    // Per step it fits an in-flight limit of 26; its grid holds 1,800 points.
    let query = "max_over_time(vector(1)[5m:1s])";
    let by_in_flight = |limit| QueryOptions {
        max_samples_in_flight: limit,
        admission: Some(&COUNTS),
        ..options()
    };
    let (ok, built) = subquery_grids_built_by(|| succeeds(query, by_in_flight(100)));
    assert!(ok, "per-step evaluation fits");
    assert_eq!(built, 0, "no room for the grid's points");
    let (ok, built) = subquery_grids_built_by(|| succeeds(query, by_in_flight(10_000)));
    assert!(ok);
    assert_eq!(built, 1, "room for the grid's points");
    assert_eq!(
        COUNTS.samples_in_flight(),
        0,
        "given back when the query ends"
    );
}
