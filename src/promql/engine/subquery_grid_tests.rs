//! A range query evaluates a subquery's inner expression once per step of the
//! subquery's union grid and slices each outer step's window out of it. These
//! pin that the answer is the one per-step evaluation gives, for every shape
//! the slicing has to get right.
use crate::common::Sample;
use crate::labels::Labels;
use crate::promql::engine::admission::Admission;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::{QueryReader, evaluate_instant, evaluate_range};
use crate::promql::exec::{
    grid_points_evaluated_by, subquery_grid_counts, subquery_grids_built_by, without_subquery_grids,
};
use crate::promql::{QueryError, QueryOptions};
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
    run_between(query, (1_500, 3_000), step)
}

/// [`run`] from `start` to `end`, in seconds from the epoch, either side of it.
fn run_between(
    query: &str,
    (start, end): (i64, i64),
    step: Duration,
) -> Vec<(Labels, Vec<Sample>)> {
    let at = |secs: i64| {
        let offset = Duration::from_secs(secs.unsigned_abs());
        if secs < 0 {
            UNIX_EPOCH - offset
        } else {
            UNIX_EPOCH + offset
        }
    };
    let stmt = EvalStmt {
        expr: parse(query).unwrap_or_else(|e| panic!("{query}: {e}")),
        start: at(start),
        end: at(end),
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
    // `@ start()` resolves against the outer query, then `offset` shifts it.
    "max_over_time(m[5m:1m] @ start() offset 2m)",
    // An inner step that divides neither outer step: windows start mid-step.
    "max_over_time(m[5m:45s])",
    // No inner step: the subquery takes the default.
    "max_over_time(m[5m:])",
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
    outcome(query, opts).is_ok()
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

/// The query `query` from 1500 s to 3000 s at a 60 s step, under `opts`.
fn outcome(query: &str, opts: QueryOptions) -> Result<(), QueryError> {
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: UNIX_EPOCH + Duration::from_secs(1_500),
        end: UNIX_EPOCH + Duration::from_secs(3_000),
        interval: Duration::from_secs(60),
        lookback_delta: Duration::from_secs(300),
    };
    evaluate_range(reader(), stmt, opts).map(|_| ())
}

/// A grid is optional work: it must never move a limit's boundary. At the
/// tightest value of each limit that per-step evaluation fits in, the query
/// succeeds with grids too; one below it, both fail the same way.
#[test]
fn a_grid_never_moves_a_limit_boundary() {
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
    let by_points = |limit| QueryOptions {
        max_points_per_series: Some(limit),
        ..options()
    };
    type Limit<'a> = (&'a str, &'a str, &'a dyn Fn(usize) -> QueryOptions);
    let limits: [Limit; 4] = [
        ("max_samples", query, &by_samples),
        ("max_samples_in_flight", query, &by_in_flight),
        // Each window holds 300 points, far more than the query charges: the
        // per-evaluation cap on a window's points binds. A union holds every
        // window, so its grid is declined before any window could pass it.
        (
            "max_samples, per evaluation",
            "max_over_time(vector(1)[5m:1s])",
            &by_samples,
        ),
        // Checked against the outer grid before anything is evaluated; the
        // grid adds no check of its own.
        ("max_points_per_series", query, &by_points),
    ];
    for (name, query, opts) in limits {
        let limit = least_per_step_limit(query, opts);
        assert!(
            outcome(query, opts(limit)).is_ok(),
            "{name} {limit}: per-step evaluation succeeds, the grid fails it"
        );
        let below = limit - 1;
        let per_step = without_subquery_grids(|| outcome(query, opts(below)));
        let cached = outcome(query, opts(below));
        match (&per_step, &cached) {
            (Err(a), Err(b)) => assert_eq!(
                std::mem::discriminant(a),
                std::mem::discriminant(b),
                "{name} {below}: per step {a}, with grids {b}"
            ),
            _ => panic!("{name} {below}: per step {per_step:?}, with grids {cached:?}"),
        }
    }
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

/// Windows and the union grid that straddle the epoch: step alignment for a
/// negative time rounds toward minus infinity on both paths.
#[test]
fn grids_answer_as_per_step_evaluation_before_the_epoch() {
    for query in [
        "max_over_time(m[5m:1m])",
        "max_over_time(vector(time())[5m:1m])",
        "count_over_time(vector(time())[5m:45s])",
        "last_over_time(m[10m:30s] offset 1m)",
    ] {
        let step = Duration::from_secs(60);
        let (cached, built) = subquery_grids_built_by(|| run_between(query, (-900, 900), step));
        let per_step = without_subquery_grids(|| run_between(query, (-900, 900), step));
        assert_eq!(cached, per_step, "{query}");
        assert!(!cached.is_empty(), "{query}: no result to compare");
        assert_eq!(built, 1, "{query}: the grid is used");
    }
}

/// An instant query has no grid of its own, but a subquery nested in another
/// is stepped over the outer one's union: three levels deep, the inner two
/// are evaluated from grids, with the answer per-step evaluation gives.
#[test]
fn nested_subqueries_of_an_instant_query_answer_as_per_step() {
    let query = "max_over_time(max_over_time(max_over_time(m[2m:30s])[5m:1m])[10m:2m])";
    let at = UNIX_EPOCH + Duration::from_secs(2_500);
    let instant = || {
        let stmt = EvalStmt {
            expr: parse(query).unwrap(),
            start: at,
            end: at,
            interval: Duration::ZERO,
            lookback_delta: Duration::from_secs(300),
        };
        format!(
            "{:?}",
            evaluate_instant(reader(), stmt, at, options()).unwrap()
        )
    };
    let (cached, built) = subquery_grids_built_by(instant);
    let per_step = without_subquery_grids(instant);
    assert_eq!(cached, per_step);
    assert!(built >= 2, "{built} grids: the two inner levels");
}

/// A union of more than `MAX_GRID_STEPS` inner steps is never prepared, so
/// no grid is attempted: the subquery evaluates per step, and answers.
#[test]
fn a_union_past_the_step_ceiling_evaluates_per_step() {
    // About 1.1 million union steps at 1 ms; each window holds 10,000.
    let query = "count_over_time(vector(1)[10s:1ms])";
    let stmt = EvalStmt {
        expr: parse(query).unwrap(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH + Duration::from_secs(1_100),
        interval: Duration::from_secs(10),
        lookback_delta: Duration::from_secs(300),
    };
    let (result, built) = subquery_grids_built_by(|| evaluate_range(reader(), stmt, options()));
    let result = result.expect("per-step evaluation answers");
    assert_eq!(built, 0, "no union, so no grid");
    assert_eq!(result.len(), 1);
    assert!(result[0].samples.iter().all(|s| s.value == 10_000.0));
}

/// How many grids `query` builds as a range query at `step` seconds.
fn grids_built(query: &str, step: u64) -> usize {
    subquery_grids_built_by(|| run(query, Duration::from_secs(step))).1
}

/// A grid pays for itself only when windows share its steps: with one window
/// per union step it evaluates what per-step evaluation would, and costs more
/// on top, so it is not built.
#[test]
fn a_grid_needs_windows_to_share_its_steps() {
    assert_eq!(
        grids_built("max_over_time(m[1m:1m])", 60),
        0,
        "one window per step"
    );
    assert_eq!(
        grids_built("max_over_time(m[2m:1m])", 60),
        1,
        "two windows per step"
    );
    assert_eq!(
        grids_built("max_over_time(m[30s:15s])", 30),
        0,
        "one window per step"
    );
    assert_eq!(
        grids_built("max_over_time(m[1m:30s] @ 2000)", 60),
        1,
        "every window is one"
    );
}

/// Labels that change at every step make a grid hold a series per point,
/// which costs more than it saves unless many windows share each step: such
/// a grid is abandoned after its first batch, and counted as declined.
#[test]
fn a_grid_of_new_series_at_every_step_needs_wide_sharing() {
    let churn = |range: &str| {
        format!(r#"sum(count_over_time(count_values("v", vector(time()))[{range}:1m]))"#)
    };
    let before = subquery_grid_counts();
    assert_eq!(grids_built(&churn("5m"), 60), 0, "five windows per step");
    assert!(subquery_grid_counts().declined > before.declined);
    assert_eq!(grids_built(&churn("10m"), 60), 1, "ten windows per step");
    assert_eq!(grids_built("max_over_time(m[5m:1m])", 60), 1, "no churn");
    for range in ["5m", "10m"] {
        assert!(same_as_per_step(&churn(range), Duration::from_secs(60)));
    }
}

/// `x{s="a"}` and `x{s="b"}`, both 1 throughout: `a` was created first, so a
/// read lists it first, but it starts at 2200 s, after `b`.
fn tied_reader() -> Arc<dyn QueryReader> {
    let querier = MemorySeriesQuerier::new();
    let a = Labels::from_pairs(&[("__name__", "x"), ("s", "a")]);
    let b = Labels::from_pairs(&[("__name__", "x"), ("s", "b")]);
    for t in 220..400 {
        querier.add_sample(&a, Sample::new(t * 10_000, 1.0));
    }
    for t in 0..400 {
        querier.add_sample(&b, Sample::new(t * 10_000, 1.0));
    }
    Arc::new(querier)
}

/// Selection operators keep the first of equal values, so a subquery's series
/// must come out of a grid in the order per-step evaluation gives them —
/// first seen in each window, not first seen in the union — or `topk` over
/// tied values picks differently depending on whether a grid was built.
#[test]
fn ties_between_subquery_series_break_as_per_step() {
    for query in [
        "topk(1, last_over_time(x[5m:1m]))",
        "bottomk(1, max_over_time(x[5m:1m]))",
        "limitk(1, last_over_time(x[5m:1m]))",
    ] {
        let run = || {
            let stmt = EvalStmt {
                expr: parse(query).unwrap(),
                start: UNIX_EPOCH + Duration::from_secs(2_400),
                end: UNIX_EPOCH + Duration::from_secs(3_000),
                interval: Duration::from_secs(60),
                lookback_delta: Duration::from_secs(300),
            };
            let result = evaluate_range(tied_reader(), stmt, options()).unwrap();
            result
                .into_iter()
                .map(|s| (s.labels, s.samples))
                .collect::<Vec<_>>()
        };
        let (cached, built) = subquery_grids_built_by(run);
        assert_eq!(built, 1, "{query}: the grid is used");
        assert_eq!(cached, without_subquery_grids(run), "{query}");
    }
}
