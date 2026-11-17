//! Generated PromQL, end to end through the engine.
//!
//! Queries are generated from a small grammar — selectors, range selectors and subqueries,
//! every operator with and without matching modifiers, aggregations, functions, and literals
//! including NaN, the infinities and durations past what the parser can hold — and evaluated
//! against a small in-memory data set, as instant and as range queries. Most generated
//! queries are ill-typed or otherwise refused; that is the point: two properties must hold
//! for every one of them.
//!
//! * Nothing panics. A query is answered with a value or an error.
//! * The static optimizer never changes an answer: where both evaluations succeed, the
//!   optimized query answers as the query as written does.
//!
//! `PROPTEST_CASES` scales the run (proptest reads it), as for the other property tests.

use crate::common::Sample;
use crate::labels::{Label, Labels};
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::promql_engine::PromqlQuerier;
use crate::promql::{QueryOptions, QueryValue, RangeSample};
use proptest::prelude::*;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, UNIX_EPOCH};

/// A fixture series: its name, its other labels, and its value at each time.
type SeriesSpec = (
    &'static str,
    &'static [(&'static str, &'static str)],
    fn(i64) -> f64,
);

/// `m` and `other` share label sets but for the name; some values are NaN or infinite.
static TSDB: LazyLock<PromqlQuerier> = LazyLock::new(|| {
    let querier = MemorySeriesQuerier::new();
    let series: [SeriesSpec; 6] = [
        ("m", &[("job", "a"), ("instance", "1")], |t| t as f64),
        ("m", &[("job", "a"), ("instance", "2")], |t| {
            (t % 7) as f64 - 3.0
        }),
        ("m", &[("job", "b"), ("instance", "1")], |t| {
            if t % 4 == 0 { f64::NAN } else { 2.0 }
        }),
        ("m", &[("job", "b"), ("instance", "3")], |t| {
            if t % 5 == 0 { f64::INFINITY } else { 1e300 }
        }),
        ("other", &[("job", "a"), ("instance", "1")], |t| {
            (t / 10) as f64
        }),
        ("hist_bucket", &[("le", "1")], |t| t as f64),
    ];
    for (name, pairs, value) in series {
        let mut labels = vec![Label::new("__name__", name)];
        labels.extend(pairs.iter().map(|(n, v)| Label::new(*n, *v)));
        let labels = Labels::new(labels);
        for step in 0..30 {
            let t = 3_600 + step * 15;
            querier.add_sample(&labels, Sample::new(t * 1_000, value(t)));
        }
    }
    PromqlQuerier::with_query_reader(Arc::new(querier))
});

/// An answer reduced to comparable data: each series' labels and points, sorted by labels.
type Answer = Vec<(String, Vec<(i64, f64)>)>;

fn pick(options: &'static [&'static str]) -> impl Strategy<Value = String> {
    proptest::sample::select(options).prop_map(str::to_string)
}

fn selector() -> impl Strategy<Value = String> {
    pick(&[
        "m",
        r#"m{job="a"}"#,
        r#"m{job=~"a|b"}"#,
        r#"m{job!="a"}"#,
        "other",
        "hist_bucket",
        "nothing_here",
    ])
}

fn duration() -> impl Strategy<Value = String> {
    pick(&["1s", "15s", "1m", "5m", "1h", "0s", "1e20", "300"])
}

fn literal() -> impl Strategy<Value = String> {
    pick(&[
        "0",
        "1",
        "-2.5",
        "0.5",
        "1e308",
        "NaN",
        "Inf",
        "-Inf",
        "time()",
        "vector(1)",
    ])
}

fn modifier() -> impl Strategy<Value = String> {
    pick(&[
        "",
        "",
        "bool",
        "on(job)",
        "ignoring(instance)",
        "on(job) group_left",
        "on(job) group_right(instance)",
    ])
}

fn binary_op() -> impl Strategy<Value = String> {
    pick(&[
        "+", "-", "*", "/", "%", "^", "==", "!=", ">", "<", ">=", "<=", "and", "or", "unless",
        "atan2",
    ])
}

fn expr() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        3 => selector(),
        2 => literal(),
        2 => (selector(), duration(), pick(&["rate", "increase", "sum_over_time",
            "max_over_time", "quantile_over_time(0.5,", "count_over_time", "last_over_time",
            "deriv", "changes", "resets", "stddev_over_time"]))
            .prop_map(|(s, d, f)| if f.ends_with(',') { format!("{f} {s}[{d}])") }
                                   else { format!("{f}({s}[{d}])") }),
    ];
    leaf.prop_recursive(4, 24, 2, |inner| {
        prop_oneof![
            (inner.clone(), binary_op(), modifier(), inner.clone())
                .prop_map(|(a, op, m, b)| format!("({a}) {op} {m} ({b})")),
            (
                pick(&[
                    "sum", "avg", "min", "max", "count", "stddev", "stdvar", "group"
                ]),
                pick(&["", " by (job)", " without (instance)", " by ()"]),
                inner.clone()
            )
                .prop_map(|(op, g, x)| format!("{op}{g} ({x})")),
            (
                pick(&[
                    "topk(2,",
                    "bottomk(1,",
                    "limitk(1,",
                    "quantile(0.9,",
                    "count_values(\"v\","
                ]),
                inner.clone()
            )
                .prop_map(|(op, x)| format!("{op} {x})")),
            (
                pick(&[
                    "abs",
                    "ceil",
                    "floor",
                    "round",
                    "sqrt",
                    "exp",
                    "ln",
                    "sort",
                    "sort_desc",
                    "absent",
                    "scalar",
                    "timestamp",
                    "-"
                ]),
                inner.clone()
            )
                .prop_map(|(f, x)| format!("{f}({x})")),
            (
                inner.clone(),
                duration(),
                pick(&["5s", "1m", ""]),
                pick(&["max_over_time", "rate", "avg_over_time", "count_over_time"])
            )
                .prop_map(|(x, d, s, f)| format!("{f}(({x})[{d}:{s}])")),
            inner
                .clone()
                .prop_map(|x| format!(r#"label_replace({x}, "dst", "$1-x", "job", "(.*)")"#)),
            inner
                .clone()
                .prop_map(|x| format!("histogram_quantile(0.5, {x})")),
            (inner.clone(), literal()).prop_map(|(x, n)| format!("clamp_max({x}, {n})")),
        ]
    })
}

/// The answer `value` gives, as comparable data.
fn answer(value: Result<QueryValue, String>) -> Result<Answer, String> {
    let mut rendered = match value? {
        QueryValue::Scalar {
            timestamp_ms,
            value,
        } => vec![(String::new(), vec![(timestamp_ms, value)])],
        QueryValue::String(s) => vec![(s, vec![])],
        QueryValue::Vector(v) => v
            .into_iter()
            .map(|s| (format!("{:?}", s.labels), vec![(s.timestamp_ms, s.value)]))
            .collect(),
        QueryValue::Matrix(m) => matrix(m),
    };
    rendered.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(rendered)
}

fn matrix(series: Vec<RangeSample>) -> Answer {
    series
        .into_iter()
        .map(|s| {
            let points = s.samples.iter().map(|p| (p.timestamp, p.value)).collect();
            (format!("{:?}", s.labels), points)
        })
        .collect()
}

fn same_value(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan())
        || a == b
        || (a.is_finite() && b.is_finite() && (a - b).abs() <= 1e-9 * a.abs().max(b.abs()))
}

fn same_answer(a: &[(String, Vec<(i64, f64)>)], b: &[(String, Vec<(i64, f64)>)]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|((la, pa), (lb, pb))| {
            la == lb
                && pa.len() == pb.len()
                && pa
                    .iter()
                    .zip(pb)
                    .all(|((ta, va), (tb, vb))| ta == tb && same_value(*va, *vb))
        })
}

fn evaluate(query: &str, optimize_queries: bool) -> [Result<Answer, String>; 2] {
    let opts = QueryOptions {
        optimize_queries,
        max_samples: 1_000_000,
        ..QueryOptions::default()
    };
    let at = UNIX_EPOCH + Duration::from_secs(4_000);
    let instant = answer(
        TSDB.eval_query(query, Some(at), &opts)
            .map_err(|e| e.to_string()),
    );
    let range = TSDB
        .eval_query_range(
            query,
            at - Duration::from_secs(120)..=at,
            Duration::from_secs(30),
            &opts,
        )
        .map(|series| {
            let mut rendered = matrix(series);
            rendered.sort_by(|a, b| a.0.cmp(&b.0));
            rendered
        })
        .map_err(|e| e.to_string());
    [instant, range]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_queries_never_panic_and_the_optimizer_keeps_their_answers(query in expr()) {
        let written = evaluate(&query, false);
        let optimized = evaluate(&query, true);
        for (kind, (w, o)) in ["instant", "range"].iter().zip(written.iter().zip(optimized.iter())) {
            if let (Ok(w), Ok(o)) = (w, o) {
                prop_assert!(same_answer(w, o),
                    "{kind} `{query}`: as written {w:?}, optimized {o:?}");
            }
        }
    }
}
