//! `promql`: how `TS.QUERY` / `TS.QUERYRANGE` queries ended, and where their time went. Counted
//! in `promql::engine::query_workers`, which admits every query and answers it.
//!
//! Every admitted query ends in exactly one of the outcome counters: succeeded, cancelled (its
//! client disconnected; `INFO ts_promql`'s `promql_queries_cancelled_total`), or failed. A failed
//! query is also counted under its reason when it has one of those below, so the reasons add up
//! to at most the failures; the rest are execution errors in the query itself. Queries refused on
//! arrival because the backlog was full are `promql_queries_rejected_total`, not counted here.

use super::duration_histogram;
use metered::entry::{counter, metric};
use metered::{BucketHistogram, Registry};
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;

/// Queries answered with a result.
pub static SUCCEEDED: AtomicU64 = AtomicU64::new(0);
/// Queries answered with an error, refused on arrival for memory included.
pub static FAILED: AtomicU64 = AtomicU64::new(0);
/// Failures because the query text did not parse or was refused before evaluation (length,
/// depth, regex size).
pub static INVALID: AtomicU64 = AtomicU64::new(0);
/// Failures because the query's deadline (`TIMEOUT`, `ts-promql-max-query-duration`) passed,
/// waiting for a worker or running.
pub static TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// Failures on `ts-promql-max-samples-per-query`.
pub static SAMPLES_LIMIT: AtomicU64 = AtomicU64::new(0);
/// Failures on `ts-promql-max-response-series` or `ts-promql-max-series-per-selector`.
pub static SERIES_LIMIT: AtomicU64 = AtomicU64::new(0);
/// Failures on `ts-promql-max-reply-bytes`.
pub static REPLY_LIMIT: AtomicU64 = AtomicU64::new(0);
/// Failures because this node or a shard refused PromQL work for load:
/// `ts-promql-max-memory-percent` or `ts-promql-max-samples-in-flight`.
pub static OVERLOADED: AtomicU64 = AtomicU64::new(0);

/// Time from a query's admission to the start of its run on a worker.
pub static QUEUE_WAIT: LazyLock<BucketHistogram> = duration_histogram();
/// Time a worker spent preparing a query's text: parsing and the checks that follow it.
pub static PARSE_DURATION: LazyLock<BucketHistogram> = duration_histogram();
/// Time a worker spent evaluating a prepared query, its reply-size check included.
pub static EVAL_DURATION: LazyLock<BucketHistogram> = duration_histogram();

static COUNTERS: &[(&str, &AtomicU64, &str)] = &[
    (
        "queries_succeeded",
        &SUCCEEDED,
        "Queries answered with a result",
    ),
    (
        "queries_failed",
        &FAILED,
        "Queries answered with an error, refusals for memory on arrival included",
    ),
    (
        "queries_invalid",
        &INVALID,
        "Failures: the query did not parse, or was refused before evaluation",
    ),
    (
        "queries_timeout",
        &TIMEOUTS,
        "Failures: the query's deadline passed, waiting or running",
    ),
    (
        "queries_samples_limit",
        &SAMPLES_LIMIT,
        "Failures on ts-promql-max-samples-per-query",
    ),
    (
        "queries_series_limit",
        &SERIES_LIMIT,
        "Failures on ts-promql-max-response-series or ts-promql-max-series-per-selector",
    ),
    (
        "queries_reply_limit",
        &REPLY_LIMIT,
        "Failures on ts-promql-max-reply-bytes",
    ),
    (
        "queries_overloaded",
        &OVERLOADED,
        "Failures because a node refused PromQL work for memory or samples in flight",
    ),
];

static HISTOGRAMS: &[(&str, &LazyLock<BucketHistogram>, &str)] = &[
    (
        "queue_wait_seconds",
        &QUEUE_WAIT,
        "Time from a query's admission to the start of its run on a worker",
    ),
    (
        "parse_duration_seconds",
        &PARSE_DURATION,
        "Time spent preparing a query's text: parsing and the checks after it",
    ),
    (
        "eval_duration_seconds",
        &EVAL_DURATION,
        "Time spent evaluating a prepared query",
    ),
];

pub(super) fn register(registry: &mut Registry<'static>) {
    for &(name, source, help) in COUNTERS {
        registry.register(counter(name).source(source).help(help));
    }
    for &(name, source, help) in HISTOGRAMS {
        registry.register(
            metric(name)
                .source(LazyLock::force(source))
                .help(help)
                .unit("seconds"),
        );
    }
}
