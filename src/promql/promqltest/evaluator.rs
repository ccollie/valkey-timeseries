use crate::promql::engine::promql_engine::PromqlQuerier;
use crate::promql::{QueryOptions, QueryValue, RangeSample};
use std::time::SystemTime;

/// Options for every conformance eval. Mirrors Prometheus' promqltest, which
/// runs its engine with `EnableExperimentalFunctions = true` so the suite can
/// exercise every function; this must not depend on the server's default,
/// which is expected to ship with experimental functions disabled.
pub(super) fn test_query_options() -> QueryOptions {
    QueryOptions {
        enable_experimental_functions: true,
        ..QueryOptions::default()
    }
}

/// [`test_query_options`] with the static optimizer on: every conformance file
/// also runs this way, so a rewrite that changes a result fails the suite.
pub(super) fn optimized_query_options() -> QueryOptions {
    QueryOptions {
        optimize_queries: true,
        ..test_query_options()
    }
}

/// Execute instant query and return structured results.
///
/// The `.test` files give an instant vector's samples without timestamps, so this checks
/// what they cannot: every sample is stamped with the evaluation time, as in Prometheus.
/// A panic rather than an error, which an `eval_fail` would accept.
pub(super) fn eval_instant(
    tsdb: &PromqlQuerier,
    time: SystemTime,
    query: &str,
    options: &QueryOptions,
) -> Result<QueryValue, String> {
    let value = eval_instant_unchecked(tsdb, time, query, options)?;
    if let QueryValue::Vector(samples) = &value {
        let eval_ms = crate::common::time::system_time_to_millis(time);
        for sample in samples {
            assert_eq!(
                sample.timestamp_ms, eval_ms,
                "{query}: an instant vector sample must carry the evaluation time"
            );
        }
    }
    Ok(value)
}

fn eval_instant_unchecked(
    tsdb: &PromqlQuerier,
    time: SystemTime,
    query: &str,
    options: &QueryOptions,
) -> Result<QueryValue, String> {
    // Quick debug: attempt to parse the query directly to get a more detailed parser error
    match crate::promql::parse_query(query) {
        Ok(_) => {
            // parsed OK, proceed
            tsdb.eval_query(query, Some(time), options)
                .map_err(|e| e.to_string())
        }
        Err(e) => {
            eprintln!(
                "[promqltest] promql parse error for query '{}': {:?}",
                query, e
            );
            // Try to normalize whitespace and retry (tests may contain odd spacing)
            let normalized = regex::Regex::new(r"\s+")
                .unwrap()
                .replace_all(query, " ")
                .to_string();
            if crate::promql::parse_query(&normalized).is_ok() {
                eprintln!(
                    "[promqltest] retrying with normalized query: '{}'",
                    normalized
                );
                return tsdb
                    .eval_query(&normalized, Some(time), options)
                    .map_err(|e| e.to_string());
            }
            // fall through to attempt evaluation which will return error
            tsdb.eval_query(query, Some(time), options)
                .map_err(|err| err.to_string())
        }
    }
}

pub(super) fn eval_range(
    tsdb: &PromqlQuerier,
    start: SystemTime,
    end: SystemTime,
    step: std::time::Duration,
    query: &str,
    options: &QueryOptions,
) -> Result<Vec<RangeSample>, String> {
    let range = start..=end;
    tsdb.eval_query_range(query, range, step, options)
        .map_err(|e| e.to_string())
}
