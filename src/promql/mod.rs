pub mod binops;
mod common;
pub(crate) mod deadline;
pub mod engine;
mod error;
mod exec;
mod functions;
mod hashers;
mod model;
mod time;
mod utils;

pub mod optimizer;
#[cfg(test)]
pub(crate) mod promqltest;

/// The PromQL push-down messages are part of the one fanout wire contract
/// (`proto/v1/promql.proto`), so they land in the same generated module as the
/// TS.* fanout types and share `Label`, `Sample` and `SeriesSelector` with them.
/// Re-exported here so the promql-side imports stay short.
pub(crate) use crate::commands::fanout_codec::generated;

// Re-export commonly used types from submodules for public API convenience.
pub use engine::QueryOptions;

use crate::promql::engine::register_fanout_commands;
pub(crate) use error::EvalResult;
pub use error::{EvaluationError, QueryError, QueryResult};
use exec::PreloadMap;
pub use exec::{EvalLabels, EvalSample, EvalSamples, StepMerger};
pub(crate) use exec::{Evaluator, ExprResult, subquery_grid_counts};
pub(crate) use model::EvalContext;
pub use model::{InstantSample, QueryValue, RangeSample};
pub(crate) use time::{MAX_GRID_STEPS, duration_ms};
pub(crate) use utils::{check_operator_chains, check_query_depth};
use valkey_module::ValkeyResult;

pub(crate) use functions::register_parser_functions;

/// Parse a PromQL query. Every query parse outside of tests should come through here.
///
/// The parser is a third-party crate fed untrusted text, and it can panic: promql-parser 0.11
/// converts a numeric duration with `Duration::from_secs_f64`, which panics past its range
/// (`foo[1e20]`). A panic here would fail the query with an internal error, or take down a
/// thread that did not guard against it; it is turned into a parse error instead.
pub(crate) fn parse_query(query: &str) -> Result<promql_parser::parser::Expr, String> {
    std::panic::catch_unwind(|| promql_parser::parser::parse(query)).unwrap_or_else(|payload| {
        let why = crate::common::threads::panic_message(payload.as_ref());
        crate::common::logging::log_debug(format!("PromQL parser panicked: {why}"));
        Err(format!("the parser refused the query: {why}"))
    })
}

pub(crate) fn register_promql_fanout() -> ValkeyResult<()> {
    register_fanout_commands()
}

#[cfg(test)]
mod parse_tests {
    /// A numeric duration past what `Duration` holds made the parser panic; any parser panic
    /// is a parse error, not a failed query.
    #[test]
    fn a_parser_panic_is_a_parse_error() {
        for query in [
            "foo[1e20]",
            "foo[5m:1e20]",
            "foo offset 1e20",
            "rate(foo[1e300])",
        ] {
            let err = super::parse_query(query).expect_err(query);
            assert!(err.contains("Duration"), "{query}: {err}");
        }
        assert!(
            super::parse_query("foo[300]").is_ok(),
            "a numeric duration in range"
        );
    }
}
