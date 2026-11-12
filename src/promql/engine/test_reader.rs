//! Fixtures shared by the engine's reader-level tests: a small fleet in an
//! in-memory reader, and a range query over it.

use crate::common::Sample;
use crate::labels::Labels;
use crate::promql::QueryOptions;
use crate::promql::engine::counting_query_reader::CountingQueryReader;
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::{QueryReader, evaluate_range};
use promql_parser::parser::EvalStmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(in crate::promql::engine) const STEP: Duration = Duration::from_secs(60);

/// Range-query window: [3_000_000 ms, 3_240_000 ms] at 60s step = 5 steps.
pub(in crate::promql::engine) const RANGE_START_MS: i64 = 3_000_000;
pub(in crate::promql::engine) const RANGE_END_MS: i64 = 3_240_000;
pub(in crate::promql::engine) const RANGE_STEPS: usize = 5;

pub(in crate::promql::engine) fn ms(ts: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts as u64)
}

/// Three series each for metrics `a` and `b`, sampled every 10s over
/// [0, 4_000_000 ms] — comfortably covering every test window plus the
/// default 5m lookback.
pub(in crate::promql::engine) fn build_data() -> Arc<dyn QueryReader> {
    let querier = MemorySeriesQuerier::new();
    for metric in ["a", "b"] {
        for l in 0..3 {
            let labels = Labels::from_pairs(&[("__name__", metric), ("l", &l.to_string())]);
            for point in 0..=400 {
                let ts = point * INTERVAL_MS;
                querier.add_sample(&labels, Sample::new(ts, point as f64));
            }
        }
    }
    Arc::new(querier)
}

pub(in crate::promql::engine) fn build_reader() -> (Arc<CountingQueryReader>, Arc<dyn QueryReader>)
{
    let counting = Arc::new(CountingQueryReader::new(build_data()));
    let reader: Arc<dyn QueryReader> = counting.clone();
    (counting, reader)
}

pub(in crate::promql::engine) fn options() -> QueryOptions {
    QueryOptions {
        deadline: None,
        ..QueryOptions::default()
    }
}

pub(in crate::promql::engine) fn run_range(reader: Arc<dyn QueryReader>, query: &str) {
    let expr = promql_parser::parser::parse(query).expect("valid test query");
    let stmt = EvalStmt {
        expr,
        start: ms(RANGE_START_MS),
        end: ms(RANGE_END_MS),
        interval: STEP,
        lookback_delta: options().lookback_delta,
    };
    evaluate_range(reader, stmt, options()).expect("range query should evaluate");
}

pub(in crate::promql::engine) const INTERVAL_MS: i64 = 10_000;
