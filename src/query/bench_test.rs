#[cfg(test)]
mod tests {
    use crate::query::run_range_query;
    use crate::query::test_metric_storage::TestMetricStorage;
    use crate::query::test_utils::{
        create_context, range_query_cases, setup_range_query_test_data,
    };
    use criterion::{BenchmarkId, Criterion};
    use metricsql_runtime::prelude::query::QueryParams;
    use metricsql_runtime::prelude::Timestamp;

    fn benchmark_range_query(crit: &mut Criterion) {
        const TEN_SECONDS: usize = 10 * 1000; // in msec
        let mut stor = TestMetricStorage::new();

        const INTERVAL: i64 = 10000; // 10s interval.
                                     // A day of data plus 10k steps.
        let num_intervals = 8640 + 10000;

        setup_range_query_test_data(&mut stor, INTERVAL, num_intervals).unwrap();
        let cases = range_query_cases();

        let _context = create_context(stor);

        for c in cases {
            let name = format!("expr={},steps={}", c.expr, c.steps);
            let benchmark_id = BenchmarkId::new(format!("range_query_{name}"), c.steps);
            crit.bench_with_input(benchmark_id, &c, |_b, c| {
                let start_ofs = (num_intervals - c.steps) * TEN_SECONDS;
                let end_ofs = num_intervals * TEN_SECONDS;

                let query_params = QueryParams {
                    start: start_ofs as Timestamp,
                    end: end_ofs as Timestamp,
                    ..Default::default()
                };

                let _ = run_range_query(&query_params).unwrap();
            });
        }
    }
}
