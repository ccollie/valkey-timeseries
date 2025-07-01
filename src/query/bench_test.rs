#[cfg(test)]
mod tests {
    use criterion::Criterion;
    use crate::query::test_metric_storage::TestMetricStorage;
    use crate::query::test_utils::{
        create_context, range_query_cases, setup_range_query_test_data,
    };

    fn benchmark_range_query(crit: &mut Criterion) {
        const TEN_SECONDS: usize = 10 * 1000; // in msec
        let mut stor = TestMetricStorage::new();

        const INTERVAL: i64 = 10000; // 10s interval.
                                     // A day of data plus 10k steps.
        let num_intervals = 8640 + 10000;

        setup_range_query_test_data(&mut stor, INTERVAL, num_intervals).unwrap();
        let cases = range_query_cases();

        let context = create_context(stor);

        for c in cases {
            let name = format!("expr={},steps={}", c.expr, c.steps);
            // crit.iterators(|| {
            //     let start_ofs = (num_intervals - c.steps) as u64 * TEN_SECONDS;
            //     let end_ofs = num_intervals * TEN_SECONDS;
            //
            //     let query_params = QueryParams {
            //         start: start_ofs as Timestamp,
            //         end: end_ofs as Timestamp,
            //         ..Default::default()
            //     };
            //
            //     base_query(&context, &query_params).unwrap()
            // });
            todo!()
        }
    }
}
