use crate::aggregators::{AggregationHandler, Aggregator, BucketTimestamp};
use crate::common::{Sample, Timestamp};
use crate::series::request_types::AggregationOptions;
use std::collections::VecDeque;

/// Helper class for minimizing monomorphization overhead for AggregationIterator
#[derive(Debug)]
struct AggregationHelper {
    aggregator: Aggregator,
    bucket_duration: u64,
    bucket_ts: BucketTimestamp,
    bucket_range_start: Timestamp,
    bucket_range_end: Timestamp,
    align_timestamp: Timestamp,
    has_samples: bool,
    count: usize,
    report_empty: bool,
}

impl AggregationHelper {
    fn with_parts(
        aggregator: Aggregator,
        bucket_duration: u64,
        bucket_ts: BucketTimestamp,
        align_timestamp: Timestamp,
        report_empty: bool,
    ) -> Self {
        Self {
            aggregator,
            bucket_duration,
            bucket_ts,
            bucket_range_start: 0,
            bucket_range_end: 0,
            align_timestamp,
            has_samples: false,
            count: 0,
            report_empty,
        }
    }

    pub(crate) fn new_from_aggregator(
        aggregator: Aggregator,
        bucket_duration: u64,
        bucket_ts: BucketTimestamp,
        align_timestamp: Timestamp,
        report_empty: bool,
    ) -> Self {
        Self::with_parts(
            aggregator,
            bucket_duration,
            bucket_ts,
            align_timestamp,
            report_empty,
        )
    }

    pub(crate) fn new(options: &AggregationOptions, align_timestamp: Timestamp) -> Self {
        let mut aggregator = options.aggregation.create_aggregator();
        if let Aggregator::Rate(r) = &mut aggregator {
            r.set_window_ms(options.bucket_duration);
        }

        Self::with_parts(
            aggregator,
            options.bucket_duration,
            options.timestamp_output,
            align_timestamp,
            options.report_empty,
        )
    }

    fn add_empty_bucket_internal(
        &self,
        samples: &mut VecDeque<Sample>,
        start_bucket: Timestamp,
        end_bucket_exclusive: Timestamp,
    ) {
        if end_bucket_exclusive <= start_bucket {
            return;
        }

        let value = AggregationHandler::empty_bucket_value(&self.aggregator);
        let count = ((end_bucket_exclusive - start_bucket) / self.bucket_duration as i64) as usize;
        samples.reserve(count);

        for bucket_start in
            (start_bucket..end_bucket_exclusive).step_by(self.bucket_duration as usize)
        {
            samples.push_back(Sample {
                timestamp: self.bucket_ts.calculate(bucket_start, self.bucket_duration),
                value,
            });
        }
    }

    fn add_empty_buckets_between_timestamps(
        &self,
        samples: &mut VecDeque<Sample>,
        first_ts: Timestamp,
        end_ts: Timestamp,
    ) {
        let start = self.calc_bucket_start(first_ts);
        let end = self.calc_bucket_start(end_ts);
        self.add_empty_bucket_internal(samples, start, end);
    }

    fn add_empty_buckets(
        &self,
        samples: &mut VecDeque<Sample>,
        first_bucket_ts: Timestamp,
        end_bucket_ts: Timestamp,
    ) {
        let value = AggregationHandler::empty_bucket_value(&self.aggregator);
        let start = self.calc_bucket_start(first_bucket_ts);
        let end = self.calc_bucket_start(end_bucket_ts);

        let count = ((end - start) / self.bucket_duration as i64) as usize;
        samples.reserve(count);

        for timestamp in (start..end).step_by(self.bucket_duration as usize) {
            samples.push_back(Sample { timestamp, value });
        }
    }

    fn output_timestamp(&self) -> Timestamp {
        self.bucket_ts
            .calculate(self.bucket_range_start, self.bucket_duration)
    }

    fn complete_bucket(
        &mut self,
        last_ts: Option<Timestamp>,
        empty_buckets: &mut VecDeque<Sample>,
    ) -> Option<Sample> {
        let bucket = if self.count > 0 {
            Some(Sample::new(
                self.output_timestamp(),
                AggregationHandler::finalize(&mut self.aggregator),
            ))
        } else if self.report_empty {
            Some(Sample::new(
                self.output_timestamp(),
                AggregationHandler::empty_value(&self.aggregator),
            ))
        } else {
            None
        };

        AggregationHandler::reset(&mut self.aggregator);

        if self.report_empty
            && let Some(last_ts) = last_ts
            && last_ts >= self.bucket_range_end
        {
            let start = self.bucket_range_end + 1;
            self.add_empty_buckets_between_timestamps(empty_buckets, start, last_ts);
        }

        self.has_samples = false;
        self.count = 0;
        bucket
    }

    fn update(&mut self, sample: Sample) {
        if self.aggregator.update(sample.timestamp, sample.value) {
            self.has_samples = true;
        }
        self.count += 1;
    }

    #[inline]
    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_range_end
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.bucket_range_start = self.calc_bucket_start(timestamp);
        self.bucket_range_end = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration);
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        (ts - ((diff % delta + delta) % delta)).max(0)
    }
}

pub fn aggregate(
    options: &AggregationOptions,
    aligned_timestamp: Timestamp,
    iter: impl Iterator<Item = Sample>,
) -> Vec<Sample> {
    let iterator = AggregateIterator::new(iter, options, aligned_timestamp);
    iterator.collect()
}

pub struct AggregateIterator<T: Iterator<Item = Sample>> {
    inner: T,
    aggregator: AggregationHelper,
    empty_buckets: VecDeque<Sample>,
    prev_ts: Timestamp,
    init: bool,
    query_range: Option<(Timestamp, Timestamp)>,
}

impl<T: Iterator<Item = Sample>> AggregateIterator<T> {
    pub fn new(inner: T, options: &AggregationOptions, aligned_timestamp: Timestamp) -> Self {
        let aggregator = AggregationHelper::new(options, aligned_timestamp);
        Self {
            inner,
            aggregator,
            empty_buckets: VecDeque::new(),
            prev_ts: 0,
            init: false,
            query_range: None,
        }
    }

    pub fn with_range(
        inner: T,
        options: &AggregationOptions,
        aligned_timestamp: Timestamp,
        query_start: Timestamp,
        query_end: Timestamp,
    ) -> Self {
        Self {
            inner,
            aggregator: AggregationHelper::new(options, aligned_timestamp),
            empty_buckets: VecDeque::new(),
            prev_ts: 0,
            init: false,
            query_range: Some((query_start, query_end)),
        }
    }

    #[inline]
    fn update(&mut self, sample: Sample) {
        self.aggregator.update(sample);
    }

    #[inline]
    fn start_new_bucket(&mut self, sample: Sample) {
        self.aggregator.update_bucket_timestamps(sample.timestamp);
        self.update(sample);
    }

    #[inline]
    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        self.aggregator.should_finalize_bucket(timestamp)
    }

    #[inline]
    fn finalize_bucket(&mut self, ts: Option<Timestamp>) -> Option<Sample> {
        self.aggregator.complete_bucket(ts, &mut self.empty_buckets)
    }

    #[inline]
    fn pop_empty_bucket(&mut self) -> Option<Sample> {
        self.empty_buckets.pop_front()
    }

    fn enqueue_leading_empty_buckets(&mut self, first_sample_ts: Timestamp) {
        if !self.aggregator.report_empty {
            return;
        }

        if let Some((query_start, _)) = self.query_range {
            let requested_first_bucket = self.aggregator.calc_bucket_start(query_start);
            let first_sample_bucket = self.aggregator.calc_bucket_start(first_sample_ts);
            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                requested_first_bucket,
                first_sample_bucket,
            );
        }
    }

    fn enqueue_full_empty_range_if_needed(&mut self) {
        if !self.aggregator.report_empty {
            return;
        }

        if let Some((query_start, query_end)) = self.query_range {
            let first_bucket = self.aggregator.calc_bucket_start(query_start);
            let end_exclusive = self
                .aggregator
                .calc_bucket_start(query_end)
                .saturating_add_unsigned(self.aggregator.bucket_duration);
            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                first_bucket,
                end_exclusive,
            );
        }
    }

    #[inline]
    fn ensure_initialized(&mut self) -> bool {
        if self.init {
            return true;
        }

        self.init = true;
        if let Some(sample) = self.inner.next() {
            self.enqueue_leading_empty_buckets(sample.timestamp);
            self.start_new_bucket(sample);
            true
        } else {
            self.enqueue_full_empty_range_if_needed();
            false
        }
    }

    fn process_bucket(&mut self) -> Option<Sample> {
        while let Some(sample) = self.inner.next() {
            if !self.aggregator.should_finalize_bucket(sample.timestamp) {
                self.update(sample);
                continue;
            }

            let bucket = self.finalize_bucket(Some(sample.timestamp));
            self.start_new_bucket(sample);

            if bucket.is_some() {
                return bucket;
            }
            // All-NaN bucket was skipped; continue processing the next samples
            // in the newly started bucket.
        }

        None
    }

    fn finalize_last_bucket_if_any(&mut self) -> Option<Sample> {
        if self.aggregator.count == 0 {
            return None;
        }

        let bucket = self.finalize_bucket(None);

        if self.aggregator.report_empty
            && let Some((_, query_end)) = self.query_range
        {
            let end_exclusive = self
                .aggregator
                .calc_bucket_start(query_end)
                .saturating_add_unsigned(self.aggregator.bucket_duration);

            self.aggregator.add_empty_bucket_internal(
                &mut self.empty_buckets,
                self.aggregator.bucket_range_end,
                end_exclusive,
            );
        }

        bucket
    }
}

impl<T: Iterator<Item = Sample>> Iterator for AggregateIterator<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(sample) = self.pop_empty_bucket() {
            return Some(sample);
        }

        if !self.ensure_initialized() {
            return self.pop_empty_bucket();
        }

        if let Some(bucket) = self.process_bucket() {
            return Some(bucket);
        }

        self.finalize_last_bucket_if_any()
            .or_else(|| self.pop_empty_bucket())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;

    fn create_test_samples() -> Vec<Sample> {
        vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            Sample::new(20, 3.0),
            Sample::new(30, 4.0),
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
            Sample::new(60, 7.0),
        ]
    }

    fn create_options(aggregator: AggregationType) -> AggregationOptions {
        AggregationOptions {
            aggregation: aggregator.into(),
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        }
    }

    #[test]
    fn test_sum_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 3.0); // 1.0 + 2.0
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 3.0);
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 4.0);
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
        assert_eq!(result[5].timestamp, 60);
        assert_eq!(result[5].value, 7.0);
    }

    #[test]
    fn test_sum_with_nans() {
        use std::f64;

        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, f64::NAN),
            Sample::new(20, 2.0),
        ];

        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        // NaN should be ignored, sum = 1.0
        assert_eq!(result[0].value, 1.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 2.0);
    }

    #[test]
    fn test_sum_all_nans() {
        let samples = vec![
            Sample::new(10, f64::NAN),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
        ];

        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        // All NaNs should be ignored, sum = 0.0
        assert_eq!(result[0].value, 0.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 0.0);
    }

    #[test]
    fn test_sum_all_nans_report_empty() {
        let samples = vec![
            Sample::new(10, f64::NAN),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
        ];

        let mut options = create_options(AggregationType::Sum);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 0.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 0.0);
    }

    #[test]
    fn test_avg_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Avg);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.5); // (1.0 + 2.0) / 2
    }

    #[test]
    fn test_max_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Max);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // max of 1.0 and 2.0
    }

    #[test]
    fn test_min_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Min);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.0); // min of 1.0 and 2.0
    }

    #[test]
    fn test_count_aggregation() {
        let samples = create_test_samples();
        let options = create_options(AggregationType::Count);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // count of values in the bucket
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 1.0);
    }

    #[test]
    fn test_count_with_nans() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
            Sample::new(25, 3.0),
            Sample::new(30, f64::NAN),
            Sample::new(35, f64::NAN),
        ];

        let options = create_options(AggregationType::Count);
        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.0); // only the non-NaN sample is counted
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 1.0); // NaN is ignored, valid sample still counts
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 0.0); // all-NaN bucket yields an empty count
    }

    #[test]
    fn test_count_all_nans() {
        let samples = vec![
            Sample::new(10, f64::NAN),
            Sample::new(15, f64::NAN),
            Sample::new(20, f64::NAN),
            Sample::new(25, f64::NAN),
        ];

        let options = create_options(AggregationType::Count);
        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 0.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 0.0);
    }

    #[test]
    fn test_empty_buckets_report_empty_true() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(AggregationType::Sum);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 3.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 0.0); // Empty bucket with value 0 for sum
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 0.0); // Empty bucket
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
    }

    // #[test] TODO
    fn test_empty_buckets_last() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 99.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(AggregationType::Last);
        options.report_empty = true;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 99.0);
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 99.0); // Empty bucket with value 0 for sum
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 99.0); // Empty bucket
        assert_eq!(result[3].timestamp, 40);
        assert_eq!(result[3].value, 5.0);
        assert_eq!(result[4].timestamp, 50);
        assert_eq!(result[4].value, 6.0);
    }
    #[test]
    fn test_bucket_timestamp_end() {
        let samples = create_test_samples();
        let mut options = create_options(AggregationType::Sum);
        options.timestamp_output = BucketTimestamp::End;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 20); // End of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_bucket_timestamp_mid() {
        let samples = create_test_samples();
        let mut options = create_options(AggregationType::Sum);
        options.timestamp_output = BucketTimestamp::Mid;

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 15); // Mid of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_empty_input() {
        let samples: Vec<Sample> = vec![];
        let options = create_options(AggregationType::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 0); // Last bucket with default value
        // assert!(result[0].value.is_nan() || result[0].value == 0.0);
    }

    #[test]
    fn test_range_aggregation_basic() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 5.0),
            Sample::new(20, 2.0),
            Sample::new(25, 8.0),
            Sample::new(30, 3.0),
            Sample::new(35, 7.0),
        ];

        let options = AggregationOptions {
            aggregation: AggregationType::Range.into(),
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);
        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 3);

        // The first bucket [10, 20): contains 1.0 and 5.0, range = 5.0 - 1.0 = 4.0
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 4.0);

        // The second bucket [20, 30): contains 2.0 and 8.0, range = 8.0 - 2.0 = 6.0
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 6.0);

        // The third bucket [30, 40): contains 3.0 and 7.0, range = 7.0 - 3.0 = 4.0
        assert_eq!(result[2].timestamp, 30);
        assert_eq!(result[2].value, 4.0);
    }

    // #[test]
    // fn test_alignment_with_offset() {
    //     let samples = vec![
    //         Sample::new(12, 1.0),
    //         Sample::new(17, 2.0),
    //         Sample::new(22, 3.0),
    //         Sample::new(32, 4.0),
    //     ];
    //
    //     let options = create_options(Aggregator::Sum);
    //
    //     let iterator = AggregateIterator::new(
    //         samples.into_iter(),
    //         options,
    //         2, // Aligned timestamp is 2
    //     );
    //
    //     let result: Vec<Sample> = iterator.collect();
    //
    //     // With alignment at 2, buckets should be [2, 12), [12, 22), [22, 32), [32, 42)
    //     assert_eq!(result.len(), 4);
    //     assert_eq!(result[0].timestamp, 2);  // First bucket starts at alignment point
    //     assert_eq!(result[0].value, 0.0);    // No values in this bucket
    //     assert_eq!(result[1].timestamp, 12); // Second bucket
    //     assert_eq!(result[1].value, 3.0);    // Sum of 1.0 and 2.0
    //     assert_eq!(result[2].timestamp, 22);
    //     assert_eq!(result[2].value, 3.0);
    //     assert_eq!(result[3].timestamp, 32);
    //     assert_eq!(result[3].value, 4.0);
    // }
}
