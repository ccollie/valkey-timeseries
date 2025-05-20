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
    last_value: f64,
    all_nans: bool,
    count: usize,
    report_empty: bool,
}

impl AggregationHelper {
    pub(crate) fn new(options: &AggregationOptions, align_timestamp: Timestamp) -> Self {
        AggregationHelper {
            align_timestamp,
            report_empty: options.report_empty,
            aggregator: options.aggregation.into(),
            bucket_duration: options.bucket_duration,
            bucket_ts: options.timestamp_output,
            bucket_range_start: 0,
            bucket_range_end: 0,
            last_value: f64::NAN,
            all_nans: true,
            count: 0,
        }
    }

    fn add_empty_buckets(
        &self,
        samples: &mut VecDeque<Sample>,
        first_bucket_ts: Timestamp,
        end_bucket_ts: Timestamp,
    ) {
        let value = match self.aggregator {
            Aggregator::Last(_) => self.last_value,
            _ => self.aggregator.empty_value(),
        };
        let start = self.calc_bucket_start(first_bucket_ts);
        let end = self.calc_bucket_start(end_bucket_ts);
        let count = self.buckets_in_range(start, end);
        samples.reserve(count);

        for timestamp in (start..end).step_by(self.bucket_duration as usize) {
            samples.push_back(Sample { timestamp, value });
        }
    }

    fn buckets_in_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> usize {
        (((end_ts - start_ts) / self.bucket_duration as i64) + 1) as usize
    }

    fn calculate_bucket_start(&self) -> Timestamp {
        self.bucket_ts
            .calculate(self.bucket_range_start, self.bucket_duration)
    }

    fn advance_current_bucket(&mut self) {
        self.bucket_range_start = self.bucket_range_end;
        self.bucket_range_end = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration);
    }

    fn finalize_internal(&mut self) -> Sample {
        let value = if self.all_nans {
            if self.count == 0 {
                self.aggregator.empty_value()
            } else {
                f64::NAN
            }
        } else {
            self.aggregator.finalize()
        };
        let timestamp = self.calculate_bucket_start();
        self.aggregator.reset();
        self.all_nans = true;
        Sample { timestamp, value }
    }

    fn finalize_bucket(
        &mut self,
        last_ts: Option<Timestamp>,
        empty_buckets: &mut VecDeque<Sample>,
    ) -> Sample {
        let bucket = self.finalize_internal();
        if self.report_empty {
            if let Some(last_ts) = last_ts {
                if last_ts >= self.bucket_range_end {
                    let start = self.bucket_range_end + 1;
                    self.add_empty_buckets(empty_buckets, start, last_ts);
                    self.update_bucket_timestamps(last_ts);
                }
            }
        } else {
            self.advance_current_bucket();
        }
        self.count = 0;
        bucket
    }

    fn update(&mut self, value: f64) {
        if !value.is_nan() {
            self.aggregator.update(value);
            self.last_value = value;
            self.all_nans = false;
        }
        self.count += 1;
    }

    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_range_end
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.bucket_range_start = self.calc_bucket_start(timestamp).max(0) as Timestamp;
        self.bucket_range_end = self
            .bucket_range_start
            .saturating_add_unsigned(self.bucket_duration);
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        ts - ((diff % delta + delta) % delta)
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
    init: bool,
}

impl<T: Iterator<Item = Sample>> AggregateIterator<T> {
    pub fn new(inner: T, options: &AggregationOptions, aligned_timestamp: Timestamp) -> Self {
        let aggregator = AggregationHelper::new(options, aligned_timestamp);
        Self {
            inner,
            aggregator,
            empty_buckets: VecDeque::new(),
            init: false,
        }
    }

    fn finalize_bucket(&mut self, last_ts: Option<Timestamp>) -> Sample {
        self.aggregator
            .finalize_bucket(last_ts, &mut self.empty_buckets)
    }
}

impl<T: Iterator<Item = Sample>> Iterator for AggregateIterator<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        // First, return any empty buckets if available
        if let Some(sample) = self.empty_buckets.pop_front() {
            return Some(sample);
        }

        // Handle initialization with the first sample
        if !self.init {
            if let Some(sample) = self.inner.next() {
                self.init = true;
                self.aggregator.update_bucket_timestamps(sample.timestamp);
                self.aggregator.update(sample.value);
            } else {
                return None; // Empty input stream
            }
        }

        // Process subsequent samples
        while let Some(sample) = self.inner.next() {
            if self.aggregator.should_finalize_bucket(sample.timestamp) {
                let bucket = self.finalize_bucket(Some(sample.timestamp));
                self.aggregator.update(sample.value);
                return Some(bucket);
            }
            self.aggregator.update(sample.value);
        }

        // Handle the final bucket if we haven't processed it yet
        if self.aggregator.count > 0 {
            return Some(self.finalize_bucket(None));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
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

    fn create_options(aggregator: Aggregation) -> AggregationOptions {
        AggregationOptions {
            aggregation: aggregator,
            bucket_duration: 10,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        }
    }

    #[test]
    fn test_sum_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Sum);

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
    fn test_avg_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Avg);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.5); // (1.0 + 2.0) / 2
    }

    #[test]
    fn test_max_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Max);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // max of 1.0 and 2.0
    }

    #[test]
    fn test_min_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Min);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.0); // min of 1.0 and 2.0
    }

    #[test]
    fn test_count_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregation::Count);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // count of values in the bucket
        assert_eq!(result[1].timestamp, 20);
        assert_eq!(result[1].value, 1.0);
    }

    #[test]
    fn test_empty_buckets() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 2.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(Aggregation::Sum);
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

    #[test]
    fn test_empty_buckets_last() {
        let samples = vec![
            Sample::new(10, 1.0),
            Sample::new(15, 99.0),
            // Gap at 20-30
            Sample::new(40, 5.0),
            Sample::new(50, 6.0),
        ];

        let mut options = create_options(Aggregation::Last);
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
        let mut options = create_options(Aggregation::Sum);
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
        let mut options = create_options(Aggregation::Sum);
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
        let options = create_options(Aggregation::Sum);

        let iterator = AggregateIterator::new(samples.into_iter(), &options, 0);

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 0); // Last bucket with default value
                                     // assert!(result[0].value.is_nan() || result[0].value == 0.0);
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
