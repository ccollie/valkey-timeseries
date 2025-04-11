use crate::aggregators::{AggOp, Aggregator, BucketAlignment, BucketTimestamp};
use crate::common::{Sample, Timestamp};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub time_delta: i64,
    pub report_empty: bool,
}

#[derive(Debug)]
pub struct AggregationHelper {
    aggregator: Aggregator,
    time_delta: i64,
    bucket_ts: BucketTimestamp,
    bucket_range_start: Timestamp,
    bucket_range_end: Timestamp,
    aligned_timestamp: Timestamp,
    report_empty: bool,
}

impl AggregationHelper {
    pub(crate) fn new(
        options: &AggregationOptions,
        aligned_timestamp: Timestamp
    ) -> Self {
        AggregationHelper {
            aligned_timestamp,
            report_empty: options.report_empty,
            aggregator: options.aggregator.clone(),
            time_delta: options.time_delta,
            bucket_ts: options.timestamp_output,
            bucket_range_start: 0,
            bucket_range_end: 0,
        }
    }

    fn add_empty_buckets(
        &self,
        samples: &mut Vec<Sample>,
        first_bucket_ts: Timestamp,
        end_bucket_ts: Timestamp,
    ) {
        let value = self.aggregator.empty_value();
        let start = self.calc_bucket_start(first_bucket_ts);
        let end = self.calc_bucket_start(end_bucket_ts);
        let count = self.buckets_in_range(start, end);
        samples.reserve(count);

        for timestamp in (start ..end).step_by(self.time_delta as usize) {
            samples.push(Sample {
                timestamp,
                value,
            });
        }
    }

    fn buckets_in_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> usize {
        (((end_ts - start_ts) / self.time_delta) + 1) as usize
    }

    fn calculate_bucket_start(&self) -> Timestamp {
        self.bucket_ts
            .calculate(self.bucket_range_start, self.time_delta)
    }

    fn advance_current_bucket(&mut self) {
        self.bucket_range_start = self.bucket_range_end;
        self.bucket_range_end += self.time_delta;
    }

    fn finalize_internal(&mut self) -> Sample {
        let value = self.aggregator.finalize();
        let timestamp = self.calculate_bucket_start();
        self.aggregator.reset();
        Sample { timestamp, value }
    }

    fn finalize_bucket(&mut self, last_ts: Option<Timestamp>, empty_buckets: &mut Vec<Sample>) -> Sample {
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
        bucket
    }

    fn update(&mut self, value: f64) {
        self.aggregator.update(value);
    }

    pub fn calculate(&mut self, iterator: impl Iterator<Item = Sample>) -> Vec<Sample> {
        let mut buckets = Vec::new();
        let mut iterator = iterator;

        if let Some(first) = iterator.by_ref().next() {
            self.update_bucket_timestamps(first.timestamp);
            self.update(first.value);
        } else {
            return buckets;
        }

        for sample in iterator {
            if sample.timestamp >= self.bucket_range_end {
                let bucket = self.finalize_bucket(Some(sample.timestamp), &mut buckets);
                buckets.push(bucket);
            }
            self.update(sample.value);
        }

        // todo: handle last bucket
        let last = self.finalize_internal();
        buckets.push(last);

        buckets
    }

    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_range_end
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.bucket_range_start = self.calc_bucket_start(timestamp).max(0) as Timestamp;
        self.bucket_range_end = self.bucket_range_start + self.time_delta;
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.aligned_timestamp;
        let delta = self.time_delta;
        ts - ((diff % delta + delta) % delta)
    }
}

pub fn aggregate(
    options: &AggregationOptions,
    aligned_timestamp: Timestamp,
    iter: impl Iterator<Item = Sample>
) -> Vec<Sample> {
    let mut aggr_iter = AggregationHelper::new(options, aligned_timestamp);
    aggr_iter.calculate(iter)
}

pub struct AggregateIterator<T: Iterator<Item = Sample>> {
    inner: T,
    aggregator: AggregationHelper,
    empty_buckets: Vec<Sample>,
    init: bool,
    count: usize
}

impl<T: Iterator<Item = Sample>> AggregateIterator<T> {
    pub fn new(inner: T, options: AggregationOptions, aligned_timestamp: Timestamp) -> Self {
        let aggregator = AggregationHelper::new(&options, aligned_timestamp);
        Self { inner, aggregator, empty_buckets: vec![], init: false, count: 0}
    }

    fn update(&mut self, value: f64) {
        self.aggregator.update(value);
        self.count += 1;
    }

    fn finalize_bucket(&mut self, last_ts: Option<Timestamp>) -> Sample {
        let bucket = self.aggregator.finalize_bucket(last_ts, &mut self.empty_buckets);
        if !self.empty_buckets.is_empty() {
            self.empty_buckets.reverse();
        }
        self.count = 0;
        bucket
    }
}

impl<T: Iterator<Item = Sample>> Iterator for AggregateIterator<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        // First return any empty buckets if available
        if let Some(sample) = self.empty_buckets.pop() {
            return Some(sample);
        }

        // Handle initialization with the first sample
        if !self.init {
            if let Some(sample) = self.inner.next() {
                self.init = true;
                self.aggregator.update_bucket_timestamps(sample.timestamp);
                self.update(sample.value);
            } else {
                return None; // Empty input stream
            }
        }

        // Process subsequent samples
        while let Some(sample) = self.inner.next() {
            if self.aggregator.should_finalize_bucket(sample.timestamp) {
                let bucket = self.finalize_bucket(Some(sample.timestamp));
                self.update(sample.value);
                return Some(bucket)
            }
            self.update(sample.value);
        }

        // Handle the final bucket if we haven't processed it yet
        if self.count > 0 {
            return Some(self.finalize_bucket(None));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregator, BucketAlignment, BucketTimestamp};
    use crate::common::Sample;
    use std::time::Duration;

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

    fn create_options(aggregator: Aggregator) -> AggregationOptions {
        AggregationOptions {
            aggregator,
            bucket_duration: Duration::from_secs(10),
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            time_delta: 10,
            report_empty: false,
        }
    }

    #[test]
    fn test_sum_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregator::Sum(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

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
        let options = create_options(Aggregator::Avg(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.5); // (1.0 + 2.0) / 2
    }

    #[test]
    fn test_max_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregator::Max(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 2.0); // max of 1.0 and 2.0
    }

    #[test]
    fn test_min_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregator::Min(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 10);
        assert_eq!(result[0].value, 1.0); // min of 1.0 and 2.0
    }

    #[test]
    fn test_count_aggregation() {
        let samples = create_test_samples();
        let options = create_options(Aggregator::Count(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

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

        let mut options = create_options(Aggregator::Sum(Default::default()));
        options.report_empty = true;

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

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
    fn test_bucket_timestamp_end() {
        let samples = create_test_samples();
        let mut options = create_options(Aggregator::Sum(Default::default()));
        options.timestamp_output = BucketTimestamp::End;

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 20); // End of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_bucket_timestamp_mid() {
        let samples = create_test_samples();
        let mut options = create_options(Aggregator::Sum(Default::default()));
        options.timestamp_output = BucketTimestamp::Mid;

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 6);
        assert_eq!(result[0].timestamp, 15); // Mid of the first bucket
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn test_empty_input() {
        let samples: Vec<Sample> = vec![];
        let options = create_options(Aggregator::Sum(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            0,
        );

        let result: Vec<Sample> = iterator.collect();

        assert_eq!(result.len(), 1); // Last bucket with default value
        assert!(result[0].value.is_nan() || result[0].value == 0.0);
    }

    #[test]
    fn test_alignment_with_offset() {
        let samples = vec![
            Sample::new(12, 1.0),
            Sample::new(17, 2.0),
            Sample::new(22, 3.0),
            Sample::new(32, 4.0),
        ];

        let options = create_options(Aggregator::Sum(Default::default()));

        let iterator = AggregateIterator::new(
            samples.into_iter(),
            options,
            2, // Aligned timestamp is 2
        );

        let result: Vec<Sample> = iterator.collect();

        // With alignment at 2, buckets should be [2, 12), [12, 22), [22, 32), [32, 42)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].timestamp, 2);  // First bucket starts at alignment point
        assert_eq!(result[0].value, 0.0);    // No values in this bucket
        assert_eq!(result[1].timestamp, 12); // Second bucket
        assert_eq!(result[1].value, 3.0);    // Sum of 1.0 and 2.0
        assert_eq!(result[2].timestamp, 22);
        assert_eq!(result[2].value, 3.0);
        assert_eq!(result[3].timestamp, 32);
        assert_eq!(result[3].value, 4.0);
    }
}