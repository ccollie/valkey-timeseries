use crate::aggregators::{AggOp, Aggregator};
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyString};
use crate::parser::timestamp::parse_timestamp;

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid,
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: i64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts + time_delta / 2,
            Self::End => ts + time_delta,
        }
    }
}
impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestamp::Start),
                '+' => return Ok(BucketTimestamp::End),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestamp::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestamp::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestamp::Mid),
            _ => {}
        }
        Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

impl BucketAlignment {
    pub fn get_aligned_timestamp(&self, start: Timestamp, end: Timestamp) -> Timestamp {
        match self {
            BucketAlignment::Default => 0,
            BucketAlignment::Start => start,
            BucketAlignment::End => end,
            BucketAlignment::Timestamp(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for BucketAlignment {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let alignment = match value {
            arg if arg.eq_ignore_ascii_case("start") => BucketAlignment::Start,
            arg if arg.eq_ignore_ascii_case("end") => BucketAlignment::End,
            arg if arg.len() == 1 => {
                let c = arg.chars().next().unwrap();
                match c {
                    '-' => BucketAlignment::Start,
                    '+' => BucketAlignment::End,
                    _ => return Err(ValkeyError::Str(error_consts::INVALID_ALIGN)),
                }
            }
            _ => {
                let timestamp = parse_timestamp(value)
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGN))?;
                BucketAlignment::Timestamp(timestamp)
            }
        };
        Ok(alignment)
    }
}

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregator: Aggregator,
    pub bucket_duration: Duration,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub time_delta: i64,
    pub empty: bool,
}

#[derive(Debug)]
pub struct AggrIterator {
    aggregator: Aggregator,
    time_delta: i64,
    bucket_ts: BucketTimestamp,
    last_timestamp: Timestamp,
    aligned_timestamp: Timestamp,
    count: usize,
    report_empty: bool,
    bucket_right_ts: Timestamp,
}

impl AggrIterator {
    pub(crate) const DEFAULT_COUNT: usize = usize::MAX - 1;

    pub(crate) fn new(
        options: &AggregationOptions,
        aligned_timestamp: Timestamp,
        count: Option<usize>,
    ) -> Self {
        AggrIterator {
            aligned_timestamp,
            report_empty: options.empty,
            aggregator: options.aggregator.clone(),
            time_delta: options.time_delta,
            bucket_ts: options.timestamp_output,
            count: count.unwrap_or(AggrIterator::DEFAULT_COUNT),
            last_timestamp: 0,
            bucket_right_ts: 0,
        }
    }

    fn add_empty_buckets(
        &self,
        samples: &mut Vec<Sample>,
        first_bucket_ts: Timestamp,
        end_bucket_ts: Timestamp,
    ) {
        let empty_bucket_count = self.calculate_empty_bucket_count(first_bucket_ts, end_bucket_ts);
        let value = self.aggregator.empty_value();
        for _ in 0..empty_bucket_count {
            samples.push(Sample {
                timestamp: end_bucket_ts,
                value,
            });
        }
    }

    fn buckets_in_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> usize {
        (((end_ts - start_ts) / self.time_delta) + 1) as usize
    }

    fn calculate_empty_bucket_count(
        &self,
        first_bucket_ts: Timestamp,
        end_bucket_ts: Timestamp,
    ) -> usize {
        let total_empty_buckets = self.buckets_in_range(first_bucket_ts, end_bucket_ts);
        let remaining_capacity = self.count - total_empty_buckets;
        total_empty_buckets.min(remaining_capacity)
    }

    fn calculate_bucket_start(&self) -> Timestamp {
        self.bucket_ts
            .calculate(self.last_timestamp, self.time_delta)
    }

    fn finalize_current_bucket(&mut self) -> Sample {
        let value = self.aggregator.finalize();
        let timestamp = self.calculate_bucket_start();
        self.aggregator.reset();
        Sample { timestamp, value }
    }

    pub fn calculate(&mut self, iterator: impl Iterator<Item = Sample>) -> Vec<Sample> {
        let mut buckets = Vec::new();
        let mut iterator = iterator;

        if let Some(first) = iterator.by_ref().next() {
            self.update_bucket_timestamps(first.timestamp);
            self.aggregator.update(first.value);
        } else {
            return buckets;
        }

        for sample in iterator {
            if self.should_finalize_bucket(sample.timestamp) {
                buckets.push(self.finalize_current_bucket());
                if buckets.len() >= self.count {
                    return buckets;
                }
                self.update_bucket_timestamps(sample.timestamp);
                if self.report_empty {
                    self.finalize_empty_buckets(&mut buckets);
                }
            }
            self.aggregator.update(sample.value);
        }

        // todo: handle last bucket
        buckets.truncate(self.count);
        buckets
    }

    fn should_finalize_bucket(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.bucket_right_ts
    }

    fn update_bucket_timestamps(&mut self, timestamp: Timestamp) {
        self.last_timestamp = self.calc_bucket_start(timestamp).max(0) as Timestamp;
        self.bucket_right_ts = self.last_timestamp + self.time_delta;
    }

    fn finalize_empty_buckets(&self, buckets: &mut Vec<Sample>) {
        let first_bucket = self.bucket_right_ts;
        let last_bucket = self.last_timestamp - self.time_delta;
        if first_bucket < self.last_timestamp {
            self.add_empty_buckets(buckets, first_bucket, last_bucket);
        }
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let timestamp_diff = ts - self.aligned_timestamp;
        let delta = self.time_delta;
        ts - ((timestamp_diff % delta + delta) % delta)
    }
}

pub fn aggregate(
    options: &AggregationOptions,
    aligned_timestamp: Timestamp,
    iter: impl Iterator<Item = Sample>,
    count: Option<usize>,
) -> Vec<Sample> {
    let mut aggr_iter = AggrIterator::new(options, aligned_timestamp, count);
    aggr_iter.calculate(iter)
}
