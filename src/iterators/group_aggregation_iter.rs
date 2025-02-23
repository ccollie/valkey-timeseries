use crate::aggregators::{AggOp, Aggregator};
use crate::common::{Sample, Timestamp};
use crate::iterators::sample_iter::SampleIter;
use crate::iterators::MultiSeriesSampleIter;

pub struct GroupAggregationIter<'a> {
    last_sample: Sample,
    bucket_count: usize,
    aggregator: Aggregator,
    inner_iter: MultiSeriesSampleIter<'a>,
}

impl<'a> GroupAggregationIter<'a> {
    pub fn new(series: Vec<SampleIter<'a>>, aggregator: Aggregator) -> Self {
        Self {
            aggregator,
            bucket_count: 0,
            last_sample: Sample {
                timestamp: i64::MIN,
                value: 0.0,
            },
            inner_iter: MultiSeriesSampleIter::new(series),
        }
    }

    fn finalize_bucket(&mut self, timestamp: Timestamp) -> Sample {
        let value = self.aggregator.finalize();
        self.aggregator.reset();
        self.bucket_count = 0;
        Sample { timestamp, value }
    }
}

impl Iterator for GroupAggregationIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.last_sample.timestamp == i64::MIN {
            // first run
            if let Some(sample) = self.inner_iter.next() {
                self.bucket_count = 1;
                self.aggregator.update(sample.value);
                self.last_sample = sample;
            } else {
                return None;
            }
        }

        let last_ts = self.last_sample.timestamp;
        while let Some(sample) = self.inner_iter.by_ref().next() {
            self.last_sample = sample;
            if sample.timestamp == last_ts {
                self.aggregator.update(sample.value);
            } else {
                break;
            }
        }

        // i don't think this condition is necessary
        if self.bucket_count > 0 {
            let value = self.finalize_bucket(self.last_sample.timestamp);
            return Some(value);
        }

        None
    }
}
