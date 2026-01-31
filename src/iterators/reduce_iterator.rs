use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::Sample;

/// Perform the GROUP BY REDUCE operation on the samples. Specifically, it
/// aggregates non-NAN samples based on the specified aggregation options.
pub fn group_reduce(samples: impl Iterator<Item = Sample>, aggregation: Aggregator) -> Vec<Sample> {
    ReduceIterator::new(samples, aggregation).collect()
}

pub struct ReduceIterator<I: Iterator<Item = Sample>> {
    iter: I,
    is_init: bool,
    aggregator: Aggregator,
    current_sample: Option<Sample>,
}

impl<I: Iterator<Item = Sample>> ReduceIterator<I> {
    pub fn new(iter: I, aggregator: Aggregator) -> Self {
        Self {
            iter,
            aggregator,
            current_sample: None,
            is_init: false,
        }
    }
}

impl<I: Iterator<Item = Sample>> Iterator for ReduceIterator<I> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        let current = if self.is_init {
            self.current_sample.take()?
        } else {
            // lazy initialization
            self.is_init = true;
            self.iter.next()?
        };

        let mut all_nans = current.value.is_nan();

        if !all_nans {
            self.aggregator.update(current.timestamp, current.value);
        }

        for next in self.iter.by_ref() {
            if next.timestamp == current.timestamp {
                let is_nan = next.value.is_nan();
                all_nans = all_nans && is_nan;

                // Only aggregate non-NaN samples; still track "all_nans" for the group.
                if !is_nan {
                    self.aggregator.update(next.timestamp, next.value);
                }
            } else {
                // Finalize the current group
                let value = if all_nans {
                    f64::NAN
                } else {
                    self.aggregator.finalize()
                };
                self.aggregator.reset();
                let result = Sample {
                    timestamp: current.timestamp,
                    value,
                };

                // Prepare for the next group
                self.current_sample = Some(next);
                return Some(result);
            }
        }

        // Finalize the last group when the inner iterator is exhausted
        let value = if all_nans {
            self.aggregator.reset();
            f64::NAN
        } else {
            self.aggregator.finalize()
        };

        Some(Sample {
            timestamp: current.timestamp,
            value,
        })
    }
}
