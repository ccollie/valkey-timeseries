use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::Sample;

/// Iterator that groups samples by timestamp
pub struct SampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    inner: I,
    buffer: Option<Sample>,
    aggregator: Aggregator,
    done: bool,
}

impl<I> SampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    pub fn new(iter: I, aggregator: Aggregator) -> Self {
        Self {
            inner: iter,
            buffer: None,
            done: false,
            aggregator,
        }
    }

    fn finalize_group(&mut self, timestamp: i64, all_nans: bool) -> Sample {
        let value = if all_nans {
            // Reset aggregator state when the group is all NaNs
            AggregationHandler::reset(&mut self.aggregator);
            f64::NAN
        } else {
            // Finalize aggregator to get the aggregated value
            AggregationHandler::finalize(&mut self.aggregator)
        };
        Sample { timestamp, value }
    }
}

impl<I> Iterator for SampleReducer<I>
where
    I: Iterator<Item = Sample>,
{
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // Get the first sample if the buffer is empty
        if self.buffer.is_none() {
            self.buffer = self.inner.next();
            if self.buffer.is_none() {
                self.done = true;
                return None;
            }
        }

        let (current_timestamp, current_value) = {
            let sample = self.buffer.as_ref().unwrap();
            (sample.timestamp, sample.value)
        };

        // Start a new group: reset the aggregator and include the first sample if it's not NaN.
        AggregationHandler::reset(&mut self.aggregator);
        let mut all_nans = current_value.is_nan();
        if !all_nans {
            AggregationHandler::update(&mut self.aggregator, current_timestamp, current_value);
        }

        // Continue consuming samples with the same timestamp
        loop {
            // Get the next sample
            let next_sample = self.inner.next();

            match next_sample {
                Some(sample) if sample.timestamp == current_timestamp => {
                    let is_nan = sample.value.is_nan();
                    all_nans = all_nans && is_nan;

                    // Only aggregate non-NaN samples; still track "all_nans" for the group.
                    if !is_nan {
                        AggregationHandler::update(
                            &mut self.aggregator,
                            sample.timestamp,
                            sample.value,
                        );
                    }
                    // Update the buffer with this sample
                    self.buffer = Some(sample);
                }
                Some(sample) => {
                    // Different timestamp: buffer the next sample for the next iteration
                    self.buffer = Some(sample);

                    // Emit current group
                    let result = self.finalize_group(current_timestamp, all_nans);
                    return Some(result);
                }
                None => {
                    // No more samples, emit the current group and finish
                    self.buffer = None;
                    self.done = true;
                    let result = self.finalize_group(current_timestamp, all_nans);
                    return Some(result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{AggregationType, Aggregator};

    fn reduce_using_sum(samples: Vec<Sample>) -> Vec<Sample> {
        let aggregator: Aggregator = AggregationType::Sum.into();
        SampleReducer::new(samples.into_iter(), aggregator).collect()
    }

    #[test]
    fn groups_samples_by_timestamp_using_sum() {
        let samples = vec![
            Sample {
                timestamp: 1,
                value: 10.0,
            },
            Sample {
                timestamp: 1,
                value: 15.0,
            },
            Sample {
                timestamp: 2,
                value: 5.0,
            },
            Sample {
                timestamp: 2,
                value: 20.0,
            },
            Sample {
                timestamp: 3,
                value: 7.0,
            },
        ];

        let result = reduce_using_sum(samples);

        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 1,
                    value: 25.0
                },
                Sample {
                    timestamp: 2,
                    value: 25.0
                },
                Sample {
                    timestamp: 3,
                    value: 7.0
                },
            ]
        );
    }

    #[test]
    fn returns_nan_when_entire_group_is_nan() {
        let samples = vec![
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 1,
                value: f64::NAN,
            },
            Sample {
                timestamp: 2,
                value: 5.0,
            },
        ];

        let result = reduce_using_sum(samples);

        assert_eq!(result.len(), 2);
        assert!(result[0].value.is_nan());
        assert_eq!(
            result[1],
            Sample {
                timestamp: 2,
                value: 5.0
            }
        );
    }

    #[test]
    fn includes_first_sample_in_aggregation() {
        // Regression test: ensure the reducer aggregates the first sample of a group.
        // If the first sample is not fed into the aggregator, sums will be wrong.
        let samples = vec![
            Sample {
                timestamp: 1,
                value: 100.0,
            }, // only sample at ts=1
            Sample {
                timestamp: 2,
                value: 95.0,
            }, // first sample for ts=2
            Sample {
                timestamp: 2,
                value: 55.0,
            }, // second sample for ts=2
        ];

        let result = reduce_using_sum(samples);

        // ts=1 -> 100
        // ts=2 -> 95 + 55 = 150
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 1,
                    value: 100.0
                },
                Sample {
                    timestamp: 2,
                    value: 150.0
                },
            ]
        );
    }
}
