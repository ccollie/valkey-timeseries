use crate::common::{Sample, Timestamp};
use crate::iterators::SampleIter;
use crate::series::ValueFilter;
use std::cmp::Ordering;

pub struct FilteredSampleIterator<'a> {
    inner: SampleIter<'a>,
    value_filter: Option<ValueFilter>,
    ts_filter: Option<&'a Vec<Timestamp>>,
    ts_index: usize,
}

impl<'a> FilteredSampleIterator<'a> {
    pub fn new(
        inner: SampleIter<'a>,
        value_filter: Option<ValueFilter>,
        ts_filter: Option<&'a Vec<Timestamp>>,
    ) -> Self {
        Self {
            inner,
            value_filter,
            ts_filter,
            ts_index: 0,
        }
    }

    fn get_next_sample_by_ts(
        &mut self,
        timestamps: &[Timestamp],
        sample: Sample,
    ) -> Option<Sample> {
        if self.ts_index >= timestamps.len() {
            return None;
        }

        let mut sample = sample;

        loop {
            let mut first_ts = timestamps[self.ts_index];
            match sample.timestamp.cmp(&first_ts) {
                Ordering::Less => {
                    sample = self.inner.next()?;
                }
                Ordering::Equal => {
                    self.ts_index += 1;
                    return Some(sample);
                }
                Ordering::Greater => {
                    while first_ts < sample.timestamp && self.ts_index < timestamps.len() {
                        self.ts_index += 1;
                        first_ts = timestamps[self.ts_index];
                        if first_ts == sample.timestamp {
                            self.ts_index += 1;
                            return Some(sample);
                        }
                    }
                }
            }
        }
    }
}

impl Iterator for FilteredSampleIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        // todo: minimize branching
        while let Some(sample) = self.inner.next() {
            match (self.value_filter.as_ref(), self.ts_filter) {
                (None, None) => return Some(sample),
                (None, Some(ts_filter)) => {
                    let sample = self.get_next_sample_by_ts(ts_filter, sample)?;
                    return Some(sample);
                }
                (Some(value_filter), None) => {
                    if value_filter.is_match(sample.value) {
                        return Some(sample);
                    }
                }
                (Some(value_filter), Some(ts_filter)) => {
                    let vf = *value_filter; // ValueFilter is Copy, so this should be cheap
                    let sample = self.get_next_sample_by_ts(ts_filter, sample)?;
                    if vf.is_match(sample.value) {
                        return Some(sample);
                    }
                }
            }
        }
        None
    }
}
