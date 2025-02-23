use crate::common::Sample;
use crate::iterators::SampleIter;
use crate::series::DuplicatePolicy;
use std::iter::Peekable;

pub struct SampleMergeIterator<'a> {
    left: Peekable<SampleIter<'a>>,
    right: Peekable<SampleIter<'a>>,
    duplicate_policy: DuplicatePolicy,
}

impl<'a> SampleMergeIterator<'a> {
    pub(crate) fn new(
        left: SampleIter<'a>,
        right: SampleIter<'a>,
        duplicate_policy: DuplicatePolicy,
    ) -> Self {
        SampleMergeIterator {
            left: left.peekable(),
            right: right.peekable(),
            duplicate_policy,
        }
    }

    pub(crate) fn next_internal(&mut self) -> Option<(Sample, bool)> {
        let mut blocked = false;

        let sample = match (self.left.peek(), self.right.peek()) {
            (Some(&left), Some(&right)) => {
                if left.timestamp == right.timestamp {
                    let ts = left.timestamp;
                    if let Ok(val) =
                        self.duplicate_policy
                            .duplicate_value(ts, left.value, right.value)
                    {
                        self.left.next();
                        self.right.next();
                        Some(Sample {
                            timestamp: ts,
                            value: val,
                        })
                    } else {
                        // block duplicate
                        blocked = true;
                        self.right.next();
                        self.left.next()
                    }
                } else if left < right {
                    self.left.next()
                } else {
                    self.right.next()
                }
            }
            (Some(_), None) => self.left.next(),
            (None, Some(_)) => self.right.next(),
            (None, None) => None,
        };

        sample.map(|sample| (sample, blocked))
    }
}

impl Iterator for SampleMergeIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            return if let Some((sample, blocked)) = self.next_internal() {
                if blocked {
                    continue;
                }
                Some(sample)
            } else {
                None
            };
        }
    }
}
