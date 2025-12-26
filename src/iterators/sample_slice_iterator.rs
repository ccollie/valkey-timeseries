use crate::common::Sample;

pub struct SampleSliceIter<'a> {
    inner: std::slice::Iter<'a, Sample>,
}

impl<'a> SampleSliceIter<'a> {
    pub fn new(samples: &'a [Sample]) -> Self {
        Self {
            inner: samples.iter(),
        }
    }
}

impl Iterator for SampleSliceIter<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().copied()
    }
}

/// An iterator that iterates over a slice of samples in reverse order.
/// This exists solely as an internal adapter to help in iterating a timeseries in reverse
pub struct ReverseSampleSliceIterator<'a> {
    samples: &'a [Sample],
    index: usize,
}

impl<'a> ReverseSampleSliceIterator<'a> {
    pub fn new(samples: &'a [Sample]) -> Self {
        Self {
            samples,
            index: samples.len(),
        }
    }
}

impl<'a> Iterator for ReverseSampleSliceIterator<'a> {
    type Item = &'a Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index > 0 {
            self.index -= 1;
            // SAFETY: index starts at samples.len() and decrements, so it's always < samples.len()
            Some(unsafe { self.samples.get_unchecked(self.index) })
        } else {
            None
        }
    }
}
