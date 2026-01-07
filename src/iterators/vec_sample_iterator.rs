use crate::common::Sample;
use std::vec::IntoIter;

pub struct VecSampleIterator {
    inner: IntoIter<Sample>,
}

impl VecSampleIterator {
    pub fn new(samples: Vec<Sample>) -> Self {
        let inner = samples.into_iter(); // slice iterator
        Self {
            inner, // slice iterator
        }
    }
}

impl Iterator for VecSampleIterator {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
