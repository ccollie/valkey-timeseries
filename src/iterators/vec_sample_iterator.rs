use crate::common::Sample;
use std::vec::IntoIter;

pub struct VecSampleIterator {
    len: usize,
    inner: IntoIter<Sample>,
}

impl VecSampleIterator {
    pub fn new(samples: Vec<Sample>) -> Self {
        let len = samples.len();
        let inner = samples.into_iter(); // slice iterator
        Self {
            len,
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
        let len = self.len;
        (len, Some(len))
    }
}
