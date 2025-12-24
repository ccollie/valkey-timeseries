use crate::common::Sample;
use min_max_heap::MinMaxHeap;
use smallvec::SmallVec;

/// Iterate over multiple Sample iterators, returning the samples in timestamp order
pub struct MultiSeriesSampleIter<T: Iterator<Item = Sample>> {
    heap: MinMaxHeap<Sample>,
    inner: Vec<T>,
}

impl<T: Iterator<Item = Sample>> MultiSeriesSampleIter<T> {
    pub fn new(list: Vec<T>) -> Self {
        let len = list.len();
        Self {
            inner: list,
            heap: MinMaxHeap::with_capacity(len),
        }
    }

    fn push_samples_to_heap(&mut self) -> bool {
        if !self.inner.is_empty() {
            let mut sample_added = false;
            for iter in self.inner.iter_mut() {
                if let Some(sample) = iter.next() {
                    sample_added = true;
                    self.heap.push(sample);
                }
            }

            return sample_added;
        }
        false
    }

    /// Load samples to the heap, trying to ensure that the iterators are roughly at the same timestamp
    fn load_heap(&mut self) -> bool {
        if !self.push_samples_to_heap() {
            return false;
        }

        let max_timestamp = match self.heap.peek_max() {
            Some(max) => max.timestamp,
            None => return false,
        };

        let mut to_remove: SmallVec<usize, 8> = SmallVec::new();
        for (i, sample_iter) in self.inner.iter_mut().enumerate() {
            let mut sample_added = false;

            for sample in sample_iter.by_ref() {
                sample_added = true;
                let stop = sample.timestamp > max_timestamp; // was >= but we want to include samples with the same timestamp
                self.heap.push(sample);
                if stop {
                    break;
                }
            }

            if !sample_added {
                to_remove.push(i);
            }
        }

        if !to_remove.is_empty() {
            for i in to_remove.iter().rev() {
                let _ = self.inner.swap_remove(*i);
            }
        }

        true
    }
}

impl<T: Iterator<Item = Sample>> Iterator for MultiSeriesSampleIter<T> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() && !self.load_heap() {
            return None;
        }
        self.heap.pop_min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_supports_duplicates() {
        let mut heap = MinMaxHeap::<u32>::new();

        heap.push(1);
        heap.push(1);
        assert_eq!(heap.len(), 2);
        heap.push(1);
        assert_eq!(heap.len(), 3);
        heap.push(2);
        heap.push(3);
        heap.push(3);
        assert_eq!(heap.len(), 6);
        let mut vec = heap.into_vec();
        vec.sort();
        assert_eq!(vec, vec![1, 1, 1, 2, 3, 3]);
    }
}
