use crate::common::Sample;
use min_max_heap::MinMaxHeap;

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

    /// Push one sample from each iterator to the heap.
    /// Returns `true` if at least one sample was added.
    fn push_samples_to_heap(&mut self) -> bool {
        let initial_len = self.heap.len();
        for iter in &mut self.inner {
            if let Some(sample) = iter.next() {
                self.heap.push(sample);
            }
        }
        self.heap.len() > initial_len
    }

    /// Load samples to the heap, trying to ensure that the iterators are roughly at the same timestamp.
    /// Removes exhausted iterators from `self.inner`.
    fn load_heap(&mut self) -> bool {
        if !self.push_samples_to_heap() {
            return false;
        }

        let max_timestamp = match self.heap.peek_max() {
            Some(max) => max.timestamp,
            None => return false,
        };

        // Use `retain` to remove exhausted iterators in a single pass
        self.inner.retain_mut(|sample_iter| {
            let mut produced_sample = false;

            for sample in sample_iter.by_ref() {
                produced_sample = true;
                let exceeds_max = sample.timestamp > max_timestamp;
                self.heap.push(sample);
                if exceeds_max {
                    break;
                }
            }

            produced_sample // Keep iterator only if it produced at least one sample
        });

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

    // Helper constructor matching the project's Sample shape (timestamp, value).
    fn make_sample(ts: i64, val: f64) -> Sample {
        Sample {
            timestamp: ts,
            value: val,
        }
    }

    #[test]
    fn merge_two_series_in_order() {
        let a = vec![
            make_sample(1, 1.0),
            make_sample(4, 4.0),
            make_sample(5, 5.0),
        ];
        let b = vec![
            make_sample(2, 2.0),
            make_sample(3, 3.0),
            make_sample(6, 6.0),
        ];
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn preserves_duplicates_and_equal_timestamps() {
        let a = vec![
            make_sample(1, 1.0),
            make_sample(2, 2.0),
            make_sample(2, 2.5),
        ];
        let b = vec![make_sample(2, 9.0), make_sample(3, 3.0)];
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        // three entries with timestamp 2 should be preserved
        assert_eq!(timestamps, vec![1, 2, 2, 2, 3]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn empty_input_yields_none() {
        let mut iter: MultiSeriesSampleIter<std::vec::IntoIter<Sample>> =
            MultiSeriesSampleIter::new(vec![]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn exhausted_iterators_removed_and_remaining_consumed() {
        let a = vec![make_sample(1, 1.0)]; // short series
        let b = vec![make_sample(2, 2.0), make_sample(3, 3.0)]; // longer series
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let collected: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(collected, vec![1, 2, 3]);
        // ensure iterator is fully exhausted afterwards
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn single_series_iterator() {
        let a = vec![
            make_sample(1, 1.0),
            make_sample(2, 2.0),
            make_sample(3, 3.0),
        ];
        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn multiple_empty_iterators() {
        let empty_a: Vec<Sample> = vec![];
        let empty_b: Vec<Sample> = vec![];
        let empty_c: Vec<Sample> = vec![];
        let mut iter = MultiSeriesSampleIter::new(vec![
            empty_a.into_iter(),
            empty_b.into_iter(),
            empty_c.into_iter(),
        ]);

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn mixed_empty_and_non_empty_iterators() {
        let empty: Vec<Sample> = vec![];
        let a = vec![make_sample(1, 1.0), make_sample(3, 3.0)];
        let b = vec![make_sample(2, 2.0)];

        let mut iter =
            MultiSeriesSampleIter::new(vec![empty.into_iter(), a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3]);
    }

    #[test]
    fn reverse_order_timestamps() {
        // Each series is ordered, but one starts higher than the other
        let a = vec![make_sample(10, 10.0), make_sample(20, 20.0)];
        let b = vec![make_sample(1, 1.0), make_sample(5, 5.0)];

        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 5, 10, 20]);
    }

    #[test]
    fn many_series_with_interleaved_timestamps() {
        let a = vec![make_sample(1, 1.0), make_sample(7, 7.0)];
        let b = vec![make_sample(2, 2.0), make_sample(8, 8.0)];
        let c = vec![make_sample(3, 3.0), make_sample(9, 9.0)];
        let d = vec![make_sample(4, 4.0), make_sample(10, 10.0)];

        let mut iter = MultiSeriesSampleIter::new(vec![
            a.into_iter(),
            b.into_iter(),
            c.into_iter(),
            d.into_iter(),
        ]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 7, 8, 9, 10]);
    }

    #[test]
    fn all_same_timestamp() {
        let a = vec![make_sample(5, 1.0), make_sample(5, 2.0)];
        let b = vec![make_sample(5, 3.0), make_sample(5, 4.0)];

        let iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let results: Vec<Sample> = iter.collect();
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|s| s.timestamp == 5));
    }

    #[test]
    fn values_preserved_correctly() {
        let a = vec![make_sample(1, 10.5), make_sample(3, 30.5)];
        let b = vec![make_sample(2, 20.5)];

        let iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let results: Vec<Sample> = iter.collect();
        assert_eq!(
            results[0],
            Sample {
                timestamp: 1,
                value: 10.5
            }
        );
        assert_eq!(
            results[1],
            Sample {
                timestamp: 2,
                value: 20.5
            }
        );
        assert_eq!(
            results[2],
            Sample {
                timestamp: 3,
                value: 30.5
            }
        );
    }

    #[test]
    fn large_timestamp_gap() {
        let a = vec![make_sample(1, 1.0), make_sample(1000000, 2.0)];
        let b = vec![make_sample(2, 2.0), make_sample(999999, 3.0)];

        let mut iter = MultiSeriesSampleIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 999999, 1000000]);
    }

    #[test]
    fn very_unbalanced_series_lengths() {
        let short = vec![make_sample(1, 1.0)];
        let long = vec![
            make_sample(2, 2.0),
            make_sample(3, 3.0),
            make_sample(4, 4.0),
            make_sample(5, 5.0),
            make_sample(6, 6.0),
        ];

        let mut iter = MultiSeriesSampleIter::new(vec![short.into_iter(), long.into_iter()]);

        let timestamps: Vec<i64> = iter.by_ref().map(|s| s.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4, 5, 6]);
    }
}
