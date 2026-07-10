use crate::common::MultiSample;
use min_max_heap::MinMaxHeap;
use std::cmp::Ordering;

/// Heap entry ordering rows by bucket timestamp only. Rows with equal
/// timestamps compare equal — the downstream reducer groups by timestamp, so
/// their relative order does not matter.
struct RowEntry(MultiSample);

impl PartialEq for RowEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.timestamp == other.0.timestamp
    }
}

impl Eq for RowEntry {}

impl PartialOrd for RowEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.timestamp.cmp(&other.0.timestamp)
    }
}

/// Iterate over multiple multi-aggregation row iterators, returning the rows
/// in timestamp order. Structural clone of `MultiSeriesSampleIter`.
pub struct MultiSeriesRowIter<T: Iterator<Item = MultiSample>> {
    heap: MinMaxHeap<RowEntry>,
    inner: Vec<T>,
}

impl<T: Iterator<Item = MultiSample>> MultiSeriesRowIter<T> {
    pub fn new(list: Vec<T>) -> Self {
        let len = list.len();
        Self {
            inner: list,
            heap: MinMaxHeap::with_capacity(len),
        }
    }

    /// Push one row from each iterator to the heap.
    /// Returns `true` if at least one row was added.
    fn push_rows_to_heap(&mut self) -> bool {
        let initial_len = self.heap.len();
        for iter in &mut self.inner {
            if let Some(row) = iter.next() {
                self.heap.push(RowEntry(row));
            }
        }
        self.heap.len() > initial_len
    }

    /// Load rows to the heap, trying to ensure that the iterators are roughly
    /// at the same timestamp. Removes exhausted iterators from `self.inner`.
    fn load_heap(&mut self) -> bool {
        if !self.push_rows_to_heap() {
            return false;
        }

        let Some(upper_bound_timestamp) = self.heap.peek_max().map(|e| e.0.timestamp) else {
            return false;
        };

        // Remove exhausted iterators in a single pass.
        self.inner.retain_mut(|row_iter| {
            let mut produced_any = false;

            for row in row_iter.by_ref() {
                produced_any = true;

                let is_beyond_upper_bound = row.timestamp > upper_bound_timestamp;
                self.heap.push(RowEntry(row));

                // Stop once we have crossed the boundary; leave remaining items for later.
                if is_beyond_upper_bound {
                    break;
                }
            }

            produced_any
        });

        true
    }
}

impl<T: Iterator<Item = MultiSample>> Iterator for MultiSeriesRowIter<T> {
    type Item = MultiSample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() && !self.load_heap() {
            return None;
        }
        self.heap.pop_min().map(|e| e.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    fn row(ts: i64, values: &[f64]) -> MultiSample {
        MultiSample {
            timestamp: ts,
            values: values.iter().copied().collect(),
        }
    }

    #[test]
    fn merge_two_series_in_order() {
        let a = vec![row(1, &[1.0, 10.0]), row(4, &[4.0, 40.0])];
        let b = vec![row(2, &[2.0, 20.0]), row(3, &[3.0, 30.0])];
        let iter = MultiSeriesRowIter::new(vec![a.into_iter(), b.into_iter()]);

        let timestamps: Vec<i64> = iter.map(|r| r.timestamp).collect();
        assert_eq!(timestamps, vec![1, 2, 3, 4]);
    }

    #[test]
    fn preserves_equal_timestamps_and_values() {
        let a = vec![row(1, &[1.0]), row(2, &[2.0])];
        let b = vec![row(2, &[9.0]), row(3, &[3.0])];
        let rows: Vec<MultiSample> =
            MultiSeriesRowIter::new(vec![a.into_iter(), b.into_iter()]).collect();

        assert_eq!(
            rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
            vec![1, 2, 2, 3]
        );
        // both timestamp-2 rows survive with their values intact
        let mut ts2: Vec<f64> = rows
            .iter()
            .filter(|r| r.timestamp == 2)
            .map(|r| r.values[0])
            .collect();
        ts2.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(ts2, vec![2.0, 9.0]);
    }

    #[test]
    fn empty_input_yields_none() {
        let mut iter: MultiSeriesRowIter<std::vec::IntoIter<MultiSample>> =
            MultiSeriesRowIter::new(vec![]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn values_preserved() {
        let a = vec![MultiSample {
            timestamp: 5,
            values: smallvec![1.5, 2.5, 3.5],
        }];
        let rows: Vec<MultiSample> = MultiSeriesRowIter::new(vec![a.into_iter()]).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.as_slice(), &[1.5, 2.5, 3.5]);
    }
}
