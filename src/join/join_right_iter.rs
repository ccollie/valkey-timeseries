use crate::common::{Sample, Timestamp};
use crate::join::JoinValue;
use joinkit::{EitherOrBoth, Joinkit};
use min_max_heap::MinMaxHeap;

/// Iterator adaptor implementing a streaming *right outer join* over two time-sorted
/// [`Sample`] streams.
///
/// This iterator yields [`JoinValue`] rows for every timestamp present on the right side:
/// - If a right timestamp has no matching left sample, it yields `JoinValue::Right(..)`
///   for each right sample at that timestamp.
/// - If both sides have the timestamp, yields `JoinValue::Both(.., ..)` for each
///   combination of the single left sample and each right sample in the right group.
///
/// ## Invariant
/// Both input iterators must be sorted by [`Timestamp`] in non-decreasing order.
/// The implementation assumes this and may produce incorrect ordering if violated.
///
/// ## Ordering
/// Output is produced in non-decreasing timestamp order. Internally, a small heap is
/// used to buffer expanded join rows (e.g., one left sample joined with multiple right
/// samples) and to emit them in sorted order.
///
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub(super) struct JoinRightIter {
    exhausted: bool,
    prev_ts: Option<Timestamp>,
    heap: MinMaxHeap<JoinValue>,
    inner: Box<dyn Iterator<Item = EitherOrBoth<Sample, Vec<Sample>>>>,
}

impl JoinRightIter {
    pub(crate) fn new<L, R, IL, IR>(left: IL, right: IR) -> Self
    where
        L: Iterator<Item = Sample> + 'static,
        R: Iterator<Item = Sample>,
        IL: IntoIterator<IntoIter = L, Item = Sample>,
        IR: IntoIterator<IntoIter = R, Item = Sample>,
    {
        let left_iter = left.into_iter().map(|sample| (sample.timestamp, sample));
        let right_iter = right.into_iter().map(|sample| (sample.timestamp, sample));

        // For each right timestamp, we can have multiple samples grouped into a Vec<Sample>.
        // The heap will store JoinValue items to be returned in sorted order. We allocate a
        // reasonable initial capacity for the heap.
        let heap = MinMaxHeap::with_capacity(4);
        let iter = left_iter.hash_join_right_outer(right_iter);

        Self {
            exhausted: false,
            prev_ts: None,
            inner: Box::new(iter),
            heap,
        }
    }

    fn push_item(&mut self, item: EitherOrBoth<Sample, Vec<Sample>>) {
        match item {
            EitherOrBoth::Left(_) => {
                // should not happen
            }
            EitherOrBoth::Right(r) => {
                for row in r.iter() {
                    self.heap.push(JoinValue::right(*row));
                }
            }
            EitherOrBoth::Both(left, right) => {
                let ts = left.timestamp;
                for sample in right.iter() {
                    let row =
                        JoinValue::both(Sample::new(ts, left.value), Sample::new(ts, sample.value));
                    self.heap.push(row);
                }
            }
        }
    }
}

impl Iterator for JoinRightIter {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(value) = self.heap.pop_min() {
                return Some(value);
            }
            if self.exhausted {
                return None;
            }

            // Iterate until we see a timestamp strictly greater than the previous boundary.
            while let Some(value) = self.inner.next() {
                self.push_item(value);

                if let Some(peek) = self.heap.peek_min() {
                    let ts = peek.sortable_timestamp();
                    let should_break = match self.prev_ts {
                        None => false,
                        Some(prev) => ts > prev,
                    };

                    self.prev_ts = Some(ts);

                    if should_break {
                        break;
                    }
                }
            }

            if self.heap.is_empty() {
                self.exhausted = true;
            }
        }
    }
}
