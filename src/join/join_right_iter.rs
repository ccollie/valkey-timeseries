use crate::common::{Sample, Timestamp};
use crate::join::JoinValue;
use joinkit::{EitherOrBoth, Joinkit};
use min_max_heap::MinMaxHeap;

/// Iterator for right outer join.
/// INVARIANT: assumes that both iterators are sorted by timestamp.
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

            // Iterate until we see a timestamp strictly greater than the previous boundary\.
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
