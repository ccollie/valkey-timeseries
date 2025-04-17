use crate::common::Sample;
use crate::join::JoinValue;
use joinkit::{EitherOrBoth, Joinkit};
use min_max_heap::MinMaxHeap;

pub(super) struct JoinRightIter {
    loaded: bool,
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
        let iter = left_iter.hash_join_right_outer(right_iter);

        Self {
            loaded: false,
            inner: Box::new(iter),
            heap: MinMaxHeap::new(),
        }
    }

    fn push_item(&mut self, item: EitherOrBoth<Sample, Vec<Sample>>) {
        match item {
            EitherOrBoth::Left(_) => {
                // should not happen
            }
            EitherOrBoth::Right(r) => {
                for row in r.iter() {
                    self.heap.push(JoinValue::right(row.timestamp, row.value));
                }
            }
            EitherOrBoth::Both(left, right) => {
                let ts = left.timestamp;
                for sample in right.iter() {
                    let row = JoinValue::both(ts, left.value, sample.value);
                    self.heap.push(row);
                }
            }
        }
    }
}

impl Iterator for JoinRightIter {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.loaded = true;
            while let Some(item) = self.inner.next() {
                self.push_item(item)
            }
        }
        self.heap.pop_min()
    }
}
