use min_max_heap::MinMaxHeap;
use super::JoinValue;
use crate::common::Sample;
use joinkit::Joinkit;

pub(super) struct JoinRightExclusiveIter {
    init: bool,
    heap: MinMaxHeap<JoinValue>,
    iter: Box<dyn Iterator<Item = JoinValue>>,
}

impl JoinRightExclusiveIter {
    pub(crate) fn new<L, R, IL, IR>(left: IL, right: IR) -> Self
    where
        L: Iterator<Item = Sample> + 'static,
        R: Iterator<Item = Sample>,
        IL: IntoIterator<IntoIter = L, Item = Sample>,
        IR: IntoIterator<IntoIter = R, Item = Sample>,
    {
        let left_iter = left.into_iter().map(|sample| (sample.timestamp, sample));
        let right_iter = right.into_iter().map(|sample| (sample.timestamp, sample));
        let iter = left_iter
            .into_iter()
            .hash_join_right_excl(right_iter)
            .flatten()
            .map(|sample| JoinValue::right(sample.timestamp, sample.value));

        Self {
            init: false,
            heap: MinMaxHeap::new(),
            iter: Box::new(iter),
        }
    }
}

impl Iterator for JoinRightExclusiveIter {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;
            while let Some(value) = self.iter.next() {
                self.heap.push(value);
            }
        }
        self.heap.pop_min()
    }
}