use super::{join_asof_samples, AsofJoinStrategy};
use crate::common::Sample;
use crate::join::JoinValue;
use joinkit::EitherOrBoth;
use std::time::Duration;

pub struct JoinAsOfIter<L, R>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    init: bool,
    left: L,
    right: R,
    strategy: AsofJoinStrategy,
    tolerance: Duration,
    allow_eq: bool,
    items: Vec<(Sample, Sample)>,
    idx: usize,
}

impl<L, R> JoinAsOfIter<L, R>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    pub fn new<IL, IR>(
        left: IL,
        right: IR,
        strategy: AsofJoinStrategy,
        tolerance: Duration,
        allow_eq: bool,
    ) -> Self
    where
        IL: IntoIterator<IntoIter = L, Item = Sample>,
        IR: IntoIterator<IntoIter = R, Item = Sample>,
    {
        Self {
            init: false,
            left: left.into_iter(),
            right: right.into_iter(),
            strategy,
            tolerance,
            allow_eq,
            items: vec![],
            idx: 0,
        }
    }

    fn init(&mut self) {
        self.init = true;
        let tolerance = self.tolerance.as_millis() as i64;
        let left: Vec<Sample> = self.left.by_ref().collect();
        let right: Vec<Sample> = self.right.by_ref().collect();
        self.items =
            join_asof_samples(&left, &right, self.strategy, Some(tolerance), self.allow_eq);
    }
}

impl<L, R> Iterator for JoinAsOfIter<L, R>
where
    L: Iterator<Item = Sample>,
    R: Iterator<Item = Sample>,
{
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init();
        }
        match self.items.get(self.idx) {
            Some((left, right)) => {
                self.idx += 1;
                Some(JoinValue {
                    timestamp: left.timestamp,
                    other_timestamp: Some(right.timestamp),
                    value: EitherOrBoth::Both(left.value, right.value),
                })
            }
            None => None,
        }
    }
}
