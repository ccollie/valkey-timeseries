use super::asof::AsOfJoinStrategy;
use super::asof::{merge_apply_asof, MergeAsOfMode};
use super::JoinValue;
use crate::common::Sample;
use joinkit::EitherOrBoth;
use std::time::Duration;

pub struct JoinAsOfIter<'a> {
    init: bool,
    left: &'a [Sample],
    right: &'a [Sample],
    merge_mode: MergeAsOfMode,
    tolerance: Duration,
    items: Vec<EitherOrBoth<&'a Sample, &'a Sample>>,
    idx: usize,
}

impl<'a> JoinAsOfIter<'a> {
    pub fn new(
        left: &'a [Sample],
        right: &'a [Sample],
        direction: AsOfJoinStrategy,
        tolerance: Duration,
    ) -> Self {
        let merge_mode = match direction {
            AsOfJoinStrategy::Next => MergeAsOfMode::RollFollowing,
            AsOfJoinStrategy::Prior => MergeAsOfMode::RollPrior,
        };
        Self {
            init: false,
            left,
            right,
            merge_mode,
            tolerance,
            items: vec![],
            idx: 0,
        }
    }
}

impl Iterator for JoinAsOfIter<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;
            self.items = merge_apply_asof(self.left, self.right, &self.tolerance, self.merge_mode);
        }
        match self.items.get(self.idx) {
            Some(item) => {
                self.idx += 1;
                Some(item.into())
            }
            None => None,
        }
    }
}
