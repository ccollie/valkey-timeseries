use super::{convert_join_item, JoinValue};
use crate::common::Sample;
use joinkit::{EitherOrBoth, Joinkit};

pub struct JoinLeftIter<'a> {
    inner: Box<dyn Iterator<Item = EitherOrBoth<&'a Sample, &'a Sample>> + 'a>,
}

impl<'a> JoinLeftIter<'a> {
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left
            .iter()
            .merge_join_left_outer_by(right, |x, y| x.timestamp.cmp(&y.timestamp));
        Self {
            inner: Box::new(iter),
        }
    }
}

impl Iterator for JoinLeftIter<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(convert_join_item)
    }
}
