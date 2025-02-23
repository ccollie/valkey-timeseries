use super::JoinValue;
use crate::common::Sample;
use joinkit::Joinkit;

// todo: accept iterators instead of slices
pub struct JoinLeftExclusiveIter<'a> {
    inner: Box<dyn Iterator<Item = &'a Sample> + 'a>,
}

impl<'a> JoinLeftExclusiveIter<'a> {
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left
            .iter()
            .merge_join_left_excl_by(right, |x, y| x.timestamp.cmp(&y.timestamp));

        Self {
            inner: Box::new(iter),
        }
    }
}

impl Iterator for JoinLeftExclusiveIter<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|item| JoinValue::left(item.timestamp, item.value))
    }
}
