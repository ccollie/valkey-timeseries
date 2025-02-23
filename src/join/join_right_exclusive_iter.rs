use super::JoinValue;
use crate::common::Sample;
use joinkit::Joinkit;

// todo: accept iterators instead of slices
pub struct JoinRightExclusiveIter<'a> {
    iter: Box<dyn Iterator<Item = &'a Sample> + 'a>,
}

impl<'a> JoinRightExclusiveIter<'a> {
    // todo: accept impl Iterator<Item=Sample>
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let left_iter = left.iter().map(|sample| (sample.timestamp, sample));
        let right_iter = right.iter().map(|sample| (sample.timestamp, sample));
        let iter = left_iter
            .into_iter()
            .hash_join_right_excl(right_iter)
            .flatten();

        Self {
            iter: Box::new(iter),
        }
    }
}

impl Iterator for JoinRightExclusiveIter<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|sample| JoinValue::right(sample.timestamp, sample.value))
    }
}
