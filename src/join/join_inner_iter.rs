use super::JoinValue;
use crate::common::Sample;
use joinkit::Joinkit;

pub struct JoinInnerIter<'a> {
    iter: Box<dyn Iterator<Item = (&'a Sample, &'a Sample)> + 'a>,
}

impl<'a> JoinInnerIter<'a> {
    // todo: accept imp Iterator<Item=Sample>
    pub fn new(left: &'a [Sample], right: &'a [Sample]) -> Self {
        let iter = left
            .iter()
            .merge_join_inner_by(right, |x, y| x.timestamp.cmp(&y.timestamp));

        Self {
            iter: Box::new(iter),
        }
    }
}

impl Iterator for JoinInnerIter<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((l, r)) = self.iter.next() {
            Some(JoinValue::both(l.timestamp, l.value, r.value))
        } else {
            None
        }
    }
}
