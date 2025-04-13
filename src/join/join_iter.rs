use super::{JoinType, JoinValue};
use crate::common::Sample;
use crate::join::join_asof_iter::JoinAsOfIter;
use crate::join::join_full_iter::JoinFullIter;
use crate::join::join_inner_iter::JoinInnerIter;
use crate::join::join_left_exclusive_iter::JoinLeftExclusiveIter;
use crate::join::join_left_iter::JoinLeftIter;
use crate::join::join_right_exclusive_iter::JoinRightExclusiveIter;
use crate::join::join_right_iter::JoinRightIter;

pub enum JoinIterator<'a> {
    Left(JoinLeftIter<'a>),
    LeftExclusive(JoinLeftExclusiveIter<'a>),
    Right(JoinRightIter<'a>),
    RightExclusive(JoinRightExclusiveIter<'a>),
    Inner(JoinInnerIter<'a>),
    Full(JoinFullIter<'a>),
    AsOf(JoinAsOfIter<'a>),
}

impl<'a> JoinIterator<'a> {
    pub(crate) fn new(left: &'a [Sample], right: &'a [Sample], join_type: JoinType) -> Self {
        match join_type {
            JoinType::AsOf(dir, tolerance) => {
                Self::AsOf(JoinAsOfIter::new(left, right, dir, tolerance))
            }
            JoinType::Left => {
                Self::Left(JoinLeftIter::new(left, right))
            }
            JoinType::LeftExclusive => {
                Self::LeftExclusive(JoinLeftExclusiveIter::new(left, right))
            }
            JoinType::Right => {
                Self::Right(JoinRightIter::new(left, right))
            }
            JoinType::RightExclusive => {
                Self::RightExclusive(JoinRightExclusiveIter::new(left, right))
            }
            JoinType::Inner => Self::Inner(JoinInnerIter::new(left, right)),
            JoinType::Full => Self::Full(JoinFullIter::new(left, right)),
        }
    }
}

impl Iterator for JoinIterator<'_> {
    type Item = JoinValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            JoinIterator::Left(iter) => iter.next(),
            JoinIterator::LeftExclusive(iter) => iter.next(),
            JoinIterator::Right(iter) => iter.next(),
            JoinIterator::RightExclusive(iter) => iter.next(),
            JoinIterator::Inner(iter) => iter.next(),
            JoinIterator::Full(iter) => iter.next(),
            JoinIterator::AsOf(iter) => iter.next(),
        }
    }
}
