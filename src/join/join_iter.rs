use super::{convert_join_item, JoinType, JoinValue};
use crate::common::Sample;
use super::JoinAsOfIter;
use joinkit::Joinkit;
use super::join_right_iter::JoinRightIter;
use super::join_right_exclusive_iter::JoinRightExclusiveIter;

pub fn create_join_iter<L, R, IL, IR>(
    left: IL,
    right: IR,
    join_type: JoinType,
) -> Box<dyn Iterator<Item = JoinValue>>
where
    L: Iterator<Item = Sample> + 'static,
    R: Iterator<Item = Sample> + 'static,
    IL: IntoIterator<IntoIter = L, Item = Sample>,
    IR: IntoIterator<IntoIter = R, Item = Sample>,
{
    match join_type {
        JoinType::AsOf(dir, tolerance) => {
            let iter = JoinAsOfIter::new(left, right, dir, tolerance);
            Box::new(iter)
        }
        JoinType::Left => Box::new(
            left.into_iter()
                .merge_join_left_outer_by(right, compare_by_timestamp)
                .map(convert_join_item),
        ),
        JoinType::LeftExclusive => {
            let iter = left
                .into_iter()
                .merge_join_left_excl_by(right, compare_by_timestamp)
                .map(|item| JoinValue::left(item.timestamp, item.value));

            Box::new(iter)
        }
        JoinType::Right => {
            let iter = JoinRightIter::new(left, right);
            Box::new(iter)
        }
        JoinType::RightExclusive => {
            let iter = JoinRightExclusiveIter::new(left, right);
            Box::new(iter)
        }
        JoinType::Inner => {
            let iter = left
                .into_iter()
                .merge_join_inner_by(right, compare_by_timestamp)
                .map(|(l, r)| JoinValue::both(l.timestamp, l.value, r.value));

            Box::new(iter)
        }
        JoinType::Full => {
            let iter = left
                .into_iter()
                .merge_join_full_outer_by(right, compare_by_timestamp)
                .map(convert_join_item);

            Box::new(iter)
        }
    }
}

#[inline]
fn compare_by_timestamp(left: &Sample, right: &Sample) -> std::cmp::Ordering {
    left.timestamp.cmp(&right.timestamp)
}
