use crate::series::types::ValueFilter;
use joinkit::EitherOrBoth;
use std::fmt::Display;
use std::ops::Deref;
use std::time::Duration;
use valkey_module::ValkeyValue;

mod asof;
mod join_handler;
mod join_iter;
pub mod join_reducer;
mod join_right_iter;

#[cfg(test)]
mod join_handler_tests;
pub mod sorted_join_semi;

use crate::common::humanize::humanize_duration;
use crate::common::{Sample, Timestamp};
use crate::series::TimestampRange;
pub use join_handler::*;
pub use join_iter::*;
use join_reducer::JoinReducer;

use crate::join::sorted_join_semi::SortedJoinSemiBy;
use crate::series::request_types::AggregationOptions;
pub(super) use asof::{AsofJoinStrategy, JoinAsOfIter};

pub trait JoinkitExt: Iterator {
    fn join_semi<K, I, R, F>(self, right: R, key_fn: F) -> SortedJoinSemiBy<Self, R::IntoIter, K, F>
    where
        Self: Sized + Iterator<Item = I>,
        R: IntoIterator<Item = I>,
        K: Eq + Ord + Clone,
        F: FnMut(&I) -> K + Clone,
    {
        SortedJoinSemiBy::new(self, right, key_fn)
    }
}

impl<T: ?Sized> JoinkitExt for T where T: Iterator {}

#[derive(Clone, Debug)]
pub struct JoinValue(EitherOrBoth<Sample, Sample>);

impl JoinValue {
    pub fn left(sample: Sample) -> Self {
        JoinValue(EitherOrBoth::Left(sample))
    }

    pub fn right(sample: Sample) -> Self {
        JoinValue(EitherOrBoth::Right(sample))
    }

    pub fn both(left: Sample, right: Sample) -> Self {
        JoinValue(EitherOrBoth::Both(left, right))
    }

    fn sortable_timestamp(&self) -> Timestamp {
        match &self.0 {
            // We use the minimum timestamp for Both to ensure consistent ordering. Consider the case
            // of ASOF joins where left and right samples may have different timestamps. In such cases,
            // using the minimum timestamp helps maintain a predictable order.
            EitherOrBoth::Both(left, right) => Timestamp::min(left.timestamp, right.timestamp),
            EitherOrBoth::Left(left) => left.timestamp,
            EitherOrBoth::Right(right) => right.timestamp,
        }
    }
}

impl PartialEq for JoinValue {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for JoinValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JoinValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left_ts = self.sortable_timestamp();
        let right_ts = other.sortable_timestamp();
        left_ts.cmp(&right_ts)
    }
}

impl Eq for JoinValue {}

impl Deref for JoinValue {
    type Target = EitherOrBoth<Sample, Sample>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<JoinValue> for ValkeyValue {
    fn from(row: JoinValue) -> Self {
        match row.0 {
            EitherOrBoth::Both(left, right) => {
                ValkeyValue::Array(vec![ValkeyValue::from(left), ValkeyValue::from(right)])
            }
            EitherOrBoth::Left(left) => {
                ValkeyValue::Array(vec![ValkeyValue::from(left), ValkeyValue::Null])
            }
            EitherOrBoth::Right(right) => {
                ValkeyValue::Array(vec![ValkeyValue::Null, ValkeyValue::from(right)])
            }
        }
    }
}

impl From<EitherOrBoth<&Sample, &Sample>> for JoinValue {
    fn from(value: EitherOrBoth<&Sample, &Sample>) -> Self {
        match value {
            EitherOrBoth::Both(l, r) => JoinValue::both(*l, *r),
            EitherOrBoth::Left(l) => Self::left(*l),
            EitherOrBoth::Right(r) => Self::right(*r),
        }
    }
}

impl From<EitherOrBoth<Sample, Sample>> for JoinValue {
    fn from(value: EitherOrBoth<Sample, Sample>) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct AsOfJoinOptions {
    pub strategy: AsofJoinStrategy,
    pub tolerance: Duration,
    pub allow_exact_match: bool,
}

#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub enum JoinType {
    Left,
    Right,
    #[default]
    Inner,
    Full,
    Semi,
    Anti,
    AsOf(AsOfJoinOptions),
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Left => {
                write!(f, "LEFT OUTER JOIN")?;
            }
            JoinType::Right => {
                write!(f, "RIGHT OUTER JOIN")?;
            }
            JoinType::Inner => {
                write!(f, "INNER JOIN")?;
            }
            JoinType::Full => {
                write!(f, "FULL JOIN")?;
            }
            JoinType::Semi => {
                write!(f, "SEMI JOIN")?;
            }
            JoinType::Anti => {
                write!(f, "ANTI JOIN")?;
            }
            JoinType::AsOf(options) => {
                write!(f, "ASOF JOIN {}", options.strategy)?;
                if !options.tolerance.is_zero() {
                    write!(f, " TOLERANCE {}", humanize_duration(&options.tolerance))?;
                }
                if options.allow_exact_match {
                    write!(f, " ALLOW EXACT MATCH")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct JoinOptions {
    pub join_type: JoinType,
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub reducer: Option<JoinReducer>,
    pub aggregation: Option<AggregationOptions>,
}
