use crate::series::types::ValueFilter;
use joinkit::EitherOrBoth;
use std::fmt::Display;
use std::time::Duration;

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

#[derive(Clone, PartialEq, Debug)]
pub struct JoinValue {
    pub timestamp: Timestamp,
    pub other_timestamp: Option<Timestamp>,
    pub value: EitherOrBoth<f64, f64>,
}

impl JoinValue {
    pub fn left(timestamp: Timestamp, value: f64) -> Self {
        JoinValue {
            timestamp,
            other_timestamp: None,
            value: EitherOrBoth::Left(value),
        }
    }
    pub fn right(timestamp: Timestamp, value: f64) -> Self {
        JoinValue {
            other_timestamp: None,
            timestamp,
            value: EitherOrBoth::Right(value),
        }
    }

    pub fn both(timestamp: Timestamp, l: f64, r: f64) -> Self {
        JoinValue {
            timestamp,
            other_timestamp: None,
            value: EitherOrBoth::Both(l, r),
        }
    }
}

impl From<EitherOrBoth<&Sample, &Sample>> for JoinValue {
    fn from(value: EitherOrBoth<&Sample, &Sample>) -> Self {
        match value {
            EitherOrBoth::Both(l, r) => {
                let mut value = Self::both(l.timestamp, l.value, r.value);
                value.other_timestamp = Some(r.timestamp);
                value
            }
            EitherOrBoth::Left(l) => Self::left(l.timestamp, l.value),
            EitherOrBoth::Right(r) => Self::right(r.timestamp, r.value),
        }
    }
}

impl From<EitherOrBoth<Sample, Sample>> for JoinValue {
    fn from(value: EitherOrBoth<Sample, Sample>) -> Self {
        convert_join_item(value)
    }
}

impl PartialOrd for JoinValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.timestamp.cmp(&other.timestamp))
    }
}

impl Ord for JoinValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl Eq for JoinValue {}

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
            JoinType::AsOf(ref options) => {
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

pub(crate) fn convert_join_item(item: EitherOrBoth<Sample, Sample>) -> JoinValue {
    match item {
        EitherOrBoth::Both(l, r) => JoinValue::both(l.timestamp, l.value, r.value),
        EitherOrBoth::Left(l) => JoinValue::left(l.timestamp, l.value),
        EitherOrBoth::Right(r) => JoinValue::right(r.timestamp, r.value),
    }
}
