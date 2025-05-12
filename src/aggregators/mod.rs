use crate::common::Timestamp;
use crate::error_consts;
use crate::parser::timestamp::parse_timestamp;
use std::fmt::Display;
use valkey_module::{ValkeyError, ValkeyString};

mod handlers;
mod iterator;

pub use handlers::*;
pub use iterator::*;

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid,
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: u64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts.saturating_add_unsigned(time_delta / 2),
            Self::End => ts.saturating_add_unsigned(time_delta),
        }
    }
}

impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ts = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "-" => BucketTimestamp::Start,
            "+" => BucketTimestamp::End,
            "~" => BucketTimestamp::Mid,
            "start" => BucketTimestamp::Start,
            "end" => BucketTimestamp::End,
            "mid" => BucketTimestamp::Mid,
        };
        match ts {
            Some(ts) => Ok(ts),
            None => Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP value")),
        }
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

impl BucketAlignment {
    pub fn get_aligned_timestamp(&self, start: Timestamp, end: Timestamp) -> Timestamp {
        match self {
            BucketAlignment::Default => 0,
            BucketAlignment::Start => start,
            BucketAlignment::End => end,
            BucketAlignment::Timestamp(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for BucketAlignment {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let alignment = match value {
            arg if arg.eq_ignore_ascii_case("start") => BucketAlignment::Start,
            arg if arg.eq_ignore_ascii_case("end") => BucketAlignment::End,
            arg if arg.len() == 1 => {
                let c = arg.chars().next().unwrap();
                match c {
                    '-' => BucketAlignment::Start,
                    '+' => BucketAlignment::End,
                    _ => return Err(ValkeyError::Str(error_consts::INVALID_ALIGN)),
                }
            }
            _ => {
                let timestamp = parse_timestamp(value, false)
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGN))?;
                BucketAlignment::Timestamp(timestamp)
            }
        };
        Ok(alignment)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Aggregation {
    Avg,
    Count,
    First,
    Last,
    Max,
    Min,
    Range,
    StdP,
    StdS,
    Sum,
    VarP,
    VarS,
}

impl Aggregation {
    pub fn name(&self) -> &'static str {
        match self {
            Aggregation::First => "first",
            Aggregation::Last => "last",
            Aggregation::Min => "min",
            Aggregation::Max => "max",
            Aggregation::Avg => "avg",
            Aggregation::Sum => "sum",
            Aggregation::Count => "count",
            Aggregation::StdS => "std.s",
            Aggregation::StdP => "std.p",
            Aggregation::VarS => "var.s",
            Aggregation::VarP => "var.p",
            Aggregation::Range => "range",
        }
    }
}
impl Display for Aggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<&str> for Aggregation {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "avg" => Aggregation::Avg,
            "count" => Aggregation::Count,
            "first" => Aggregation::First,
            "last" => Aggregation::Last,
            "min" => Aggregation::Min,
            "max" => Aggregation::Max,
            "sum" => Aggregation::Sum,
            "range" => Aggregation::Range,
            "std.s" => Aggregation::StdS,
            "std.p" => Aggregation::StdP,
            "var.s" => Aggregation::VarS,
            "var.p" => Aggregation::VarP,
        };

        match value {
            Some(agg) => Ok(agg),
            None => Err(ValkeyError::Str("TSDB: invalid AGGREGATION value")),
        }
    }
}

impl TryFrom<&ValkeyString> for Aggregation {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        let str = value.to_string_lossy();
        Aggregation::try_from(str.as_str())
    }
}
