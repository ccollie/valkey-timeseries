use crate::common::Timestamp;
use crate::error_consts;
use crate::parser::timestamp::parse_timestamp;
use std::fmt::Display;
use valkey_module::{ValkeyError, ValkeyString};

mod handlers;
mod iterator;

pub use handlers::*;
pub use iterator::*;

#[derive(Debug, Default, PartialEq, Clone, Copy, Eq)]
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

#[derive(Debug, Default, PartialEq, Clone, Copy, Eq)]
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

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum AggregationType {
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

impl AggregationType {
    pub fn name(&self) -> &'static str {
        match self {
            AggregationType::First => "first",
            AggregationType::Last => "last",
            AggregationType::Min => "min",
            AggregationType::Max => "max",
            AggregationType::Avg => "avg",
            AggregationType::Sum => "sum",
            AggregationType::Count => "count",
            AggregationType::StdS => "std.s",
            AggregationType::StdP => "std.p",
            AggregationType::VarS => "var.s",
            AggregationType::VarP => "var.p",
            AggregationType::Range => "range",
        }
    }
}
impl Display for AggregationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<&str> for AggregationType {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "avg" => AggregationType::Avg,
            "count" => AggregationType::Count,
            "first" => AggregationType::First,
            "last" => AggregationType::Last,
            "min" => AggregationType::Min,
            "max" => AggregationType::Max,
            "sum" => AggregationType::Sum,
            "range" => AggregationType::Range,
            "std.s" => AggregationType::StdS,
            "std.p" => AggregationType::StdP,
            "var.s" => AggregationType::VarS,
            "var.p" => AggregationType::VarP,
        };

        match value {
            Some(agg) => Ok(agg),
            None => Err(ValkeyError::Str("TSDB: invalid AGGREGATION value")),
        }
    }
}

impl TryFrom<&ValkeyString> for AggregationType {
    type Error = ValkeyError;

    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        let str = value.to_string_lossy();
        AggregationType::try_from(str.as_str())
    }
}


impl TryFrom<u8> for AggregationType {
    type Error = ValkeyError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AggregationType::Avg),
            1 => Ok(AggregationType::Count),
            2 => Ok(AggregationType::First),
            3 => Ok(AggregationType::Last),
            4 => Ok(AggregationType::Min),
            5 => Ok(AggregationType::Max),
            6 => Ok(AggregationType::Sum),
            7 => Ok(AggregationType::Range),
            _ => Err(ValkeyError::Str("TSDB: invalid AGGREGATION value")),
        }
    }
}

impl From<AggregationType> for u8 {
    fn from(value: AggregationType) -> Self {
        match value {
            AggregationType::Avg => 0,
            AggregationType::Count => 1,
            AggregationType::First => 2,
            AggregationType::Last => 3,
            AggregationType::Min => 4,
            AggregationType::Max => 5,
            AggregationType::Sum => 6,
            AggregationType::Range => 7,
            _ => unreachable!(),
        }
    }
}

/// Calculates the start of the bucket for a given timestamp, aligning it to the specified
/// `align_timestamp` and `bucket_duration`.
pub fn calc_bucket_start(ts: Timestamp, align_timestamp: Timestamp, bucket_duration: u64) -> Timestamp {
    let diff = ts - align_timestamp;
    let delta = bucket_duration as i64;
    ts - ((diff % delta + delta) % delta)
}
