use crate::common::Timestamp;
use crate::error_consts;
use crate::parser::timestamp::parse_timestamp;
use std::fmt::Display;
use valkey_module::{ValkeyError, ValkeyString};

mod aggregate_iterator;
mod filtered;
mod handlers;
#[cfg(test)]
mod handlers_tests;
mod kahan;

pub use aggregate_iterator::*;
pub use filtered::*;
pub use handlers::*;

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
        let alignment = hashify::tiny_map_ignore_case! {
            value.as_bytes(),
            "start" => BucketAlignment::Start,
            "end" => BucketAlignment::End,
            "-" => BucketAlignment::Start,
            "+" => BucketAlignment::End,
        };
        match alignment {
            Some(alignment) => Ok(alignment),
            None => {
                let timestamp = parse_timestamp(value, false)
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGN))?;
                Ok(BucketAlignment::Timestamp(timestamp))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for BucketAlignment {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        let str = value.to_string_lossy();
        BucketAlignment::try_from(str.as_str())
    }
}

impl From<Timestamp> for BucketAlignment {
    fn from(value: Timestamp) -> Self {
        BucketAlignment::Timestamp(value)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
#[repr(u8)]
pub enum AggregationType {
    All,
    Any,
    Avg,
    Count,
    CountIf,
    First,
    Increase,
    IRate,
    Last,
    Max,
    Min,
    None,
    Range,
    Rate,
    Share,
    StdP,
    StdS,
    Sum,
    SumIf,
    VarP,
    VarS,
}

impl AggregationType {
    pub fn name(&self) -> &'static str {
        match self {
            AggregationType::All => "all",
            AggregationType::Any => "any",
            AggregationType::Avg => "avg",
            AggregationType::Count => "count",
            AggregationType::CountIf => "countif",
            AggregationType::First => "first",
            AggregationType::Increase => "increase",
            AggregationType::IRate => "irate",
            AggregationType::Last => "last",
            AggregationType::Min => "min",
            AggregationType::Max => "max",
            AggregationType::None => "none",
            AggregationType::Share => "share",
            AggregationType::StdS => "std.s",
            AggregationType::StdP => "std.p",
            AggregationType::Sum => "sum",
            AggregationType::SumIf => "sumif",
            AggregationType::Range => "range",
            AggregationType::Rate => "rate",
            AggregationType::VarS => "var.s",
            AggregationType::VarP => "var.p",
        }
    }

    pub fn is_filtered(&self) -> bool {
        matches!(
            self,
            AggregationType::All
                | AggregationType::Any
                | AggregationType::CountIf
                | AggregationType::SumIf
                | AggregationType::Share
                | AggregationType::None
        )
    }

    pub fn has_filtered_variant(&self) -> bool {
        matches!(self, AggregationType::Count | AggregationType::Sum)
    }

    /// Returns true if the aggregation type can be used for GroupBy Reduce operations
    pub fn is_groupable(&self) -> bool {
        !self.is_filtered()
    }

    // In the aggregation logic where Last/First are handled:
    // When is_reverse is true, swap the behavior of First and Last
    pub fn apply_reverse_adjusted(&self, is_reverse: bool) -> AggregationType {
        if is_reverse {
            match self {
                AggregationType::First => AggregationType::Last,
                AggregationType::Last => AggregationType::First,
                other => *other,
            }
        } else {
            *self
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
            "all" => AggregationType::All,
            "any" => AggregationType::Any,
            "avg" => AggregationType::Avg,
            "count" => AggregationType::Count,
            "countif" => AggregationType::CountIf,
            "first" => AggregationType::First,
            "increase" => AggregationType::Increase,
            "irate" => AggregationType::IRate,
            "last" => AggregationType::Last,
            "min" => AggregationType::Min,
            "max" => AggregationType::Max,
            "none" => AggregationType::None,
            "range" => AggregationType::Range,
            "rate" => AggregationType::Rate,
            "share" => AggregationType::Share,
            "std.s" => AggregationType::StdS,
            "std.p" => AggregationType::StdP,
            "sum" => AggregationType::Sum,
            "sumif" => AggregationType::SumIf,
            "var.s" => AggregationType::VarS,
            "var.p" => AggregationType::VarP,
        };

        match value {
            Some(agg) => Ok(agg),
            None => Err(ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE)),
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
            0 => Ok(AggregationType::All),
            1 => Ok(AggregationType::Any),
            2 => Ok(AggregationType::Avg),
            3 => Ok(AggregationType::Count),
            4 => Ok(AggregationType::CountIf),
            5 => Ok(AggregationType::First),
            6 => Ok(AggregationType::Increase),
            7 => Ok(AggregationType::IRate),
            8 => Ok(AggregationType::Last),
            9 => Ok(AggregationType::Max),
            10 => Ok(AggregationType::Min),
            11 => Ok(AggregationType::None),
            12 => Ok(AggregationType::Range),
            13 => Ok(AggregationType::Rate),
            14 => Ok(AggregationType::Share),
            15 => Ok(AggregationType::StdP),
            16 => Ok(AggregationType::StdS),
            17 => Ok(AggregationType::Sum),
            18 => Ok(AggregationType::SumIf),
            19 => Ok(AggregationType::VarP),
            20 => Ok(AggregationType::VarS),
            _ => Err(ValkeyError::Str("TSDB: invalid AGGREGATION value")),
        }
    }
}

impl From<AggregationType> for u8 {
    fn from(value: AggregationType) -> Self {
        match value {
            AggregationType::All => 0,
            AggregationType::Any => 1,
            AggregationType::Avg => 2,
            AggregationType::Count => 3,
            AggregationType::CountIf => 4,
            AggregationType::First => 5,
            AggregationType::Increase => 6,
            AggregationType::IRate => 7,
            AggregationType::Last => 8,
            AggregationType::Max => 9,
            AggregationType::Min => 10,
            AggregationType::None => 11,
            AggregationType::Range => 12,
            AggregationType::Rate => 13,
            AggregationType::Share => 14,
            AggregationType::Sum => 15,
            AggregationType::SumIf => 16,
            AggregationType::StdP => 17,
            AggregationType::StdS => 18,
            AggregationType::VarP => 19,
            AggregationType::VarS => 20,
        }
    }
}

/// Calculates the start of the bucket for a given timestamp, aligning it to the specified
/// `align_timestamp` and `bucket_duration`.
pub fn calc_bucket_start(
    ts: Timestamp,
    align_timestamp: Timestamp,
    bucket_duration: u64,
) -> Timestamp {
    let diff = ts - align_timestamp;
    let delta = bucket_duration as i64;
    0.max(ts - ((diff % delta + delta) % delta))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_timestamp_calculates_correctly_for_start() {
        let ts = Timestamp::from(1000);
        let delta = 500;
        assert_eq!(BucketTimestamp::Start.calculate(ts, delta), ts);
    }

    #[test]
    fn bucket_timestamp_calculates_correctly_for_mid() {
        let ts = Timestamp::from(1000);
        let delta = 500;
        assert_eq!(
            BucketTimestamp::Mid.calculate(ts, delta),
            Timestamp::from(1250)
        );
    }

    #[test]
    fn bucket_timestamp_calculates_correctly_for_end() {
        let ts = Timestamp::from(1000);
        let delta = 500;
        assert_eq!(
            BucketTimestamp::End.calculate(ts, delta),
            Timestamp::from(1500)
        );
    }

    #[test]
    fn bucket_timestamp_try_from_str_parses_valid_values() {
        assert_eq!(
            BucketTimestamp::try_from("start").unwrap(),
            BucketTimestamp::Start
        );
        assert_eq!(
            BucketTimestamp::try_from("end").unwrap(),
            BucketTimestamp::End
        );
        assert_eq!(
            BucketTimestamp::try_from("mid").unwrap(),
            BucketTimestamp::Mid
        );
        assert_eq!(
            BucketTimestamp::try_from("-").unwrap(),
            BucketTimestamp::Start
        );
        assert_eq!(
            BucketTimestamp::try_from("+").unwrap(),
            BucketTimestamp::End
        );
        assert_eq!(
            BucketTimestamp::try_from("~").unwrap(),
            BucketTimestamp::Mid
        );
    }

    #[test]
    fn bucket_timestamp_try_from_str_returns_error_for_invalid_value() {
        assert!(BucketTimestamp::try_from("invalid").is_err());
    }

    #[test]
    fn bucket_alignment_gets_correct_aligned_timestamp() {
        let start = Timestamp::from(1000);
        let end = Timestamp::from(2000);
        assert_eq!(
            BucketAlignment::Default.get_aligned_timestamp(start, end),
            0
        );
        assert_eq!(
            BucketAlignment::Start.get_aligned_timestamp(start, end),
            start
        );
        assert_eq!(BucketAlignment::End.get_aligned_timestamp(start, end), end);
        assert_eq!(
            BucketAlignment::Timestamp(Timestamp::from(1500)).get_aligned_timestamp(start, end),
            Timestamp::from(1500)
        );
    }

    #[test]
    fn bucket_alignment_try_from_str_parses_valid_values() {
        assert_eq!(
            BucketAlignment::try_from("start").unwrap(),
            BucketAlignment::Start
        );
        assert_eq!(
            BucketAlignment::try_from("end").unwrap(),
            BucketAlignment::End
        );
        assert_eq!(
            BucketAlignment::try_from("-").unwrap(),
            BucketAlignment::Start
        );
        assert_eq!(
            BucketAlignment::try_from("+").unwrap(),
            BucketAlignment::End
        );
        assert_eq!(
            BucketAlignment::try_from("1500").unwrap(),
            BucketAlignment::Timestamp(Timestamp::from(1500))
        );
    }

    #[test]
    fn bucket_alignment_try_from_str_returns_error_for_invalid_value() {
        assert!(BucketAlignment::try_from("invalid").is_err());
    }

    #[test]
    fn aggregation_name_returns_correct_value() {
        assert_eq!(AggregationType::All.name(), "all");
        assert_eq!(AggregationType::Any.name(), "any");
        assert_eq!(AggregationType::Avg.name(), "avg");
        assert_eq!(AggregationType::Count.name(), "count");
        assert_eq!(AggregationType::CountIf.name(), "countif");
        assert_eq!(AggregationType::First.name(), "first");
        assert_eq!(AggregationType::Increase.name(), "increase");
        assert_eq!(AggregationType::Last.name(), "last");
        assert_eq!(AggregationType::Min.name(), "min");
        assert_eq!(AggregationType::Max.name(), "max");
        assert_eq!(AggregationType::None.name(), "none");
        assert_eq!(AggregationType::Share.name(), "share");
        assert_eq!(AggregationType::Sum.name(), "sum");
        assert_eq!(AggregationType::SumIf.name(), "sumif");
        assert_eq!(AggregationType::Range.name(), "range");
        assert_eq!(AggregationType::Rate.name(), "rate");
        assert_eq!(AggregationType::StdS.name(), "std.s");
        assert_eq!(AggregationType::StdP.name(), "std.p");
        assert_eq!(AggregationType::VarS.name(), "var.s");
        assert_eq!(AggregationType::VarP.name(), "var.p");
    }

    #[test]
    fn aggregation_type_try_from_str_parses_valid_values() {
        assert_eq!(
            AggregationType::try_from("all").unwrap(),
            AggregationType::All
        );
        assert_eq!(
            AggregationType::try_from("any").unwrap(),
            AggregationType::Any
        );
        assert_eq!(
            AggregationType::try_from("avg").unwrap(),
            AggregationType::Avg
        );
        assert_eq!(
            AggregationType::try_from("count").unwrap(),
            AggregationType::Count
        );
        assert_eq!(
            AggregationType::try_from("CountIf").unwrap(),
            AggregationType::CountIf
        );
        assert_eq!(
            AggregationType::try_from("first").unwrap(),
            AggregationType::First
        );
        assert_eq!(
            AggregationType::try_from("increase").unwrap(),
            AggregationType::Increase
        );
        assert_eq!(
            AggregationType::try_from("irate").unwrap(),
            AggregationType::IRate
        );
        assert_eq!(
            AggregationType::try_from("last").unwrap(),
            AggregationType::Last
        );
        assert_eq!(
            AggregationType::try_from("min").unwrap(),
            AggregationType::Min
        );
        assert_eq!(
            AggregationType::try_from("max").unwrap(),
            AggregationType::Max
        );
        assert_eq!(
            AggregationType::try_from("none").unwrap(),
            AggregationType::None
        );
        assert_eq!(
            AggregationType::try_from("sum").unwrap(),
            AggregationType::Sum
        );
        assert_eq!(
            AggregationType::try_from("sumif").unwrap(),
            AggregationType::SumIf
        );
        assert_eq!(
            AggregationType::try_from("range").unwrap(),
            AggregationType::Range
        );
        assert_eq!(
            AggregationType::try_from("rate").unwrap(),
            AggregationType::Rate
        );
        assert_eq!(
            AggregationType::try_from("share").unwrap(),
            AggregationType::Share
        );
        assert_eq!(
            AggregationType::try_from("std.s").unwrap(),
            AggregationType::StdS
        );
        assert_eq!(
            AggregationType::try_from("std.p").unwrap(),
            AggregationType::StdP
        );
        assert_eq!(
            AggregationType::try_from("var.s").unwrap(),
            AggregationType::VarS
        );
        assert_eq!(
            AggregationType::try_from("var.p").unwrap(),
            AggregationType::VarP
        );
    }

    #[test]
    fn aggregation_try_from_str_returns_error_for_invalid_value() {
        assert!(AggregationType::try_from("invalid").is_err());
    }

    #[test]
    fn aggregation_type_to_u8_conversion() {
        assert_eq!(u8::from(AggregationType::All), 0);
        assert_eq!(u8::from(AggregationType::Any), 1);
        assert_eq!(u8::from(AggregationType::Avg), 2);
        assert_eq!(u8::from(AggregationType::Count), 3);
        assert_eq!(u8::from(AggregationType::CountIf), 4);
        assert_eq!(u8::from(AggregationType::First), 5);
        assert_eq!(u8::from(AggregationType::Increase), 6);
        assert_eq!(u8::from(AggregationType::IRate), 7);
        assert_eq!(u8::from(AggregationType::Last), 8);
        assert_eq!(u8::from(AggregationType::Max), 9);
        assert_eq!(u8::from(AggregationType::Min), 10);
        assert_eq!(u8::from(AggregationType::None), 11);
        assert_eq!(u8::from(AggregationType::Range), 12);
        assert_eq!(u8::from(AggregationType::Rate), 13);
        assert_eq!(u8::from(AggregationType::Share), 14);
        assert_eq!(u8::from(AggregationType::Sum), 15);
        assert_eq!(u8::from(AggregationType::SumIf), 16);
        assert_eq!(u8::from(AggregationType::StdP), 17);
        assert_eq!(u8::from(AggregationType::StdS), 18);
        assert_eq!(u8::from(AggregationType::VarP), 19);
        assert_eq!(u8::from(AggregationType::VarS), 20);
    }

    #[test]
    fn aggregation_type_from_u8_conversion() {
        assert_eq!(
            AggregationType::try_from(0u8).unwrap(),
            AggregationType::All
        );
        assert_eq!(
            AggregationType::try_from(1u8).unwrap(),
            AggregationType::Any
        );
        assert_eq!(
            AggregationType::try_from(2u8).unwrap(),
            AggregationType::Avg
        );
        assert_eq!(
            AggregationType::try_from(3u8).unwrap(),
            AggregationType::Count
        );
        assert_eq!(
            AggregationType::try_from(4u8).unwrap(),
            AggregationType::CountIf
        );
        assert_eq!(
            AggregationType::try_from(5u8).unwrap(),
            AggregationType::First
        );
        assert_eq!(
            AggregationType::try_from(6u8).unwrap(),
            AggregationType::Increase
        );
        assert_eq!(
            AggregationType::try_from(7u8).unwrap(),
            AggregationType::IRate
        );
        assert_eq!(
            AggregationType::try_from(8u8).unwrap(),
            AggregationType::Last
        );
        assert_eq!(
            AggregationType::try_from(9u8).unwrap(),
            AggregationType::Max
        );
        assert_eq!(
            AggregationType::try_from(10u8).unwrap(),
            AggregationType::Min
        );
        assert_eq!(
            AggregationType::try_from(11u8).unwrap(),
            AggregationType::None
        );
        assert_eq!(
            AggregationType::try_from(12u8).unwrap(),
            AggregationType::Range
        );
        assert_eq!(
            AggregationType::try_from(13u8).unwrap(),
            AggregationType::Rate
        );
        assert_eq!(
            AggregationType::try_from(14u8).unwrap(),
            AggregationType::Share
        );
        assert_eq!(
            AggregationType::try_from(15u8).unwrap(),
            AggregationType::StdP
        );
        assert_eq!(
            AggregationType::try_from(16u8).unwrap(),
            AggregationType::StdS
        );
        assert_eq!(
            AggregationType::try_from(17u8).unwrap(),
            AggregationType::Sum
        );
        assert_eq!(
            AggregationType::try_from(18u8).unwrap(),
            AggregationType::SumIf
        );
        assert_eq!(
            AggregationType::try_from(19u8).unwrap(),
            AggregationType::VarP
        );
        assert_eq!(
            AggregationType::try_from(20u8).unwrap(),
            AggregationType::VarS
        );
    }

    #[test]
    fn aggregation_type_from_u8_invalid_values() {
        assert!(AggregationType::try_from(255u8).is_err());

        // Test that the error message is correct
        match AggregationType::try_from(33u8) {
            Err(err) => assert_eq!(err.to_string(), "TSDB: invalid AGGREGATION value"),
            Ok(_) => panic!("Expected error for invalid u8 value"),
        }
    }
}
