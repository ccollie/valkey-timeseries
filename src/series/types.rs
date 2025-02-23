use crate::common::rounding::RoundingStrategy;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::labels::Label;
use crate::series::chunks::ChunkCompression;
use crate::series::settings::SERIES_SETTINGS;
use get_size::GetSize;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy, GetSize)]
/// The policy to use when a duplicate sample is encountered
pub enum DuplicatePolicy {
    /// ignore any newly reported value and reply with an error
    #[default]
    Block,
    /// ignore any newly reported value
    KeepFirst,
    /// overwrite the existing value with the new value
    KeepLast,
    /// only override if the value is lower than the existing value
    Min,
    /// only override if the value is higher than the existing value
    Max,
    /// append the new value to the existing value
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DuplicatePolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::KeepFirst => "first",
            DuplicatePolicy::KeepLast => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        }
    }

    pub fn duplicate_value(self, ts: Timestamp, old: f64, new: f64) -> TsdbResult<f64> {
        use DuplicatePolicy::*;
        let has_nan = old.is_nan() || new.is_nan();
        if has_nan && self != Block {
            // take the valid sample regardless of policy
            let value = if new.is_nan() { old } else { new };
            return Ok(value);
        }
        Ok(match self {
            Block => {
                // todo: format ts as iso-8601 or rfc3339
                let msg = format!("{new} @ {ts}");
                return Err(TsdbError::DuplicateSample(msg));
            }
            KeepFirst => old,
            KeepLast => new,
            Min => old.min(new),
            Max => old.max(new),
            Sum => old + new,
        })
    }
}

impl FromStr for DuplicatePolicy {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DuplicatePolicy::*;

        match s.to_ascii_lowercase().as_str() {
            "block" => Ok(Block),
            "first" => Ok(KeepFirst),
            "last" => Ok(KeepLast),
            "min" => Ok(Min),
            "max" => Ok(Max),
            "sum" => Ok(Sum),
            _ => Err(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY)),
        }
    }
}

impl TryFrom<&str> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        DuplicatePolicy::from_str(s)
    }
}

impl TryFrom<String> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        DuplicatePolicy::from_str(&s)
    }
}

impl TryFrom<u8> for DuplicatePolicy {
    type Error = TsdbError;

    fn try_from(n: u8) -> Result<Self, Self::Error> {
        match n {
            0 => Ok(DuplicatePolicy::Block),
            1 => Ok(DuplicatePolicy::KeepFirst),
            2 => Ok(DuplicatePolicy::KeepLast),
            4 => Ok(DuplicatePolicy::Min),
            8 => Ok(DuplicatePolicy::Max),
            16 => Ok(DuplicatePolicy::Sum),
            _ => Err(TsdbError::General(format!("invalid duplicate policy: {n}"))),
        }
    }
}

#[derive(Copy, Clone, Debug, GetSize, PartialEq)]
pub struct SampleDuplicatePolicy {
    pub policy: DuplicatePolicy,
    /// The maximum difference between the new and existing timestamp to consider them duplicates
    pub max_time_delta: u64,
    /// The maximum difference between the new and existing value to consider them duplicates
    pub max_value_delta: f64,
}

impl Default for SampleDuplicatePolicy {
    fn default() -> Self {
        Self {
            policy: DuplicatePolicy::Block,
            max_time_delta: 0,
            max_value_delta: 0.0,
        }
    }
}

impl SampleDuplicatePolicy {
    pub fn is_duplicate(
        &self,
        current_sample: &Sample,
        last_sample: &Sample,
        override_policy: Option<DuplicatePolicy>,
    ) -> bool {
        let policy = override_policy.unwrap_or(self.policy);
        let last_ts = last_sample.timestamp;

        if current_sample.timestamp >= last_ts && policy == DuplicatePolicy::KeepLast {
            if !self.max_time_delta.is_zero()
                && (current_sample.timestamp - last_ts).abs() < self.max_time_delta as i64
            {
                return true;
            }
            if (last_sample.value - current_sample.value).abs() <= self.max_value_delta {
                return true;
            }
        }
        false
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SampleAddResult {
    Ok(Timestamp),
    Duplicate,
    Ignored(Timestamp),
    TooOld,
    Error(&'static str),
    CapacityFull,
    InvalidKey,
}

impl SampleAddResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, SampleAddResult::Ok(_))
    }
}

impl Display for SampleAddResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SampleAddResult::Ok(ts) => write!(f, "Added @ {}", ts),
            SampleAddResult::Duplicate => write!(f, "{}", error_consts::DUPLICATE_SAMPLE),
            SampleAddResult::Ignored(ts) => write!(f, "Ignored. Using ts: {}", ts),
            SampleAddResult::TooOld => write!(f, "{}", error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::Error(e) => write!(f, "{}", e),
            SampleAddResult::CapacityFull => write!(f, "Capacity full"),
            SampleAddResult::InvalidKey => write!(f, "Invalid key"),
        }
    }
}

impl From<SampleAddResult> for ValkeyValue {
    fn from(res: SampleAddResult) -> Self {
        match res {
            SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => ValkeyValue::Integer(ts),
            _ => ValkeyValue::Null,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TimeSeriesOptions {
    pub chunk_compression: Option<ChunkCompression>,
    pub chunk_size: Option<usize>,
    pub retention: Option<Duration>,
    pub sample_duplicate_policy: SampleDuplicatePolicy,
    pub labels: Vec<Label>,
    pub rounding: Option<RoundingStrategy>,
    pub on_duplicate: Option<DuplicatePolicy>,
}

impl TimeSeriesOptions {
    pub fn retention(&mut self, retention: Duration) {
        self.retention = Some(retention);
    }

    pub fn set_defaults_from_config(&mut self) {
        let globals = *SERIES_SETTINGS;
        self.chunk_compression = self
            .chunk_compression
            .unwrap_or(globals.chunk_compression.unwrap_or_default())
            .into();
        self.chunk_size = self.chunk_size.unwrap_or(globals.chunk_size_bytes).into();
        if self.retention.is_none() && globals.retention_period.is_some() {
            self.retention = globals.retention_period;
        }
        self.sample_duplicate_policy.policy = globals.duplicate_policy;
        if let Some(delta) = globals.dedupe_interval {
            self.sample_duplicate_policy.max_time_delta = delta.as_millis() as u64;
        }
        if self.rounding.is_none() {
            self.rounding = globals.rounding;
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct ValueFilter {
    pub min: f64,
    pub max: f64,
}

impl ValueFilter {
    pub(crate) fn new(min: f64, max: f64) -> ValkeyResult<Self> {
        if min > max {
            return Err(ValkeyError::Str("ERR invalid range"));
        }
        Ok(Self { min, max })
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct RangeFilter {
    pub value: Option<ValueFilter>,
    pub timestamps: Option<Vec<Timestamp>>,
}

impl RangeFilter {
    pub fn filter(&self, timestamp: Timestamp, value: f64) -> bool {
        if let Some(value_filter) = &self.value {
            if value < value_filter.min || value > value_filter.max {
                return false;
            }
        }
        if let Some(timestamps) = &self.timestamps {
            if !timestamps.contains(&timestamp) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::DuplicatePolicy;
    use crate::error::TsdbError;
    use std::str::FromStr;

    #[test]
    fn test_duplicate_policy_parse() {
        assert!(matches!(
            DuplicatePolicy::from_str("block"),
            Ok(DuplicatePolicy::Block)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("last"),
            Ok(DuplicatePolicy::KeepLast)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("first"),
            Ok(DuplicatePolicy::KeepFirst)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("min"),
            Ok(DuplicatePolicy::Min)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("max"),
            Ok(DuplicatePolicy::Max)
        ));
        assert!(matches!(
            DuplicatePolicy::from_str("sum"),
            Ok(DuplicatePolicy::Sum)
        ));
    }

    #[test]
    fn test_duplicate_policy_handle_duplicate() {
        let dp = DuplicatePolicy::Block;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert!(matches!(
            dp.duplicate_value(ts, old, new),
            Err(TsdbError::DuplicateSample(_))
        ));

        let dp = DuplicatePolicy::KeepFirst;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::KeepLast;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Min;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old);

        let dp = DuplicatePolicy::Max;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), new);

        let dp = DuplicatePolicy::Sum;
        let ts = 0;
        let old = 1.0;
        let new = 2.0;
        assert_eq!(dp.duplicate_value(ts, old, new).unwrap(), old + new);
    }

    #[test]
    fn test_duplicate_policy_handle_nan() {
        use DuplicatePolicy::*;

        let dp = Block;
        let ts = 0;
        let old = 1.0;
        let new = f64::NAN;
        assert!(matches!(
            dp.duplicate_value(ts, old, new),
            Err(TsdbError::DuplicateSample(_))
        ));

        let policies = [KeepFirst, KeepLast, Min, Max, Sum];
        for policy in policies {
            assert_eq!(policy.duplicate_value(ts, 10.0, f64::NAN).unwrap(), 10.0);
            assert_eq!(policy.duplicate_value(ts, f64::NAN, 8.0).unwrap(), 8.0);
        }
    }
}
