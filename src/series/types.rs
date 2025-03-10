use crate::common::rounding::RoundingStrategy;
use crate::common::serialization::rdb_load_string;
use crate::common::{Sample, Timestamp};
use crate::config::get_series_config_settings;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::labels::Label;
use crate::series::chunks::ChunkEncoding;
use crate::series::settings::ConfigSettings;
use get_size::GetSize;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{raw, ValkeyError, ValkeyResult, ValkeyValue};

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy, GetSize)]
/// The policy to use when a duplicate sample is encountered
pub enum DuplicatePolicy {
    #[default]
    Block,
    KeepFirst,
    KeepLast,
    Min,
    Max,
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
        if (old.is_nan() || new.is_nan()) && self != Block {
            return Ok(if new.is_nan() { old } else { new });
        }
        match self {
            Block => Err(TsdbError::DuplicateSample(format!("{new} @ {ts}"))),
            KeepFirst => Ok(old),
            KeepLast => Ok(new),
            Min => Ok(old.min(new)),
            Max => Ok(old.max(new)),
            Sum => Ok(old + new),
        }
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
        let config = get_series_config_settings();
        config.duplicate_policy
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

    pub(crate) fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        let tmp = self.policy.as_str();
        raw::save_string(rdb, tmp);
        raw::save_unsigned(rdb, self.max_time_delta);
        raw::save_double(rdb, self.max_value_delta);
    }

    pub(crate) fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<SampleDuplicatePolicy> {
        let policy = rdb_load_string(rdb)?;
        let max_time_delta = raw::load_unsigned(rdb)?;
        let max_value_delta = raw::load_double(rdb)?;
        let duplicate_policy = DuplicatePolicy::try_from(policy)?;
        Ok(SampleDuplicatePolicy {
            policy: duplicate_policy,
            max_time_delta,
            max_value_delta,
        })
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

#[derive(Debug, Clone)]
pub struct TimeSeriesOptions {
    pub chunk_compression: Option<ChunkEncoding>,
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
}

impl Default for TimeSeriesOptions {
    fn default() -> Self {
        let config = get_series_config_settings();
        config.into()
    }
}

impl From<ConfigSettings> for TimeSeriesOptions {
    fn from(settings: ConfigSettings) -> Self {
        Self {
            chunk_compression: settings.chunk_encoding,
            chunk_size: Some(settings.chunk_size_bytes),
            retention: settings.retention_period,
            sample_duplicate_policy: settings.duplicate_policy,
            labels: vec![],
            rounding: settings.rounding,
            on_duplicate: None,
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
