use crate::common::rdb::rdb_load_string;
use crate::common::rounding::RoundingStrategy;
use crate::common::{Sample, Timestamp};
use crate::config::{
    ConfigSettings, CHUNK_ENCODING, CHUNK_SIZE, CHUNK_SIZE_DEFAULT, DUPLICATE_POLICY,
    IGNORE_MAX_TIME_DIFF, IGNORE_MAX_VALUE_DIFF, RETENTION_PERIOD, ROUNDING_STRATEGY,
};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::labels::Label;
use crate::series::chunks::ChunkEncoding;
use crate::series::SeriesRef;
use get_size::GetSize;
use num_traits::Zero;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;
use std::time::Duration;
use valkey_module::{raw, ValkeyError, ValkeyResult, ValkeyValue};

#[derive(Debug, Default, PartialEq, Deserialize, Serialize, Clone, Copy, GetSize, Hash)]
/// The policy to use when a duplicate sample is encountered
pub enum DuplicatePolicy {
    /// Block the sample and return an error
    #[default]
    Block,
    /// Keep the first sample
    KeepFirst,
    /// Keep the last (current) sample
    KeepLast,
    /// Keep the minimum value of the current and old sample
    Min,
    /// Keep the maximum value of the current and old sample
    Max,
    /// Sum the current and old sample
    Sum,
}

impl Display for DuplicatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DuplicatePolicy {
    pub const fn as_str(&self) -> &'static str {
        match self {
            DuplicatePolicy::Block => "block",
            DuplicatePolicy::KeepFirst => "first",
            DuplicatePolicy::KeepLast => "last",
            DuplicatePolicy::Min => "min",
            DuplicatePolicy::Max => "max",
            DuplicatePolicy::Sum => "sum",
        }
    }

    /// Handles duplicate values for a given timestamp based on the `DuplicatePolicy`
    /// defined for the current instance.
    ///
    /// # Parameters
    /// - `self`: The current `DuplicatePolicy` instance.
    /// - `ts`: The `Timestamp` of the duplicate sample.
    /// - `old`: The previously stored value.
    /// - `new`: The newly encountered value.
    ///
    /// # Returns
    /// - `TsdbResult<f64>`: A result containing the resolved value as per the duplicate
    ///   policy, or an error if the policy is `Block` and a duplicate value is encountered.
    ///
    /// # Behavior
    /// The behavior of the method is determined by the policy represented by `self`:
    ///
    /// - **`Block`**:
    ///   - Always returns an error of type `TsdbError::DuplicateSample` with information about
    ///     the conflicting value and timestamp.
    ///
    /// - **`KeepFirst`**:
    ///   - Retains and returns the `old` value.
    ///
    /// - **`KeepLast`**:
    ///   - Replaces the `old` value with the `new` value and returns `new`.
    ///
    /// - **`Min`**:
    ///   - Returns the smaller of the `old` and `new` values (`old.min(new)`).
    ///
    /// - **`Max`**:
    ///   - Returns the larger of the `old` and `new` values (`old.max(new)`).
    ///
    /// - **`Sum`**:
    ///   - Returns the sum of `old` and `new` values (`old + new`).
    ///
    /// # Special Cases
    /// - If either `old` or `new` is NaN (Not-a-Number):
    ///   - If the policy is not `Block`, the method returns the non-NaN value
    ///     (`new` if `old` is NaN or `old` if `new` is NaN).
    ///   - If both `old` and `new` are NaN, it returns `old`.
    ///
    /// # Errors
    /// - Returns `TsdbError::DuplicateSample` if the `DuplicatePolicy` is `Block`
    ///   and a duplicate value is encountered.
    ///
    /// # Example
    /// ```rust
    /// let result = duplicate_policy.duplicate_value(ts, 42.0, 43.0);
    /// match result {
    ///     Ok(value) => println!("Resolved value: {}", value),
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// ```
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

fn get_policy_from_bytes(bytes: &[u8]) -> Option<DuplicatePolicy> {
    use DuplicatePolicy::*;
    hashify::tiny_map_ignore_case! {
        bytes,
        "block" => Block,
        "first"  => KeepFirst,
        "last"   => KeepLast,
        "min"    => Min,
        "max"    => Max,
        "sum"    => Sum,
    }
}

impl FromStr for DuplicatePolicy {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(policy) = get_policy_from_bytes(s.as_bytes()) {
            Ok(policy)
        } else {
            Err(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
        }
    }
}

impl TryFrom<&[u8]> for DuplicatePolicy {
    type Error = ValkeyError;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        get_policy_from_bytes(bytes).ok_or(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
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

/// A struct that defines the policy for determining and handling duplicate samples in a dataset.
#[derive(Copy, Clone, Default, Debug, GetSize, PartialEq)]
pub struct SampleDuplicatePolicy {
    pub policy: Option<DuplicatePolicy>,
    /// The maximum difference between the new and existing timestamp to consider them duplicates
    pub max_time_delta: u64,
    /// The maximum difference between the new and existing value to consider them duplicates
    pub max_value_delta: f64,
}

impl Hash for SampleDuplicatePolicy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.policy.hash(state);
        self.max_time_delta.hash(state);
        self.max_value_delta.to_bits().hash(state);
    }
}

impl SampleDuplicatePolicy {
    pub fn is_duplicate(
        &self,
        current_sample: &Sample,
        last_sample: &Sample,
        override_policy: Option<DuplicatePolicy>,
    ) -> bool {
        let policy = self.resolve_policy(override_policy);
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

    pub fn resolve_policy(&self, override_policy: Option<DuplicatePolicy>) -> DuplicatePolicy {
        override_policy.or(self.policy).unwrap_or_else(|| {
            *DUPLICATE_POLICY
                .lock()
                .expect("error unlocking duplicate policy")
        })
    }

    pub(crate) fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        if let Some(policy) = self.policy {
            raw::save_string(rdb, policy.as_str());
        } else {
            raw::save_string(rdb, "-");
        }
        raw::save_unsigned(rdb, self.max_time_delta);
        raw::save_double(rdb, self.max_value_delta);
    }

    pub(crate) fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<SampleDuplicatePolicy> {
        let policy = rdb_load_string(rdb)?;
        let max_time_delta = raw::load_unsigned(rdb)?;
        let max_value_delta = raw::load_double(rdb)?;
        let duplicate_policy = if policy == "-" {
            None
        } else {
            Some(DuplicatePolicy::from_str(&policy)?)
        };
        Ok(SampleDuplicatePolicy {
            policy: duplicate_policy,
            max_time_delta,
            max_value_delta,
        })
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum SampleAddResult {
    Ok(Sample),
    Duplicate,
    Ignored(Timestamp),
    TooOld,
    Error(&'static str),
    CapacityFull,
    #[default]
    InvalidKey,
    InvalidPermissions,
    InvalidValue,
    InvalidTimestamp,
    NegativeTimestamp,
}

impl SampleAddResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, SampleAddResult::Ok(_))
    }
}

impl Display for SampleAddResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SampleAddResult::Ok(sample) => write!(f, "Added {sample}"),
            SampleAddResult::Duplicate => write!(f, "{}", error_consts::DUPLICATE_SAMPLE),
            SampleAddResult::Ignored(ts) => write!(f, "Ignored. Using ts: {ts}"),
            SampleAddResult::TooOld => write!(f, "{}", error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::Error(e) => write!(f, "{e}"),
            SampleAddResult::CapacityFull => write!(f, "Capacity full"),
            SampleAddResult::InvalidKey => write!(f, "{}", error_consts::INVALID_TIMESERIES_KEY),
            SampleAddResult::InvalidPermissions => write!(f, "{}", error_consts::PERMISSION_DENIED),
            SampleAddResult::InvalidValue => write!(f, "{}", error_consts::INVALID_VALUE),
            SampleAddResult::InvalidTimestamp => write!(f, "{}", error_consts::INVALID_TIMESTAMP),
            SampleAddResult::NegativeTimestamp => write!(f, "{}", error_consts::NEGATIVE_TIMESTAMP),
        }
    }
}

impl From<SampleAddResult> for ValkeyValue {
    fn from(res: SampleAddResult) -> Self {
        match res {
            SampleAddResult::Ok(ts) => ValkeyValue::Integer(ts.timestamp),
            SampleAddResult::Ignored(ts) => ValkeyValue::Integer(ts),
            SampleAddResult::Duplicate => {
                ValkeyValue::SimpleStringStatic(error_consts::DUPLICATE_SAMPLE)
            }
            SampleAddResult::TooOld => ValkeyValue::StaticError(error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::InvalidTimestamp => {
                ValkeyValue::StaticError(error_consts::INVALID_TIMESTAMP)
            }
            SampleAddResult::InvalidPermissions => {
                ValkeyValue::StaticError(error_consts::PERMISSION_DENIED)
            }
            SampleAddResult::InvalidKey => {
                ValkeyValue::StaticError(error_consts::INVALID_TIMESERIES_KEY)
            }
            SampleAddResult::InvalidValue => ValkeyValue::StaticError(error_consts::INVALID_VALUE),
            SampleAddResult::NegativeTimestamp => {
                ValkeyValue::StaticError(error_consts::NEGATIVE_TIMESTAMP)
            }
            SampleAddResult::Error(_) => {
                ValkeyValue::StaticError(error_consts::ERROR_ADDING_SAMPLE)
            }
            SampleAddResult::CapacityFull => ValkeyValue::StaticError(error_consts::CAPACITY_FULL),
        }
    }
}

impl From<SampleAddResult> for ValkeyResult {
    fn from(result: SampleAddResult) -> Self {
        match result {
            SampleAddResult::Ok(sample) => Ok(sample.into()),
            SampleAddResult::Ignored(ts) => Ok(ValkeyValue::Integer(ts)),
            SampleAddResult::Duplicate => Err(ValkeyError::Str(error_consts::DUPLICATE_SAMPLE)),
            SampleAddResult::TooOld => Err(ValkeyError::Str(error_consts::SAMPLE_TOO_OLD)),
            SampleAddResult::InvalidTimestamp => {
                Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP))
            }
            SampleAddResult::InvalidPermissions => {
                Err(ValkeyError::Str(error_consts::PERMISSION_DENIED))
            }
            SampleAddResult::InvalidKey => {
                Err(ValkeyError::Str(error_consts::INVALID_TIMESERIES_KEY))
            }
            SampleAddResult::InvalidValue => Err(ValkeyError::Str(error_consts::INVALID_VALUE)),
            SampleAddResult::NegativeTimestamp => {
                Err(ValkeyError::Str(error_consts::NEGATIVE_TIMESTAMP))
            }
            SampleAddResult::Error(e) => Err(ValkeyError::Str(e)),
            SampleAddResult::CapacityFull => Err(ValkeyError::Str(error_consts::CAPACITY_FULL)),
        }
    }
}

/// Options for time series configuration
#[derive(Debug, Clone)]
pub struct TimeSeriesOptions {
    /// The source ID of the series, if this is a derived series
    pub src_id: Option<SeriesRef>,
    pub chunk_compression: ChunkEncoding,
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

    pub fn from_config() -> Self {
        let policy = *DUPLICATE_POLICY
            .lock()
            .expect("error unlocking duplicate policy");
        let rounding = *ROUNDING_STRATEGY
            .lock()
            .expect("error unlocking rounding strategy");
        let chunk_size = CHUNK_SIZE.load(std::sync::atomic::Ordering::SeqCst) as usize;
        let chunk_encoding = *CHUNK_ENCODING
            .lock()
            .expect("error unlocking chunk encoding");

        let max_time_delta = IGNORE_MAX_TIME_DIFF.load(std::sync::atomic::Ordering::SeqCst) as u64;

        let max_value_delta = *IGNORE_MAX_VALUE_DIFF
            .lock()
            .expect("error unlocking max value diff");

        let retention_period = *RETENTION_PERIOD
            .lock()
            .expect("error unlocking retention period");

        TimeSeriesOptions {
            retention: if retention_period.as_millis() > 0 {
                Some(retention_period)
            } else {
                None
            },
            chunk_compression: chunk_encoding,
            chunk_size: Some(chunk_size),
            rounding,
            sample_duplicate_policy: SampleDuplicatePolicy {
                policy: Some(policy),
                max_time_delta,
                max_value_delta,
            },
            ..Default::default()
        }
    }
}

impl Default for TimeSeriesOptions {
    fn default() -> Self {
        Self {
            src_id: None,
            chunk_compression: ChunkEncoding::default(),
            chunk_size: Some(CHUNK_SIZE_DEFAULT as usize),
            retention: None,
            sample_duplicate_policy: SampleDuplicatePolicy::default(),
            labels: vec![],
            rounding: None,
            on_duplicate: None,
        }
    }
}

impl From<&ConfigSettings> for TimeSeriesOptions {
    fn from(settings: &ConfigSettings) -> Self {
        Self {
            src_id: None,
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

impl From<ConfigSettings> for TimeSeriesOptions {
    fn from(settings: ConfigSettings) -> Self {
        Self::from(&settings)
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

    pub fn greater_than(value: f64) -> Self {
        Self {
            min: value,
            max: f64::MAX,
        }
    }

    pub fn less_than(value: f64) -> Self {
        Self {
            min: f64::MIN,
            max: value,
        }
    }

    pub fn is_match(&self, value: f64) -> bool {
        value >= self.min && value <= self.max
    }
}

#[cfg(test)]
mod tests {
    use super::DuplicatePolicy;
    use crate::common::Sample;
    use crate::error::TsdbError;
    use crate::series::SampleDuplicatePolicy;
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

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_keep_last() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 10,
            max_value_delta: 0.001,
        };

        // Test time delta check - within threshold
        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };
        assert!(policy.is_duplicate(&current_sample, &last_sample, None));

        // Test time delta check - outside threshold
        let current_sample = Sample {
            timestamp: 120,
            value: 100.0,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // Test value delta check - within threshold
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0005,
        };
        assert!(policy.is_duplicate(&current_sample, &last_sample, None));

        // Test value delta check - outside threshold
        let current_sample = Sample {
            timestamp: 205,
            value: 10.1,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // Test older timestamp - should return false regardless of deltas
        let current_sample = Sample {
            timestamp: 95,
            value: 10.0,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));
    }

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_with_override_policy() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::Block),
            max_time_delta: 10,
            max_value_delta: 0.001,
        };

        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };

        // With original Block policy - should not be considered duplicate
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // With override to KeepLast - should be considered duplicate
        assert!(policy.is_duplicate(
            &current_sample,
            &last_sample,
            Some(DuplicatePolicy::KeepLast)
        ));
    }

    #[test]
    fn test_sample_duplicate_policy_is_duplicate_with_zero_deltas() {
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 0, // Zero time delta
            max_value_delta: 0.001,
        };

        let last_sample = Sample {
            timestamp: 100,
            value: 10.0,
        };
        let current_sample = Sample {
            timestamp: 105,
            value: 30.0,
        };

        // With zero time delta, should not detect as duplicate based on time
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));

        // But still should detect as duplicate based on value
        let policy = SampleDuplicatePolicy {
            policy: Some(DuplicatePolicy::KeepLast),
            max_time_delta: 10,
            max_value_delta: 0.0, // Zero value delta
        };

        // With the exact same values
        let current_sample = Sample {
            timestamp: 105,
            value: 10.0,
        };
        assert!(policy.is_duplicate(&current_sample, &last_sample, None));

        // With a slight value difference
        let current_sample = Sample {
            timestamp: 125,
            value: 10.00001,
        };
        assert!(!policy.is_duplicate(&current_sample, &last_sample, None));
    }
}
