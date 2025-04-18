use crate::common::constants::MILLIS_PER_YEAR;
use crate::common::humanize::humanize_duration_ms;
use crate::common::rounding::RoundingStrategy;
use crate::error_consts;
use crate::parser::number::parse_number;
use crate::parser::parse_duration_value;
use crate::series::chunks::{validate_chunk_size, ChunkEncoding};
use crate::series::settings::ConfigSettings;
use crate::series::DuplicatePolicy;
use lazy_static::lazy_static;
use std::sync::RwLock;
use std::time::Duration;
use valkey_module::configuration::ConfigurationContext;
use valkey_module::{ConfigurationValue, ValkeyError, ValkeyGILGuard, ValkeyResult, ValkeyString};
// See https://redis.io/docs/latest/develop/data-types/timeseries/configuration/

/// Minimal Valkey version that supports the TimeSeries Module
pub const TIMESERIES_MIN_SUPPORTED_VERSION: &[i64; 3] = &[8, 0, 0];
pub const SPLIT_FACTOR: f64 = 1.2;
const SERIES_WORKER_INTERVAL_MIN: i64 = 5000;
const SERIES_WORKER_INTERVAL_MAX: i64 = 60 * 60 * 1000 * 24;

pub(super) const SERIES_WORKER_INTERVAL_DEFAULT: &str = "5000";

pub const CHUNK_SIZE_MIN: i64 = 64;
pub const CHUNK_SIZE_MAX: i64 = 1024 * 1024;
pub const CHUNK_SIZE_DEFAULT: i64 = 4 * 1024;
pub const DECIMAL_DIGITS_MAX: i64 = 18;
pub const DECIMAL_DIGITS_MIN: i64 = 0;
pub const SIGNIFICANT_DIGITS_MAX: i64 = 18;
pub const SIGNIFICANT_DIGITS_MIN: i64 = 0;

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub const DEFAULT_CHUNK_ENCODING: ChunkEncoding = ChunkEncoding::Gorilla;
pub(super) const CHUNK_ENCODING_DEFAULT_STRING: &str = DEFAULT_CHUNK_ENCODING.name();
pub(super) const CHUNK_SIZE_DEFAULT_STRING: &str = "4096";
pub const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = DuplicatePolicy::Block;
pub(super) const DUPLICATE_POLICY_DEFAULT_STRING: &str = DEFAULT_DUPLICATE_POLICY.as_str();

pub const DEFAULT_RETENTION_PERIOD: Duration = Duration::ZERO;
pub(super) const RETENTION_POLICY_DEFAULT_STRING: &str = "0";
pub const DEFAULT_SERIES_WORKER_INTERVAL: Duration = Duration::from_secs(60);
pub(super) const IGNORE_MAX_TIME_DIFF_DEFAULT_STRING: &str = "0";
pub const IGNORE_MAX_TIME_DIFF_MIN: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MAX: i64 = i64::MAX;
pub(super) const IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING: &str = "0";
pub const IGNORE_MAX_VALUE_DIFF_MIN: f64 = 0.0;
pub const IGNORE_MAX_VALUE_DIFF_MAX: f64 = f64::MAX;

pub(super) const SIGNIFICANT_DIGITS_DEFAULT_STRING: &str = "none";
pub(super) const DECIMAL_DIGITS_DEFAULT_STRING: &str = "none";

const ONE_DAY_MS: i64 = 24 * 60 * 60 * 1000;
const ONE_YEAR_MS: i64 = 365 * ONE_DAY_MS;
const RETENTION_POLICY_MIN: i64 = 0;
pub const RETENTION_POLICY_MAX: i64 = 10 * ONE_YEAR_MS; //

lazy_static! {
    pub static ref CONFIG_SETTINGS: RwLock<ConfigSettings> = RwLock::new(ConfigSettings::default());
    pub(super) static ref CHUNK_SIZE_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, CHUNK_SIZE_DEFAULT_STRING));
    pub(super) static ref CHUNK_ENCODING_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DEFAULT_CHUNK_ENCODING.name()));
    pub(super) static ref DUPLICATE_POLICY_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(
            None,
            DEFAULT_DUPLICATE_POLICY.as_str()
        ));
    pub(super) static ref RETENTION_POLICY_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, RETENTION_POLICY_DEFAULT_STRING));
    pub(super) static ref IGNORE_MAX_TIME_DIFF_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(
            None,
            IGNORE_MAX_TIME_DIFF_DEFAULT_STRING
        ));
    pub(super) static ref IGNORE_MAX_VALUE_DIFF_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(
            None,
            IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING
        ));
    pub(super) static ref DECIMAL_DIGITS_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DECIMAL_DIGITS_DEFAULT_STRING));
    pub(super) static ref SIGNIFICANT_DIGITS_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(
            None,
            SIGNIFICANT_DIGITS_DEFAULT_STRING
        ));
    pub(super) static ref SERIES_WORKER_INTERVAL: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, SERIES_WORKER_INTERVAL_DEFAULT));
}

fn parse_duration_in_range(name: &str, value: &str, min: i64, max: i64) -> ValkeyResult<i64> {
    let duration = parse_duration_value(value).map_err(|_e| {
        ValkeyError::String(format!(
            "error parsing \"{name}\". Expected duration, got {value}"
        ))
    })?;
    if duration < 0 {
        return Err(ValkeyError::String(format!(
            "Invalid duration value ({duration}) for \"{name}\". Must be positive",
        )));
    }
    if duration < min || duration > max {
        let upper_value = if duration > (50 * MILLIS_PER_YEAR) as i64 {
            // ignore reporting upper value if it is too high
            "+inf".to_string()
        } else {
            humanize_duration_ms(max)
        };
        return Err(ValkeyError::String(format!(
            "Invalid value ({duration}) for \"{name}\". Must be in the range [{}, {upper_value}]",
            humanize_duration_ms(min),
        )));
    }
    Ok(duration)
}

fn validate_number_range(name: &str, value: f64, min: f64, max: f64) -> ValkeyResult<()> {
    if value < min || value > max {
        return Err(ValkeyError::String(format!(
            "Invalid value ({value}) for \"{name}\". Must be in the range [{min}, {max}]",
        )));
    }
    Ok(())
}

const SETTINGS_EXPECT_MSG: &str = "We expect the CONFIG_SETTINGS static to exist.";

pub(crate) fn on_string_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx);
    let value_str = v.to_string_lossy();
    match name {
        "ts-chunk-size" => {
            let chunk_size = parse_number(&value_str)? as usize;
            validate_chunk_size(chunk_size)?;

            CONFIG_SETTINGS
                .write()
                .expect(SETTINGS_EXPECT_MSG)
                .chunk_size_bytes = chunk_size;
            Ok(())
        }
        "ts-duplicate-policy" => DuplicatePolicy::try_from(value_str)
            .map(|policy| {
                CONFIG_SETTINGS
                    .write()
                    .expect(SETTINGS_EXPECT_MSG)
                    .duplicate_policy
                    .policy = policy;
            })
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY)),
        "ts-encoding" => ChunkEncoding::try_from(value_str)
            .map(|encoding| {
                CONFIG_SETTINGS
                    .write()
                    .expect(SETTINGS_EXPECT_MSG)
                    .chunk_encoding = encoding
            })
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING)),
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

pub(crate) fn on_duration_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx).to_string_lossy();
    match name {
        "ts-retention-policy" => {
            let duration =
                parse_duration_in_range(name, &v, RETENTION_POLICY_MIN, RETENTION_POLICY_MAX)?
                    as u64;
            CONFIG_SETTINGS
                .write()
                .expect(SETTINGS_EXPECT_MSG)
                .retention_period = Some(Duration::from_millis(duration));
            Ok(())
        }
        "ts-ignore-max-time-diff" => {
            let duration = parse_duration_in_range(
                name,
                &v,
                IGNORE_MAX_TIME_DIFF_MIN,
                IGNORE_MAX_TIME_DIFF_MAX,
            )? as u64;
            CONFIG_SETTINGS
                .write()
                .expect(SETTINGS_EXPECT_MSG)
                .duplicate_policy
                .max_time_delta = duration;
            Ok(())
        }
        "series-worker-interval" => {
            let duration = parse_duration_in_range(
                name,
                &v,
                SERIES_WORKER_INTERVAL_MIN,
                SERIES_WORKER_INTERVAL_MAX,
            )?;
            let interval = Duration::from_millis(duration as u64);
            CONFIG_SETTINGS
                .write()
                .expect(SETTINGS_EXPECT_MSG)
                .worker_interval = interval;
            Ok(())
        }
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

pub(crate) fn on_float_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx);
    let value_str = v.to_string_lossy();
    let value = parse_number(&value_str)
        .map_err(|_| ValkeyError::String(format!("Invalid value for {name}. Expected a number")))?;
    match name {
        "ts-ignore-max-value-diff" => {
            validate_number_range(
                "ts-ignore-max-value-diff",
                value,
                IGNORE_MAX_VALUE_DIFF_MIN,
                IGNORE_MAX_VALUE_DIFF_MAX,
            )?;
            CONFIG_SETTINGS
                .write()
                .expect(SETTINGS_EXPECT_MSG)
                .duplicate_policy
                .max_value_delta = value;
            Ok(())
        }
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

pub(super) fn on_rounding_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let value_str = val.get(config_ctx).to_string_lossy();
    if value_str.eq_ignore_ascii_case("none") {
        CONFIG_SETTINGS.write().expect(SETTINGS_EXPECT_MSG).rounding = None;
        return Ok(());
    }
    let value = parse_number(&value_str)
        .map_err(|_| ValkeyError::String(format!("Invalid value for {name}. Expected a number")))?;
    match name {
        "ts-decimal-digits" => {
            validate_number_range(
                name,
                value,
                DECIMAL_DIGITS_MIN as f64,
                DECIMAL_DIGITS_MAX as f64,
            )?;
            CONFIG_SETTINGS.write().expect(SETTINGS_EXPECT_MSG).rounding =
                Some(RoundingStrategy::DecimalDigits(value as i32));
            Ok(())
        }
        "ts-significant-digits" => {
            validate_number_range(
                name,
                value,
                SIGNIFICANT_DIGITS_MIN as f64,
                SIGNIFICANT_DIGITS_MAX as f64,
            )?;
            CONFIG_SETTINGS.write().expect(SETTINGS_EXPECT_MSG).rounding =
                Some(RoundingStrategy::SignificantDigits(value as i32));
            Ok(())
        }
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}
