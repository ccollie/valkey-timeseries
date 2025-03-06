use crate::common::rounding::RoundingStrategy;
use crate::parser::number::parse_number;
use crate::parser::parse_duration_value;
use crate::series::chunks::ChunkCompression;
use crate::series::settings::ConfigSettings;
use crate::series::{DuplicatePolicy, SampleDuplicatePolicy};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{Context, ValkeyError};
use valkey_module::{ValkeyResult, ValkeyString};

// See https://redis.io/docs/latest/develop/data-types/timeseries/configuration/

pub const SPLIT_FACTOR: f64 = 1.2;

const MILLIS_PER_SEC: u64 = 1000;
const MILLIS_PER_MIN: u64 = 60 * MILLIS_PER_SEC;
pub const DEFAULT_MAX_SERIES_LIMIT: usize = 1_000;

/// Default step used if not set.
pub const DEFAULT_STEP: Duration = Duration::from_millis(5 * MILLIS_PER_MIN);
pub const DEFAULT_ROUND_DIGITS: u8 = 0;

pub const DEFAULT_KEY_PREFIX: &str = "__vts__";

const KEY_PREFIX_KEY: &str = "KEY_PREFIX";
const QUERY_DEFAULT_STEP_KEY: &str = "query.default_step";
const QUERY_ROUND_DIGITS_KEY: &str = "query.round_digits";

const SERIES_RETENTION_KEY: &str = "RETENTION_POLICY";
const SERIES_CHUNK_ENCODING_KEY: &str = "ENCODING";
const SERIES_CHUNK_SIZE_KEY: &str = "CHUNK_SIZE_BYTES";
const SERIES_DEDUPE_INTERVAL_KEY: &str = "IGNORE_MAX_TIME_DIFF";
const SERIES_DEDUPE_VALUE_KEY: &str = "IGNORE_MAX_VALUE_DIFF";
const SERIES_DUPLICATE_POLICY_KEY: &str = "DUPLICATE_POLICY";
const SERIES_ROUND_DIGITS_KEY: &str = "ROUND_DIGITS";
const SERIES_SIGNIFICANT_DIGITS_KEY: &str = "SIGNIFICANT_DIGITS";
const SERIES_WORKER_INTERVAL_KEY: &str = "ts.worker_interval";

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub const DEFAULT_CHUNK_COMPRESSION: ChunkCompression = ChunkCompression::Gorilla;
pub const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = DuplicatePolicy::KeepLast;
pub const DEFAULT_RETENTION_PERIOD: Duration = Duration::ZERO;
pub const DEFAULT_SERIES_WORKER_INTERVAL: Duration = Duration::from_secs(60);

static _KEY_PREFIX: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(DEFAULT_KEY_PREFIX.to_string()));

static _QUERY_ROUND_DIGITS: LazyLock<Mutex<Option<u8>>> = LazyLock::new(|| Mutex::new(None));
static _QUERY_DEFAULT_STEP: LazyLock<Mutex<Duration>> = LazyLock::new(|| Mutex::new(DEFAULT_STEP));

static _SERIES_SETTINGS: LazyLock<Mutex<ConfigSettings>> =
    LazyLock::new(|| Mutex::new(ConfigSettings::default()));

pub static KEY_PREFIX: LazyLock<String> = LazyLock::new(|| {
    let key_prefix = _KEY_PREFIX.lock().unwrap();
    if key_prefix.is_empty() {
        DEFAULT_KEY_PREFIX.to_string()
    } else {
        key_prefix.clone()
    }
});

pub static QUERY_DEFAULT_STEP: LazyLock<Duration> =
    LazyLock::new(|| *_QUERY_DEFAULT_STEP.lock().unwrap());

fn find_config_value<'a>(args: &'a [ValkeyString], name: &str) -> Option<&'a ValkeyString> {
    args.iter()
        .skip_while(|item| !item.as_slice().eq(name.as_bytes()))
        .nth(1)
}

fn get_duration_config_value_ms(
    args: &[ValkeyString],
    name: &str,
    default_duration: Option<i64>,
) -> ValkeyResult<i64> {
    if let Some(value) = find_config_value(args, name) {
        let str_value = value.try_as_str()?;
        let duration = parse_duration_value(str_value, 1).map_err(|_| {
            ValkeyError::String(format!(
                "error parsing value for \"{name}\". Expected duration, got \"{str_value}\""
            ))
        })?;
        Ok(duration)
    } else {
        Ok(default_duration.unwrap_or_default()) // ????
    }
}

// Returns chrono::Duration
fn get_duration_config_value(
    args: &[ValkeyString],
    name: &str,
    default_duration: Option<Duration>,
) -> ValkeyResult<Duration> {
    get_duration_config_value_ms(args, name, default_duration.map(|d| d.as_millis() as i64))
        .map(|ms| Duration::from_millis(ms as u64))
}

fn get_optional_duration_config_value(
    args: &[ValkeyString],
    name: &str,
) -> ValkeyResult<Option<Duration>> {
    if find_config_value(args, name).is_some() {
        get_duration_config_value(args, name, None).map(Some)
    } else {
        Ok(None)
    }
}

fn get_number_config_value(
    args: &[ValkeyString],
    name: &str,
    default_value: Option<f64>,
) -> ValkeyResult<f64> {
    if let Some(value) = find_config_value(args, name) {
        let string_value = value.try_as_str()?;
        let value = parse_number(string_value).map_err(|_e| {
            ValkeyError::String(format!(
                "error parsing \"{name}\". Expected number, got for {string_value}"
            ))
        })?;
        Ok(value)
    } else {
        Ok(default_value.unwrap_or(0.0))
    }
}

fn get_optional_number_config_value(
    args: &[ValkeyString],
    name: &str,
) -> ValkeyResult<Option<f64>> {
    if find_config_value(args, name).is_some() {
        Ok(Some(get_number_config_value(args, name, None)?))
    } else {
        Ok(None)
    }
}

fn get_rounding_strategy_digit_value(
    args: &[ValkeyString],
    name: &str,
) -> ValkeyResult<Option<i32>> {
    if let Some(index) = args
        .iter()
        .position(|arg| arg.as_slice().eq(name.as_bytes()))
    {
        return if let Some(digits_str) = args.get(index + 1) {
            let digits = digits_str.parse_integer()? as i32;
            Ok(Some(digits))
        } else {
            Ok(None)
        };
    }
    Ok(None)
}

#[allow(clippy::field_reassign_with_default)]
fn load_series_config(args: &[ValkeyString]) -> ValkeyResult<()> {
    let mut config = ConfigSettings::default();

    config.retention_period = get_optional_duration_config_value(args, SERIES_RETENTION_KEY)?;
    config.chunk_size_bytes = get_number_config_value(
        args,
        SERIES_CHUNK_SIZE_KEY,
        Some(DEFAULT_CHUNK_SIZE_BYTES as f64),
    )? as usize;
    // todo: validate chunk_size_bytes

    config.duplicate_policy.max_time_delta = get_duration_config_value_ms(args, SERIES_DEDUPE_INTERVAL_KEY, Some(0))? as u64;
    config.duplicate_policy.max_value_delta = get_number_config_value(args, SERIES_DEDUPE_VALUE_KEY, Some(0.0))?;
    
    if let Some(policy) = find_config_value(args, SERIES_DUPLICATE_POLICY_KEY) {
        let temp = policy.try_as_str()?;
        if let Ok(policy) = DuplicatePolicy::try_from(temp) {
            config.duplicate_policy.policy = policy;
        } else {
            return Err(ValkeyError::String(format!(
                "Invalid value for {}: {temp}",
                SERIES_DUPLICATE_POLICY_KEY
            )));
        }
    }
    

    if let Some(compression) = find_config_value(args, SERIES_CHUNK_ENCODING_KEY) {
        let temp = compression.try_as_str()?;
        if let Ok(compression) = ChunkCompression::try_from(temp) {
            config.chunk_encoding = Some(compression);
        } else {
            return Err(ValkeyError::String(format!(
                "Error parsing compression chunk value for \"{}\", got  \"{temp}\"",
                SERIES_CHUNK_ENCODING_KEY
            )));
        }
    }

    if let Some(significant_digits) =
        get_rounding_strategy_digit_value(args, SERIES_SIGNIFICANT_DIGITS_KEY)?
    {
        if significant_digits.abs() > 18 {
            return Err(ValkeyError::String(format!(
                "Max number of significant figures for {}. Got {}",
                SERIES_SIGNIFICANT_DIGITS_KEY, significant_digits
            )));
        }
        config.rounding = Some(RoundingStrategy::SignificantDigits(significant_digits));
    }

    if let Some(decimal_digits) = get_rounding_strategy_digit_value(args, SERIES_ROUND_DIGITS_KEY)?
    {
        if decimal_digits.abs() > 18 {
            return Err(ValkeyError::String(format!(
                "Max number of decimal digits exceeded for \"{}\", got {}",
                SERIES_ROUND_DIGITS_KEY, decimal_digits
            )));
        }
        config.rounding = Some(RoundingStrategy::DecimalDigits(decimal_digits));
    }

    config.worker_interval = get_duration_config_value(
        args,
        SERIES_WORKER_INTERVAL_KEY,
        Some(DEFAULT_SERIES_WORKER_INTERVAL),
    )?;
    let mut res = _SERIES_SETTINGS
        .lock()
        .map_err(|_| ValkeyError::String("mutex lock error setting series config".to_string()))?;

    *res = config;

    Ok(())
}

pub(crate) fn get_series_config_settings() -> ConfigSettings {
    *_SERIES_SETTINGS.lock().unwrap()
}

pub fn load_config(_ctx: &Context, args: &[ValkeyString]) -> ValkeyResult<()> {
    let key_prefix = find_config_value(args, KEY_PREFIX_KEY)
        .map(|v| v.try_as_str())
        .transpose()?
        .unwrap_or(DEFAULT_KEY_PREFIX);

    let mut prefix = _KEY_PREFIX
        .lock()
        .map_err(|_| ValkeyError::String("mutex lock error setting key prefix".to_string()))?;

    *prefix = key_prefix.to_string();

    let round_digits =
        get_optional_number_config_value(args, QUERY_ROUND_DIGITS_KEY)?.map(|v| v as u8);
    let mut temp = _QUERY_ROUND_DIGITS.lock().map_err(|_| {
        ValkeyError::String("mutex lock error setting query round digits".to_string())
    })?;

    *temp = round_digits;

    let default_step = get_duration_config_value(args, QUERY_DEFAULT_STEP_KEY, Some(DEFAULT_STEP))?;
    let mut temp = _QUERY_DEFAULT_STEP.lock().map_err(|_| {
        ValkeyError::String("mutex lock error setting query default step".to_string())
    })?;

    *temp = default_step;

    load_series_config(args)?;

    Ok(())
}
