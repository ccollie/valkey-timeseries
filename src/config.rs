use crate::common::constants::MILLIS_PER_YEAR;
use crate::common::humanize::humanize_duration_ms;
use crate::common::rounding::RoundingStrategy;
use crate::error_consts;
use crate::parser::number::parse_number;
use crate::parser::parse_duration_value;
use crate::series::chunks::{validate_chunk_size, ChunkEncoding};
use crate::series::{DuplicatePolicy, SampleDuplicatePolicy};
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::{Mutex, RwLock};
use std::time::Duration;
use valkey_module::configuration::ConfigurationContext;
use valkey_module::{
    ConfigurationValue, Context, ValkeyError, ValkeyGILGuard, ValkeyResult, ValkeyString,
};
use valkey_module_macros::config_changed_event_handler;
// See https://redis.io/docs/latest/develop/data-types/timeseries/configuration/

/// Minimal Valkey version that supports the TimeSeries Module
pub const TIMESERIES_MIN_SUPPORTED_VERSION: &[i64; 3] = &[8, 0, 0];
pub const SPLIT_FACTOR: f64 = 1.2;
pub(super) const SERIES_WORKER_INTERVAL_MIN: i64 = 5000;
pub(super) const SERIES_WORKER_INTERVAL_MAX: i64 = 60 * 60 * 1000 * 24;

pub(super) const SERIES_WORKER_INTERVAL_DEFAULT: &str = "5000";

pub const CHUNK_SIZE_MIN: i64 = 64;
pub const CHUNK_SIZE_MAX: i64 = 1024 * 1024;
pub const CHUNK_SIZE_DEFAULT: i64 = 4 * 1024;
pub const DECIMAL_DIGITS_MAX: i64 = 18;
pub const DECIMAL_DIGITS_MIN: i64 = 0;
pub const DECIMAL_DIGITS_DEFAULT: i64 = 18;
pub const SIGNIFICANT_DIGITS_MAX: i64 = 18;
pub const SIGNIFICANT_DIGITS_MIN: i64 = 0;
pub const SIGNIFICANT_DIGITS_DEFAULT: i64 = 18;

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
pub const IGNORE_MAX_TIME_DIFF_DEFAULT: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MIN: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MAX: i64 = i64::MAX;
pub(super) const IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING: &str = "0";
pub const IGNORE_MAX_VALUE_DIFF_MIN: f64 = 0.0;
pub const IGNORE_MAX_VALUE_DIFF_MAX: f64 = f64::MAX;

pub(super) const SIGNIFICANT_DIGITS_DEFAULT_STRING: &str = "none";
pub(super) const DECIMAL_DIGITS_DEFAULT_STRING: &str = "none";

pub const MIN_THREADS: i64 = 1;
pub const MAX_THREADS: i64 = 16;
pub const DEFAULT_THREADS: i64 = 1;

const ONE_DAY_MS: i64 = 24 * 60 * 60 * 1000;
const ONE_YEAR_MS: i64 = 365 * ONE_DAY_MS;
const RETENTION_POLICY_MIN: i64 = 0;
pub const RETENTION_POLICY_MAX: i64 = 10 * ONE_YEAR_MS; //

#[derive(Clone, Copy)]
pub struct ConfigSettings {
    pub retention_period: Option<Duration>,
    pub chunk_encoding: ChunkEncoding,
    pub chunk_size_bytes: usize,
    pub rounding: Option<RoundingStrategy>,
    pub worker_interval: Duration,
    pub duplicate_policy: SampleDuplicatePolicy,
}

impl Default for ConfigSettings {
    fn default() -> Self {
        Self {
            retention_period: None,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunk_encoding: ChunkEncoding::Gorilla,
            worker_interval: DEFAULT_SERIES_WORKER_INTERVAL,
            rounding: None,
            duplicate_policy: SampleDuplicatePolicy::default(),
        }
    }
}

lazy_static! {
    pub static ref CONFIG_SETTINGS: RwLock<ConfigSettings> = RwLock::new(ConfigSettings::default());
    pub static ref CHUNK_SIZE: AtomicI64 = AtomicI64::new(CHUNK_SIZE_DEFAULT);
    pub static ref ROUNDING_STRATEGY: Mutex<Option<RoundingStrategy>> = Mutex::new(None);
    pub static ref DECIMAL_DIGITS: AtomicI64 = AtomicI64::new(DECIMAL_DIGITS_MAX);
    pub static ref SIGNIFICANT_DIGITS: AtomicI64 = AtomicI64::new(SIGNIFICANT_DIGITS_MAX);
    pub static ref IGNORE_MAX_TIME_DIFF: AtomicI64 = AtomicI64::new(IGNORE_MAX_TIME_DIFF_DEFAULT);
    pub static ref IGNORE_MAX_VALUE_DIFF: Mutex<f64> = Mutex::new(0.0);
    pub static ref RETENTION_PERIOD: Mutex<Duration> = Mutex::new(DEFAULT_RETENTION_PERIOD);
    pub static ref CHUNK_ENCODING: Mutex<ChunkEncoding> = Mutex::new(DEFAULT_CHUNK_ENCODING);
    pub static ref SERIES_WORKER_INTERVAL: AtomicU64 =
        AtomicU64::new(SERIES_WORKER_INTERVAL_MIN as u64);
    pub static ref DUPLICATE_POLICY: Mutex<DuplicatePolicy> = Mutex::new(DEFAULT_DUPLICATE_POLICY);
    pub static ref NUM_THREADS: AtomicI64 = AtomicI64::new(DEFAULT_THREADS);
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
    pub(super) static ref SERIES_WORKER_INTERVAL_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, SERIES_WORKER_INTERVAL_DEFAULT));
}

#[allow(dead_code)]
fn handle_config_update() {
    let policy = *DUPLICATE_POLICY
        .lock()
        .expect("error unlocking duplicate policy");
    let rounding = *ROUNDING_STRATEGY
        .lock()
        .expect("error unlocking rounding strategy");
    let chunk_size_bytes = CHUNK_SIZE.load(std::sync::atomic::Ordering::SeqCst) as usize;
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

    let series_worker_interval = SERIES_WORKER_INTERVAL.load(std::sync::atomic::Ordering::SeqCst);

    let modified = ConfigSettings {
        retention_period: if retention_period.as_millis() > 0 {
            Some(retention_period)
        } else {
            None
        },
        chunk_encoding,
        chunk_size_bytes,
        rounding,
        worker_interval: Duration::from_millis(series_worker_interval),
        duplicate_policy: SampleDuplicatePolicy {
            policy,
            max_time_delta,
            max_value_delta,
        },
    };

    let mut cfg = CONFIG_SETTINGS
        .write()
        .expect("Failed to acquire write lock on CONFIG_SETTINGS");

    *cfg = modified;
}

#[config_changed_event_handler]
fn config_changed_event_handler(ctx: &Context, _changed_configs: &[&str]) {
    ctx.log_notice("config changed");
    // handle_config_update()
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

pub(crate) fn on_string_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let value_str = val.get(config_ctx).to_string_lossy();
    match name {
        "ts-chunk-size" => {
            let chunk_size = parse_number(&value_str)? as usize;
            validate_chunk_size(chunk_size)?;
            CHUNK_SIZE.store(chunk_size as i64, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        "ts-duplicate-policy" => DuplicatePolicy::try_from(value_str)
            .map(|policy| {
                *DUPLICATE_POLICY
                    .lock()
                    .expect("duplicate policy lock poisoned") = policy;
            })
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY)),
        "ts-encoding" => ChunkEncoding::try_from(value_str)
            .map(|encoding| {
                *CHUNK_ENCODING
                    .lock()
                    .expect("chunk encoding policy lock poisoned") = encoding;
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
                parse_duration_in_range(name, &v, RETENTION_POLICY_MIN, RETENTION_POLICY_MAX)?;

            *RETENTION_PERIOD
                .lock()
                .expect("retention period lock poisoned") = Duration::from_millis(duration as u64);

            Ok(())
        }
        "ts-ignore-max-time-diff" => {
            let duration = parse_duration_in_range(
                name,
                &v,
                IGNORE_MAX_TIME_DIFF_MIN,
                IGNORE_MAX_TIME_DIFF_MAX,
            )?;
            IGNORE_MAX_TIME_DIFF.store(duration, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        "ts-series-worker-interval" => {
            let duration = parse_duration_in_range(
                name,
                &v,
                SERIES_WORKER_INTERVAL_MIN,
                SERIES_WORKER_INTERVAL_MAX,
            )?;
            SERIES_WORKER_INTERVAL.store(duration as u64, std::sync::atomic::Ordering::SeqCst);

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

            let mut ignore_max_value_diff = IGNORE_MAX_VALUE_DIFF
                .lock()
                .expect("ignore max value diff lock poisoned");

            *ignore_max_value_diff = value;
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
    fn handle_update(val: Option<RoundingStrategy>) -> Result<(), ValkeyError> {
        let mut strategy = ROUNDING_STRATEGY
            .lock()
            .expect("rounding strategy lock poisoned");
        *strategy = val;
        Ok(())
    }

    let value_str = val.get(config_ctx).to_string_lossy();
    let digits = if value_str.eq_ignore_ascii_case("none") {
        return handle_update(None);
    } else {
        let digits = parse_number(&value_str).map_err(|_| {
            ValkeyError::String(format!("Invalid value for {name}. Expected a number"))
        })? as i32;
        if digits < 0 {
            return Err(ValkeyError::String(format!(
                "Invalid value for {name}. Expected a positive number"
            )));
        }
        digits
    };

    match name {
        "ts-decimal-digits" => {
            validate_number_range(
                name,
                digits as f64,
                DECIMAL_DIGITS_MIN as f64,
                DECIMAL_DIGITS_MAX as f64,
            )?;

            handle_update(Some(RoundingStrategy::DecimalDigits(digits)))
        }
        "ts-significant-digits" => {
            validate_number_range(
                name,
                digits as f64,
                SIGNIFICANT_DIGITS_MIN as f64,
                SIGNIFICANT_DIGITS_MAX as f64,
            )?;

            handle_update(Some(RoundingStrategy::SignificantDigits(digits)))
        }
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}
