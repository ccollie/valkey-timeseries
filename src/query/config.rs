use crate::commands::{parse_duration, parse_number_with_unit};
use crate::common::humanize::humanize_bytes;
use lazy_static::lazy_static;
use metricsql_runtime::prelude::SessionConfig;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::configuration::ConfigurationContext;
use valkey_module::{ConfigurationValue, ValkeyError, ValkeyGILGuard, ValkeyString};

pub static QUERY_CONTEXT_CONFIG: LazyLock<Mutex<SessionConfig>> =
    LazyLock::new(|| Mutex::new(SessionConfig::default()));

// Query Context related
// https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/app/vmselect/prometheus/prometheus.go

pub const MIN_QUERY_LEN: usize = 1; // 1 byte
pub const MAX_QUERY_LEN: usize = 1024 * 1024; // 1MB
pub const MAX_MEMORY_PER_QUERY: usize = 5 * 1024 * 1024; // 5 MB

pub(crate) const DEFAULT_MAX_QUERY_LEN_STRING: &str = "8k";
pub(crate) const DEFAULT_MAX_QUERY_LENGTH: usize = 8 * 1024; // 8k

pub const DEFAULT_QUERY_ROUNDING_DIGITS: i64 = 100; // 1 hour
pub const DEFAULT_MAX_UNIQUE_SERIES: i64 = 1000; // 1000 series
pub const DEFAULT_MAX_MEMORY_PER_QUERY: usize = 10 * 1024 * 1024; // 10 MB
pub const DEFAULT_MAX_POINTS_PER_SERIES: usize = 30000; // 1000 points per series
pub const DEFAULT_QUERY_MAX_DURATION: Duration = Duration::from_secs(5);
pub const DEFAULT_QUERY_STEP_STRING: &str = "5m";
pub const DEFAULT_QUERY_STEP: Duration = Duration::from_secs(5 * 60); // 5 minutes

lazy_static! {
    pub(crate) static ref QUERY_MAX_LENGTH_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DEFAULT_MAX_QUERY_LEN_STRING));

    pub(crate) static ref QUERY_DEFAULT_STEP_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DEFAULT_QUERY_STEP_STRING));

    pub(crate) static ref QUERY_MAX_LENGTH: AtomicI64 = AtomicI64::new(DEFAULT_MAX_QUERY_LENGTH as i64);
    pub(crate) static ref QUERY_MAX_MEMORY: AtomicI64 = AtomicI64::new(0);
    pub(crate) static ref QUERY_DEFAULT_STEP: Mutex<Duration> = Mutex::new(DEFAULT_QUERY_STEP);
    pub(crate) static ref QUERY_ROUNDING_DIGITS: AtomicI64 = AtomicI64::new(DEFAULT_QUERY_ROUNDING_DIGITS);
    pub(crate) static ref QUERY_MAX_UNIQUE_SERIES: AtomicI64 = AtomicI64::new(DEFAULT_MAX_UNIQUE_SERIES);
    pub(crate) static ref QUERY_MAX_STALENESS_INTERVAL: Mutex<Duration> = Mutex::new(Duration::ZERO); // Default is 0, which means it is automatically calculated
    pub(crate) static ref QUERY_MAX_MEMORY_PER_QUERY: AtomicU64 = AtomicU64::new(DEFAULT_MAX_MEMORY_PER_QUERY as u64);
    pub(crate) static ref QUERY_MAX_RESPONSE_SERIES: AtomicI64 = AtomicI64::new(1000);
    pub(crate) static ref QUERY_MAX_POINTS_PER_SERIES: AtomicI64 = AtomicI64::new(DEFAULT_MAX_POINTS_PER_SERIES as i64);
    pub(crate) static ref QUERY_MAX_DURATION: Mutex<Duration> = Mutex::new(DEFAULT_QUERY_MAX_DURATION);
    pub(crate) static ref QUERY_MAX_STEP_FOR_POINTS_ADJUSTMENT: Mutex<Duration> = Mutex::new(Duration::from_secs(60)); // Default 1 second
    pub(crate) static ref QUERY_STATS_ENABLED: AtomicBool = AtomicBool::default(); // Default is false
    pub(crate) static ref QUERY_SET_LOOKBACK_TO_STEP: AtomicBool = AtomicBool::default(); // Default is false
    pub(crate) static ref QUERY_TRACE_ENABLED: AtomicBool = AtomicBool::default(); // Default is false
    pub(crate) static ref QUERY_DISABLE_CACHE: AtomicBool = AtomicBool::default(); // Default is false
    pub(crate) static ref QUERY_MAX_POINTS_SUBQUERY: AtomicI64 = AtomicI64::new(DEFAULT_MAX_POINTS_PER_SERIES as i64);
}

fn parse_duration_config(name: &str, value: &str) -> Result<Duration, ValkeyError> {
    parse_duration(value)
        .map_err(|_| ValkeyError::String(format!("Invalid duration value: \"{value}\" for {name}")))
}

fn parse_bool_config(name: &str, value: &str) -> Result<bool, ValkeyError> {
    if value == "true" || value == "1" {
        Ok(true)
    } else if value == "false" || value == "0" {
        Ok(false)
    } else {
        Err(ValkeyError::String(format!(
            "Invalid boolean value: \"{value}\" for {name}"
        )))
    }
}

fn parse_i64_in_range(name: &str, v: &str, min: i64, max: i64) -> Result<i64, ValkeyError> {
    let value = parse_number_with_unit(v).map_err(|_| {
        ValkeyError::String(format!("config: number expected for {name} value: got {v}"))
    })? as i64;
    if !(min..=max).contains(&value) {
        let msg = format!("{name} must be between {min} and {max}");
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

// Custom on_set handlers to add validation and conditionally
// reject upon config change.
fn on_string_config_set<T: ConfigurationValue<ValkeyString>>(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static T,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx).to_string_lossy();
    match name {
        "ts-query-max-len" => {
            let max_length = parse_i64_in_range(name, &v, 1, MAX_QUERY_LEN as i64)?;
            QUERY_MAX_LENGTH.store(max_length, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-default-step" => {
            let interval = parse_duration_config(name, &v)?;
            *QUERY_DEFAULT_STEP
                .lock()
                .expect("config: error acquiring QUERY_DEFAULT_STEP lock") = interval;
        }
        "ts-query-round-digits" => {
            let digits = parse_i64_in_range(name, &v, 0, 36)?;
            QUERY_ROUNDING_DIGITS.store(digits, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-max-response-series" => {
            let max_series = parse_i64_in_range(name, &v, 0, 100000)?;
            QUERY_MAX_RESPONSE_SERIES.store(max_series, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-max-unique-series" => {
            let max_unique_series = parse_i64_in_range(name, &v, 0, 100000)?;
            QUERY_MAX_UNIQUE_SERIES.store(max_unique_series, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-max-memory-per-query" => {
            let max_memory = parse_number_with_unit(&v)
                .map_err(|_| ValkeyError::String(format!("Invalid value \"{v}\" for {name}")))?
                as usize;
            if !(1..=MAX_MEMORY_PER_QUERY).contains(&max_memory) {
                let msg = format!(
                    "{name} must be between 1 byte and {}",
                    humanize_bytes(MAX_MEMORY_PER_QUERY as f64)
                );
                return Err(ValkeyError::String(msg));
            }
            QUERY_MAX_MEMORY_PER_QUERY
                .store(max_memory as u64, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-max-staleness-interval" => {
            let interval = parse_duration_config(name, &v)?;
            *QUERY_MAX_STALENESS_INTERVAL
                .lock()
                .expect("config: unable to acquire QUERY_MAX_STALENESS_INTERVAL lock") = interval;
        }
        "ts-query-max-step-for-points-adjustment" => {
            let duration = parse_duration_config(name, &v)?;
            *QUERY_MAX_STEP_FOR_POINTS_ADJUSTMENT
                .lock()
                .expect("config: QUERY_MAX_STEP_FOR_POINTS lock poisoned") = duration;
        }
        "ts-query-max-query-duration" => {
            let duration = parse_duration_config(name, &v)?;
            *QUERY_MAX_DURATION
                .lock()
                .expect("config: error acquiring QUERY_MAX_DURATION lock") = duration;
        }
        "ts-query-max-points-subquery-per-series" => {
            let max_points = parse_i64_in_range(name, &v, 0, 100000)?;
            QUERY_MAX_POINTS_SUBQUERY
                .store(max_points as i64, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-stats-enabled" => {
            let enabled = parse_bool_config(name, &v)?;
            QUERY_STATS_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-trace-enabled" => {
            let enabled = parse_bool_config(name, &v)?;
            QUERY_TRACE_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
        }
        "ts-query-set-lookback-to-step" => {
            let enabled = parse_bool_config(name, &v)?;
            QUERY_SET_LOOKBACK_TO_STEP.store(enabled, std::sync::atomic::Ordering::Relaxed);
        }
        _ => {}
    }
    Ok(())
}
