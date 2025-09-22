use crate::common::constants::MILLIS_PER_YEAR;
use crate::common::humanize::humanize_duration_ms;
use crate::common::rounding::RoundingStrategy;
use crate::error_consts;
use crate::parser::number::parse_number;
use crate::parser::parse_duration_value;
use crate::series::chunks::{ChunkEncoding, validate_chunk_size};
use crate::series::{
    DuplicatePolicy, SampleDuplicatePolicy, add_compaction_policies_from_config,
    clear_compaction_policy_config,
};
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::Duration;
use valkey_module::configuration::{
    ConfigurationContext, ConfigurationFlags, register_i64_configuration,
    register_string_configuration,
};
use valkey_module::{
    ConfigurationValue, Context, ValkeyError, ValkeyGILGuard, ValkeyResult, ValkeyString,
};

/// Minimal Valkey version that supports the TimeSeries Module
pub const TIMESERIES_MIN_SUPPORTED_VERSION: &[i64; 3] = &[8, 0, 0];
pub const SPLIT_FACTOR: f64 = 1.2;

const MULTI_SHARD_COMMAND_TIMEOUT_MIN: i64 = 500;
const MULTI_SHARD_COMMAND_TIMEOUT_MAX: i64 = 10000;
const MULTI_SHARD_COMMAND_TIMEOUT_DEFAULT: &str = "5000";

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
pub const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = DuplicatePolicy::Block;
pub const DEFAULT_RETENTION_PERIOD: Duration = Duration::ZERO;
pub const IGNORE_MAX_TIME_DIFF_DEFAULT: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MIN: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MAX: i64 = i64::MAX;
pub const IGNORE_MAX_VALUE_DIFF_MIN: f64 = 0.0;
pub const IGNORE_MAX_VALUE_DIFF_MAX: f64 = f64::MAX;

pub const MIN_THREADS: i64 = 1;
pub const MAX_THREADS: i64 = 16;
pub const DEFAULT_THREADS: i64 = 1;

const ONE_DAY_MS: i64 = 24 * 60 * 60 * 1000;
const ONE_YEAR_MS: i64 = 365 * ONE_DAY_MS;

pub const RETENTION_POLICY_MIN: i64 = 0;
pub const RETENTION_POLICY_MAX: i64 = 10 * ONE_YEAR_MS; //

// Default values as strings for Valkey configuration registration
const IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING: &str = "0";
const RETENTION_POLICY_DEFAULT_STRING: &str = "0";
const IGNORE_MAX_TIME_DIFF_DEFAULT_STRING: &str = "0";
const SIGNIFICANT_DIGITS_DEFAULT_STRING: &str = "none";
const DECIMAL_DIGITS_DEFAULT_STRING: &str = "none";
const COMPACTION_POLICY_CONFIG_NAME: &str = "ts-compaction-policy";
const COMPACTION_POLICY_DEFAULT_STRING: &str = "";
const CHUNK_ENCODING_DEFAULT_STRING: &str = DEFAULT_CHUNK_ENCODING.name();
const CHUNK_SIZE_DEFAULT_STRING: &str = "4096";

#[derive(Clone, Copy)]
pub struct ConfigSettings {
    pub retention_period: Option<Duration>,
    pub chunk_encoding: ChunkEncoding,
    pub chunk_size_bytes: usize,
    pub rounding: Option<RoundingStrategy>,
    pub duplicate_policy: SampleDuplicatePolicy,
}

impl Default for ConfigSettings {
    fn default() -> Self {
        Self {
            retention_period: None,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunk_encoding: ChunkEncoding::Gorilla,
            rounding: None,
            duplicate_policy: SampleDuplicatePolicy::default(),
        }
    }
}

pub static CHUNK_SIZE: AtomicI64 = AtomicI64::new(CHUNK_SIZE_DEFAULT);
pub static NUM_THREADS: AtomicI64 = AtomicI64::new(DEFAULT_THREADS);

lazy_static! {
    pub static ref CONFIG_SETTINGS: RwLock<ConfigSettings> = RwLock::new(ConfigSettings::default());
    pub static ref ROUNDING_STRATEGY: Mutex<Option<RoundingStrategy>> = Mutex::new(None);
    pub static ref DECIMAL_DIGITS: AtomicI64 = AtomicI64::new(DECIMAL_DIGITS_MAX);
    pub static ref SIGNIFICANT_DIGITS: AtomicI64 = AtomicI64::new(SIGNIFICANT_DIGITS_MAX);
    pub static ref IGNORE_MAX_TIME_DIFF: AtomicI64 = AtomicI64::new(IGNORE_MAX_TIME_DIFF_DEFAULT);
    pub static ref IGNORE_MAX_VALUE_DIFF: Mutex<f64> = Mutex::new(0.0);
    pub static ref RETENTION_PERIOD: Mutex<Duration> = Mutex::new(DEFAULT_RETENTION_PERIOD);
    pub static ref CHUNK_ENCODING: Mutex<ChunkEncoding> = Mutex::new(DEFAULT_CHUNK_ENCODING);
    pub static ref MULTI_SHARD_COMMAND_TIMEOUT: AtomicU64 = AtomicU64::new(5000); // ??? Move to const
    pub static ref DUPLICATE_POLICY: Mutex<DuplicatePolicy> = Mutex::new(DEFAULT_DUPLICATE_POLICY);

    static ref CHUNK_SIZE_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, CHUNK_SIZE_DEFAULT_STRING));
    static ref CHUNK_ENCODING_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DEFAULT_CHUNK_ENCODING.name()));
    static ref DUPLICATE_POLICY_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(
            None,
            DEFAULT_DUPLICATE_POLICY.as_str()
        ));
    static ref RETENTION_POLICY_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, RETENTION_POLICY_DEFAULT_STRING));
    static ref COMPACTION_POLICY_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, COMPACTION_POLICY_DEFAULT_STRING));
    static ref IGNORE_MAX_TIME_DIFF_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, IGNORE_MAX_TIME_DIFF_DEFAULT_STRING));
    static ref IGNORE_MAX_VALUE_DIFF_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING));
    static ref DECIMAL_DIGITS_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DECIMAL_DIGITS_DEFAULT_STRING));
    static ref SIGNIFICANT_DIGITS_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, SIGNIFICANT_DIGITS_DEFAULT_STRING));
    static ref MULTI_SHARD_COMMAND_TIMEOUT_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, MULTI_SHARD_COMMAND_TIMEOUT_DEFAULT));
}

#[allow(dead_code)]
fn handle_config_update_internal() {
    let policy = *DUPLICATE_POLICY
        .lock()
        .expect("error unlocking duplicate policy");
    let rounding = *ROUNDING_STRATEGY
        .lock()
        .expect("error unlocking rounding strategy");
    let chunk_size_bytes = CHUNK_SIZE.load(Ordering::Relaxed) as usize;
    let chunk_encoding = *CHUNK_ENCODING
        .lock()
        .expect("error unlocking chunk encoding");

    let max_time_delta = IGNORE_MAX_TIME_DIFF.load(Ordering::Relaxed) as u64;

    let max_value_delta = *IGNORE_MAX_VALUE_DIFF
        .lock()
        .expect("error unlocking max value diff");

    let retention_period = *RETENTION_PERIOD
        .lock()
        .expect("error unlocking retention period");

    let modified = ConfigSettings {
        retention_period: if retention_period.as_millis() > 0 {
            Some(retention_period)
        } else {
            None
        },
        chunk_encoding,
        chunk_size_bytes,
        rounding,

        duplicate_policy: SampleDuplicatePolicy {
            policy: Some(policy),
            max_time_delta,
            max_value_delta,
        },
    };

    let mut cfg = CONFIG_SETTINGS
        .write()
        .expect("Failed to acquire write lock on CONFIG_SETTINGS");

    *cfg = modified;
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

fn parse_number_in_range(name: &str, value: &str, min: f64, max: f64) -> ValkeyResult<f64> {
    let number = parse_number(value).map_err(|_e| {
        ValkeyError::String(format!(
            "error parsing \"{name}\". Expected number, got {value}"
        ))
    })?;
    validate_number_range(name, number, min, max)?;
    Ok(number)
}

fn update_chunk_size(val: &str) -> ValkeyResult<()> {
    let chunk_size = parse_number(val)? as usize;
    validate_chunk_size(chunk_size)?;
    CHUNK_SIZE.store(chunk_size as i64, Ordering::SeqCst);
    Ok(())
}

fn update_duplicate_policy(val: &str) -> ValkeyResult<()> {
    let policy = DuplicatePolicy::try_from(val)
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))?;
    *DUPLICATE_POLICY
        .lock()
        .expect("error unlocking duplicate policy") = policy;
    Ok(())
}

fn update_chunk_encoding(val: &str) -> ValkeyResult<()> {
    let encoding = ChunkEncoding::try_from(val)
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING))?;
    *CHUNK_ENCODING
        .lock()
        .expect("error unlocking chunk encoding policy") = encoding;
    Ok(())
}

fn update_compaction_policy(v: &str) -> ValkeyResult<()> {
    if v.is_empty() || v.eq_ignore_ascii_case("none") {
        clear_compaction_policy_config();
        return Ok(());
    }
    add_compaction_policies_from_config(v, true)
}

fn update_num_threads(val: &str) -> ValkeyResult<()> {
    let threads = parse_number(val)? as i64;
    if !(MIN_THREADS..=MAX_THREADS).contains(&threads) {
        return Err(ValkeyError::String(format!(
            "Invalid value ({threads}) for \"ts-num-threads\". Must be in the range [{MIN_THREADS}, {MAX_THREADS}]",
        )));
    }
    NUM_THREADS.store(threads, Ordering::SeqCst);
    Ok(())
}

fn update_retention_policy(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-retention-policy",
        val,
        RETENTION_POLICY_MIN,
        RETENTION_POLICY_MAX,
    )?;
    let duration = if duration > 0 {
        Duration::from_millis(duration as u64)
    } else {
        Duration::ZERO
    };
    *RETENTION_PERIOD
        .lock()
        .expect("retention period lock poisoned") = duration;
    Ok(())
}

fn update_ignore_max_time_diff(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-ignore-max-time-diff",
        val,
        IGNORE_MAX_TIME_DIFF_MIN,
        IGNORE_MAX_TIME_DIFF_MAX,
    )?;
    IGNORE_MAX_TIME_DIFF.store(duration, Ordering::SeqCst);
    Ok(())
}

fn update_ignore_max_value_diff(val: &str) -> ValkeyResult<()> {
    let value = parse_number_in_range(
        "ts-ignore-max-value-diff",
        val,
        IGNORE_MAX_VALUE_DIFF_MIN,
        IGNORE_MAX_VALUE_DIFF_MAX,
    )?;
    *IGNORE_MAX_VALUE_DIFF
        .lock()
        .expect("ignore max value diff lock poisoned") = value;
    Ok(())
}

fn update_multi_shard_command_timeout(val: &str) -> ValkeyResult<()> {
    let duration = parse_duration_in_range(
        "ts-multi-shard-command-timeout",
        val,
        MULTI_SHARD_COMMAND_TIMEOUT_MIN,
        MULTI_SHARD_COMMAND_TIMEOUT_MAX,
    )?;
    MULTI_SHARD_COMMAND_TIMEOUT.store(duration as u64, Ordering::SeqCst);
    Ok(())
}

fn on_string_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx).to_string_lossy();

    match name {
        "ts-chunk-size" => update_chunk_size(&v),
        "ts-duplicate-policy" => update_duplicate_policy(&v),
        "ts-encoding" => update_chunk_encoding(&v),
        "ts-compaction-policy" => update_compaction_policy(&v),
        "ts-num-threads" => update_num_threads(&v),
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

fn on_duration_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx).to_string_lossy();
    match name {
        "ts-retention-policy" => update_retention_policy(&v),
        "ts-ignore-max-time-diff" => update_ignore_max_time_diff(&v),
        "ts-multi-shard-command-timeout" => update_multi_shard_command_timeout(&v),
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

fn on_float_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx).to_string_lossy();
    match name {
        "ts-ignore-max-value-diff" => update_ignore_max_value_diff(&v),
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

fn update_decimal_digits(val: &ValkeyString) -> ValkeyResult<()> {
    let digits = if val.as_slice().eq_ignore_ascii_case(b"none") {
        0
    } else {
        parse_number_in_range(
            "ts-decimal-digits",
            &val.to_string_lossy(),
            DECIMAL_DIGITS_MIN as f64,
            DECIMAL_DIGITS_MAX as f64,
        )? as i64
    };

    if digits == 0 {
        *ROUNDING_STRATEGY
            .lock()
            .expect("rounding strategy lock poisoned") = None;
        DECIMAL_DIGITS.store(DECIMAL_DIGITS_MAX, Ordering::SeqCst);
        return Ok(());
    }
    DECIMAL_DIGITS.store(digits, Ordering::SeqCst);
    let mut strategy = ROUNDING_STRATEGY
        .lock()
        .expect("rounding strategy lock poisoned");
    match *strategy {
        Some(RoundingStrategy::DecimalDigits(_)) | None => {
            *strategy = Some(RoundingStrategy::DecimalDigits(digits as i32));
            Ok(())
        }
        Some(RoundingStrategy::SignificantDigits(_)) => Err(ValkeyError::String(
            "Cannot set both ts-decimal-digits and ts-significant-digits".to_string(),
        )),
    }
}

fn update_significant_digits(val: &ValkeyString) -> ValkeyResult<()> {
    let digits = if val.as_slice().eq_ignore_ascii_case(b"none") {
        0
    } else {
        parse_number_in_range(
            "ts-significant-digits",
            &val.to_string_lossy(),
            SIGNIFICANT_DIGITS_MIN as f64,
            SIGNIFICANT_DIGITS_MAX as f64,
        )? as i64
    };

    if digits == 0 {
        *ROUNDING_STRATEGY
            .lock()
            .expect("rounding strategy lock poisoned") = None;
        SIGNIFICANT_DIGITS.store(SIGNIFICANT_DIGITS_MAX, Ordering::SeqCst);
        return Ok(());
    }
    SIGNIFICANT_DIGITS.store(digits, Ordering::SeqCst);
    let mut strategy = ROUNDING_STRATEGY
        .lock()
        .expect("rounding strategy lock poisoned");
    match *strategy {
        Some(RoundingStrategy::SignificantDigits(_)) | None => {
            *strategy = Some(RoundingStrategy::SignificantDigits(digits as i32));
            Ok(())
        }
        Some(RoundingStrategy::DecimalDigits(_)) => Err(ValkeyError::String(
            "Cannot set both ts-decimal-digits and ts-significant-digits".to_string(),
        )),
    }
}

fn on_rounding_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx);
    match name {
        "ts-decimal-digits" => update_decimal_digits(&v),
        "ts-significant-digits" => update_significant_digits(&v),
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

fn on_thread_config_set(
    _config_ctx: &ConfigurationContext,
    _name: &str,
    atomic: &'static AtomicI64,
) -> Result<(), ValkeyError> {
    let threads = atomic.load(Ordering::SeqCst);
    log::info!("Setting number of threads to {threads}");
    // todo: reset thread pool size
    Ok(())
}

fn on_chunk_size_config_set(
    _config_ctx: &ConfigurationContext,
    _name: &str,
    atomic: &'static AtomicI64,
) -> Result<(), ValkeyError> {
    let chunk_size = atomic.load(Ordering::SeqCst) as usize;
    validate_chunk_size(chunk_size)?;
    log::info!("Setting chunk size to {chunk_size}");
    Ok(())
}

pub(super) fn register_config(ctx: &Context) {
    register_i64_configuration(
        ctx,
        "ts-chunk-size",
        &CHUNK_SIZE,
        CHUNK_SIZE_DEFAULT,
        CHUNK_SIZE_MIN,
        CHUNK_SIZE_MAX,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_chunk_size_config_set)),
    );

    register_i64_configuration(
        ctx,
        "ts-num-threads",
        &NUM_THREADS,
        DEFAULT_THREADS, // todo: num_cpus::get() as i64,
        1,
        MAX_THREADS,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_thread_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-encoding",
        &CHUNK_ENCODING_STRING,
        CHUNK_ENCODING_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_string_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-duplicate-policy",
        &DUPLICATE_POLICY_STRING,
        DEFAULT_DUPLICATE_POLICY.as_str(),
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_string_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-compaction-policy",
        &COMPACTION_POLICY_STRING,
        "",
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_string_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-decimal-digits",
        &DECIMAL_DIGITS_STRING,
        DECIMAL_DIGITS_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_rounding_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-significant-digits",
        &SIGNIFICANT_DIGITS_STRING,
        SIGNIFICANT_DIGITS_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_rounding_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-retention-policy",
        &RETENTION_POLICY_STRING,
        RETENTION_POLICY_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_duration_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-ignore-max-time-diff",
        &IGNORE_MAX_TIME_DIFF_STRING,
        IGNORE_MAX_TIME_DIFF_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_duration_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-ignore-max-value-diff",
        &IGNORE_MAX_VALUE_DIFF_STRING,
        IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_float_config_set)),
    );

    register_string_configuration::<ValkeyGILGuard<ValkeyString>>(
        ctx,
        "ts-multi-shard-command-timeout",
        &MULTI_SHARD_COMMAND_TIMEOUT_STRING,
        MULTI_SHARD_COMMAND_TIMEOUT_DEFAULT,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_duration_config_set)),
    );
}
