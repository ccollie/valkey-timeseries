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
use valkey_module::configuration::{
    register_string_configuration, ConfigurationContext, ConfigurationFlags,
};
use valkey_module::{
    ConfigurationValue, Context, ValkeyError, ValkeyGILGuard, ValkeyResult, ValkeyString,
};
// See https://redis.io/docs/latest/develop/data-types/timeseries/configuration/

pub const SPLIT_FACTOR: f64 = 1.2;
const SERIES_RETENTION_KEY: &str = "RETENTION_POLICY";
const SERIES_CHUNK_ENCODING_KEY: &str = "ENCODING";
const SERIES_CHUNK_SIZE_KEY: &str = "CHUNK_SIZE_BYTES";
const SERIES_DECIMAL_DIGITS_KEY: &str = "DECIMAL_DIGITS";
const SERIES_SIGNIFICANT_DIGITS_KEY: &str = "SIGNIFICANT_DIGITS";
const SERIES_WORKER_INTERVAL_KEY: &str = "WORKER_INTERVAL";
const SERIES_WORKER_INTERVAL_MIN: i64 = 5000;
const SERIES_WORKER_INTERVAL_MAX: i64 = 60 * 60 * 1000 * 24;

const SERIES_DUPLICATE_POLICY_KEY: &str = "DUPLICATE_POLICY";
pub const CHUNK_SIZE_MIN: i64 = 64;
pub const CHUNK_SIZE_MAX: i64 = 1024 * 1024;
pub const CHUNK_SIZE_DEFAULT: i64 = 4 * 1024;
pub const DECIMAL_DIGITS_MAX: i64 = 18;
pub const DECIMAL_DIGITS_MIN: i64 = 0;
pub const SIGNIFICANT_DIGITS_MAX: i64 = 18;
pub const SIGNIFICANT_DIGITS_MIN: i64 = 0;

pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 4 * 1024;
pub const DEFAULT_CHUNK_ENCODING: ChunkEncoding = ChunkEncoding::Gorilla;
const CHUNK_ENCODING_DEFAULT_STRING: &str = DEFAULT_CHUNK_ENCODING.name();
const CHUNK_SIZE_DEFAULT_STRING: &str = "4096";
pub const DEFAULT_DUPLICATE_POLICY: DuplicatePolicy = DuplicatePolicy::Block;
const DUPLICATE_POLICY_DEFAULT_STRING: &str = DEFAULT_DUPLICATE_POLICY.as_str();

pub const DEFAULT_RETENTION_PERIOD: Duration = Duration::ZERO;
const RETENTION_POLICY_DEFAULT_STRING: &str = "0";
pub const DEFAULT_SERIES_WORKER_INTERVAL: Duration = Duration::from_secs(60);

const IGNORE_MAX_TIME_DIFF_KEY: &str = "IGNORE_MAX_TIME_DIFF";
const IGNORE_MAX_TIME_DIFF_DEFAULT_STRING: &str = "0";
pub const IGNORE_MAX_TIME_DIFF_MIN: i64 = 0;
pub const IGNORE_MAX_TIME_DIFF_MAX: i64 = i64::MAX;

const IGNORE_MAX_VALUE_DIFF_KEY: &str = "IGNORE_MAX_VALUE_DIFF";
const IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING: &str = "0";
pub const IGNORE_MAX_VALUE_DIFF_MIN: f64 = 0.0;
pub const IGNORE_MAX_VALUE_DIFF_MAX: f64 = f64::MAX;

const SIGNIFICANT_DIGITS_DEFAULT_STRING: &str = "none";
const DECIMAL_DIGITS_DEFAULT_STRING: &str = "none";

const ONE_DAY_MS: i64 = 24 * 60 * 60 * 1000;
const ONE_YEAR_MS: i64 = 365 * ONE_DAY_MS;
const RETENTION_POLICY_MIN: i64 = 0;
pub const RETENTION_POLICY_MAX: i64 = 10 * ONE_YEAR_MS; //

lazy_static! {
    pub static ref CONFIG_SETTINGS: RwLock<ConfigSettings> = RwLock::new(ConfigSettings::default());
    static ref CHUNK_SIZE_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, CHUNK_SIZE_DEFAULT_STRING));
    static ref CHUNK_ENCODING_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DEFAULT_CHUNK_ENCODING.name()));
    static ref DUPLICATE_POLICY_STRING: ValkeyGILGuard<ValkeyString> = ValkeyGILGuard::new(
        ValkeyString::create(None, DEFAULT_DUPLICATE_POLICY.as_str())
    );
    static ref RETENTION_POLICY_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, RETENTION_POLICY_DEFAULT_STRING));
    static ref IGNORE_MAX_TIME_DIFF_STRING: ValkeyGILGuard<ValkeyString> = ValkeyGILGuard::new(
        ValkeyString::create(None, IGNORE_MAX_TIME_DIFF_DEFAULT_STRING)
    );
    static ref IGNORE_MAX_VALUE_DIFF_STRING: ValkeyGILGuard<ValkeyString> = ValkeyGILGuard::new(
        ValkeyString::create(None, IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING)
    );
    static ref DECIMAL_DIGITS_STRING: ValkeyGILGuard<ValkeyString> =
        ValkeyGILGuard::new(ValkeyString::create(None, DECIMAL_DIGITS_DEFAULT_STRING));
    static ref SIGNIFICANT_DIGITS_STRING: ValkeyGILGuard<ValkeyString> = ValkeyGILGuard::new(
        ValkeyString::create(None, SIGNIFICANT_DIGITS_DEFAULT_STRING)
    );
}

fn find_config_value<'a>(args: &'a [ValkeyString], name: &str) -> Option<&'a ValkeyString> {
    args.iter()
        .skip_while(|item| !item.as_slice().eq(name.as_bytes()))
        .nth(1)
}

fn get_duration_config_value_ms(
    args: &[ValkeyString],
    name: &str,
    min_value: i64,
    max_value: i64,
) -> ValkeyResult<Option<i64>> {
    if let Some(config_str) = find_config_value(args, name) {
        let config_str = config_str.try_as_str()?;
        let v = parse_duration_in_range(name, config_str, min_value, max_value)?;
        return Ok(Some(v));
    }
    Ok(None)
}

fn get_number_in_range(
    args: &[ValkeyString],
    name: &str,
    min: f64,
    max: f64,
) -> ValkeyResult<Option<f64>> {
    if let Some(value) = find_config_value(args, name) {
        let value_string = value.to_string_lossy();
        let value = parse_number_in_range(name, &value_string, min, max)?;
        return Ok(Some(value));
    }
    Ok(None)
}

fn parse_duration_in_range(name: &str, value: &str, min: i64, max: i64) -> ValkeyResult<i64> {
    let duration = parse_duration_value(value, 1).map_err(|_e| {
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

fn parse_number_in_range(name: &str, string_value: &str, min: f64, max: f64) -> ValkeyResult<f64> {
    let value = parse_number(name).map_err(|_e| {
        ValkeyError::String(format!(
            "error parsing \"{name}\". Expected number, got {string_value}"
        ))
    })?;
    validate_number_range(name, value, min, max)?;
    Ok(value)
}

fn validate_number_range(name: &str, value: f64, min: f64, max: f64) -> ValkeyResult<()> {
    if value < min || value > max {
        return Err(ValkeyError::String(format!(
            "Invalid value ({value}) for \"{name}\". Must be in the range [{min}, {max}]",
        )));
    }
    Ok(())
}

#[allow(clippy::field_reassign_with_default)]
fn load_series_config(args: &[ValkeyString]) -> ValkeyResult<()> {
    let mut config = ConfigSettings::default();

    if let Some(v) = get_duration_config_value_ms(
        args,
        SERIES_RETENTION_KEY,
        RETENTION_POLICY_MIN,
        RETENTION_POLICY_MAX,
    )? {
        config.duplicate_policy.max_time_delta = v as u64;
    }

    if let Some(v) = get_number_in_range(
        args,
        SERIES_CHUNK_SIZE_KEY,
        CHUNK_SIZE_MIN as f64,
        CHUNK_SIZE_MAX as f64,
    )? {
        let chunk_size = v as usize;
        validate_chunk_size(chunk_size)?;
        config.chunk_size_bytes = chunk_size;
    }

    if let Some(v) = get_duration_config_value_ms(
        args,
        IGNORE_MAX_TIME_DIFF_KEY,
        IGNORE_MAX_TIME_DIFF_MIN,
        IGNORE_MAX_TIME_DIFF_MAX,
    )? {
        config.duplicate_policy.max_time_delta = v as u64;
    }

    if let Some(v) = get_number_in_range(
        args,
        IGNORE_MAX_VALUE_DIFF_KEY,
        IGNORE_MAX_VALUE_DIFF_MIN,
        IGNORE_MAX_VALUE_DIFF_MAX,
    )? {
        config.duplicate_policy.max_value_delta = v;
    }

    if let Some(policy) = find_config_value(args, SERIES_DUPLICATE_POLICY_KEY) {
        let temp = policy.try_as_str()?;
        if let Ok(policy) = DuplicatePolicy::try_from(temp) {
            config.duplicate_policy.policy = policy;
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY));
        }
    }

    if let Some(compression) = find_config_value(args, SERIES_CHUNK_ENCODING_KEY) {
        let temp = compression.try_as_str()?;
        if let Ok(encoding) = ChunkEncoding::try_from(temp) {
            config.chunk_encoding = encoding;
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING));
        }
    }

    if let Some(significant_digits) = get_number_in_range(
        args,
        SERIES_SIGNIFICANT_DIGITS_KEY,
        SIGNIFICANT_DIGITS_MIN as f64,
        SIGNIFICANT_DIGITS_MAX as f64,
    )? {
        config.rounding = Some(RoundingStrategy::SignificantDigits(
            significant_digits as i32,
        ));
    }

    if let Some(decimal_digits) = get_number_in_range(
        args,
        SERIES_DECIMAL_DIGITS_KEY,
        DECIMAL_DIGITS_MIN as f64,
        DECIMAL_DIGITS_MAX as f64,
    )? {
        config.rounding = Some(RoundingStrategy::DecimalDigits(decimal_digits as i32));
    }

    if let Some(v) = get_duration_config_value_ms(
        args,
        SERIES_WORKER_INTERVAL_KEY,
        SERIES_WORKER_INTERVAL_MIN,
        SERIES_WORKER_INTERVAL_MAX,
    )? {
        config.worker_interval = Duration::from_millis(v as u64);
    }

    let mut res = CONFIG_SETTINGS
        .write()
        .expect("mutex lock error setting series config");

    *res = config;

    Ok(())
}

pub fn load_config(_ctx: &Context, args: &[ValkeyString]) -> ValkeyResult<()> {
    load_series_config(args)?;

    Ok(())
}

const SETTINGS_EXPECT_MSG: &str = "We expect the CONFIG_SETTINGS static to exist.";

fn on_string_config_set(
    config_ctx: &ConfigurationContext,
    name: &str,
    val: &'static ValkeyGILGuard<ValkeyString>,
) -> Result<(), ValkeyError> {
    let v = val.get(config_ctx);
    let value_str = v.to_string_lossy();
    match name {
        "ts-chunk-size" => {
            let chunk_size = parse_number(&value_str)?;
            validate_chunk_size(chunk_size as usize)?;
            CONFIG_SETTINGS
                .write()
                .expect(SETTINGS_EXPECT_MSG)
                .chunk_size_bytes = chunk_size as usize;
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

fn on_duration_config_set(
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
        _ => Err(ValkeyError::Str("Unknown configuration parameter")),
    }
}

fn on_float_config_set(
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

fn on_rounding_config_set(
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

pub(super) fn register_config_handlers(ctx: &Context) {
    register_string_configuration(
        ctx,
        "ts-duplicate-policy",
        &*DUPLICATE_POLICY_STRING,
        DUPLICATE_POLICY_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_string_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-chunk-size",
        &*CHUNK_SIZE_STRING,
        CHUNK_SIZE_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_string_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-chunk-encoding",
        &*CHUNK_ENCODING_STRING,
        CHUNK_ENCODING_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_string_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-retention-policy",
        &*RETENTION_POLICY_STRING,
        RETENTION_POLICY_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_duration_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-ignore-max-time-diff",
        &*IGNORE_MAX_TIME_DIFF_STRING,
        IGNORE_MAX_TIME_DIFF_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_duration_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-ignore-max-value-diff",
        &*IGNORE_MAX_VALUE_DIFF_STRING,
        IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_float_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-significant-digits",
        &*SIGNIFICANT_DIGITS_STRING,
        SIGNIFICANT_DIGITS_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_rounding_config_set)),
    );
    register_string_configuration(
        ctx,
        "ts-decimal-digits",
        &*DECIMAL_DIGITS_STRING,
        DECIMAL_DIGITS_DEFAULT_STRING,
        ConfigurationFlags::DEFAULT,
        None,
        Some(Box::new(on_rounding_config_set)),
    );
}
