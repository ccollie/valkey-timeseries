use crate::commands::CommandArgIterator;
use crate::common::context::replies::*;
use crate::common::humanize::{humanize_bytes, humanize_duration};
use crate::common::rounding::RoundingStrategy;
use crate::config::{
    CHUNK_ENCODING_DEFAULT_STRING, CHUNK_SIZE_MAX, CHUNK_SIZE_MIN, CLUSTER_MAP_EXPIRATION_MAX_MS,
    CLUSTER_MAP_EXPIRATION_MIN_MS, CLUSTER_MAP_EXPIRATION_MS_DEFAULT, ConfigSettings,
    DECIMAL_DIGITS_DEFAULT, DECIMAL_DIGITS_MAX, DEFAULT_CHUNK_SIZE_BYTES, DEFAULT_RETENTION_PERIOD,
    DEFAULT_THREADS, FANOUT_COMMAND_TIMEOUT_DEFAULT, FANOUT_COMMAND_TIMEOUT_MAX,
    FANOUT_COMMAND_TIMEOUT_MIN, IGNORE_MAX_TIME_DIFF_DEFAULT, IGNORE_MAX_TIME_DIFF_MAX,
    IGNORE_MAX_TIME_DIFF_MIN, IGNORE_MAX_VALUE_DIFF_MAX, IGNORE_MAX_VALUE_DIFF_MIN, MAX_THREADS,
    MIN_THREADS, RETENTION_POLICY_MAX, RETENTION_POLICY_MIN, SIGNIFICANT_DIGITS_DEFAULT,
    SIGNIFICANT_DIGITS_MAX, get_config,
};
use std::convert::Into;
use std::fmt::Display;
use std::time::Duration;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

/// The type of value a config holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Integer,
    Float,
    String,
    Duration,
    Enum,
}

impl ConfigType {
    pub fn as_str(self) -> &'static str {
        match self {
            ConfigType::Integer => "integer",
            ConfigType::Float => "float",
            ConfigType::String => "string",
            ConfigType::Duration => "duration",
            ConfigType::Enum => "enum",
        }
    }
}

#[derive(Clone)]
enum ConfigValue {
    Integer(i64),
    Float(f64),
    String(String),
    Duration(Duration),
    Enum(String),
}

impl ConfigValue {
    fn from_duration_ms(duration_ms: i64) -> Self {
        ConfigValue::Duration(Duration::from_millis(duration_ms as u64))
    }

    fn reply(&self, ctx: &Context) {
        let _ = match self {
            ConfigValue::Integer(v) => reply_with_i64(ctx, *v),
            ConfigValue::Float(v) => reply_with_double(ctx, *v),
            ConfigValue::String(s) => reply_with_bulk_string(ctx, s),
            ConfigValue::Duration(d) => reply_with_duration(ctx, *d),
            ConfigValue::Enum(s) => reply_with_bulk_string(ctx, s),
        };
    }
}

impl Display for ConfigValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigValue::Integer(v) => write!(f, "{}", v),
            ConfigValue::Float(v) => write!(f, "{}", v),
            ConfigValue::String(s) => write!(f, "{}", s),
            ConfigValue::Duration(d) => write!(f, "{}", humanize_duration(d)),
            ConfigValue::Enum(s) => write!(f, "{}", s),
        }
    }
}

impl From<usize> for ConfigValue {
    fn from(value: usize) -> Self {
        ConfigValue::Integer(value as i64)
    }
}

impl From<i64> for ConfigValue {
    fn from(value: i64) -> Self {
        ConfigValue::Integer(value)
    }
}

impl From<f64> for ConfigValue {
    fn from(value: f64) -> Self {
        ConfigValue::Float(value)
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        ConfigValue::String(value)
    }
}

impl From<Duration> for ConfigValue {
    fn from(value: Duration) -> Self {
        ConfigValue::Duration(value)
    }
}

impl From<&'static str> for ConfigValue {
    fn from(value: &'static str) -> Self {
        ConfigValue::String(value.to_string())
    }
}

impl From<&[u8]> for ConfigValue {
    fn from(value: &[u8]) -> Self {
        ConfigValue::String(String::from_utf8_lossy(value).to_string())
    }
}

impl<T> From<Option<T>> for ConfigValue
where
    T: Into<ConfigValue>,
{
    fn from(option: Option<T>) -> Self {
        match option {
            Some(value) => value.into(),
            None => ConfigValue::String("none".to_string()),
        }
    }
}

/// Metadata describing a single configuration parameter.
struct ConfigMeta {
    name: &'static str,
    config_type: ConfigType,
    default: ConfigValue,
    min: Option<ConfigValue>,
    max: Option<ConfigValue>,
    description: &'static str,
}

impl ConfigMeta {
    /// Number of fields emitted per config in VERBOSE mode.
    const VERBOSE_FIELD_COUNT: usize = 12;
}

/// Registry of all configuration parameters.
fn get_registry() -> Vec<ConfigMeta> {
    vec![
        ConfigMeta {
            name: "ts-chunk-size",
            config_type: ConfigType::Integer,
            default: DEFAULT_CHUNK_SIZE_BYTES.into(),
            min: Some(CHUNK_SIZE_MIN.into()),
            max: Some(CHUNK_SIZE_MAX.into()),
            description: "Maximum memory used for each time series chunk in bytes",
        },
        ConfigMeta {
            name: "ts-encoding",
            config_type: ConfigType::Enum,
            default: CHUNK_ENCODING_DEFAULT_STRING.into(),
            min: None,
            max: None,
            description: "Default chunk encoding: GORILLA or UNCOMPRESSED",
        },
        ConfigMeta {
            name: "ts-duplicate-policy",
            config_type: ConfigType::Enum,
            default: "BLOCK".into(),
            min: None,
            max: None,
            description: "Policy for handling duplicate samples: BLOCK, FIRST, LAST, MIN, MAX, SUM",
        },
        ConfigMeta {
            name: "ts-retention-policy",
            config_type: ConfigType::Duration,
            default: DEFAULT_RETENTION_PERIOD.into(),
            min: Some(ConfigValue::from_duration_ms(RETENTION_POLICY_MIN)),
            max: Some(ConfigValue::from_duration_ms(RETENTION_POLICY_MAX)),
            description: "Default retention period in milliseconds (0 = no expiry)",
        },
        ConfigMeta {
            name: "ts-compaction-policy",
            config_type: ConfigType::String,
            default: "None".into(),
            min: None,
            max: None,
            description: "Default compaction rules applied to all new time series",
        },
        ConfigMeta {
            name: "ts-decimal-digits",
            config_type: ConfigType::Integer,
            default: DECIMAL_DIGITS_DEFAULT.into(),
            min: Some(0usize.into()),
            max: Some(DECIMAL_DIGITS_MAX.into()),
            description: "Round sample values to this many decimal places (none = disabled)",
        },
        ConfigMeta {
            name: "ts-significant-digits",
            config_type: ConfigType::Integer,
            default: SIGNIFICANT_DIGITS_DEFAULT.into(),
            min: Some(0usize.into()),
            max: Some(SIGNIFICANT_DIGITS_MAX.into()),
            description: "Round sample values to this many significant digits (none = disabled)",
        },
        ConfigMeta {
            name: "ts-ignore-max-time-diff",
            config_type: ConfigType::Duration,
            default: ConfigValue::from_duration_ms(IGNORE_MAX_TIME_DIFF_MIN),
            min: Some(ConfigValue::from_duration_ms(IGNORE_MAX_TIME_DIFF_MIN)),
            max: Some(ConfigValue::from_duration_ms(IGNORE_MAX_TIME_DIFF_MAX)),
            description: "Max time delta (ms) for which a duplicate sample is ignored",
        },
        ConfigMeta {
            name: "ts-ignore-max-value-diff",
            config_type: ConfigType::Float,
            default: IGNORE_MAX_TIME_DIFF_DEFAULT.into(),
            min: Some(IGNORE_MAX_VALUE_DIFF_MIN.into()),
            max: Some(IGNORE_MAX_VALUE_DIFF_MAX.into()),
            description: "Max value delta for which a duplicate sample is ignored",
        },
        ConfigMeta {
            name: "ts-num-threads",
            config_type: ConfigType::Integer,
            default: DEFAULT_THREADS.into(),
            min: Some(MIN_THREADS.into()),
            max: Some(MAX_THREADS.into()),
            description: "Number of worker threads for parallel query processing",
        },
        ConfigMeta {
            name: "ts-fanout-command-timeout",
            config_type: ConfigType::Duration,
            default: FANOUT_COMMAND_TIMEOUT_DEFAULT.into(),
            min: Some(ConfigValue::from_duration_ms(FANOUT_COMMAND_TIMEOUT_MIN)),
            max: Some(ConfigValue::from_duration_ms(FANOUT_COMMAND_TIMEOUT_MAX)),
            description: "Timeout in milliseconds for fanout (cluster scatter/gather) commands",
        },
        ConfigMeta {
            name: "ts-cluster-map-expiration-ms",
            config_type: ConfigType::Duration,
            default: ConfigValue::from_duration_ms(CLUSTER_MAP_EXPIRATION_MS_DEFAULT as i64),
            min: Some(ConfigValue::from_duration_ms(CLUSTER_MAP_EXPIRATION_MIN_MS)),
            max: Some(ConfigValue::from_duration_ms(CLUSTER_MAP_EXPIRATION_MAX_MS)),
            description: "How long (ms) cluster slot-map entries are cached (0 = no cache)",
        },
    ]
}

fn reply_with_duration(ctx: &Context, duration: Duration) -> Status {
    let value = humanize_duration(&duration);
    reply_with_str(ctx, &value)
}

fn reply_with_duration_ms(ctx: &Context, duration_ms: u64) -> Status {
    let duration = Duration::from_millis(duration_ms);
    reply_with_duration(ctx, duration)
}

fn reply_with_size(ctx: &Context, size: usize) -> Status {
    let value = humanize_bytes(size as f64);
    reply_with_str(ctx, &value)
}

fn reply_with_config_value(ctx: &Context, cfg: &ConfigMeta, settings: &ConfigSettings) {
    hashify::fnc_map_ignore_case!(cfg.name.as_bytes(),
        "ts-chunk-size" => { let _ = reply_with_usize(ctx, settings.chunk_size_bytes); },
        "ts-encoding" => { let _ = reply_with_bulk_string(ctx, settings.chunk_encoding.name()); },
        "ts-duplicate-policy" => {
            let policy = settings.duplicate_policy.policy.unwrap_or_default();
            let _ = reply_with_str(ctx, policy.as_str());
        },
        "ts-retention-policy" => {
            if let Some(retention) = settings.retention_period {
                let _ = reply_with_duration(ctx, retention);
            } else {
                let _ = reply_with_bulk_string(ctx, "none");
            }
        },
        "ts-compaction-policy" => { let _ = reply_with_bulk_string(ctx, &settings.compaction_policy); },
        "ts-decimal-digits" => {
            if let Some(RoundingStrategy::DecimalDigits(digits)) = settings.rounding {
                let _ = reply_with_i64(ctx, digits as i64);
            } else {
                let _ = reply_with_str(ctx, "none");
            }
        },
        "ts-significant-digits" => {
            if let Some(RoundingStrategy::SignificantDigits(digits)) = settings.rounding {
                let _ = reply_with_i64(ctx, digits as i64);
            } else {
                let _ = reply_with_str(ctx, "none");
            }
        },
        "ts-ignore-max-time-diff" => {
            let max_diff = settings.duplicate_policy.max_time_delta;
            let _ = reply_with_duration_ms(ctx, max_diff);
        },
        "ts-ignore-max-value-diff" => {
            let max_value = settings.duplicate_policy.max_value_delta;
            let _ = reply_with_double(ctx, max_value);
        },
        "ts-num-threads" => { let _ = reply_with_usize(ctx, settings.num_threads); },
        "ts-fanout-command-timeout" => { let _ = reply_with_duration(ctx, settings.fanout_command_timeout); },
        "ts-cluster-map-expiration-ms" => { let _ = reply_with_duration(ctx, settings.cluster_map_expiration); },
         _ => { let _ = reply_with_str(ctx, "<unknown>"); }
    );
}

/// Emits a single config entry in compact (name-only) format.
fn reply_config_compact(ctx: &Context, cfg: &ConfigMeta) {
    reply_with_bulk_string(ctx, cfg.name);
}

/// Emits a single config entry in verbose format (name, type, default, min, max, description,
/// visibility, mutable) as a flat key/value list suitable for RESP3 maps or RESP2 arrays.
fn reply_config_verbose(ctx: &Context, cfg: &ConfigMeta, settings: &ConfigSettings) {
    reply_with_array(ctx, ConfigMeta::VERBOSE_FIELD_COUNT);

    reply_with_str(ctx, "name");
    reply_with_bulk_string(ctx, cfg.name);

    reply_with_str(ctx, "type");
    reply_with_str(ctx, cfg.config_type.as_str());

    reply_with_str(ctx, "default");
    cfg.default.reply(ctx);

    reply_with_str(ctx, "min");
    match &cfg.min {
        Some(v) => v.reply(ctx),
        None => {
            let _ = reply_with_str(ctx, "none");
        }
    }

    reply_with_str(ctx, "max");
    match &cfg.max {
        Some(v) => v.reply(ctx),
        None => {
            let _ = reply_with_str(ctx, "none");
        }
    }

    // value
    reply_with_str(ctx, "value");
    reply_with_config_value(ctx, cfg, settings);
}

/// Lists configuration options with optional filtering.
///
/// Syntax: `TS._DEBUG LIST_CONFIGS [VERBOSE]`
///
/// Without VERBOSE, replies with a flat array of config names.
/// With VERBOSE, replies with an array of arrays, each containing key/value pairs
/// for name, type, default, min, max, description, and visibility.
pub(super) fn list_configs_cmd(ctx: &Context, itr: &mut CommandArgIterator) -> ValkeyResult<()> {
    let mut verbose = false;

    if let Some(arg) = itr.peek() {
        if arg.eq_ignore_ascii_case(b"verbose") {
            verbose = true;
            itr.next();
        } else {
            return Err(ValkeyError::Str(
                "Syntax error: unexpected argument, expected VERBOSE",
            ));
        }
    }

    // Apply visibility filter
    let configs = get_registry(); // In this example, all configs are visible. Add filtering logic here if needed.
    let settings_guard = get_config();

    let settings = settings_guard.clone();

    reply_with_array(ctx, configs.len());

    if verbose {
        for cfg in &configs {
            reply_config_verbose(ctx, cfg, &settings);
        }
    } else {
        for cfg in &configs {
            reply_config_compact(ctx, cfg);
        }
    }

    Ok(())
}
