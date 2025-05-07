extern crate get_size;
extern crate strum;
extern crate strum_macros;
extern crate valkey_module_macros;

use crate::fanout::register_cluster_message_handlers;
use valkey_module::{
    configuration::ConfigurationFlags, logging, valkey_module, Context, Status, ValkeyString,
    Version,
};

pub mod aggregators;
pub(crate) mod commands;
pub mod common;
pub mod config;
mod error;
pub mod error_consts;
mod fanout;
pub mod iterators;
mod join;
mod labels;
mod parser;
mod series;
mod server_events;
mod tests;

use crate::series::index::init_croaring_allocator;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{start_series_background_worker, stop_series_background_worker};
use crate::server_events::{generic_key_event_handler, register_server_events};

pub const VK_TIMESERIES_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "ts";

pub fn valid_server_version(version: Version) -> bool {
    let server_version = &[
        version.major.into(),
        version.minor.into(),
        version.patch.into(),
    ];
    server_version >= config::TIMESERIES_MIN_SUPPORTED_VERSION
}

fn preload(ctx: &Context, args: &[ValkeyString]) -> Status {
    // perform preload validations here, useful for MODULE LOAD
    // unlike init which is called at the end of the valkey_module! macro this is called at the beginning
    let version = ctx.get_server_version().unwrap();
    ctx.log_notice(&format!(
        "preload for server version {:?} with args: {:?}",
        version, args
    ));

    let ver = ctx
        .get_server_version()
        .expect("Unable to get server version!");

    if !valid_server_version(ver) {
        ctx.log_warning(
            format!(
                "The minimum supported Valkey server version for the valkey-timeseries module is {:?}",
                config::TIMESERIES_MIN_SUPPORTED_VERSION
            )
                .as_str(),
        );
        return Status::Err;
    }

    // respond with either Status::Ok or Status::Err (if you want to prevent module loading)
    Status::Ok
}

fn initialize(ctx: &Context, _args: &[ValkeyString]) -> Status {
    init_croaring_allocator();

    start_series_background_worker(ctx);

    // todo: check if we're clustered first
    register_cluster_message_handlers(ctx);

    match register_server_events(ctx) {
        Ok(_) => {
            logging::log_debug("After initializing server events");
            Status::Ok
        }
        Err(e) => {
            let msg = format!("Failed to register server events: {}", e);
            ctx.log_warning(&msg);
            Status::Err
        }
    }
}

fn deinitialize(ctx: &Context) -> Status {
    ctx.log_notice("deinitialize");
    stop_series_background_worker(ctx);
    Status::Ok
}

#[cfg(not(test))]
macro_rules! get_allocator {
    () => {
        valkey_module::alloc::ValkeyAlloc
    };
}

#[cfg(test)]
macro_rules! get_allocator {
    () => {
        std::alloc::System
    };
}

valkey_module! {
    name: MODULE_NAME,
    version: VK_TIMESERIES_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VK_TIME_SERIES_TYPE],
    preload: preload,
    init: initialize,
    deinit: deinitialize,
    acl_categories: [
        "ts",
    ]
    commands: [
        ["TS.CREATE", commands::create, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.ALTER", commands::alter_series, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.ADD", commands::add, "write fast deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.GET", commands::get, "readonly fast", 1, 1, 1, "fast read timeseries"],
        ["TS.MGET", commands::mget, "readonly fast", 0, 0, -1, "fast read timeseries"],
        ["TS.MADD", commands::madd, "write deny-oom", 1, -1, 3, "fast write timeseries"],
        ["TS.DEL", commands::del, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.DECRBY", commands::decrby, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.INCRBY", commands::incrby, "write deny-oom", 1, 1, 1, "write timeseries"],
        ["TS.JOIN", commands::join, "readonly", 1, 2, 1, "read timeseries"],
        ["TS.MREVRANGE", commands::mrevrange, "readonly deny-oom", 0, 0, -1, "fast read timeseries"],
        ["TS.MRANGE", commands::mrange, "readonly deny-oom", 0, 0, -1, "fast read timeseries"],
        ["TS.RANGE", commands::range, "readonly deny-oom", 1, 1, 1, "fast read timeseries"],
        ["TS.REVRANGE", commands::rev_range, "readonly deny-oom", 1, 1, 1, "fast read timeseries"],
        ["TS.INFO", commands::info, "readonly", 0, 0, 0, "fast read timeseries"],
        ["TS.QUERYINDEX", commands::query_index, "readonly", 0, 0, 0, "fast read timeseries"],
        ["TS.CARD", commands::cardinality, "readonly fast", 0, 0, 0, "fast read timeseries"],
        ["TS.LABELNAMES", commands::label_names, "readonly fast", 0, 0, 0, "fast read timeseries"],
        ["TS.LABELVALUES", commands::label_values, "readonly fast", 0, 0, 0, "fast read timeseries"],
        ["TS.STATS", commands::stats, "readonly", 0, 0, 0, "read timeseries"],
    ]
    event_handlers: [
        [@SET @STRING @GENERIC @EVICTED @EXPIRED : generic_key_event_handler]
    ]
    configurations: [
        i64: [
            ["ts-num-threads", &*config::NUM_THREADS, config::DEFAULT_THREADS, config::MIN_THREADS, config::MAX_THREADS, ConfigurationFlags::DEFAULT, None],
        ],
        string: [
            ["ts-chunk-size", &*config::CHUNK_SIZE_STRING, config::CHUNK_SIZE_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_string_config_set))],
            ["ts-series-worker-interval", &*config::SERIES_WORKER_INTERVAL_STRING, config::SERIES_WORKER_INTERVAL_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_duration_config_set))],
            ["ts-encoding", &*config::CHUNK_ENCODING_STRING, config::CHUNK_ENCODING_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_string_config_set))],
            ["ts-decimal-digits", &*config::DECIMAL_DIGITS_STRING, config::DECIMAL_DIGITS_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_rounding_config_set))],
            ["ts-duplicate-policy", &*config::DUPLICATE_POLICY_STRING, config::DUPLICATE_POLICY_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_string_config_set))],
            ["ts-ignore-max-time-diff", &*config::IGNORE_MAX_TIME_DIFF_STRING, config::IGNORE_MAX_TIME_DIFF_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_duration_config_set))],
            ["ts-ignore-max-value-diff", &*config::IGNORE_MAX_VALUE_DIFF_STRING, config::IGNORE_MAX_VALUE_DIFF_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_float_config_set))],
            ["ts-retention-policy", &*config::RETENTION_POLICY_STRING, config::RETENTION_POLICY_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_duration_config_set))],
            ["ts-significant-digits", &*config::SIGNIFICANT_DIGITS_STRING, config::SIGNIFICANT_DIGITS_DEFAULT_STRING, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_rounding_config_set))],
            ["ts-multi-shard-command-timeout", &*config::MULTI_SHARD_COMMAND_TIMEOUT_STRING, config::MULTI_SHARD_COMMAND_TIMEOUT_DEFAULT, ConfigurationFlags::DEFAULT, None, Some(Box::new(config::on_duration_config_set))],
        ],
        bool: [],
        enum: [],
        module_args_as_configuration: true,
    ]
}

// todo: handle @TRIMMED
