extern crate get_size;
extern crate valkey_module_macros;
extern crate strum;
extern crate strum_macros;

use crate::config::load_config;
use crate::module::VK_TIME_SERIES_TYPE;
use valkey_module::{logging, valkey_module, Context, Status, ValkeyString};
use valkey_module_macros::config_changed_event_handler;

pub mod aggregators;
mod arg_types;
pub mod common;
pub mod config;
mod error;
pub mod error_consts;
pub mod iterators;
mod join;
mod labels;
mod module;
mod parser;
mod series;
mod tests;

use crate::module::server_events::{generic_key_event_handler, register_server_events};
use crate::series::settings::{start_series_background_worker, stop_series_background_worker};
use module::*;

pub const VKMETRICS_VERSION: i32 = 1;
pub const MODULE_NAME: &str = "ts";

fn initialize(ctx: &Context, args: &[ValkeyString]) -> Status {
    logging::log_debug("initialize");

    if load_config(ctx, args).is_err() {
        logging::log_warning("Failed to load configuration");
        return Status::Err;
    }

    start_series_background_worker();

    match register_server_events(ctx) {
        Ok(_) => Status::Ok,
        Err(e) => {
            let msg = format!("Failed to register server events: {}", e);
            logging::log_warning(msg);
            Status::Err
        }
    }
}

fn deinitialize(_ctx: &Context) -> Status {
    logging::log_notice("deinitialize");
    stop_series_background_worker();
    Status::Ok
}

#[config_changed_event_handler]
fn config_changed_event_handler(ctx: &Context, _changed_configs: &[&str]) {
    ctx.log_notice("config changed")
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

// https://github.com/redis/redis/blob/a38c29b6c861ee59637acdb1618f8f84645061d5/src/module.c
valkey_module! {
    name: MODULE_NAME,
    version: VKMETRICS_VERSION,
    allocator: (get_allocator!(), get_allocator!()),
    data_types: [VK_TIME_SERIES_TYPE],
    init: initialize,
    deinit: deinitialize,
    commands: [
        ["TS.CREATE", commands::create, "write deny-oom", 1, 1, 1],
        ["TS.ALTER", commands::alter_series, "write deny-oom", 1, 1, 1],
        ["TS.ADD", commands::add, "write fast deny-oom", 1, 1, 1],
        ["TS.GET", commands::get, "readonly fast", 1, 1, 1],
        ["TS.MGET", commands::mget, "readonly fast", 0, 0, -1],
        ["TS.MADD", commands::madd, "write deny-oom", 1, -1, 3],
        ["TS.DEL", commands::del, "write deny-oom", 1, 1, 1],
        ["TS.DECRBY", commands::decrby, "write deny-oom", 1, 1, 1],
        ["TS.INCRBY", commands::incrby, "write deny-oom", 1, 1, 1],
        ["TS.JOIN", commands::join, "readonly", 1, 2, 1],
        ["TS.MREVRANGE", commands::mrevrange, "readonly deny-oom", 0, 0, -1],
        ["TS.MRANGE", commands::mrange, "readonly deny-oom", 0, 0, -1],
        ["TS.RANGE", commands::range, "readonly deny-oom", 1, 1, 1],
        ["TS.REVRANGE", commands::rev_range, "readonly deny-oom", 1, 1, 1],
        ["TS.INFO", commands::info, "readonly", 0, 0, 0],
        ["TS.QUERYINDEX", commands::query_index, "readonly", 0, 0, 0],
        ["TS.CARD", commands::cardinality, "readonly fast", 0, 0, 0],
        ["TS.LABELNAMES", commands::label_names, "readonly fast", 0, 0, 0],
        ["TS.LABELVALUES", commands::label_values, "readonly fast", 0, 0, 0],
        ["TS.STATS", commands::stats, "readonly", 0, 0, 0],
    ],
     event_handlers: [
        [@SET @STRING @GENERIC @EVICTED @EXPIRED : generic_key_event_handler]
    ],
}

// todo: handle @TRIMMED
