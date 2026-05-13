use crate::common::context::{get_current_db, register_server_event_handler, set_current_db};
use crate::series::index::*;
use crate::series::{TimeSeries, get_timeseries, get_timeseries_mut};
use std::os::raw::c_void;
use std::sync::Mutex;
use valkey_module::{Context, NotifyEvent, ValkeyResult, logging, raw};

static RENAME_FROM_KEY: Mutex<Vec<u8>> = Mutex::new(vec![]);
static MOVE_FROM_DB: Mutex<i32> = Mutex::new(-1);

fn handle_key_restore(ctx: &Context, key: &[u8]) {
    handle_index_key_restore(ctx, key);
}

fn reindex_series(ctx: &Context, series: &TimeSeries, key: &[u8]) -> ValkeyResult<()> {
    let index = get_timeseries_index(ctx);
    index.reindex_timeseries(series, key);
    Ok(())
}

fn handle_key_rename(ctx: &Context, _old_key: &[u8], new_key: &[u8]) {
    let index = get_timeseries_index(ctx);
    let key = ctx.create_string(new_key);
    let Ok(Some(series)) = get_timeseries(ctx, &key, None, false) else {
        logging::log_warning("Failed to load series for key rename");
        return;
    };
    index.reindex_timeseries(&series, new_key);
}

/// Handle the "loaded" event, which is triggered for each key loaded from disk during server startup.
fn handle_loaded(ctx: &Context, key: &[u8]) {
    handle_index_key_restore(ctx, key);
}

fn handle_key_move(ctx: &Context, key: &[u8], old_db: i32) {
    let new_db = get_current_db(ctx);
    // fetch the series from the new
    let valkey_key = ctx.create_string(key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &valkey_key, false, None) else {
        logging::log_warning("Failed to load series for key move");
        return;
    };

    // remove the series from the old db index
    let old_index = get_db_index(old_db);
    old_index.remove_timeseries(&series);

    // add the series to the new db index
    series._db = Some(new_db);
    let new_index = get_db_index(new_db);
    new_index.index_timeseries(&series, key);
}

pub(super) fn generic_key_events_handler(
    ctx: &Context,
    _event_type: NotifyEvent,
    event: &str,
    key: &[u8],
) {
    hashify::fnc_map!(event.as_bytes(),
        "loaded" => {
            handle_loaded(ctx, key);
        },
        "move_from" => {
            *MOVE_FROM_DB.lock().unwrap() = get_current_db(ctx);
        },
        "move_to" => {
            let mut lock = MOVE_FROM_DB.lock().unwrap();
            let old_db = *lock;
            *lock = -1;
            if old_db != -1 {
                handle_key_move(ctx, key, old_db);
            }
        },
        "rename_from" => {
            *RENAME_FROM_KEY.lock().unwrap() = key.to_vec();
        },
        "rename_to" => {
            let mut old_key = RENAME_FROM_KEY.lock().unwrap();
            if !old_key.is_empty() {
                handle_key_rename(ctx, &old_key, key);
                old_key.clear();
            }
        },
        "restore" => {
            handle_key_restore(ctx, key);
        },
        _ => {}
    );
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_END {
        let fi: &raw::RedisModuleFlushInfo = unsafe { &*(data as *mut raw::RedisModuleFlushInfo) };

        if fi.dbnum == -1 {
            clear_all_timeseries_indexes();
        } else {
            let ctx = Context::new(ctx);
            set_current_db(&ctx, fi.dbnum);
            clear_timeseries_index(&ctx);
        }
    };
}

unsafe extern "C" fn on_swap_db_event(
    _ctx: *mut raw::RedisModuleCtx,
    eid: raw::RedisModuleEvent,
    _sub_event: u64,
    data: *mut c_void,
) {
    if eid.id == raw::REDISMODULE_EVENT_SWAPDB {
        let ei: &raw::RedisModuleSwapDbInfo =
            unsafe { &*(data as *mut raw::RedisModuleSwapDbInfo) };

        let from_db = ei.dbnum_first;
        let to_db = ei.dbnum_second;

        swap_timeseries_index_dbs(from_db, to_db);
    }
}

pub fn register_server_events(ctx: &Context) -> ValkeyResult<()> {
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_FLUSHDB, Some(on_flush_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_SWAPDB, Some(on_swap_db_event))?;
    Ok(())
}
