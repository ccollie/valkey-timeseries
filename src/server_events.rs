use crate::common::db::{get_current_db, set_current_db};
use crate::series::index::*;
use crate::series::{get_timeseries, with_timeseries, TimeSeries};
use std::os::raw::c_void;
use std::sync::Mutex;
use valkey_module::{logging, raw, Context, NotifyEvent, ValkeyError, ValkeyResult};

static RENAME_FROM_KEY: Mutex<Vec<u8>> = Mutex::new(vec![]);
static MOVE_FROM_DB: Mutex<i32> = Mutex::new(-1);

fn handle_key_restore(ctx: &Context, key: &[u8]) {
    let _key = ctx.create_string(key);
    with_timeseries(ctx, &_key, false, |series| reindex_series(ctx, series, key))
        .expect("Unexpected panic handling series restore");
}

fn reindex_series(ctx: &Context, series: &TimeSeries, key: &[u8]) -> ValkeyResult<()> {
    with_timeseries_index(ctx, |index| {
        index.reindex_timeseries(series, key);
        Ok(())
    })
}

fn handle_key_rename(ctx: &Context, _old_key: &[u8], new_key: &[u8]) {
    let _key = ctx.create_string(new_key);
    match get_timeseries(ctx, _key, None, false) {
        Ok(Some(series)) => {
            with_timeseries_index(ctx, move |index| {
                index.remove_timeseries(&series);
                index.index_timeseries(&series, new_key);
            });
        }
        Ok(None) => {
            // ignore the error if the series is not found
        }
        Err(_e) => {
            // ignore wrong type errors
        }
    }
}

fn remove_key_from_index(ctx: &Context, key: &[u8]) {
    with_timeseries_index(ctx, |ts_index| {
        // todo: batch these and run in the background, since lookups by key can be slow
        ts_index.slow_remove_series_by_key(key)
    });
}

fn handle_loaded(ctx: &Context, key: &[u8]) {
    let _key = ctx.create_string(key);
    let _ = with_timeseries(ctx, &_key, false, |series| {
        with_timeseries_index(ctx, |index| {
            if !index.has_id(series.id) {
                index.index_timeseries(series, key);
                // On module load, our series id generator would have been reset to zero. We have to ensure
                // that after a load the counter has the value of the highest series id
                TIMESERIES_ID.fetch_max(series.id, std::sync::atomic::Ordering::Relaxed);
            } else {
                logging::log_warning("Trying to load a series that is already in the index");
            }
            Ok(())
        })
    });
}

fn handle_key_move(ctx: &Context, key: &[u8], old_db: i32) {
    let new_db = get_current_db(ctx);
    // fetch the series from the old db
    let valkey_key = ctx.create_string(key);
    set_current_db(ctx, old_db);
    let Ok(Some(series)) = get_timeseries(ctx, valkey_key, None, false) else {
        return;
    };
    with_timeseries_index(ctx, |index| {
        index.remove_timeseries(&series);
    });
    set_current_db(ctx, new_db);
    with_timeseries_index(ctx, |index| {
        index.index_timeseries(&series, key);
    });
}

pub(crate) fn generic_key_event_handler(
    ctx: &Context,
    _event_type: NotifyEvent,
    event: &str,
    key: &[u8],
) {
    // todo: AddPostNotificationJob(ctx, event, key);
    match event {
        "loaded" => {
            handle_loaded(ctx, key);
        }
        "del" | "evict" | "evicted" | "expire" | "expired" | "set" | "trimmed" => {
            remove_key_from_index(ctx, key);
        }
        "move_from" => {
            *MOVE_FROM_DB.lock().unwrap() = get_current_db(ctx);
        }
        "move_to" => {
            let mut lock = MOVE_FROM_DB.lock().unwrap();
            let old_db = *lock;
            *lock = -1;
            if old_db != -1 {
                handle_key_move(ctx, key, old_db);
            }
        }
        // SAFETY: This is safe because the key is only used in the closure and this function
        // is not called concurrently
        "rename_from" => {
            *RENAME_FROM_KEY.lock().unwrap() = key.to_vec();
        }
        "rename_to" => {
            let mut old_key = RENAME_FROM_KEY.lock().unwrap();
            if !old_key.is_empty() {
                handle_key_rename(ctx, &old_key, key);
                old_key.clear()
            }
        }
        "restore" => {
            handle_key_restore(ctx, key);
        }
        _ => {}
    }
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_END {
        let ctx = Context::new(ctx);
        let fi: &raw::RedisModuleFlushInfo = unsafe { &*(data as *mut raw::RedisModuleFlushInfo) };

        if fi.dbnum == -1 {
            clear_all_timeseries_indexes();
        } else {
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

pub fn register_server_event_handler(
    ctx: &Context,
    server_event: u64,
    inner_callback: raw::RedisModuleEventCallback,
) -> Result<(), ValkeyError> {
    let res = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: server_event,
                dataver: 1,
            },
            inner_callback,
        )
    };
    if res != raw::REDISMODULE_OK as i32 {
        return Err(ValkeyError::Str("Failed subscribing to server event"));
    }

    Ok(())
}

pub fn register_server_events(ctx: &Context) -> ValkeyResult<()> {
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_FLUSHDB, Some(on_flush_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_SWAPDB, Some(on_swap_db_event))?;
    Ok(())
}
