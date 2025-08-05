use valkey_module::{logging, RedisModuleIO, RedisModuleTypeMethods};
use valkey_module::{
    native_types::ValkeyType, RedisModuleDefragCtx, RedisModuleDigest, RedisModuleString,
    ValkeyString,
};
use valkey_module::{Context, REDISMODULE_AUX_BEFORE_RDB};

use crate::common::db::get_current_db;
use crate::series::defrag_series;
use crate::series::index::{
    get_timeseries_index_for_db, next_timeseries_id, with_timeseries_index, TIMESERIES_INDEX,
};
use crate::series::serialization::{rdb_load_series, rdb_save_series};
use crate::series::TimeSeries;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::digest::Digest;
use valkey_module::server_events::FlushSubevent;
use valkey_module_macros::flush_event_handler;

/// TimeSeries Module data type RDB encoding version.
const TIMESERIES_TYPE_ENCODING_VERSION: i32 = 1;

pub static VK_TIME_SERIES_TYPE: ValkeyType = ValkeyType::new(
    "vktseries",
    TIMESERIES_TYPE_ENCODING_VERSION,
    RedisModuleTypeMethods {
        version: valkey_module::TYPE_METHOD_VERSION,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: None,
        free: Some(free),
        mem_usage: Some(mem_usage),
        digest: Some(series_digest),
        aux_load: None,
        aux_save: None,
        aux_save_triggers: REDISMODULE_AUX_BEFORE_RDB as i32,
        free_effort: None,
        unlink: Some(unlink),
        copy: Some(copy),
        defrag: Some(defrag),
        mem_usage2: None,
        free_effort2: None,
        unlink2: None,
        copy2: None,
        aux_save2: None,
    },
);

/// This variable is used to indicate that the module is in the midst of a `FLUSHALL` or `FLUSHDB` operation.
/// It is set to true when the operation starts and set to false when it ends.
///
/// We use this to optimize index management during deletes. To be specific, if all keys are being removed, there's
/// no need for individual updates as happens per single `free`. However, in Valkey, the "flushdb" notification is
/// only sent after ALL the appropriate keys are deleted, so from the `free` callback alone it is not obvious if
/// all keys are being removed.
///
/// All interested parties should use the `is_flushing_in_process` function to check if a flush is in process.
static IS_FLUSHING: AtomicBool = AtomicBool::new(false);

#[flush_event_handler]
fn flushed_event_handler(_ctx: &Context, flush_event: FlushSubevent) {
    IS_FLUSHING.store(FlushSubevent::Started == flush_event, Ordering::SeqCst);
}

#[inline]
pub fn is_flushing_in_process() -> bool {
    IS_FLUSHING.load(Ordering::Relaxed)
}

fn remove_series_from_index(ts: &TimeSeries) {
    // if we are in the middle of a flush, we don't want to remove the series from the index
    // since the entire index will be cleared by the flush operation.
    if is_flushing_in_process() {
        return;
    }
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(ts._db, &guard);
    index.remove_timeseries(ts);
    drop(guard);
}

unsafe extern "C" fn rdb_save(rdb: *mut RedisModuleIO, value: *mut c_void) {
    let series = &*value.cast::<TimeSeries>();
    rdb_save_series(series, rdb);
}

unsafe extern "C" fn rdb_load(rdb: *mut RedisModuleIO, enc_ver: c_int) -> *mut c_void {
    match rdb_load_series(rdb, enc_ver) {
        Ok(series) => Box::into_raw(Box::new(series)) as *mut std::ffi::c_void,
        Err(e) => {
            logging::log_notice(format!("Failed to load series from RDB. {e:?}"));
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn mem_usage(value: *const c_void) -> usize {
    let series = unsafe { &*(value as *mut TimeSeries) };
    series.memory_usage()
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    let sm = value.cast::<TimeSeries>();
    let series = Box::from_raw(sm);
    // todo: it may be helpful to push index deletion to a background thread
    remove_series_from_index(&series);
    drop(series);
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    with_timeseries_index(&guard, |index| {
        let old_series = &*value.cast::<TimeSeries>();
        let mut new_series = old_series.clone();
        new_series._db = get_current_db(&guard);
        new_series.id = next_timeseries_id();
        new_series.src_series = None;
        new_series.rules.clear();
        let key = ValkeyString::from_redis_module_string(guard.ctx, to_key);
        index.index_timeseries(&new_series, key.as_slice());
        let boxed = Box::new(new_series);
        Box::into_raw(boxed).cast::<c_void>()
    })
}

unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    if value.is_null() {
        return;
    }
    let series = &*value.cast::<TimeSeries>();
    remove_series_from_index(series);
}

unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    if value.is_null() {
        return 0;
    }
    // Convert the pointer to a TimeSeries so we can operate on it.
    let series: &mut TimeSeries = &mut *(*value).cast::<TimeSeries>();
    match defrag_series(series) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

/// # Safety
/// Raw handler for the Timeseries digest callback.
unsafe extern "C" fn series_digest(md: *mut RedisModuleDigest, value: *mut c_void) {
    let mut digest = Digest::new(md);
    let val = &*(value.cast::<TimeSeries>());
    val.debug_digest(&mut digest);
    digest.end_sequence();
}
