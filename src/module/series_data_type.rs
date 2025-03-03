use valkey_module::REDISMODULE_AUX_BEFORE_RDB;
use valkey_module::{logging, RedisModuleTypeMethods};
use valkey_module::{
    native_types::ValkeyType, RedisModuleDefragCtx, RedisModuleString, ValkeyString,
};

use crate::series::defrag_series;
use crate::series::index::serialization::{ts_index_rdb_aux_load, ts_index_rdb_aux_save};
use crate::series::index::with_timeseries_index;
use crate::series::serialization::{rdb_load_series, rdb_save_series};
use crate::series::TimeSeries;
use std::os::raw::{c_int, c_void};
use valkey_module::raw;
// see https://github.com/redis/redis/blob/unstable/tests/modules

/// Used for decoding and encoding `TimeSeries`. Currently used in AOF Rewrite.
/// This value must be incremented when the `TimeSeries` struct change.
pub const TIMESERIES_TYPE_OBJECT_VERSION: u8 = 1;

/// Bloom Module data type RDB encoding version.
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
        digest: None,
        aux_load: Some(ts_index_rdb_aux_load),
        aux_save: Some(ts_index_rdb_aux_save),
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

unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let series = &*value.cast::<TimeSeries>();
    rdb_save_series(series, rdb);
}

unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, enc_ver: c_int) -> *mut c_void {
    match rdb_load_series(rdb, enc_ver) {
        Ok(series) => Box::into_raw(Box::new(series)) as *mut std::ffi::c_void,
        Err(e) => {
            logging::log_notice(format!("Failed to load series from RDB. {:?}", e));
            std::ptr::null_mut()
        }
    }
}

unsafe extern "C" fn mem_usage(value: *const c_void) -> usize {
    let sm = unsafe { &*(value as *mut TimeSeries) };
    sm.memory_usage()
}

#[allow(unused)]
unsafe extern "C" fn free(value: *mut c_void) {
    if value.is_null() {
        return;
    }
    let sm = value as *mut TimeSeries;
    Box::from_raw(sm);
}

#[allow(non_snake_case, unused)]
unsafe extern "C" fn copy(
    from_key: *mut RedisModuleString,
    to_key: *mut RedisModuleString,
    value: *const c_void,
) -> *mut c_void {
    let guard = valkey_module::MODULE_CONTEXT.lock();
    with_timeseries_index(&guard, |index| {
        let sm = &*(value as *mut TimeSeries);
        let mut new_series = sm.clone();
        // set id to 0 so indexer can allocate a new id
        new_series.id = 0;
        let key = ValkeyString::from_redis_module_string(guard.ctx, to_key);
        index.index_timeseries(&new_series, key.as_slice());
        Box::into_raw(Box::new(new_series)).cast::<c_void>()
    })
}

unsafe extern "C" fn unlink(_key: *mut RedisModuleString, value: *const c_void) {
    let series = &*(value as *mut TimeSeries);
    if value.is_null() {
        return;
    }
    let guard = valkey_module::MODULE_CONTEXT.lock();
    with_timeseries_index(&guard, |ts_index| {
        ts_index.remove_timeseries(series);
    });
}

unsafe extern "C" fn defrag(
    _ctx: *mut RedisModuleDefragCtx,
    _key: *mut RedisModuleString,
    value: *mut *mut c_void,
) -> c_int {
    let series = &mut *(value as *mut TimeSeries);
    if value.is_null() {
        return 0;
    }
    match defrag_series(series) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}
