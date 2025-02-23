use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::index::with_timeseries_index;
use crate::series::TimeSeries;
use std::fmt::Display;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub(crate) fn invalid_series_key_error<K: Display>(key: &K) -> ValkeyError {
    ValkeyError::String(format!(
        "TS: the key \"{}\" does not exist or is not a timeseries key",
        key
    ))
}

pub fn with_timeseries<R>(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let redis_key = ctx.open_key(key);
    if let Some(series) = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)? {
        f(series)
    } else {
        Err(invalid_series_key_error(key))
    }
}

pub fn with_timeseries_by_id<R>(
    ctx: &Context,
    id: u64,
    f: impl FnOnce(&TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    with_timeseries_index(ctx, |index| {
        if let Some(key) = index.get_key_by_id(ctx, id) {
            let key = ctx.create_string(&key[0..]);
            with_timeseries(ctx, &key, f)
        } else {
            Err(ValkeyError::String(format!(
                "TS: series with id \"{}\" does not exist",
                id
            )))
        }
    })
}

pub fn with_timeseries_mut<R>(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&mut TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    // expect should not panic, since must_exist will cause an error if the key is non-existent, and `?` will ensure it propagates
    f(get_timeseries_mut(ctx, key, true)?.expect("key does not exist"))
}

pub fn get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    must_exist: bool,
) -> ValkeyResult<Option<&'a mut TimeSeries>> {
    let redis_key = ctx.open_key_writable(key);
    // Safety: According to docs for `get_value`, it Will panic if RedisModule_ModuleTypeGetValue is missing in redismodule. h
    // it that happens we have a bigger problem than a panic since we're compiling against an incompatible version of valkey.
    let series = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)?;
    match series {
        Some(series) => Ok(Some(series)),
        None => {
            let msg = format!("TS: the key \"{}\" is not a timeseries", key);
            if must_exist {
                Err(ValkeyError::String(msg))
            } else {
                Ok(None)
            }
        }
    }
}
