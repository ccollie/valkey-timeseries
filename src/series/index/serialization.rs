use super::{TimeSeriesIndex, TIMESERIES_INDEX};
use crate::common::hash::IntMap;
use crate::common::serialization::rdb_save_usize;
use crate::module::server_events::is_async_loading_in_progress;
use crate::series::index::memory_postings::{KeyType, MemoryPostings};
use crate::series::TimeseriesId;
use blart::AsBytes;
use std::os::raw::c_int;
use std::sync::RwLock;
use std::sync::{LazyLock, Mutex};
use valkey_module::{logging, raw, ValkeyError, ValkeyResult};

pub(super) static STAGED_TIMESERIES_INDEX: LazyLock<
    Mutex<std::collections::HashMap<i32, TimeSeriesIndex>>,
> = LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

fn serialize_memory_postings(
    rdb: *mut raw::RedisModuleIO,
    postings: &MemoryPostings,
) -> ValkeyResult<()> {
    let mut buffer: Vec<u8> = Vec::new();
    postings
        .serialize_into(&mut buffer)
        .map_err(|_e| ValkeyError::Str("ERR serializing posting"))?;
    raw::save_slice(rdb, buffer.as_bytes());
    Ok(())
}

fn deserialize_memory_postings(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<MemoryPostings> {
    let buffer = raw::load_string_buffer(rdb)?;
    let mut slice = buffer.as_ref();
    MemoryPostings::deserialize_from(&mut slice)
        .map_err(|_e| ValkeyError::Str("ERR deserializing postings"))
}

fn serialize_int_key_map(rdb: *mut raw::RedisModuleIO, map: &IntMap<TimeseriesId, KeyType>) {
    // Serialize the IntMap data to the RDB file here.
    let count = map.len();
    rdb_save_usize(rdb, count);

    for (key, value) in map.iter() {
        raw::save_unsigned(rdb, *key);
        raw::save_slice(rdb, value.as_bytes());
    }
}

fn serialize_index_inner(rdb: *mut raw::RedisModuleIO, inner: &MemoryPostings) -> ValkeyResult<()> {
    serialize_memory_postings(rdb, inner)?;
    serialize_int_key_map(rdb, &inner.id_to_key);
    Ok(())
}

fn deserialize_index_inner(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<MemoryPostings> {
    deserialize_memory_postings(rdb)
}

pub fn serialize_timeseries_index(
    rdb: *mut raw::RedisModuleIO,
    index: &TimeSeriesIndex,
) -> ValkeyResult<()> {
    let inner = index.inner.read()?;
    serialize_index_inner(rdb, &inner)?;
    Ok(())
}

pub fn deserialize_timeseries_index(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<TimeSeriesIndex> {
    let inner = deserialize_index_inner(rdb)?;
    Ok(TimeSeriesIndex {
        inner: RwLock::new(inner),
    })
}

fn aux_save(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<()> {
    let map = TIMESERIES_INDEX.pin();
    let len = map.len() as u64;
    raw::save_unsigned(rdb, len);

    for (k, v) in map.iter() {
        raw::save_unsigned(rdb, *k as u64);
        serialize_timeseries_index(rdb, v)?;
    }
    Ok(())
}

fn aux_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<()> {
    let len = raw::load_unsigned(rdb)?;

    if is_async_loading_in_progress() {
        let mut staged = STAGED_TIMESERIES_INDEX
            .lock()
            .map_err(|_| ValkeyError::Str("Error loading AUX fields"))?;
        for _ in 0..len {
            let ts_id = raw::load_signed(rdb)? as i32;
            let index = deserialize_timeseries_index(rdb)?;
            staged.insert(ts_id, index);
        }
    } else {
        let map = TIMESERIES_INDEX.pin();

        for _ in 0..len {
            let ts_id = raw::load_signed(rdb)? as i32;
            let index = deserialize_timeseries_index(rdb)?;
            map.insert(ts_id, index);
        }
    }

    Ok(())
}

/// Load the auxiliary data outside the regular keyspace from the RDB file
pub extern "C" fn ts_index_rdb_aux_load(
    rdb: *mut raw::RedisModuleIO,
    _enc_ver: c_int,
    _when: c_int,
) -> c_int {
    logging::log_notice("Loading timeseries AUX fields during RDB load.");
    if let Err(e) = aux_load(rdb) {
        logging::log_warning(format!("Error loading AUX fields: {}", e));
        return raw::Status::Err as i32;
    }
    raw::Status::Ok as i32
}

pub extern "C" fn ts_index_rdb_aux_save(rdb: *mut raw::RedisModuleIO, when: c_int) {
    logging::log_notice(format!("Saving AUX fields for time {}.", when).as_str());
    match aux_save(rdb) {
        Ok(()) => (),
        Err(e) => logging::log_warning(format!("Error saving AUX fields: {}", e)),
    }
}

pub fn series_on_async_load_done(completed: bool) {
    let staged = std::mem::take(&mut *STAGED_TIMESERIES_INDEX.lock().unwrap());
    if completed {
        let ts_index = TIMESERIES_INDEX.pin();
        // todo: it's much faster to do a swap, but LazyLock doesn't support it and using
        // a Mutex would be a performance hit.
        for (k, v) in staged.into_iter() {
            ts_index.insert(k, v);
        }
    }
}
