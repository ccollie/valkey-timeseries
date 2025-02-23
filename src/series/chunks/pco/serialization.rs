use crate::common::serialization::{rdb_load_usize, rdb_save_usize};
use crate::series::chunks::PcoChunk;
use valkey_module::{raw, ValkeyResult};

pub fn rdb_save_pco_chunk(chunk: &PcoChunk, rdb: *mut raw::RedisModuleIO) {
    raw::save_signed(rdb, chunk.min_time);
    raw::save_signed(rdb, chunk.max_time);
    rdb_save_usize(rdb, chunk.max_size);
    raw::save_double(rdb, chunk.last_value);
    rdb_save_usize(rdb, chunk.count);
    raw::save_slice(rdb, &chunk.timestamps);
    raw::save_slice(rdb, &chunk.values);
}

pub fn rdb_load_pco_chunk(rdb: *mut raw::RedisModuleIO, _encver: i32) -> ValkeyResult<PcoChunk> {
    let min_time = raw::load_signed(rdb)?;
    let max_time = raw::load_signed(rdb)?;
    let max_size = rdb_load_usize(rdb)?;
    let last_value = raw::load_double(rdb)?;
    let count = rdb_load_usize(rdb)?;
    let ts = raw::load_string_buffer(rdb)?;
    let vals = raw::load_string_buffer(rdb)?;
    let timestamps: Vec<u8> = Vec::from(ts.as_ref());
    let values: Vec<u8> = Vec::from(vals.as_ref());

    Ok(PcoChunk {
        min_time,
        max_time,
        max_size,
        last_value,
        count,
        timestamps,
        values,
    })
}
