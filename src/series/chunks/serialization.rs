use crate::common::serialization::{rdb_load_string, rdb_load_usize, rdb_save_usize};
use crate::common::Sample;
use crate::series::chunks::gorilla::{rdb_load_gorilla_chunk, rdb_save_gorilla_chunk};
use crate::series::chunks::pco::{rdb_load_pco_chunk, rdb_save_pco_chunk};
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk};
use valkey_module::{raw, ValkeyError, ValkeyResult};

fn rdb_save_uncompressed_chunk(chunk: &UncompressedChunk, rdb: *mut raw::RedisModuleIO) {
    // todo: compress ?
    rdb_save_usize(rdb, chunk.max_size);
    rdb_save_usize(rdb, chunk.max_elements);
    rdb_save_usize(rdb, chunk.samples.len());
    for Sample { timestamp, value } in chunk.samples.iter() {
        raw::save_signed(rdb, *timestamp);
        raw::save_double(rdb, *value);
    }
}

fn rdb_load_compressed_chunk(
    rdb: *mut raw::RedisModuleIO,
    _encver: i32,
) -> Result<UncompressedChunk, valkey_module::error::Error> {
    let max_size = rdb_load_usize(rdb)?;
    let max_elements = rdb_load_usize(rdb)?;
    let len = rdb_load_usize(rdb)?;
    let mut samples = Vec::with_capacity(len);
    for _ in 0..len {
        let ts = raw::load_signed(rdb)?;
        let val = raw::load_double(rdb)?;
        samples.push(Sample {
            timestamp: ts,
            value: val,
        });
    }
    Ok(UncompressedChunk {
        max_size,
        samples,
        max_elements,
    })
}

fn save_chunk_type(chunk: &TimeSeriesChunk, rdb: *mut raw::RedisModuleIO) {
    match chunk {
        TimeSeriesChunk::Uncompressed(_) => {
            raw::save_string(rdb, "uncompressed");
        }
        TimeSeriesChunk::Gorilla(_) => {
            raw::save_string(rdb, "gorilla");
        }
        TimeSeriesChunk::Pco(_) => {
            raw::save_string(rdb, "pco");
        }
    }
}

pub fn rdb_save_series_chunk(chunk: &TimeSeriesChunk, rdb: *mut raw::RedisModuleIO) {
    save_chunk_type(chunk, rdb);
    match chunk {
        TimeSeriesChunk::Uncompressed(chunk) => {
            rdb_save_uncompressed_chunk(chunk, rdb);
        }
        TimeSeriesChunk::Gorilla(chunk) => {
            rdb_save_gorilla_chunk(chunk, rdb);
        }
        TimeSeriesChunk::Pco(chunk) => {
            rdb_save_pco_chunk(chunk, rdb);
        }
    }
}

pub fn rdb_load_series_chunk(
    rdb: *mut raw::RedisModuleIO,
    enc_ver: i32,
) -> ValkeyResult<TimeSeriesChunk> {
    let chunk_type = rdb_load_string(rdb)?;
    let chunk = match chunk_type.as_str() {
        "uncompressed" => TimeSeriesChunk::Uncompressed(rdb_load_compressed_chunk(rdb, enc_ver)?),
        "gorilla" => TimeSeriesChunk::Gorilla(rdb_load_gorilla_chunk(rdb, enc_ver)?),
        "pco" => TimeSeriesChunk::Pco(rdb_load_pco_chunk(rdb, enc_ver)?),
        _ => return Err(ValkeyError::Str("Invalid chunk type")),
    };
    Ok(chunk)
}
