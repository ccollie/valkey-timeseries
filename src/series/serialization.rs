use crate::common::serialization::*;
use crate::common::Sample;
use crate::labels::InternedMetricName;
use crate::series::chunks::{Chunk, ChunkEncoding, TimeSeriesChunk};
use crate::series::{SampleDuplicatePolicy, TimeSeries, TimeseriesId};
use valkey_module::{raw, ValkeyResult};


pub fn rdb_save_series(series: &TimeSeries, rdb: *mut raw::RedisModuleIO) {
    raw::save_unsigned(rdb, series.id);
    series.labels.to_rdb(rdb);

    rdb_save_duration(rdb, &series.retention);
    let tmp = series.chunk_compression.name();
    raw::save_string(rdb, tmp);

    rdb_save_optional_rounding(rdb, &series.rounding);
    series.sample_duplicates.rdb_save(rdb);
    rdb_save_usize(rdb, series.chunk_size_bytes);
    rdb_save_usize(rdb, series.chunks.len());
    for chunk in series.chunks.iter() {
        chunk.save_rdb(rdb);
    }
}

pub fn rdb_load_series(rdb: *mut raw::RedisModuleIO, enc_ver: i32) -> ValkeyResult<TimeSeries> {
    let id = raw::load_unsigned(rdb)? as TimeseriesId;
    let labels = InternedMetricName::from_rdb(rdb)?;

    let retention = rdb_load_duration(rdb)?;
    let chunk_compression = ChunkEncoding::try_from(rdb_load_string(rdb)?)?;

    let rounding = rdb_load_optional_rounding(rdb)?;
    let sample_duplicates = SampleDuplicatePolicy::rdb_load(rdb)?;
    let chunk_size_bytes = rdb_load_usize(rdb)?;
    let chunks_len = rdb_load_usize(rdb)?;
    let mut chunks = Vec::with_capacity(chunks_len);
    let mut total_samples: usize = 0;
    let mut first_timestamp = 0;

    let mut last_sample: Option<Sample> = None;

    for _ in 0..chunks_len {
        let chunk = TimeSeriesChunk::load_rdb(rdb, enc_ver)?;
        total_samples += chunk.len();
        if first_timestamp == 0 {
            first_timestamp = chunk.first_timestamp();
        }
        last_sample = chunk.last_sample();
        chunks.push(chunk);
    }

    let ts = TimeSeries {
        id,
        labels,
        retention,
        chunk_compression,
        sample_duplicates,
        rounding,
        chunk_size_bytes,
        chunks,
        total_samples,
        first_timestamp,
        last_sample,
    };

    // ts.update_meta();
    // add to index
    Ok(ts)
}
