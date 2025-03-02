use crate::common::serialization::*;
use crate::common::Sample;
use crate::labels::InternedMetricName;
use crate::series::chunks::{
    rdb_load_series_chunk, rdb_save_series_chunk, Chunk, ChunkCompression,
};
use crate::series::{DuplicatePolicy, SampleDuplicatePolicy, TimeSeries, TimeseriesId};
use valkey_module::{raw, ValkeyResult};

pub const SERIES_ENC_VERSION: u64 = 1;

fn rdb_save_sample_duplicate_policy(
    rdb: *mut raw::RedisModuleIO,
    sample_policy: &SampleDuplicatePolicy,
) {
    let tmp = sample_policy.policy.as_str();
    raw::save_string(rdb, tmp);
    raw::save_unsigned(rdb, sample_policy.max_time_delta);
    raw::save_double(rdb, sample_policy.max_value_delta);
}

fn rdb_load_sample_duplicate_policy(
    rdb: *mut raw::RedisModuleIO,
) -> ValkeyResult<SampleDuplicatePolicy> {
    let policy = rdb_load_string(rdb)?;
    let max_time_delta = raw::load_unsigned(rdb)?;
    let max_value_delta = raw::load_double(rdb)?;
    let duplicate_policy = DuplicatePolicy::try_from(policy)?;
    Ok(SampleDuplicatePolicy {
        policy: duplicate_policy,
        max_time_delta,
        max_value_delta,
    })
}

pub fn rdb_save_series(series: &TimeSeries, rdb: *mut raw::RedisModuleIO) {
    raw::save_unsigned(rdb, series.id);
    series.labels.to_rdb(rdb);

    rdb_save_duration(rdb, &series.retention);
    let tmp = series.chunk_compression.name();
    raw::save_string(rdb, tmp);

    rdb_save_optional_rounding(rdb, &series.rounding);
    rdb_save_sample_duplicate_policy(rdb, &series.sample_duplicates);
    rdb_save_usize(rdb, series.chunk_size_bytes);
    rdb_save_usize(rdb, series.chunks.len());
    for chunk in series.chunks.iter() {
        rdb_save_series_chunk(chunk, rdb);
    }
}

pub fn rdb_load_series(rdb: *mut raw::RedisModuleIO, enc_ver: i32) -> ValkeyResult<TimeSeries> {
    let id = raw::load_unsigned(rdb)? as TimeseriesId;
    let labels = InternedMetricName::from_rdb(rdb)?;

    let retention = rdb_load_duration(rdb)?;
    let chunk_compression = ChunkCompression::try_from(rdb_load_string(rdb)?)?;

    let rounding = rdb_load_optional_rounding(rdb)?;
    let sample_duplicates = rdb_load_sample_duplicate_policy(rdb)?;
    let chunk_size_bytes = rdb_load_usize(rdb)?;
    let chunks_len = rdb_load_usize(rdb)?;
    let mut chunks = Vec::with_capacity(chunks_len);
    let mut total_samples: usize = 0;
    let mut first_timestamp = 0;

    let mut last_sample: Option<Sample> = None;

    for _ in 0..chunks_len {
        let chunk = rdb_load_series_chunk(rdb, enc_ver)?;
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
