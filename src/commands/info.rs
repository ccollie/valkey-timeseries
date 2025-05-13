use crate::common::constants::META_KEY_LABEL;
use crate::common::rounding::RoundingStrategy;
use crate::series::{
    chunks::{Chunk, TimeSeriesChunk},
    with_timeseries, TimeSeries,
};
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

pub fn info(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    let debugging = if let Ok(val) = args.next_str() {
        val.eq_ignore_ascii_case("debug")
    } else {
        false
    };

    args.done()?;

    with_timeseries(ctx, &key, true, |series| {
        Ok(get_ts_info(series, debugging, None))
    })
}

fn get_ts_info(ts: &TimeSeries, debug: bool, key: Option<&ValkeyString>) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(ts.labels.len() + 1);
    let metric = ts.prometheus_metric_name();
    map.insert("metric".into(), metric.into());
    map.insert(
        "totalSamples".into(),
        ValkeyValue::Integer(ts.total_samples as i64),
    );
    map.insert(
        "memoryUsage".into(),
        ValkeyValue::Integer(ts.memory_usage() as i64),
    );
    map.insert(
        "firstTimestamp".into(),
        ValkeyValue::Integer(ts.first_timestamp),
    );
    if let Some(last_sample) = ts.last_sample {
        map.insert(
            "lastTimestamp".into(),
            ValkeyValue::Integer(last_sample.timestamp),
        );
    } else {
        map.insert(
            "lastTimestamp".into(),
            ValkeyValue::Integer(ts.first_timestamp),
        );
    }
    map.insert(
        "retentionTime".into(),
        ValkeyValue::Integer(ts.retention.as_millis() as i64),
    );
    map.insert(
        "chunkCount".into(),
        ValkeyValue::Integer(ts.chunks.len() as i64),
    );
    map.insert(
        "chunkSize".into(),
        ValkeyValue::Integer(ts.chunk_size_bytes as i64),
    );

    if ts.chunk_compression.is_compressed() {
        map.insert("chunkType".into(), "compressed".into());
    } else {
        map.insert("chunkType".into(), "uncompressed".into());
    }

    if let Some(policy) = ts.sample_duplicates.policy {
        map.insert("duplicatePolicy".into(), policy.as_str().into());
    } else {
        map.insert("duplicatePolicy".into(), ValkeyValue::Null);
    }

    if let Some(key) = key {
        map.insert(
            ValkeyValueKey::String(META_KEY_LABEL.into()),
            ValkeyValue::from(key),
        );
    }

    if ts.labels.is_empty() {
        map.insert("labels".into(), ValkeyValue::Null);
    } else {
        let mut labels = ts.labels.to_label_vec();
        labels.sort();

        let labels_value = labels
            .into_iter()
            .map(|label| label.into())
            .collect::<Vec<ValkeyValue>>();

        map.insert("labels".into(), ValkeyValue::from(labels_value));
    }

    map.insert(
        "ignoreMaxTimeDiff".into(),
        ValkeyValue::Integer(ts.sample_duplicates.max_time_delta as i64),
    );
    map.insert(
        "ignoreMaxValDiff".into(),
        ValkeyValue::Float(ts.sample_duplicates.max_value_delta),
    );

    if let Some(rounding) = ts.rounding {
        let (name, digits) = match rounding {
            RoundingStrategy::SignificantDigits(d) => ("significantDigits", d),
            RoundingStrategy::DecimalDigits(d) => ("decimalDigits", d),
        };
        let result = ValkeyValue::Array(vec![
            ValkeyValue::from(name),
            ValkeyValue::Integer(digits.into()), // do we have negative digits?
        ]);
        map.insert("rounding".into(), result);
    }

    if debug {
        map.insert("keySelfName".into(), ValkeyValue::from(key));
        // yes, I know its title case, but that's what redis does
        map.insert("Chunks".into(), get_chunks_info(ts));
    }

    ValkeyValue::Map(map)
}

fn get_chunks_info(ts: &TimeSeries) -> ValkeyValue {
    let items = ts
        .chunks
        .iter()
        .map(get_one_chunk_info)
        .collect::<Vec<ValkeyValue>>();

    ValkeyValue::Array(items)
}

fn get_one_chunk_info(chunk: &TimeSeriesChunk) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(6);
    map.insert(
        "startTimestamp".into(),
        ValkeyValue::Integer(chunk.first_timestamp()),
    );
    map.insert(
        "endTimestamp".into(),
        ValkeyValue::Integer(chunk.last_timestamp()),
    );
    map.insert("samples".into(), ValkeyValue::Integer(chunk.len() as i64));
    map.insert("size".into(), ValkeyValue::Integer(chunk.size() as i64));
    map.insert(
        "bytesPerSample".into(),
        ValkeyValue::BulkString(chunk.bytes_per_sample().to_string()),
    );
    ValkeyValue::Map(map)
}
