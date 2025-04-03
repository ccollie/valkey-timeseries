use crate::labels::InternedLabel;
use crate::module::result::META_KEY_LABEL;
use crate::module::with_timeseries;
use crate::series::{
    chunks::{Chunk, TimeSeriesChunk},
    TimeSeries,
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

    with_timeseries(ctx, &key, true,|series| Ok(get_ts_info(series, debugging, None)))
}

fn get_ts_info(ts: &TimeSeries, debug: bool, key: Option<&ValkeyString>) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(ts.labels.len() + 1);
    let metric = ts.prometheus_metric_name();
    map.insert("metric".into(), metric.into());
    map.insert("totalSamples".into(), ts.total_samples.into());
    map.insert("memoryUsage".into(), ts.memory_usage().into());
    map.insert("firstTimestamp".into(), ts.first_timestamp.into());
    if let Some(last_sample) = ts.last_sample {
        map.insert("lastTimestamp".into(), last_sample.timestamp.into());
    }
    map.insert(
        "retentionTime".into(),
        (ts.retention.as_millis() as f64).into(),
    );
    map.insert("chunkCount".into(), (ts.chunks.len() as f64).into());
    map.insert("chunkSize".into(), ts.chunk_size_bytes.into());

    if ts.chunk_compression.is_compressed() {
        map.insert("chunkType".into(), "compressed".into());
    } else {
        map.insert("chunkType".into(), "uncompressed".into());
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
        let mut labels_map: HashMap<ValkeyValueKey, ValkeyValue> =
            HashMap::with_capacity(ts.labels.len() + 1);

        for InternedLabel { name, value } in ts.labels.iter() {
            labels_map.insert(
                ValkeyValueKey::String(name.into()),
                ValkeyValue::from(value),
            );
        }
        map.insert("labels".into(), ValkeyValue::from(labels_map));
    }

    if debug {
        map.insert("keySelfName".into(), ValkeyValue::from(key));
        map.insert("chunks".into(), get_chunks_info(ts));
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
    map.insert("startTimestamp".into(), chunk.first_timestamp().into());
    map.insert("endTimestamp".into(), chunk.last_timestamp().into());
    map.insert("samples".into(), chunk.len().into());
    map.insert("size".into(), chunk.size().into());
    map.insert("bytesPerSample".into(), chunk.bytes_per_sample().into());
    ValkeyValue::Map(map)
}
