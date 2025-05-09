use crate::commands::arg_parse::parse_integer_arg;
use crate::fanout::cluster::is_clustered;
use crate::fanout::perform_remote_stats_request;
use crate::series::index::{with_timeseries_index, PostingStat, PostingsStats, StatsMaxHeap};
use ahash::AHashMap;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

pub const DEFAULT_STATS_RESULTS_LIMIT: usize = 10;

/// https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats
pub fn stats(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let limit = match args.len() {
        0 => DEFAULT_STATS_RESULTS_LIMIT,
        1 => {
            let next = parse_integer_arg(&args[0], "LIMIT", false)?;
            if next > usize::MAX as i64 {
                return Err(ValkeyError::Str("ERR LIMIT too large"));
            } else if next == 0 {
                return Err(ValkeyError::Str("ERR LIMIT must be greater than 0"));
            }
            next as usize
        }
        _ => {
            return Err(ValkeyError::WrongArity);
        }
    };

    if is_clustered(ctx) {
        perform_remote_stats_request(ctx, limit, on_stats_request_done)?;
        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }

    with_timeseries_index(ctx, |index| {
        let stats = index.stats("", limit);
        posting_stats_to_valkey_value(&stats)
    })
}

pub fn posting_stats_to_valkey_value(stats: &PostingsStats) -> ValkeyResult {
    let mut data = HashMap::with_capacity(4);
    data.insert(
        "numSeries".into(),
        ValkeyValue::Integer(stats.series_count as i64),
    );
    data.insert(
        "numLabels".into(),
        ValkeyValue::Integer(stats.num_labels as i64),
    );
    data.insert(
        "numLabelPairs".into(),
        ValkeyValue::Integer(stats.num_label_pairs as i64),
    );
    data.insert(
        "seriesCountByMetricName".into(),
        stats_slice_to_value(&stats.cardinality_metrics_stats),
    );
    data.insert(
        "labelValueCountByLabelName".into(),
        stats_slice_to_value(&stats.cardinality_label_stats),
    );
    data.insert(
        "memoryInBytesByLabelPair".into(),
        stats_slice_to_value(&stats.label_value_stats),
    );
    data.insert(
        "seriesCountByLabelPair".into(),
        stats_slice_to_value(&stats.label_value_pairs_stats),
    );

    Ok(ValkeyValue::Map(data))
}

fn stats_slice_to_value(items: &[PostingStat]) -> ValkeyValue {
    let res: Vec<ValkeyValue> = items.iter().map(posting_stat_to_value).collect();
    ValkeyValue::Array(res)
}

fn posting_stat_to_value(stat: &PostingStat) -> ValkeyValue {
    let mut res: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(1);
    res.insert(
        ValkeyValueKey::from(&stat.name),
        ValkeyValue::Integer(stat.count as i64),
    );
    ValkeyValue::Map(res)
}

fn on_stats_request_done(ctx: &ThreadSafeContext<BlockedClient>, res: Vec<PostingsStats>, limit: u64) {
    // todo: we need limit....
    let limit = 10;

    let mut series_count = 0;
    let mut num_labels = 0;
    let mut num_label_pairs = 0;

    let mut cardinality_metrics_map: AHashMap<String, PostingStat> = AHashMap::new();
    let mut cardinality_labels_map: AHashMap<String, PostingStat> = AHashMap::new();
    let mut label_values_map: AHashMap<String, PostingStat> = AHashMap::new();
    let mut pairs_map: AHashMap<String, PostingStat> = AHashMap::new();

    for result in res.iter() {
        num_labels += result.num_labels;
        num_label_pairs += result.num_label_pairs;
        series_count += result.series_count;
        collate_stats_values(
            &mut cardinality_metrics_map,
            &result.cardinality_label_stats,
        );
        collate_stats_values(
            &mut cardinality_labels_map,
            &result.cardinality_metrics_stats,
        );
        collate_stats_values(&mut label_values_map, &result.label_value_stats);
        collate_stats_values(&mut pairs_map, &result.label_value_pairs_stats);
    }

    let result = PostingsStats {
        series_count,
        num_labels,
        num_label_pairs,
        cardinality_metrics_stats: collect_map_values(cardinality_metrics_map, limit),
        cardinality_label_stats: collect_map_values(cardinality_labels_map, limit),
        label_value_stats: collect_map_values(label_values_map, limit),
        label_value_pairs_stats: collect_map_values(pairs_map, limit),
    };

    let stats = posting_stats_to_valkey_value(&result);

    ctx.reply(stats);
}

fn collate_stats_values(map: &mut AHashMap<String, PostingStat>, values: &[PostingStat]) {
    for result in values.iter() {
        if let Some(stat) = map.get_mut(result.name.as_str()) {
            stat.count += result.count;
        } else {
            map.insert(result.name.clone(), result.clone());
        }
    }
}

fn collect_map_values(map: AHashMap<String, PostingStat>, limit: usize) -> Vec<PostingStat> {
    let mut map = map;
    let mut heap = StatsMaxHeap::new(limit);
    for stat in map.drain().map(|(_, v)| v) {
        heap.push(stat);
    }
    heap.into_vec()
}
