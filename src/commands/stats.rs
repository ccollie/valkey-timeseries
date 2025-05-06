use crate::commands::arg_parse::parse_integer_arg;
use crate::series::index::{with_timeseries_index, PostingStat};
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

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

    with_timeseries_index(ctx, |index| {
        let series_count = index.count();
        let stats = index.stats("", limit);

        let mut data = HashMap::with_capacity(4);
        data.insert(
            "numSeries".into(),
            ValkeyValue::Integer(series_count as i64),
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
    })
}

pub fn process_stats_query(ctx: &Context, limit: Option<usize>) -> ValkeyResult {
    let limit = limit.unwrap_or(DEFAULT_STATS_RESULTS_LIMIT);
    with_timeseries_index(ctx, |index| {
        let series_count = index.count();
        let stats = index.stats("", limit);

        let mut data = HashMap::with_capacity(4);
        data.insert(
            "numSeries".into(),
            ValkeyValue::Integer(series_count as i64),
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
    })
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
