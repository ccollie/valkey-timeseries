use std::collections::HashMap;
use valkey_module::ValkeyValue;
use valkey_module::redisvalue::ValkeyValueKey;

/// Stat holds values for a single cardinality statistic.
#[derive(Debug, Clone, Default)]
pub struct PostingStat {
    pub name: String,
    pub count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PostingsStats {
    pub cardinality_metrics_stats: Vec<PostingStat>,
    pub cardinality_label_stats: Vec<PostingStat>,
    pub label_value_stats: Vec<PostingStat>,
    pub label_value_pairs_stats: Vec<PostingStat>,
    pub num_label_pairs: usize,
    pub num_labels: usize,
    pub series_count: u64,
}

impl From<PostingsStats> for ValkeyValue {
    fn from(stats: PostingsStats) -> Self {
        (&stats).into()
    }
}

impl From<&PostingsStats> for ValkeyValue {
    fn from(stats: &PostingsStats) -> Self {
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

        ValkeyValue::Map(data)
    }
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

#[derive(Debug)]
pub(crate) struct StatsMaxHeap {
    max_length: usize,
    min_value: usize,
    min_index: usize,
    items: Vec<PostingStat>,
}

impl StatsMaxHeap {
    pub fn new(length: usize) -> Self {
        StatsMaxHeap {
            max_length: length,
            min_value: usize::MAX,
            min_index: 0,
            items: Vec::with_capacity(length),
        }
    }

    pub fn push(&mut self, item: PostingStat) {
        if self.items.len() < self.max_length {
            if item.count < self.min_value as u64 {
                self.min_value = item.count as usize;
                self.min_index = self.items.len();
            }
            self.items.push(item);
            return;
        }
        if item.count < self.min_value as u64 {
            return;
        }

        self.min_value = item.count as usize;
        self.items[self.min_index] = item;

        for (i, stat) in self.items.iter().enumerate() {
            if stat.count < self.min_value as u64 {
                self.min_value = stat.count as usize;
                self.min_index = i;
            }
        }
    }

    pub fn into_vec(self) -> Vec<PostingStat> {
        self.items
    }
}
