use fred::prelude::RedisValue;

fn push_flag(args: &mut Vec<RedisValue>, flag: &'static str, value: impl Into<RedisValue>) {
    args.push(flag.into());
    args.push(value.into());
}

fn push_label_pairs(args: &mut Vec<RedisValue>, labels: &[(String, String)]) {
    if labels.is_empty() {
        return;
    }

    args.push("LABELS".into());
    for (k, v) in labels {
        args.push(k.clone().into());
        args.push(v.clone().into());
    }
}

#[derive(Debug, Clone, Default)]
pub struct CreateOptions {
    pub retention: Option<i64>,
    pub chunk_size: Option<i64>,
    pub encoding: Option<String>,
    pub duplicate_policy: Option<String>,
    pub labels: Vec<(String, String)>,
}

impl CreateOptions {
    pub fn retention(mut self, millis: i64) -> Self {
        self.retention = Some(millis);
        self
    }

    pub fn chunk_size(mut self, bytes: i64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    pub fn encoding(mut self, encoding: impl Into<String>) -> Self {
        self.encoding = Some(encoding.into());
        self
    }

    pub fn duplicate_policy(mut self, policy: impl Into<String>) -> Self {
        self.duplicate_policy = Some(policy.into());
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if let Some(v) = self.retention {
            push_flag(args, "RETENTION", v);
        }
        if let Some(v) = self.chunk_size {
            push_flag(args, "CHUNK_SIZE", v);
        }
        if let Some(v) = &self.encoding {
            push_flag(args, "ENCODING", v.clone());
        }
        if let Some(v) = &self.duplicate_policy {
            push_flag(args, "DUPLICATE_POLICY", v.clone());
        }

        push_label_pairs(args, &self.labels);
    }
}

#[derive(Debug, Clone, Default)]
pub struct AlterOptions {
    pub retention: Option<i64>,
    pub chunk_size: Option<i64>,
    pub duplicate_policy: Option<String>,
    pub labels: Vec<(String, String)>,
}

impl AlterOptions {
    pub fn retention(mut self, millis: i64) -> Self {
        self.retention = Some(millis);
        self
    }

    pub fn chunk_size(mut self, bytes: i64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    pub fn duplicate_policy(mut self, policy: impl Into<String>) -> Self {
        self.duplicate_policy = Some(policy.into());
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if let Some(v) = self.retention {
            push_flag(args, "RETENTION", v);
        }
        if let Some(v) = self.chunk_size {
            push_flag(args, "CHUNK_SIZE", v);
        }
        if let Some(v) = &self.duplicate_policy {
            push_flag(args, "DUPLICATE_POLICY", v.clone());
        }

        push_label_pairs(args, &self.labels);
    }
}

#[derive(Debug, Clone, Default)]
pub struct AddOptions {
    pub retention: Option<i64>,
    pub chunk_size: Option<i64>,
    pub on_duplicate: Option<String>,
    pub labels: Vec<(String, String)>,
}

impl AddOptions {
    pub fn retention(mut self, millis: i64) -> Self {
        self.retention = Some(millis);
        self
    }

    pub fn chunk_size(mut self, bytes: i64) -> Self {
        self.chunk_size = Some(bytes);
        self
    }

    pub fn on_duplicate(mut self, policy: impl Into<String>) -> Self {
        self.on_duplicate = Some(policy.into());
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if let Some(v) = self.retention {
            push_flag(args, "RETENTION", v);
        }
        if let Some(v) = self.chunk_size {
            push_flag(args, "CHUNK_SIZE", v);
        }
        if let Some(v) = &self.on_duplicate {
            push_flag(args, "ON_DUPLICATE", v.clone());
        }

        push_label_pairs(args, &self.labels);
    }
}

#[derive(Debug, Clone, Default)]
pub struct IncrDecrOptions {
    pub timestamp: Option<RedisValue>,
    pub retention: Option<i64>,
    pub labels: Vec<(String, String)>,
}

impl IncrDecrOptions {
    pub fn timestamp(mut self, timestamp: impl Into<RedisValue>) -> Self {
        self.timestamp = Some(timestamp.into());
        self
    }

    pub fn retention(mut self, millis: i64) -> Self {
        self.retention = Some(millis);
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if let Some(v) = &self.timestamp {
            push_flag(args, "TIMESTAMP", v.clone());
        }
        if let Some(v) = self.retention {
            push_flag(args, "RETENTION", v);
        }

        push_label_pairs(args, &self.labels);
    }
}

#[derive(Debug, Clone, Default)]
pub struct GetOptions {
    pub latest: bool,
}

impl GetOptions {
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = latest;
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if self.latest {
            args.push("LATEST".into());
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InfoOptions {
    pub debug: bool,
}

impl InfoOptions {
    pub fn debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if self.debug {
            args.push("DEBUG".into());
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MGetOptions {
    pub latest: bool,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
}

impl MGetOptions {
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = latest;
        self
    }

    pub fn with_labels(mut self, with_labels: bool) -> Self {
        self.with_labels = with_labels;
        self
    }

    pub fn selected_label(mut self, label: impl Into<String>) -> Self {
        self.selected_labels.push(label.into());
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if self.latest {
            args.push("LATEST".into());
        }
        if self.with_labels {
            args.push("WITHLABELS".into());
        }
        if !self.selected_labels.is_empty() {
            args.push("SELECTED_LABELS".into());
            for label in &self.selected_labels {
                args.push(label.clone().into());
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RangeOptions {
    pub latest: bool,
    pub count: Option<i64>,
    pub aggregation: Option<(String, i64)>,
    pub align: Option<RedisValue>,
    pub bucket_timestamp: Option<String>,
    pub empty: bool,
    pub filter_by_ts: Vec<i64>,
    pub filter_by_value: Option<(f64, f64)>,
}

impl RangeOptions {
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = latest;
        self
    }

    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    pub fn aggregation(mut self, agg: impl Into<String>, bucket: i64) -> Self {
        self.aggregation = Some((agg.into(), bucket));
        self
    }

    pub fn align(mut self, align: impl Into<RedisValue>) -> Self {
        self.align = Some(align.into());
        self
    }

    pub fn bucket_timestamp(mut self, bucket_timestamp: impl Into<String>) -> Self {
        self.bucket_timestamp = Some(bucket_timestamp.into());
        self
    }

    pub fn empty(mut self, empty: bool) -> Self {
        self.empty = empty;
        self
    }

    pub fn filter_by_ts(mut self, timestamp: i64) -> Self {
        self.filter_by_ts.push(timestamp);
        self
    }

    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.filter_by_value = Some((min, max));
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if self.latest {
            args.push("LATEST".into());
        }

        if !self.filter_by_ts.is_empty() {
            args.push("FILTER_BY_TS".into());
            for ts in &self.filter_by_ts {
                args.push((*ts).into());
            }
        }

        if let Some((min, max)) = self.filter_by_value {
            args.push("FILTER_BY_VALUE".into());
            args.push(min.into());
            args.push(max.into());
        }

        if let Some(count) = self.count {
            push_flag(args, "COUNT", count);
        }

        if let Some((agg, bucket)) = &self.aggregation {
            args.push("AGGREGATION".into());
            args.push(agg.clone().into());
            args.push((*bucket).into());

            if let Some(align) = &self.align {
                push_flag(args, "ALIGN", align.clone());
            }
            if let Some(bucket_timestamp) = &self.bucket_timestamp {
                push_flag(args, "BUCKETTIMESTAMP", bucket_timestamp.clone());
            }
            if self.empty {
                args.push("EMPTY".into());
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MRangeOptions {
    pub latest: bool,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub count: Option<i64>,
    pub aggregation: Option<(String, i64)>,
    pub align: Option<RedisValue>,
    pub bucket_timestamp: Option<String>,
    pub empty: bool,
    pub filter_by_ts: Vec<i64>,
    pub filter_by_value: Option<(f64, f64)>,
    pub groupby: Option<(String, String)>,
}

impl MRangeOptions {
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = latest;
        self
    }

    pub fn with_labels(mut self, with_labels: bool) -> Self {
        self.with_labels = with_labels;
        self
    }

    pub fn selected_label(mut self, label: impl Into<String>) -> Self {
        self.selected_labels.push(label.into());
        self
    }

    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    pub fn aggregation(mut self, agg: impl Into<String>, bucket: i64) -> Self {
        self.aggregation = Some((agg.into(), bucket));
        self
    }

    pub fn align(mut self, align: impl Into<RedisValue>) -> Self {
        self.align = Some(align.into());
        self
    }

    pub fn bucket_timestamp(mut self, bucket_timestamp: impl Into<String>) -> Self {
        self.bucket_timestamp = Some(bucket_timestamp.into());
        self
    }

    pub fn empty(mut self, empty: bool) -> Self {
        self.empty = empty;
        self
    }

    pub fn filter_by_ts(mut self, timestamp: i64) -> Self {
        self.filter_by_ts.push(timestamp);
        self
    }

    pub fn filter_by_value(mut self, min: f64, max: f64) -> Self {
        self.filter_by_value = Some((min, max));
        self
    }

    pub fn groupby(mut self, label: impl Into<String>, reducer: impl Into<String>) -> Self {
        self.groupby = Some((label.into(), reducer.into()));
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if self.latest {
            args.push("LATEST".into());
        }
        if self.with_labels {
            args.push("WITHLABELS".into());
        }
        if !self.selected_labels.is_empty() {
            args.push("SELECTED_LABELS".into());
            for label in &self.selected_labels {
                args.push(label.clone().into());
            }
        }
        if !self.filter_by_ts.is_empty() {
            args.push("FILTER_BY_TS".into());
            for ts in &self.filter_by_ts {
                args.push((*ts).into());
            }
        }
        if let Some((min, max)) = self.filter_by_value {
            args.push("FILTER_BY_VALUE".into());
            args.push(min.into());
            args.push(max.into());
        }
        if let Some(count) = self.count {
            push_flag(args, "COUNT", count);
        }

        if let Some((agg, bucket)) = &self.aggregation {
            args.push("AGGREGATION".into());
            args.push(agg.clone().into());
            args.push((*bucket).into());

            if let Some(align) = &self.align {
                push_flag(args, "ALIGN", align.clone());
            }
            if let Some(bucket_timestamp) = &self.bucket_timestamp {
                push_flag(args, "BUCKETTIMESTAMP", bucket_timestamp.clone());
            }
            if self.empty {
                args.push("EMPTY".into());
            }
        }

        if let Some((label, reducer)) = &self.groupby {
            args.push("GROUPBY".into());
            args.push(label.clone().into());
            args.push("REDUCE".into());
            args.push(reducer.clone().into());
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OutliersOptions {
    pub latest: bool,
    pub count: Option<i64>,
}

impl OutliersOptions {
    pub fn latest(mut self, latest: bool) -> Self {
        self.latest = latest;
        self
    }

    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    pub fn write_args(&self, args: &mut Vec<RedisValue>) {
        if self.latest {
            args.push("LATEST".into());
        }
        if let Some(count) = self.count {
            push_flag(args, "COUNT", count);
        }
    }
}
