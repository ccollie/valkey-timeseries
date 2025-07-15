use ahash::AHashMap;
use metricsql_runtime::prelude::MetricName;
use std::collections::hash_map::Entry;
use std::sync::RwLock;

use super::memory_postings::{MemoryPostings, PostingsBitmap, ALL_POSTINGS_KEY_NAME};
use super::posting_stats::{PostingStat, PostingsStats, StatsMaxHeap};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::error_consts;
use crate::labels::{InternedLabel, InternedMetricName, Label, SeriesLabel};
use crate::series::index::index_key::format_key_for_label_value;
use crate::series::{SeriesRef, TimeSeries};
use get_size::GetSize;
use std::mem::size_of;
use valkey_module::{ValkeyError, ValkeyResult};

pub struct TimeSeriesIndex {
    pub(crate) inner: RwLock<MemoryPostings>,
}

impl Default for TimeSeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TimeSeriesIndex {
    fn clone(&self) -> Self {
        let inner = self.inner.read().unwrap();
        let new_inner = inner.clone();
        TimeSeriesIndex {
            inner: RwLock::new(new_inner),
        }
    }
}
impl TimeSeriesIndex {
    pub fn new() -> Self {
        TimeSeriesIndex {
            inner: RwLock::new(MemoryPostings::default()),
        }
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    // todo: can this deadlock ?
    pub fn swap(&self, other: &Self) {
        let mut self_inner = self.inner.write().unwrap();
        let mut other_inner = other.inner.write().unwrap();
        self_inner.swap(&mut other_inner);
    }

    pub fn index_timeseries(&self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let mut inner = self.inner.write().unwrap();

        let id = ts.id;
        let measurement = ts.labels.get_measurement();
        if !measurement.is_empty() {
            // todo: error !
        }

        for InternedLabel { name, value } in ts.labels.iter() {
            inner.add_posting_for_label_value(id, name, value);
        }

        inner.add_id_to_all_postings(id);
        inner.set_timeseries_key(id, key);
    }

    pub fn reindex_timeseries(&self, series: &TimeSeries, key: &[u8]) {
        self.remove_timeseries(series);
        self.index_timeseries(series, key);
    }

    pub fn remove_timeseries(&self, series: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_timeseries(series);
    }

    pub fn has_id(&self, id: SeriesRef) -> bool {
        let inner = self.inner.read().unwrap();
        inner.has_id(id)
    }

    /// Return all series ids corresponding to the given label value pairs
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let inner = self.inner.read().unwrap();
        inner.postings_by_labels(labels)
    }

    /// This exists primarily to ensure that we disallow duplicate metric names
    pub fn posting_by_labels(&self, labels: &[Label]) -> ValkeyResult<Option<SeriesRef>> {
        let acc = self.postings_by_labels(labels);
        match acc.cardinality() {
            0 => Ok(None),
            1 => Ok(Some(acc.iter().next().expect("cardinality should be 1"))),
            _ => Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES)),
        }
    }

    /// Returns a unique series corresponding to the given labels. This exists primarily to ensure that we
    /// disallow indexing of multiple series with the same metric name and labels;
    ///
    /// NOTE, this will return a value for a subset of labels, as it would be expensive to check all labels. For example, if you
    /// have a series with labels `a=1, b=2, c=3` and you query for `a=1, b=2`, it will return an id if a single id exists.
    ///  In that case, checking for a pre-existing series is a higher layer concern.
    pub fn unique_series_id_by_labels(
        &self,
        labels: &InternedMetricName,
    ) -> ValkeyResult<Option<SeriesRef>> {
        let inner = self.inner.read().expect("poisoned memory lock");
        let mut key: String = String::new();
        let mut first = true;
        let mut acc = PostingsBitmap::new();
        let mut count = 0;

        for label in labels.iter() {
            format_key_for_label_value(&mut key, label.name(), label.value());
            let Some(bmp) = inner.label_index.get(key.as_bytes()) else {
                return Ok(None);
            };
            if bmp.is_empty() {
                return Ok(None);
            }
            if first {
                acc |= bmp;
                first = false;
            } else {
                acc &= bmp;
            }
            count += 1;
        }

        // disallow subset matches
        if count < labels.len() {
            return Ok(None);
        }

        if !inner.stale_ids.is_empty() {
            acc.andnot_inplace(&inner.stale_ids);
        }

        match acc.cardinality() {
            0 => Ok(None),
            1 => Ok(Some(acc.iter().next().expect("cardinality should be 1"))),
            _ => Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES)),
        }
    }

    pub fn posting_by_metric_name(&self, metric: &MetricName) -> ValkeyResult<Option<SeriesRef>> {
        let inner = self.inner.read().unwrap();
        inner.posting_by_metric_name(metric)
    }

    pub fn stats(&self, label: &str, limit: usize) -> PostingsStats {
        #[derive(Clone, Copy)]
        struct SizeAccumulator {
            size: usize,
            count: u64,
        }

        let mut count_map: AHashMap<&str, SizeAccumulator> = AHashMap::new();
        let mut metrics = StatsMaxHeap::new(limit);
        let mut labels = StatsMaxHeap::new(limit);
        let mut label_value_length = StatsMaxHeap::new(limit);
        let mut label_value_pairs = StatsMaxHeap::new(limit);
        let mut num_label_pairs = 0;

        let inner = self.inner.read().unwrap();

        for (key, bitmap) in inner.label_index.iter() {
            let count = bitmap.cardinality();
            if let Some((name, value)) = key.split() {
                let size = key.get_size() + get_bitmap_size(bitmap);
                match count_map.entry(name) {
                    Entry::Occupied(mut entry) => {
                        let acc = entry.get_mut();
                        acc.count += count;
                        acc.size += size;
                    }
                    Entry::Vacant(entry) => {
                        let acc = SizeAccumulator { size, count };
                        entry.insert(acc);
                    }
                }

                label_value_pairs.push(PostingStat {
                    name: format!("{name}={value}"),
                    count,
                });
                num_label_pairs += 1;

                if label == name {
                    metrics.push(PostingStat {
                        name: name.to_string(),
                        count,
                    });
                }
            }
        }

        let mut num_labels: usize = 0;

        for (name, v) in count_map {
            labels.push(PostingStat {
                name: name.to_string(),
                count: v.count,
            });
            label_value_length.push(PostingStat {
                name: name.to_string(),
                count: v.size as u64,
            });
            if name != METRIC_NAME_LABEL && name != ALL_POSTINGS_KEY_NAME {
                num_labels += 1;
            }
        }

        let series_count = self.count() as u64;

        PostingsStats {
            cardinality_metrics_stats: metrics.into_vec(),
            cardinality_label_stats: labels.into_vec(),
            label_value_stats: label_value_length.into_vec(),
            label_value_pairs_stats: label_value_pairs.into_vec(),
            num_label_pairs,
            num_labels,
            series_count,
        }
    }

    pub fn count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.count()
    }

    #[allow(dead_code)]
    pub fn label_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.label_index.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn with_postings<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&MemoryPostings, &mut STATE) -> R,
    {
        let inner = self.inner.read().unwrap();
        f(&inner, state)
    }

    #[cfg(test)]
    pub fn with_postings_mut<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&mut MemoryPostings, &mut STATE) -> R,
    {
        let mut inner = self.inner.write().unwrap();
        f(&mut inner, state)
    }

    pub fn rename_series(&self, old_key: &[u8], new_key: &[u8]) -> bool {
        let mut inner = self.inner.write().unwrap();
        inner.rename_series_key(old_key, new_key).is_some()
    }

    /// Immediately after a series is removed, it's data still exist in the index. This function marks the key as stale, so the index
    /// can be cleaned up lazily. This is a workaround for the fact that we don't have access to the series data when the server 'del'
    /// and associated events are triggered.
    pub fn mark_key_as_stale(&self, key: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.mark_key_as_stale(key);
    }

    pub fn mark_id_as_stale(&self, id: SeriesRef) {
        let mut inner = self.inner.write().unwrap();
        inner.mark_id_as_stale(id);
    }

    pub fn remove_stale_ids(&self) -> usize {
        const BATCH_SIZE: usize = 100;

        let mut inner = self.inner.write().expect("TimeSeries lock poisoned");
        let old_count = inner.stale_ids.cardinality();
        if old_count == 0 {
            return 0; // No stale IDs to remove
        }

        let mut cursor = None;
        while let Some(new_cursor) = inner.remove_stale_ids(cursor, BATCH_SIZE) {
            // Continue removing stale IDs in batches
            cursor = Some(new_cursor);
        }

        (old_count - inner.stale_ids.cardinality()) as usize // Return number of removed IDs
    }

    pub fn optimize(&self) {
        let mut inner = self.inner.write().unwrap();
        let mut keys_to_remove = Vec::new();
        for (key, bmp) in inner.label_index.iter_mut() {
            if bmp.is_empty() {
                keys_to_remove.push(key.clone());
            }
        }
        for key in keys_to_remove {
            inner.label_index.remove(&key);
        }
    }
}

fn get_bitmap_size(bmp: &PostingsBitmap) -> usize {
    bmp.cardinality() as usize * size_of::<SeriesRef>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::InternedMetricName;
    use crate::parser::metric_name::parse_metric_name;
    use crate::series::index::next_timeseries_id;
    use crate::series::time_series::TimeSeries;

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();

        let labels = parse_metric_name(prometheus_name).unwrap();
        ts.labels = InternedMetricName::new(&labels);
        ts
    }

    fn create_test_series(metric_name: &str, labels: &[(&str, &str)]) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();

        let mut new_labels: InternedMetricName =
            InternedMetricName::with_capacity(labels.len() + 1);
        new_labels.add_label(METRIC_NAME_LABEL, metric_name);
        for (key, value) in labels {
            new_labels.add_label(key, value);
        }

        ts.labels = new_labels;
        ts
    }

    #[test]
    fn test_index_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        assert_eq!(index.count(), 1);
        assert_eq!(index.label_count(), 3); // metric_name + region + env
    }

    #[test]
    fn test_reindex_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        ts.labels.add_label("service", "web");
        ts.labels.add_label("pod", "pod-1");

        index.reindex_timeseries(&ts, b"time-series-1");

        assert_eq!(index.count(), 1);
        assert_eq!(index.label_count(), 5); // metric_name + region + env
    }

    #[test]
    fn test_remove_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        index.remove_timeseries(&ts);

        assert_eq!(index.count(), 0);
        assert_eq!(index.label_count(), 0);
    }

    #[test]
    fn test_get_id_by_name_and_labels() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        let labels = ts
            .labels
            .iter()
            .map(|x| Label::new(x.name, x.value))
            .collect::<Vec<_>>();
        let id = index
            .posting_by_labels(&labels)
            .unwrap()
            .unwrap_or_default();
        assert_eq!(id, ts.id);
    }

    #[test]
    fn test_unique_series_id_by_labels_single_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);

        index.index_timeseries(&ts, b"key1");

        let result = index.unique_series_id_by_labels(&ts.labels);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(ts.id));
    }

    #[test]
    fn test_unique_series_id_by_labels_no_matching_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);

        index.index_timeseries(&ts, b"key1");

        // Query for different labels
        let query_labels = InternedMetricName::from(vec![
            Label::new("__name__", "memory_usage"),
            Label::new("host", "server1"),
            Label::new("env", "prod"),
        ]);

        let result = index.unique_series_id_by_labels(&query_labels);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_unique_id_by_labels_partial_match() {
        let index = TimeSeriesIndex::new();
        let ts = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);

        index.index_timeseries(&ts, b"key1");

        // Query with only a subset of labels
        let query_labels = InternedMetricName::from(vec![
            Label::new("__name__", "cpu_usage"),
            Label::new("host", "server1"),
        ]);

        let result = index.unique_series_id_by_labels(&query_labels);

        assert!(result.is_ok());
        // NOTE: if we find an efficient way to handle partial matches, this should return Ok(None)
        assert_eq!(result.unwrap(), Some(ts.id));
    }

    #[test]
    fn test_unique_series_id_by_labels_duplicate_series_error() {
        let index = TimeSeriesIndex::new();

        // Create two series with identical labels
        let ts1 = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);
        let ts2 = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);

        index.index_timeseries(&ts1, b"key1");
        index.index_timeseries(&ts2, b"key2");

        let result = index.unique_series_id_by_labels(&ts1.labels);

        assert!(result.is_err());
        match result.unwrap_err() {
            ValkeyError::Str(msg) => assert_eq!(msg, error_consts::DUPLICATE_SERIES),
            _ => panic!("Expected DUPLICATE_SERIES error"),
        }
    }

    #[test]
    fn test_unique_series_id_by_labels_multiple_unique_series() {
        let index = TimeSeriesIndex::new();

        let ts1 = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);
        let ts2 = create_test_series("cpu_usage", &[("host", "server2"), ("env", "prod")]);
        let ts3 = create_test_series("memory_usage", &[("host", "server1"), ("env", "prod")]);

        index.index_timeseries(&ts1, b"key1");
        index.index_timeseries(&ts2, b"key2");
        index.index_timeseries(&ts3, b"key3");

        // Query for ts1
        let result1 = index.unique_series_id_by_labels(&ts1.labels);
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), Some(ts1.id));

        // Query for ts2
        let result2 = index.unique_series_id_by_labels(&ts2.labels);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), Some(ts2.id));

        // Query for ts3
        let result3 = index.unique_series_id_by_labels(&ts3.labels);
        assert!(result3.is_ok());
        assert_eq!(result3.unwrap(), Some(ts3.id));
    }

    #[test]
    fn test_unique_series_id_by_labels_after_removal() {
        let index = TimeSeriesIndex::new();
        let ts = create_test_series("cpu_usage", &[("host", "server1"), ("env", "prod")]);

        index.index_timeseries(&ts, b"key1");

        let result = index.unique_series_id_by_labels(&ts.labels);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(ts.id));

        // Remove the series
        index.remove_timeseries(&ts);

        // Query should return None after removal
        let result_after_removal = index.unique_series_id_by_labels(&ts.labels);
        assert!(result_after_removal.is_ok());
        assert_eq!(result_after_removal.unwrap(), None);
    }

    #[test]
    fn test_unique_series_id_by_labels_with_metric_name_only() {
        let index = TimeSeriesIndex::new();
        let ts = create_test_series("cpu_usage", &[]);

        index.index_timeseries(&ts, b"key1");

        let labels = InternedMetricName::from(vec![Label::new("__name__", "cpu_usage")]);
        let result = index.unique_series_id_by_labels(&labels);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(ts.id));
    }

    #[test]
    fn test_unique_series_id_by_labels_with_extra_labels() {
        let index = TimeSeriesIndex::new();
        let ts = create_test_series("cpu_usage", &[("host", "server1")]);

        index.index_timeseries(&ts, b"key1");

        // Query with extra labels not in the series
        let labels = InternedMetricName::from(vec![
            Label::new("__name__", "cpu_usage"),
            Label::new("host", "server1"),
            Label::new("env", "prod"), // This label doesn't exist in the series
        ]);

        let result = index.unique_series_id_by_labels(&labels);

        assert!(result.is_ok());
        // Should return None because the extra label doesn't match
        assert_eq!(result.unwrap(), None);
    }
}
