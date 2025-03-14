use ahash::AHashMap;
use std::collections::hash_map::Entry;
use std::sync::RwLock;

use super::memory_postings::{
    MemoryPostings, PostingsBitmap, ALL_POSTINGS_KEY, ALL_POSTINGS_KEY_NAME,
};
use super::posting_stats::{PostingStat, PostingsStats, StatsMaxHeap};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::error_consts;
use crate::labels::{InternedLabel, Label, SeriesLabel};
use crate::series::{SeriesRef, TimeSeries};
use get_size::GetSize;
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
                    name: format!("{}={}", name, value),
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

        PostingsStats {
            cardinality_metrics_stats: metrics.into_vec(),
            cardinality_label_stats: labels.into_vec(),
            label_value_stats: label_value_length.into_vec(),
            label_value_pairs_stats: label_value_pairs.into_vec(),
            num_label_pairs,
            num_labels,
        }
    }

    pub fn count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.count()
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
    
    pub fn slow_rename_series(&self, old_key: &[u8], new_key: &[u8]) -> bool {
        // this is split in two parts because we need to do a linear scan on the id->key
        // map to find the id for the old key. We hold a write lock only for the update
        let id = {
            let inner = self.inner.read().unwrap();
            inner.get_id_for_key(old_key)
        };
        if let Some(id) = id {
            let mut inner = self.inner.write().unwrap();
            inner.set_timeseries_key(id, new_key);
            true
        } else {
            false
        }
    }

    pub fn slow_remove_series_by_ids(&self, ids: &[SeriesRef]) -> usize {
        const BATCH_SIZE: usize = 500;

        let mut inner = self.inner.write().unwrap();
        let all_postings = inner
            .label_index
            .get_mut(&*ALL_POSTINGS_KEY)
            .expect("ALL_POSTINGS_KEY should always exist");

        let old_count = all_postings.cardinality();
        all_postings.remove_all(ids.iter().cloned());
        let removed_count = old_count - all_postings.cardinality();

        inner.id_to_key.retain(|id, _| !ids.contains(id));

        let range = inner.get_key_range();

        // we process in batches to avoid holding the lock for too long
        if let Some((start, end)) = range {
            let end = end.clone();
            let mut current_start = start.clone();
            let mut keys_to_remove = Vec::new();

            drop(inner);

            loop {
                let mut i: usize = 0;
                let mut inner = self.inner.write().unwrap();

                for (key, bmp) in inner
                    .label_index
                    .range_mut(current_start.clone()..=end.clone())
                {
                    let count = bmp.cardinality();
                    bmp.remove_all(ids.iter().cloned());
                    if bmp.cardinality() != count
                        && bmp.is_empty()
                        && key.as_str() != ALL_POSTINGS_KEY.as_str()
                    {
                        keys_to_remove.push(key.clone());
                    }
                    i += 1;
                    if i == BATCH_SIZE {
                        current_start = key.clone();
                        continue;
                    }
                }

                // If we processed less than BATCH_SIZE items, we're done
                if i < BATCH_SIZE {
                    break;
                }

                drop(inner);
            }

            if !keys_to_remove.is_empty() {
                let mut inner = self.inner.write().unwrap();
                for key in keys_to_remove {
                    inner.label_index.remove(&key);
                }
            }
        }
        removed_count as usize
    }

    pub fn slow_remove_series_by_key(&self, key: &[u8]) -> bool {
        let id = {
            let inner = self.inner.read().unwrap();
            inner.get_id_for_key(key)
        };
        if let Some(id) = id {
            let removed = self.slow_remove_series_by_ids(&[id]);
            return removed > 0;
        }
        false
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
    use crate::series::time_series::TimeSeries;

    fn contains(set: &[String], needle: &str) -> bool {
        set.iter().any(|s| s == needle)
    }

    fn label_count(index: &TimeSeriesIndex) -> usize {
        let stats = index.stats("", 10);
        stats.num_label_pairs // num_labels
    }

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        let labels = parse_metric_name(prometheus_name).unwrap();
        ts.labels = InternedMetricName::new(&labels);
        ts
    }

    fn create_series(metric_name: &str, labels: Vec<Label>) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.labels = InternedMetricName::new(&labels);
        ts
    }

    #[test]
    fn test_index_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");

        assert_eq!(index.count(), 1);
        assert_eq!(label_count(&index), 3); // metric_name + region + env
    }

    #[test]
    fn test_reindex_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&mut ts, b"time-series-1");

        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="prod"}"#);
        index.reindex_timeseries(&ts, b"time-series-1");

        assert_eq!(index.count(), 2);
        assert_eq!(label_count(&index), 3); // metric_name + region + env
    }

    #[test]
    fn test_remove_time_series() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        index.remove_timeseries(&ts);

        assert_eq!(index.count(), 0);
        assert_eq!(label_count(&index), 0);
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
}
