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
use std::mem::size_of;

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

    pub fn remove_series_by_ids(&self, ids: &[SeriesRef]) -> usize {
        const BATCH_SIZE: usize = 500;

        // First phase: Update all_postings and id_to_key
        let mut inner = self.inner.write().unwrap_or_else(|e| {
            // Handle poisoned lock gracefully
            // log::warn!("Acquiring write lock failed: {:?}", e);
            e.into_inner()
        });
        
        let all_postings = inner
            .label_index
            .get_mut(&*ALL_POSTINGS_KEY)
            .expect("ALL_POSTINGS_KEY should always exist");

        let old_count = all_postings.cardinality();
        // Use copied() instead of cloned() since SeriesRef is Copy
        all_postings.remove_all(ids.iter().copied());
        let removed_count = old_count - all_postings.cardinality();

        // Use a more efficient lookup for bulk operations
        let ids_set: ahash::AHashSet<SeriesRef> = ids.iter().copied().collect();
        inner.id_to_key.retain(|id, _| !ids_set.contains(id));

        // Only proceed if we need to update the index
        if removed_count == 0 {
            return 0;
        }

        // Second phase: Process label index in batches
        if let Some((start, end)) = inner.get_key_range() {
            // Store these outside the loop to avoid repeated clones
            let end_key = end.clone();
            let mut current_key = start.clone();
            let all_postings_str = ALL_POSTINGS_KEY.as_str();
            
            // Process batches of keys to remove
            loop {
                let mut batch_keys_to_remove = Vec::with_capacity(BATCH_SIZE);
                let mut processed = 0;
                
                // Process one batch with the lock held
                for (key, bmp) in inner
                    .label_index
                    .range_mut(current_key.clone()..=end_key.clone())
                {
                    if key.as_str() == all_postings_str {
                        processed += 1;
                        if processed == BATCH_SIZE {
                            current_key = key.clone();
                            break; // Break out of for loop after BATCH_SIZE
                        }
                        continue; // Skip ALL_POSTINGS_KEY
                    }
                    
                    let old_cardinality = bmp.cardinality();
                    bmp.remove_all(ids.iter().copied());
                    
                    if bmp.cardinality() != old_cardinality && bmp.is_empty() {
                        batch_keys_to_remove.push(key.clone());
                    }
                    
                    processed += 1;
                    if processed == BATCH_SIZE {
                        current_key = key.clone();
                        break; // Break out of for loop after BATCH_SIZE
                    }
                }
            
                // Remove the keys marked for deletion in this batch
                for key in batch_keys_to_remove {
                    inner.label_index.remove(&key);
                }
                
                // If we processed fewer than BATCH_SIZE items, we're done
                if processed < BATCH_SIZE {
                    break;
                }
                
                // Release the lock between batches to give other threads a chance
                drop(inner);
                inner = self.inner.write().unwrap_or_else(|e| {
                    // log::warn!("Reacquiring write lock failed: {:?}", e);
                    e.into_inner()
                });
            }
        }

        removed_count as usize
    }

    pub fn remove_series_by_key(&self, key: &[u8]) -> bool {
        // todo: have a tombstone for the key, so we can remove it from the index lazily
        let id = {
            let inner = self.inner.read().unwrap();
            inner.get_id_for_key(key)
        };
        if let Some(id) = id {
            let removed = self.remove_series_by_ids(&[id]);
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
    use crate::series::index::next_timeseries_id;
    use crate::series::time_series::TimeSeries;

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();

        let labels = parse_metric_name(prometheus_name).unwrap();
        ts.labels = InternedMetricName::new(&labels);
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
    fn test_remove_series_by_ids_single() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let id = ts.id;

        index.index_timeseries(&ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        let removed = index.remove_series_by_ids(&[id]);

        assert_eq!(removed, 1);
        assert_eq!(index.count(), 0);
        assert!(!index.has_id(id));
    }

    #[test]
    fn test_remove_series_by_ids_multiple() {
        let index = TimeSeriesIndex::new();

        let ts1 = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let ts2 = create_series_from_metric_name(r#"cpu{region="us-west-2",env="prod"}"#);
        let ts3 = create_series_from_metric_name(r#"memory{region="eu-west-1",env="dev"}"#);

        let id1 = ts1.id;
        let id2 = ts2.id;
        let id3 = ts3.id;

        index.index_timeseries(&ts1, b"time-series-1");
        index.index_timeseries(&ts2, b"time-series-2");
        index.index_timeseries(&ts3, b"time-series-3");
        assert_eq!(index.count(), 3);

        let removed = index.remove_series_by_ids(&[id1, id3]);

        assert_eq!(removed, 2);
        assert_eq!(index.count(), 1);
        assert!(!index.has_id(id1));
        assert!(index.has_id(id2));
        assert!(!index.has_id(id3));
    }

    #[test]
    fn test_remove_series_by_ids_nonexistent() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let id = ts.id;
        let nonexistent_id = id + 1000;

        index.index_timeseries(&ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        let removed = index.remove_series_by_ids(&[nonexistent_id]);

        assert_eq!(removed, 0);
        assert_eq!(index.count(), 1);
        assert!(index.has_id(id));
    }

    #[test]
    fn test_remove_series_by_ids_empty_list() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        let removed = index.remove_series_by_ids(&[]);

        assert_eq!(removed, 0);
        assert_eq!(index.count(), 1);
    }

    #[test]
    fn test_remove_series_by_ids_empty_index() {
        let index = TimeSeriesIndex::new();
        let removed = index.remove_series_by_ids(&[1, 2, 3]);

        assert_eq!(removed, 0);
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn test_remove_series_by_ids_batch_boundary() {
        const BATCH_TEST_SIZE: usize = 510; // Just over the batch size of 500
        let index = TimeSeriesIndex::new();
        let mut series_ids = Vec::with_capacity(BATCH_TEST_SIZE);

        // Create and index more series than the batch size
        for i in 0..BATCH_TEST_SIZE {
            let ts = create_series_from_metric_name(&format!(r#"metric_{}{{region="region-{}"}}"#, i, i));
            let id = ts.id;
            series_ids.push(id);
            index.index_timeseries(&ts, format!("time-series-{}", i).as_bytes());
        }

        assert_eq!(index.count(), BATCH_TEST_SIZE);

        // Remove all series
        let removed = index.remove_series_by_ids(&series_ids);

        assert_eq!(removed, BATCH_TEST_SIZE);
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn test_remove_series_by_ids_partial_batch() {
        const BATCH_TEST_SIZE: usize = 750; // 1.5x the batch size
        let index = TimeSeriesIndex::new();
        let mut all_series_ids = Vec::with_capacity(BATCH_TEST_SIZE);

        // Create and index more series than the batch size
        for i in 0..BATCH_TEST_SIZE {
            let ts = create_series_from_metric_name(&format!(r#"metric_{}{{region="region-{}"}}"#, i, i));
            let id = ts.id;
            all_series_ids.push(id);
            index.index_timeseries(&ts, format!("time-series-{}", i).as_bytes());
        }

        assert_eq!(index.count(), BATCH_TEST_SIZE);

        // Remove the first half of the series
        let first_half: Vec<_> = all_series_ids.iter().take(BATCH_TEST_SIZE / 2).copied().collect();
        let removed = index.remove_series_by_ids(&first_half);

        assert_eq!(removed, BATCH_TEST_SIZE / 2);
        assert_eq!(index.count(), BATCH_TEST_SIZE - (BATCH_TEST_SIZE / 2));

        // Check that the right IDs were removed
        for id in &first_half {
            assert!(!index.has_id(*id));
        }

        // Check that the remaining IDs still exist
        for id in all_series_ids.iter().skip(BATCH_TEST_SIZE / 2) {
            assert!(index.has_id(*id));
        }
    }

    #[test]
    fn test_remove_series_by_ids_shared_labels() {
        let index = TimeSeriesIndex::new();

        // Create series with shared labels
        let ts1 = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let ts2 = create_series_from_metric_name(r#"latency{region="us-east-1",env="prod"}"#);
        let ts3 = create_series_from_metric_name(r#"cpu{region="us-east-1",env="qa"}"#);

        let id1 = ts1.id;
        let id2 = ts2.id;
        let id3 = ts3.id;

        index.index_timeseries(&ts1, b"time-series-1");
        index.index_timeseries(&ts2, b"time-series-2");
        index.index_timeseries(&ts3, b"time-series-3");

        assert_eq!(index.count(), 3);

        // Remove series with shared labels
        let removed = index.remove_series_by_ids(&[id1, id3]);

        assert_eq!(removed, 2);
        assert_eq!(index.count(), 1);

        // Verify the right series was kept
        assert!(!index.has_id(id1));
        assert!(index.has_id(id2));
        assert!(!index.has_id(id3));

        // Check the label index structure is intact
        let labels = vec![Label::new("region", "us-east-1"), Label::new("env", "prod")];
        let result = index.postings_by_labels(&labels);
        assert_eq!(result.cardinality(), 1);
        assert_eq!(result.iter().next(), Some(id2));
    }

    #[test]
    fn test_remove_series_by_ids_multiple_calls() {
        let index = TimeSeriesIndex::new();

        let ts1 = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let ts2 = create_series_from_metric_name(r#"cpu{region="us-west-2",env="prod"}"#);
        let ts3 = create_series_from_metric_name(r#"memory{region="eu-west-1",env="dev"}"#);

        let id1 = ts1.id;
        let id2 = ts2.id;
        let id3 = ts3.id;

        index.index_timeseries(&ts1, b"time-series-1");
        index.index_timeseries(&ts2, b"time-series-2");
        index.index_timeseries(&ts3, b"time-series-3");

        // Remove one series
        let removed1 = index.remove_series_by_ids(&[id1]);
        assert_eq!(removed1, 1);
        assert_eq!(index.count(), 2);

        // Remove another series
        let removed2 = index.remove_series_by_ids(&[id2]);
        assert_eq!(removed2, 1);
        assert_eq!(index.count(), 1);

        // Remove the last series
        let removed3 = index.remove_series_by_ids(&[id3]);
        assert_eq!(removed3, 1);
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn test_remove_series_by_ids_consistency() {
        let index = TimeSeriesIndex::new();

        // Create a number of series
        const NUM_SERIES: usize = 100;
        let mut series_ids = Vec::with_capacity(NUM_SERIES);

        for i in 0..NUM_SERIES {
            let ts = create_series_from_metric_name(
                &format!(r#"metric_{}{{region="region-{}",env="env-{}"}}"#, i % 5, i % 10, i % 3),
            );
            let id = ts.id;
            series_ids.push(id);
            index.index_timeseries(&ts, format!("time-series-{}", i).as_bytes());
        }

        assert_eq!(index.count(), NUM_SERIES);

        // Remove half the series
        let to_remove: Vec<_> = series_ids.iter().step_by(2).copied().collect();
        let removed = index.remove_series_by_ids(&to_remove);

        assert_eq!(removed, to_remove.len());
        assert_eq!(index.count(), NUM_SERIES - to_remove.len());

        // Check for consistency - removed IDs should not exist
        for id in &to_remove {
            assert!(!index.has_id(*id));
        }

        // Remaining IDs should still exist
        for (i, id) in series_ids.iter().enumerate() {
            if i % 2 == 1 {  // Keep only odd indices
                assert!(index.has_id(*id));
            }
        }
    }
}