use super::index_key::{get_key_for_label_prefix, get_key_for_label_value, IndexKey};
use ahash::AHashMap;
use blart::AsBytes;
use std::collections::hash_map::Entry;
use std::collections::BTreeSet;
use std::io;
use std::io::{Read, Write};
use std::ops::ControlFlow;
use std::sync::RwLock;

use super::memory_postings::{
    MemoryPostings, PostingsBitmap, ALL_POSTINGS_KEY, ALL_POSTINGS_KEY_NAME,
};
use super::posting_stats::{PostingStat, PostingsStats, StatsMaxHeap};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::error_consts;
use crate::labels::{InternedLabel, Label, SeriesLabel};
use crate::series::{SeriesRef, TimeSeries};
use croaring::Bitmap64;
use get_size::GetSize;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

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

    pub fn clear(&mut self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    // todo: can this deadlock ?
    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.inner, &mut other.inner);
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

    pub fn set_timeseries_key(&mut self, id: SeriesRef, new_key: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.set_timeseries_key(id, new_key);
    }

    pub fn remove_timeseries(&self, series: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_timeseries(series);
    }

    pub fn postings_for_all_label_values(&self, label_name: &str) -> PostingsBitmap {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_prefix(label_name);
        let mut result = PostingsBitmap::new();
        for (_, map) in inner.label_index.prefix(prefix.as_bytes()) {
            result |= map;
        }
        result
    }

    pub fn max_id(&self) -> SeriesRef {
        let inner = self.inner.read().unwrap();
        inner.max_id()
    }

    pub fn get_id_for_key(&self, key: &[u8]) -> Option<SeriesRef> {
        let inner = self.inner.read().unwrap();
        inner.get_id_for_key(key)
    }

    pub fn get_key_by_id(&self, ctx: &Context, id: SeriesRef) -> Option<ValkeyString> {
        let inner = self.inner.read().unwrap();
        inner
            .get_key_by_id(id)
            .map(|key| ctx.create_string(key.as_bytes()))
    }
    
    pub fn has_id(&self, id: SeriesRef) -> bool {
        let inner = self.inner.read().unwrap();
        inner.has_id(id)
    }

    /// `postings` returns the postings list iterator for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    pub fn postings(&self, name: &str, values: &[String]) -> PostingsBitmap {
        let inner = self.inner.read().unwrap();
        inner.postings(name, values)
    }

    pub fn postings_for_label_value(&self, name: &str, value: &str) -> PostingsBitmap {
        let key = IndexKey::for_label_value(name, value);
        let inner = self.inner.read().unwrap();
        if let Some(bmp) = inner.label_index.get(&key) {
            bmp.clone()
        } else {
            PostingsBitmap::default()
        }
    }

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which `match_fn` returns true. If no postings are found having at least one matching label,
    /// an empty bitmap is returned.
    pub fn postings_for_label_matching<F, STATE>(
        &self,
        name: &str,
        state: &mut STATE,
        match_fn: F,
    ) -> PostingsBitmap
    where
        F: Fn(&str, &mut STATE) -> bool,
    {
        let inner = self.inner.read().unwrap();
        inner.postings_for_label_matching(name, state, match_fn)
    }

    pub fn label_names_for(&self, postings: impl Iterator<Item = SeriesRef>) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let bitmap: Bitmap64 = Bitmap64::from_iter(postings);
        // Slow
        let mut set: BTreeSet<String> = BTreeSet::new();
        if !bitmap.is_empty() {
            for (k, postings) in inner.label_index.iter() {
                if let Some((key, _)) = k.split() {
                    if key != ALL_POSTINGS_KEY_NAME
                        && !set.contains(key)
                        && postings.intersect(&bitmap)
                    {
                        set.insert(key.to_string());
                    }
                }
            }
        }
        set.into_iter().collect::<Vec<_>>()
    }

    /// label_names returns all the unique label names.
    pub fn label_names(&self) -> Vec<String> {
        let mut set: BTreeSet<String> = BTreeSet::new();
        let inner = self.inner.read().unwrap();

        for key in inner.label_index.keys() {
            if let Some((label, _)) = key.split() {
                if !label.is_empty() {
                    set.insert(label.to_string());
                }
            }
        }
        set.into_iter().collect()
    }

    pub fn label_values(&self, name: &str) -> Vec<String> {
        let mut values = Vec::new();
        let _ = self.process_label_values(
            name,
            &mut values,
            |_, _| true,
            |values, value, _| {
                values.push(value.to_string());
                ControlFlow::<Option<()>>::Continue(())
            },
        );
        values.sort();

        values
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

    pub fn process_label_values<T, CONTEXT, F, PRED>(
        &self,
        label: &str,
        ctx: &mut CONTEXT,
        predicate: PRED,
        f: F,
    ) -> Option<T>
    where
        F: Fn(&mut CONTEXT, &str, &PostingsBitmap) -> ControlFlow<Option<T>>,
        PRED: Fn(&str, &PostingsBitmap) -> bool,
    {
        let inner = self.inner.read().unwrap();
        let prefix = get_key_for_label_prefix(label);
        let start_pos = prefix.len();
        for (key, map) in inner.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if predicate(value, map) {
                match f(ctx, value, map) {
                    ControlFlow::Break(v) => {
                        return v;
                    }
                    ControlFlow::Continue(_) => continue,
                }
            }
        }
        None
    }

    pub fn label_value_for(&self, id: SeriesRef, label: &str) -> Option<String> {
        let mut state = ();
        self.process_label_values(
            label,
            &mut state,
            |_, postings| postings.contains(id),
            |_, value, _| ControlFlow::Break(Some(value.to_string())),
        )
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

    pub fn series_count_by_metric_name(
        &self,
        limit: usize,
        prefix: Option<&str>,
    ) -> Vec<(String, usize)> {
        let mut metrics = StatsMaxHeap::new(limit);
        let inner = self.inner.read().unwrap();
        
        let prefix = get_key_for_label_value(METRIC_NAME_LABEL, prefix.unwrap_or(""));
        for (key, bmp) in inner.label_index.prefix(prefix.as_bytes()) {
            // keys and values are expected to be utf-8. If we panic, we have bigger issues
            if let Some((_name, value)) = key.split() {
                metrics.push(PostingStat {
                    name: value.to_string(),
                    count: bmp.cardinality(),
                });
            }
        }
        let items = metrics.into_vec();
        let mut result = Vec::new();
        for item in items {
            result.push((item.name, item.count as usize));
        }
        result
    }

    pub fn serialize_into<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let inner = self.inner.read().unwrap();
        inner.serialize_into(writer)
    }

    pub fn deserialize_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let inner = MemoryPostings::deserialize_from(reader)?;
        Ok(Self {
            inner: RwLock::new(inner),
        })
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

    pub fn slow_remove_series_by_ids(&self, ids: &[SeriesRef]) {
        const BATCH_SIZE: usize = 500;

        let mut inner = self.inner.write().unwrap();
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
    }

    pub fn slow_remove_series_by_key(&self, key: &[u8]) {
        let id = {
            let inner = self.inner.read().unwrap();
            inner.get_id_for_key(key)
        };
        if let Some(id) = id {
            self.slow_remove_series_by_ids(&[id]);
        }
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
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&mut ts, b"time-series-1");

        assert_eq!(index.count(), 1);
        assert_eq!(label_count(&index), 3); // metric_name + region + env
    }

    #[test]
    fn test_reindex_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&mut ts, b"time-series-1");

        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="prod"}"#);
        index.reindex_timeseries(&mut ts, b"time-series-1");

        assert_eq!(index.count(), 2);
        assert_eq!(label_count(&index), 3); // metric_name + region + env
    }

    #[test]
    fn test_remove_time_series() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&mut ts, b"time-series-1");
        assert_eq!(index.count(), 1);

        index.remove_timeseries(&ts);

        assert_eq!(index.count(), 0);
        assert_eq!(label_count(&index), 0);
    }

    #[test]
    fn test_get_label_values() {
        let index = TimeSeriesIndex::new();
        let mut ts1 = create_series(
            "latency",
            vec![
                Label {
                    name: "region".to_string(),
                    value: "us-east1".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "dev".to_string(),
                },
            ],
        );
        let mut ts2 = create_series(
            "latency",
            vec![
                Label {
                    name: "region".to_string(),
                    value: "us-east2".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "qa".to_string(),
                },
            ],
        );

        index.index_timeseries(&mut ts1, b"time-series-1");
        index.index_timeseries(&mut ts2, b"time-series-2");

        let values = index.label_values("region");
        assert_eq!(values.len(), 2);
        assert!(contains(&values, "us-east1"));
        assert!(contains(&values, "us-east2"));

        let values = index.label_values("env");
        assert_eq!(values.len(), 2);
        assert!(contains(&values, "dev"));
        assert!(contains(&values, "qa"));
    }

    #[test]
    fn test_get_id_by_name_and_labels() {
        let index = TimeSeriesIndex::new();
        let mut ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);

        index.index_timeseries(&mut ts, b"time-series-1");

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
