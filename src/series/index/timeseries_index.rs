use ahash::AHashMap;
use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::sync::{RwLock, RwLockReadGuard};

use super::posting_stats::{PostingStat, PostingsStats, StatsMaxHeap};
use super::postings::{ALL_POSTINGS_KEY_NAME, Postings, PostingsBitmap};
use crate::common::context::is_real_user_client;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::labels::{Label, SeriesLabel};
use crate::series::acl::{clone_permissions, has_all_keys_permissions};
use crate::series::index::IndexKey;
use crate::series::{SeriesRef, TimeSeries};
use get_size2::GetSize;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

/// A read-only guard for accessing Postings data.
/// This provides a safe, ergonomic way to read from Postings without allowing modification.
pub struct PostingsReadGuard<'a> {
    guard: RwLockReadGuard<'a, Postings>,
}

impl<'a> PostingsReadGuard<'a> {
    /// Creates a new PostingsReadGuard from an RwLockReadGuard
    pub(crate) fn new(guard: RwLockReadGuard<'a, Postings>) -> Self {
        Self { guard }
    }
}

impl<'a> Deref for PostingsReadGuard<'a> {
    type Target = Postings;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

pub struct PostingsWriteGuard<'a> {
    guard: std::sync::RwLockWriteGuard<'a, Postings>,
}

impl<'a> PostingsWriteGuard<'a> {
    fn new(guard: std::sync::RwLockWriteGuard<'a, Postings>) -> Self {
        Self { guard }
    }
}

impl<'a> Deref for PostingsWriteGuard<'a> {
    type Target = Postings;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for PostingsWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

pub struct TimeSeriesIndex {
    pub(crate) inner: RwLock<Postings>,
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
            inner: RwLock::new(Postings::default()),
        }
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    pub fn swap(&self, other: &Self) {
        let mut self_inner = self.inner.write().unwrap();
        let mut other_inner = other.inner.write().unwrap();
        self_inner.swap(&mut other_inner);
    }

    pub fn index_timeseries(&self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let mut inner = self.inner.write().unwrap();
        inner.index_timeseries(ts, key);
    }

    pub fn reindex_timeseries(&self, series: &TimeSeries, key: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_timeseries(series);
        inner.index_timeseries(series, key);
    }

    pub fn remove_timeseries(&self, series: &TimeSeries) {
        let mut inner = self.inner.write().unwrap();
        inner.remove_timeseries(series);
    }

    pub fn has_id(&self, id: SeriesRef) -> bool {
        let inner = self.inner.read().unwrap();
        inner.has_id(id)
    }

    pub fn get_postings(&'_ self) -> PostingsReadGuard<'_> {
        let guard = self.inner.read().unwrap();
        PostingsReadGuard::new(guard)
    }

    pub fn get_postings_mut(&'_ self) -> PostingsWriteGuard<'_> {
        let guard = self.inner.write().unwrap();
        PostingsWriteGuard::new(guard)
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

    /// Retrieves the series identifier (ID) corresponding to a specific set of labels.
    ///
    /// This method looks up the series ID in the underlying postings index based on the provided
    /// labels. For instance, if we have a time series with the metric name
    ///
    /// `http_requests_total{status="200", method="GET", service="inference"}`,
    ///
    /// we can retrieve its series ID by passing the appropriate labels to this function.
    ///
    /// ```
    /// let labels = vec![
    ///     Label::new("__name__", "http_requests_total"),
    ///     Label::new("status", "200"),
    ///     Label::new("method", "GET"),
    ///     Label::new("service", "inference")
    /// ];
    /// if let Some(series_id) = index.series_id_by_labels(&labels) {
    ///     println!("Found series ID: {:?}", series_id);
    /// } else {
    ///    println!("No series found with the given labels.");
    /// }
    /// ```
    /// # Arguments
    ///
    /// * `labels` - A slice of `Label` objects that describe the series to look up.
    ///
    /// # Returns
    ///
    /// * `Option<SeriesRef>` - Returns `Some(SeriesRef)` if a matching series ID is found,
    ///
    pub fn series_id_by_labels(&self, labels: &[Label]) -> Option<SeriesRef> {
        let inner = self.inner.read().unwrap();
        inner.posting_id_by_labels(labels)
    }

    /// `postings_for_filters` assembles a single postings iterator against the series index
    /// based on the given matchers.
    #[allow(dead_code)]
    pub fn postings_for_selector(&self, selector: &SeriesSelector) -> ValkeyResult<PostingsBitmap> {
        let mut state = ();
        self.with_postings(&mut state, move |inner, _| {
            let postings = inner.postings_for_selector(selector)?;
            let res = postings.into_owned();
            Ok(res)
        })
    }

    pub fn get_label_names(&self) -> BTreeSet<String> {
        let inner = self.inner.read().unwrap();
        inner.get_label_names()
    }

    pub fn get_label_values(&self, label_name: &str) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.get_label_values(label_name)
    }

    /// Returns the series keys that match the given selectors.
    /// If `acl_permissions` is provided, it checks if the current user has the required permissions
    /// to access all the keys.
    ///
    /// ## Note
    /// If the user does not have permission to access all keys, an error is returned.
    /// Non-user clients (e.g., AOF client) bypass permission checks.
    pub fn keys_for_selectors(
        &self,
        ctx: &Context,
        filters: &[SeriesSelector],
        acl_permissions: Option<AclPermissions>,
    ) -> ValkeyResult<Vec<ValkeyString>> {
        let mut keys: Vec<ValkeyString> = Vec::new();
        let mut missing_keys: Vec<SeriesRef> = Vec::new();

        let postings = self.inner.read()?;
        // get keys from ids
        let ids = postings.postings_for_selectors(filters)?;

        let mut expected_count = ids.cardinality() as usize;
        if expected_count == 0 {
            return Ok(Vec::new());
        }

        keys.reserve(expected_count);

        let current_user = ctx.get_current_user();
        let is_user_client = is_real_user_client(ctx);

        let cloned_perms = acl_permissions.as_ref().map(clone_permissions);
        let can_access_all_keys = has_all_keys_permissions(ctx, &current_user, acl_permissions);

        for series_ref in ids.iter() {
            let key = postings.get_key_by_id(series_ref);
            match key {
                Some(key) => {
                    let real_key = ctx.create_string(key.as_ref());
                    if is_user_client
                        && !can_access_all_keys
                        && let Some(perms) = &cloned_perms
                    {
                        // check if the user has permission for this key
                        if ctx
                            .acl_check_key_permission(&current_user, &real_key, perms)
                            .is_err()
                        {
                            break;
                        }
                    }
                    keys.push(real_key);
                }
                None => {
                    // this should not happen, but in case it does, we log an error and continue
                    missing_keys.push(series_ref);
                }
            }
        }

        expected_count -= missing_keys.len();

        if keys.len() != expected_count {
            // User does not have permission to read some keys, or some keys are missing
            // Customize the error message accordingly
            match cloned_perms {
                Some(perms) => {
                    if perms.contains(AclPermissions::DELETE) {
                        return Err(ValkeyError::Str(
                            error_consts::ALL_KEYS_WRITE_PERMISSION_ERROR,
                        ));
                    }
                    if perms.contains(AclPermissions::UPDATE) {
                        return Err(ValkeyError::Str(
                            error_consts::ALL_KEYS_WRITE_PERMISSION_ERROR,
                        ));
                    }
                    return Err(ValkeyError::Str(
                        error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                    ));
                }
                None => {
                    // todo: fix the problem here, for now we just log a warning
                    ctx.log_warning("Index consistency: some keys are missing from the index.");
                }
            }
        }

        if !missing_keys.is_empty() {
            let msg = format!(
                "Index consistency: {} keys are missing from the index.",
                missing_keys.len()
            );
            ctx.log_warning(&msg);

            let mut postings = self.inner.write()?;
            for missing_id in missing_keys {
                postings.mark_id_as_stale(missing_id);
            }
        }

        Ok(keys)
    }

    pub fn get_cardinality_by_selectors(
        &self,
        selectors: &[SeriesSelector],
    ) -> ValkeyResult<usize> {
        if selectors.is_empty() {
            return Ok(0);
        }

        let mut state = ();

        self.with_postings(&mut state, move |inner, _state| {
            let filter = &selectors[0];
            let first = inner.postings_for_selector(filter)?;
            if selectors.len() == 1 {
                return Ok(first.cardinality() as usize);
            }
            let mut result = first.into_owned();
            for selector in &selectors[1..] {
                let postings = inner.postings_for_selector(selector)?;
                result.and_inplace(&postings);
            }

            Ok(result.cardinality() as usize)
        })
    }

    pub fn stats(&self, label: &str, limit: usize) -> PostingsStats {
        #[derive(Copy, Clone)]
        struct SizeAccumulator {
            size: usize,
            count: u64,
        }

        let mut count_map: AHashMap<&str, SizeAccumulator> = AHashMap::new();
        let mut metrics = StatsMaxHeap::new(limit);
        let mut labels = StatsMaxHeap::new(limit);
        let mut label_value_length = StatsMaxHeap::new(limit);
        let mut label_value_pairs = StatsMaxHeap::new(limit);

        let inner = self.inner.read().unwrap();

        let posting_count = inner.label_index.len();
        let num_label_pairs = if inner.has_all_postings() {
            posting_count.saturating_sub(1)
        } else {
            posting_count
        };

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

                if label == name {
                    metrics.push(PostingStat {
                        name: value.to_string(),
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
            if name != ALL_POSTINGS_KEY_NAME {
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
        F: FnOnce(&Postings, &mut STATE) -> R,
    {
        let inner = self.inner.read().unwrap();
        f(&inner, state)
    }

    pub fn with_postings_mut<F, R, STATE>(&self, state: &mut STATE, f: F) -> R
    where
        F: FnOnce(&mut Postings, &mut STATE) -> R,
    {
        let mut inner = self.inner.write().unwrap();
        f(&mut inner, state)
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

    pub fn optimize_incremental(
        &self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        let mut inner = self.inner.write().unwrap();
        inner.optimize_postings(start_prefix, count)
    }
}

fn get_bitmap_size(bmp: &PostingsBitmap) -> usize {
    bmp.cardinality() as usize * size_of::<SeriesRef>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::index::next_timeseries_id;
    use crate::series::time_series::TimeSeries;

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();
        ts.labels = prometheus_name.parse().unwrap();
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
    fn test_get_cardinality_by_selectors_empty() {
        let index = TimeSeriesIndex::new();

        // When no selectors are provided, cardinality should be zero.
        let selectors: Vec<SeriesSelector> = Vec::new();
        let cardinality = index
            .get_cardinality_by_selectors(&selectors)
            .expect("call should succeed");

        assert_eq!(cardinality, 0);
    }

    #[test]
    fn test_get_cardinality_by_single_selector() {
        let index = TimeSeriesIndex::new();

        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        index.index_timeseries(&ts, b"time-series-1");

        // Build a selector that matches the inserted series.
        let selector = SeriesSelector::parse("region='us-east-1'").unwrap();
        let selectors = vec![selector];

        let cardinality = index
            .get_cardinality_by_selectors(&selectors)
            .expect("call should succeed");

        // We inserted exactly one matching series.
        assert_eq!(cardinality, 1);
    }

    #[test]
    fn test_get_cardinality_by_multiple_selectors_and_intersection() {
        let index = TimeSeriesIndex::new();

        // Two series that share some labels but differ on at least one.
        let ts1 =
            create_series_from_metric_name(r#"latency{region="us-east-1",env="qa",service="api"}"#);
        let ts2 = create_series_from_metric_name(
            r#"latency{region="us-east-1",env="prod",service="api"}"#,
        );

        index.index_timeseries(&ts1, b"time-series-1");
        index.index_timeseries(&ts2, b"time-series-2");

        // You should construct these selectors so that:
        //   selector_env_qa matches only ts1
        //   selector_region matches both ts1 and ts2
        //
        // After AND-ing them, only ts1 should remain, so the
        // resulting cardinality must be 1.
        let selector_env_qa = SeriesSelector::parse("env='qa'").unwrap();
        let selector_region = SeriesSelector::parse(r#"region="us-east-1""#).unwrap();

        let selectors = vec![selector_region, selector_env_qa];

        let cardinality = index
            .get_cardinality_by_selectors(&selectors)
            .expect("call should succeed");

        assert_eq!(
            cardinality, 1,
            "intersection of selectors should yield exactly one series"
        );
    }
}
