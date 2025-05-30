#[cfg(test)]
mod tests {
    use crate::labels::InternedMetricName;
    use crate::parser::metric_name::parse_metric_name;
    use crate::series::index::index_key::IndexKey;
    use crate::series::index::{next_timeseries_id, TimeSeriesIndex};
    use crate::series::time_series::TimeSeries;
    use blart::AsBytes;

    fn create_series_from_metric_name(prometheus_name: &str) -> TimeSeries {
        let mut ts = TimeSeries::new();
        ts.id = next_timeseries_id();

        let labels = parse_metric_name(prometheus_name).unwrap();
        ts.labels = InternedMetricName::new(&labels);
        ts
    }

    #[test]
    fn test_rename_series() {
        let index = TimeSeriesIndex::new();

        // Create and index a time series
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let old_key = b"old-key";
        index.index_timeseries(&ts, old_key);

        // Create a new key for renaming
        let new_key = b"new-key";

        // Test successful rename
        let removed = index.rename_series(old_key, new_key);
        assert!(removed);

        // Verify internal state after renaming
        let mut state = ();
        index.with_postings_mut(&mut state, |postings, _| {
            // Old key should no longer exist
            let key: IndexKey = IndexKey::from(old_key.as_bytes());
            assert!(postings.key_to_id.get(&key).is_none());

            // The new key should point to the same series ID
            let key: IndexKey = IndexKey::from(new_key.as_bytes());
            let id_by_new_key = postings.key_to_id.get(&key).cloned();
            assert_eq!(id_by_new_key, Some(ts.id));

            // Series ID should point to the new key
            let key_by_id = postings.id_to_key.get(&ts.id).cloned();
            let expected = new_key.to_vec().into_boxed_slice();
            assert_eq!(key_by_id, Some(expected));
        });

        // Test rename with a non-existent key
        let non_existent_key = b"non-existent-key";
        assert!(!index.rename_series(non_existent_key, b"another-key"));

        // Renaming to the same key should succeed and return true
        assert!(index.rename_series(new_key, new_key));
    }

    #[test]
    fn test_mark_key_as_stale() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let key = b"time-series-1";

        // Index a time series
        index.index_timeseries(&ts, key);
        assert_eq!(index.count(), 1);

        // Mark the key as stale
        index.mark_key_as_stale(key);

        // Verify the inner state by checking if the id was marked as stale
        index.with_postings_mut(&mut (), |inner, _| {
            assert!(inner.stale_ids.contains(ts.id));
        });

        assert_eq!(index.count(), 0);

        // Now remove the stale ids
        let removed = index.remove_stale_ids();

        // One series should have been removed
        assert_eq!(removed, 1);
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn test_mark_nonexistent_key_as_stale() {
        let index = TimeSeriesIndex::new();
        let nonexistent_key = b"nonexistent-key";

        // Mark a key that doesn't exist as stale
        index.mark_key_as_stale(nonexistent_key);

        // This shouldn't affect the index
        assert_eq!(index.count(), 0);

        // No series should be removed when we call remove_stale_ids
        let removed = index.remove_stale_ids();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_mark_multiple_keys_as_stale() {
        let index = TimeSeriesIndex::new();

        // Create and index multiple time series
        let ts1 = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let key1 = b"time-series-1";
        index.index_timeseries(&ts1, key1);

        let ts2 = create_series_from_metric_name(r#"memory{region="us-west-2",env="prod"}"#);
        let key2 = b"time-series-2";
        index.index_timeseries(&ts2, key2);

        let ts3 = create_series_from_metric_name(r#"cpu{region="eu-west-1",env="dev"}"#);
        let key3 = b"time-series-3";
        index.index_timeseries(&ts3, key3);

        assert_eq!(index.count(), 3);

        // Mark two keys as stale
        index.mark_key_as_stale(key1);
        index.mark_key_as_stale(key3);

        // Verify the inner state
        index.with_postings_mut(&mut (), |inner, _| {
            assert!(inner.stale_ids.contains(ts1.id));
            assert!(!inner.stale_ids.contains(ts2.id));
            assert!(inner.stale_ids.contains(ts3.id));
        });

        // Remove stale ids
        let removed = index.remove_stale_ids();

        // Two series should have been removed
        assert_eq!(removed, 2);
        assert_eq!(index.count(), 1);

        // Only ts2 should remain in the index
        index.with_postings_mut(&mut (), |inner, _| {
            assert!(!inner.id_to_key.contains_key(&ts1.id));
            assert!(inner.id_to_key.contains_key(&ts2.id));
            assert!(!inner.id_to_key.contains_key(&ts3.id));
        });
    }

    #[test]
    fn test_mark_key_as_stale_idempotent() {
        let index = TimeSeriesIndex::new();
        let ts = create_series_from_metric_name(r#"latency{region="us-east-1",env="qa"}"#);
        let key = b"time-series-1";

        // Index a time series
        index.index_timeseries(&ts, key);

        // Mark the key as stale multiple times
        index.mark_key_as_stale(key);
        index.mark_key_as_stale(key);
        index.mark_key_as_stale(key);

        // Verify the stale_ids set still only contains one entry
        index.with_postings_mut(&mut (), |inner, _| {
            assert_eq!(inner.stale_ids.cardinality(), 1);
            assert!(inner.stale_ids.contains(ts.id));
        });

        // Remove stale ids
        let removed = index.remove_stale_ids();
        assert_eq!(removed, 1);
        assert_eq!(index.count(), 0);
    }
}
