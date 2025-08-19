#[cfg(test)]
mod tests {
    use crate::labels::InternedMetricName;
    use crate::parser::metric_name::parse_metric_name;
    use crate::series::index::index_key::IndexKey;
    use crate::series::index::{TimeSeriesIndex, next_timeseries_id};
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
}
