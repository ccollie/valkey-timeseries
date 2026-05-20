use valkey_timeseries_fred::{MRangeOptions, RangeOptions, ValkeyTimeseriesClient};

#[test]
fn range_options_builder_tracks_variadic_filters() {
    let options = RangeOptions::default()
        .latest(true)
        .filter_by_ts(10)
        .filter_by_ts(20)
        .filter_by_value(1.0, 9.0)
        .aggregation("avg", 60_000)
        .count(100)
        .empty(true);

    assert!(options.latest);
    assert_eq!(options.filter_by_ts, vec![10, 20]);
    assert_eq!(options.filter_by_value, Some((1.0, 9.0)));
    assert_eq!(options.aggregation, Some(("avg".to_string(), 60_000)));
    assert_eq!(options.count, Some(100));
    assert!(options.empty);
}

#[test]
fn mrange_options_builder_tracks_groupby_and_selected_labels() {
    let options = MRangeOptions::default()
        .with_labels(true)
        .selected_label("host")
        .selected_label("region")
        .groupby("region", "sum");

    assert!(options.with_labels);
    assert_eq!(options.selected_labels, vec!["host", "region"]);
    assert_eq!(
        options.groupby,
        Some(("region".to_string(), "sum".to_string()))
    );
}

#[test]
fn client_is_cloneable_for_pooling_usage() {
    let client = ValkeyTimeseriesClient::from_config(fred::types::RedisConfig::default());
    let _clone = client.clone();
}
