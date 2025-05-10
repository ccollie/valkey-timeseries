use crate::aggregators::{AggOp, AggregateIterator, Aggregator};
use crate::common::{Sample, Timestamp};
use crate::labels::InternedLabel;
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::series::TimeSeries;

pub(crate) fn get_range(
    series: &TimeSeries,
    args: &RangeOptions,
    check_retention: bool,
) -> Vec<Sample> {
    let (start_timestamp, end_timestamp) =
        args.date_range
            .get_series_range(series, None, check_retention);
    let mut range = series.get_range_filtered(
        start_timestamp,
        end_timestamp,
        args.timestamp_filter.as_deref(),
        args.value_filter,
    );

    if let Some(aggr_options) = &args.aggregation {
        aggregate_samples(
            range.into_iter(),
            start_timestamp,
            end_timestamp,
            aggr_options,
        )
    } else {
        if let Some(count) = args.count {
            range.truncate(count);
        }
        range
    }
    // group by
}

pub(crate) fn aggregate_samples<T: Iterator<Item = Sample>>(
    iter: T,
    start_ts: Timestamp,
    end_ts: Timestamp,
    aggr_options: &AggregationOptions,
) -> Vec<Sample> {
    let aligned_timestamp = aggr_options
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    let iter = AggregateIterator::new(iter, aggr_options, aligned_timestamp);
    iter.collect::<Vec<_>>()
}

pub fn get_series_labels<'a>(
    series: &'a TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<InternedLabel<'a>>> {
    if !with_labels || selected_labels.is_empty() {
        return vec![];
    }

    if selected_labels.is_empty() {
        series.labels.iter().map(Some).collect::<Vec<_>>()
    } else {
        selected_labels
            .iter()
            .map(|name| series.get_label(name))
            .collect::<Vec<_>>()
    }
}

/// Perform the GROUP BY REDUCE operation on the samples. Specifically, it
/// aggregates non-NAN samples based on the specified aggregation options.
pub(crate) fn group_reduce(
    samples: impl Iterator<Item = Sample>,
    aggregator : Aggregator,
) -> Vec<Sample> {
    let mut samples = samples.into_iter()
        .filter(|sample| !sample.value.is_nan());;
    let mut aggregator = aggregator;
    
    let mut current = if let Some(current) = samples.next() {
        aggregator.update(current.value);
        current
    } else {
        return vec![];
    };
    
    let mut result = vec![];

    for next in samples{
        if next.timestamp == current.timestamp {
            aggregator.update(next.value);
        } else {
            let value = aggregator.finalize();
            result.push(Sample {
                timestamp: current.timestamp,
                value,
            });
            aggregator.update(next.value);
            current = next;
        }
    }

    // Finalize last
    let value = aggregator.finalize();
    result.push(Sample {
        timestamp: current.timestamp,
        value,
    });

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregator, BucketAlignment, BucketTimestamp};
    use crate::series::{TimeSeriesOptions, TimestampRange, TimestampValue, ValueFilter};
    use std::time::Duration;

    fn create_test_series() -> TimeSeries {
        let options = TimeSeriesOptions {
            retention: Some(Duration::from_secs(3600)),
            ..Default::default()
        };

        let mut series = TimeSeries::with_options(options).unwrap();

        // Add samples at 10ms intervals from 100 to 200
        for i in 0..11 {
            let ts = 100 + (i * 10);
            let value = i as f64 * 1.5;
            series.add(ts, value, None);
        }

        series
    }

    #[test]
    fn test_get_range_basic() {
        let series = create_test_series();
        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(120),
                end: TimestampValue::Specific(160),
            },
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 5);
        assert_eq!(result[0].timestamp, 120);
        assert_eq!(result[0].value, 3.0);
        assert_eq!(result[4].timestamp, 160);
        assert_eq!(result[4].value, 9.0);
    }

    #[test]
    fn test_get_range_with_count_limit() {
        let series = create_test_series();
        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            count: Some(3),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[1].timestamp, 110);
        assert_eq!(result[2].timestamp, 120);
    }

    #[test]
    fn test_get_range_with_aggregation() {
        let series = create_test_series();

        let aggr_options = AggregationOptions {
            aggregator: Aggregator::Sum(Default::default()),
            bucket_duration: 20,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(180),
            },
            aggregation: Some(aggr_options),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 5);
        // First bucket [100-120): values 0.0 + 1.5 = 1.5
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[0].value, 1.5);
        // Second bucket [120-140): values 3.0 + 4.5 = 7.5
        assert_eq!(result[1].timestamp, 120);
        assert_eq!(result[1].value, 7.5);
    }

    #[test]
    fn test_get_range_with_value_filter() {
        let series = create_test_series();
        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            value_filter: Some(ValueFilter::greater_than(4.0)),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, false);

        // Only samples with value > 4.0 should be included
        assert_eq!(result.len(), 8); // From timestamps 130 to 200
        assert_eq!(result[0].timestamp, 130);
        assert_eq!(result[0].value, 4.5);
    }

    #[test]
    fn test_get_range_with_timestamp_filter() {
        let series = create_test_series();
        let timestamps = vec![110, 130, 150, 170];

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            timestamp_filter: Some(timestamps),
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].timestamp, 110);
        assert_eq!(result[1].timestamp, 130);
        assert_eq!(result[2].timestamp, 150);
        assert_eq!(result[3].timestamp, 170);
    }

    // #[test]
    // fn test_get_range_without_retention_check() {
    //     let mut series = create_test_series();
    //
    //     let range = series.last_timestamp() - series.first_timestamp;
    //     series.retention = Duration::from_secs(range as u64);
    //
    //     // Add an old timestamp that would be filtered out by retention
    //     series.add(50, 100.0, None);
    //
    //     let range_options = RangeOptions {
    //         date_range: TimestampRange {
    //             start: TimestampValue::Specific(0),
    //             end: TimestampValue::Specific(200),
    //         },
    //         ..Default::default()
    //     };
    //
    //     // With retention check (should filter out old sample)
    //     let result_with_retention = get_range(&series, &range_options, true);
    //     // Without retention check (should include old sample)
    //     let result_without_retention = get_range(&series, &range_options, false);
    //
    //     assert!(result_with_retention.len() < result_without_retention.len());
    //     assert_eq!(result_without_retention[0].timestamp, 50);
    //     assert_eq!(result_without_retention[0].value, 100.0);
    // }

    #[test]
    fn test_get_range_empty_result() {
        let series = create_test_series();

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(300),
                end: TimestampValue::Specific(400),
            },
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_range_with_all_filters() {
        let series = create_test_series();
        let timestamps = vec![120, 130, 140, 150, 160];

        let aggr_options = AggregationOptions {
            aggregator: Aggregator::Avg(Default::default()),
            bucket_duration: 20,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: false,
        };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            timestamp_filter: Some(timestamps),
            value_filter: Some(ValueFilter::greater_than(3.0)),
            aggregation: Some(aggr_options),
            count: None, // count is ignored for aggregated results
            ..Default::default()
        };

        let result = get_range(&series, &range_options, true);

        // Should apply timestamp filter, then value filter, then aggregate, then limit count
        assert_eq!(result.len(), 3);
    }
}
