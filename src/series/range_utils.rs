use crate::common::{Sample, Timestamp};
use crate::iterators::TimeSeriesRangeIterator;
use crate::labels::InternedLabel;
use crate::series::TimeSeries;
use crate::series::request_types::RangeOptions;
use std::cmp::Ordering;
use valkey_module::Context;

#[allow(dead_code)]
pub fn filter_sample_by_timestamps_internal(
    timestamp: Timestamp,
    timestamps: &[Timestamp],
    index: &mut usize,
) -> Option<bool> {
    if *index >= timestamps.len() {
        return None;
    }

    let mut first_ts = timestamps[*index];
    match timestamp.cmp(&first_ts) {
        Ordering::Less => Some(false),
        Ordering::Equal => {
            *index += 1;
            Some(true)
        }
        Ordering::Greater => {
            while first_ts < timestamp && *index < timestamps.len() {
                *index += 1;
                first_ts = timestamps[*index];
                if first_ts == timestamp {
                    *index += 1;
                    return Some(true);
                }
            }
            Some(false)
        }
    }
}

#[allow(dead_code)]
pub(super) fn filter_samples_by_timestamps(samples: &mut Vec<Sample>, timestamps: &[Timestamp]) {
    let mut index = 0;
    samples.retain(move |sample| {
        filter_sample_by_timestamps_internal(sample.timestamp, timestamps, &mut index)
            .unwrap_or_default()
    });
}

pub(crate) fn get_range(
    ctx: Option<&Context>,
    series: &TimeSeries,
    args: &RangeOptions,
) -> Vec<Sample> {
    let iter = TimeSeriesRangeIterator::new(ctx, series, args, false);
    iter.collect::<Vec<Sample>>()
}

pub fn get_series_labels<'a>(
    series: &'a TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<InternedLabel<'a>>> {
    if !with_labels && selected_labels.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
    use crate::series::request_types::AggregationOptions;
    use crate::series::{TimeSeriesOptions, TimestampRange, TimestampValue, ValueFilter};
    use std::time::Duration;

    fn create_test_series() -> TimeSeries {
        let options = TimeSeriesOptions {
            retention: Some(Duration::from_secs(3600)),
            ..Default::default()
        };

        let mut series = TimeSeries::with_options(options).unwrap();

        // Add samples at 10 ms intervals from 100 to 200
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

        let result = get_range(None, &series, &range_options);

        assert_eq!(result.len(), 5);
        assert_eq!(result[0], Sample::new(120, 3.0));
        assert_eq!(result[1], Sample::new(130, 4.5));
        assert_eq!(result[2], Sample::new(140, 6.0));
        assert_eq!(result[3], Sample::new(150, 7.5));
        assert_eq!(result[4], Sample::new(160, 9.0));
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

        let result = get_range(None, &series, &range_options);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[1].timestamp, 110);
        assert_eq!(result[2].timestamp, 120);
    }

    #[test]
    fn test_get_range_with_aggregation() {
        let series = create_test_series();

        let aggr_options = AggregationOptions {
            aggregation: AggregationType::Sum,
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

        let result = get_range(None, &series, &range_options);

        assert_eq!(result.len(), 5);
        // First bucket [100-120): values 0.0 + 1.5 = 1.5
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[0].value, 1.5);
        // Second bucket [120-140): values 3.0 + 4.5 = 7.5
        assert_eq!(result[1].timestamp, 120);
        assert_eq!(result[1].value, 7.5);
    }

    #[test]
    fn test_get_range_with_aggregation_report_empty_false() {
        let samples = vec![
            Sample::new(100, 10.0),
            Sample::new(110, 20.0),
            Sample::new(150, 30.0),
            Sample::new(160, 40.0),
            Sample::new(200, 50.0),
        ];

        let mut series = TimeSeries::new();
        for sample in samples {
            series.add(sample.timestamp, sample.value, None);
        }

        let aggr_options = AggregationOptions {
            aggregation: AggregationType::Sum,
            bucket_duration: 25,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Timestamp(0),
            report_empty: false,
        };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            aggregation: Some(aggr_options),
            ..Default::default()
        };

        let result = get_range(None, &series, &range_options);

        assert_eq!(result.len(), 3);

        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[0].value, 30.);

        assert_eq!(result[1].timestamp, 150);
        assert_eq!(result[1].value, 70.0);

        assert_eq!(result[2].timestamp, 200);
        assert_eq!(result[2].value, 50.0);
    }

    // #[test]
    // fn test_range_with_aggregation_1() {
    //     // let mut series = create_test_series();
    //     let mut series = TimeSeries::with_options(TimeSeriesOptions::default()).unwrap();
    //     // Add samples at 1000 ms intervals
    //     for i in 0..100 {
    //         let ts = (i + 1) * 1000;
    //         let value = (i + 1) as f64 * 10.;
    //         series.add(ts, value, None);
    //     }
    //
    //     // let aggr_options = AggregationOptions {
    //     //     aggregation: Aggregation::Sum,
    //     //     bucket_duration: 2000,
    //     //     timestamp_output: BucketTimestamp::Start,
    //     //     alignment: BucketAlignment::Start,
    //     //     report_empty: true,
    //     // };
    //
    //     let range_options = RangeOptions {
    //         date_range: TimestampRange {
    //             start: TimestampValue::Earliest,
    //             end: TimestampValue::Latest,
    //         },
    //         value_filter: Some(ValueFilter::new(20.0, 50.0).unwrap()),
    //         ..Default::default()
    //     };
    //
    //     let result = get_range(None, &series, &range_options, true);
    //
    //     assert_eq!(result.len(), 5);
    //     // First bucket [100-120): values 0.0 + 1.5 = 1.5
    //     assert_eq!(result[0], Sample::new(100, 1.5));
    //     // Second bucket [120-140): values 3.0 + 4.5 = 7.5
    //     assert_eq!(result[1], Sample::new(120, 1.5));
    // }

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

        let result = get_range(None, &series, &range_options);

        // Only samples with value > 4.0 should be included
        assert_eq!(result.len(), 8); // From timestamps 130 to 200
        for sample in &result {
            assert!(sample.value > 4.0);
        }
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

        let result = get_range(None, &series, &range_options);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0].timestamp, 110);
        assert_eq!(result[1].timestamp, 130);
        assert_eq!(result[2].timestamp, 150);
        assert_eq!(result[3].timestamp, 170);
    }

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

        let result = get_range(None, &series, &range_options);

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_get_range_with_all_filters() {
        let series = create_test_series();
        let timestamps = vec![120, 130, 140, 150, 160];

        let aggr_options = AggregationOptions {
            aggregation: AggregationType::Avg,
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
            latest: false,
            timestamp_filter: Some(timestamps),
            value_filter: Some(ValueFilter::greater_than(3.0)),
            aggregation: Some(aggr_options),
            count: None, // count is ignored for aggregated results
        };

        let result = get_range(None, &series, &range_options);

        // Should apply timestamp filter, then value filter, then aggregate, then limit count
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_get_range_with_aggregation_report_empty_no_gaps() {
        // Negative test - don't return empty buckets when there are no gaps
        let series = create_test_series();

        // Create aggregation options with report_empty = true
        let aggr_options = AggregationOptions {
            aggregation: AggregationType::Sum,
            bucket_duration: 30, // 30ms buckets to create gaps
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Start,
            report_empty: true, // This should include empty buckets
        };

        let range_options = RangeOptions {
            date_range: TimestampRange {
                start: TimestampValue::Specific(100),
                end: TimestampValue::Specific(200),
            },
            aggregation: Some(aggr_options),
            ..Default::default()
        };

        let result = get_range(None, &series, &range_options);

        // With 30ms buckets over 100ms range (100-200), we should get buckets:
        // [100-130): contains samples at 100, 110, 120 -> sum = 0.0 + 1.5 + 3.0 = 4.5
        // [130-160): contains samples at 130, 140, 150 -> sum = 4.5 + 6.0 + 7.5 = 18.0
        // [160-190): contains samples at 160, 170, 180 -> sum = 9.0 + 10.5 + 12.0 = 31.5
        // [190-220): contains sample at 190, 200 -> sum = 13.5 + 15.0 = 28.5

        // Should have 4 buckets with report_empty=true
        assert_eq!(result.len(), 4);

        // Verify bucket timestamps (should be start of each bucket)
        assert_eq!(result[0].timestamp, 100);
        assert_eq!(result[1].timestamp, 130);
        assert_eq!(result[2].timestamp, 160);
        assert_eq!(result[3].timestamp, 190);

        assert_eq!(result[0].value, 4.5); // Sum of 0.0 + 1.5 + 3.0
        assert_eq!(result[1].value, 18.0); // Sum of 4.5 + 6.0 + 7.5
        assert_eq!(result[2].value, 31.5); // Sum of 9.0 + 10.5 + 12.0
        assert_eq!(result[3].value, 28.5); // Sum of 13.5 + 15.0
    }
}
