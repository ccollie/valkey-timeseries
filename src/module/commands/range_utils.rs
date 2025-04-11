use crate::aggregators::{AggOp, AggregationHelper, AggregationOptions};
use crate::arg_types::{RangeGroupingOptions, RangeOptions};
use crate::common::{Sample, Timestamp};
use crate::labels::InternedLabel;
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
        let mut aggr_iterator = get_series_aggregator(series, args, aggr_options, check_retention);
        aggr_iterator.calculate(range.into_iter())
    } else {
        if let Some(count) = args.count {
            range.truncate(count);
        }
        range
    }
    // group by
}

fn get_series_aggregator(
    series: &TimeSeries,
    args: &RangeOptions,
    aggr_options: &AggregationOptions,
    check_retention: bool,
) -> AggregationHelper {
    let (start_timestamp, end_timestamp) =
        args.date_range
            .get_series_range(series, None, check_retention);
    let aligned_timestamp = aggr_options
        .alignment
        .get_aligned_timestamp(start_timestamp, end_timestamp);

    AggregationHelper::new(aggr_options, aligned_timestamp)
}

pub(crate) fn aggregate_samples(
    iter: impl Iterator<Item = Sample>,
    start_ts: Timestamp,
    end_ts: Timestamp,
    aggr_options: &AggregationOptions,
) -> Vec<Sample> {
    let aligned_timestamp = aggr_options
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    let mut aggr = AggregationHelper::new(aggr_options, aligned_timestamp);
    aggr.calculate(iter)
}

pub fn get_series_labels<'a>(
    series: &'a TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<InternedLabel<'a>> {
    if !with_labels || selected_labels.is_empty() {
        return vec![];
    }

    let mut dest = Vec::new();

    if !selected_labels.is_empty() {
        for label in series.labels.iter() {
            if selected_labels.iter().any(|name| name == label.name) {
                dest.push(label)
            }
        }
        return dest;
    }

    for label in series.labels.iter() {
        dest.push(label);
    }

    dest
}

pub(super) fn group_samples_internal(
    samples: impl Iterator<Item = Sample>,
    option: &RangeGroupingOptions,
) -> Vec<Sample> {
    let mut iter = samples;
    let mut aggregator = option.aggregator.clone();
    let mut current = if let Some(current) = iter.next() {
        aggregator.update(current.value);
        current
    } else {
        return vec![];
    };

    let mut result = vec![];

    for next in iter {
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
