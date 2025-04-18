use crate::aggregators::{AggOp, AggregateIterator, AggregationOptions};
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
        series.labels
            .iter()
            .map(Some)
            .collect::<Vec<_>>()
    } else  {
        selected_labels
            .iter()
            .map(|name| series.get_label(name))
            .collect::<Vec<_>>()
    }
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
