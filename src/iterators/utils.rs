use crate::aggregators::AggregateIterator;
use crate::common::{Sample, Timestamp};
use crate::iterators::ReduceIterator;
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use smallvec::SmallVec;

pub fn create_aggregate_iterator<I>(
    iter: I,
    range: &RangeOptions,
    aggregation: &AggregationOptions,
) -> AggregateIterator<I>
where
    I: Iterator<Item = Sample>,
{
    let (start_ts, end_ts) = range.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    AggregateIterator::new(iter, aggregation, aligned_timestamp)
}

macro_rules! apply_iter_limit {
    ($iter:expr, $limit:expr) => {
        if let Some(limit) = $limit {
            Box::new($iter.take(limit as usize)) as Box<dyn Iterator<Item = _> + '_>
        } else {
            Box::new($iter) as Box<dyn Iterator<Item = _> + '_>
        }
    };
}

pub fn create_sample_iterator_adapter<'a, T: Iterator<Item = Sample> + 'a>(
    base_iter: T,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    #[inline]
    fn copy_ts_filter(timestamps: &[Timestamp]) -> SmallVec<Timestamp, 8> {
        SmallVec::from_slice_copy(timestamps)
    }

    fn convert_inner<'a>(
        base_iter: impl Iterator<Item = Sample> + 'a,
        options: &RangeOptions,
        grouping: &Option<RangeGroupingOptions>,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        let count = options.count;
        match (&options.aggregation, &grouping) {
            (Some(agg), Some(grouping)) => {
                let grouping_aggregation = grouping.aggregation;
                let aggr_iter = create_aggregate_iterator(base_iter, options, agg);
                let reducer = ReduceIterator::new(aggr_iter, grouping_aggregation);
                apply_iter_limit!(reducer, count)
            }
            (None, Some(grouping)) => {
                let aggregation = grouping.aggregation;
                let reducer = ReduceIterator::new(base_iter, aggregation);
                apply_iter_limit!(reducer, count)
            }
            (Some(aggregation), None) => {
                let aggr_iter = create_aggregate_iterator(base_iter, options, aggregation);
                apply_iter_limit!(aggr_iter, count)
            }
            _ => {
                apply_iter_limit!(base_iter, count)
            }
        }
    }

    // done as a match to minimize boxing and closures
    match (&options.timestamp_filter, &options.value_filter) {
        (Some(ts_filter), Some(val_filter)) => {
            let timestamps = copy_ts_filter(ts_filter);
            let filter = *val_filter;
            let filtered = base_iter.filter(move |sample| {
                timestamps.contains(&sample.timestamp) && filter.is_match(sample.value)
            });
            convert_inner(filtered, options, grouping)
        }
        (Some(ts_filter), None) => {
            let timestamps = copy_ts_filter(ts_filter);
            let filtered = base_iter.filter(move |sample| timestamps.contains(&sample.timestamp));
            convert_inner(filtered, options, grouping)
        }
        (None, Some(val_filter)) => {
            let filter = *val_filter;
            let filtered = base_iter.filter(move |sample| filter.is_match(sample.value));
            convert_inner(filtered, options, grouping)
        }
        (None, None) => convert_inner(base_iter, options, grouping),
    }
}
