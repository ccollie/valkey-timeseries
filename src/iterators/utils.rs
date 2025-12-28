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
    is_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    #[inline]
    fn copy_ts_filter(timestamps: &[Timestamp]) -> SmallVec<Timestamp, 8> {
        SmallVec::from_slice_copy(timestamps)
    }

    fn handle_reverse<'a>(
        base_iter: impl Iterator<Item = Sample> + 'a,
        is_reverse: bool,
        count: Option<usize>,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        if is_reverse {
            let iter = ReverseSampleIter::new(base_iter);
            apply_iter_limit!(iter, count)
        } else {
            apply_iter_limit!(base_iter, count)
        }
    }

    fn convert_inner<'a>(
        base_iter: impl Iterator<Item = Sample> + 'a,
        options: &RangeOptions,
        grouping: &Option<RangeGroupingOptions>,
        is_reverse: bool,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        let count = options.count;
        match (&options.aggregation, &grouping) {
            (Some(agg), Some(grouping)) => {
                let grouping_aggregation = grouping.aggregation;
                let aggr_iter = create_aggregate_iterator(base_iter, options, agg);
                let reducer = ReduceIterator::new(aggr_iter, grouping_aggregation);
                handle_reverse(reducer, is_reverse, count)
            }
            (None, Some(grouping)) => {
                let aggregation = grouping.aggregation;
                let reducer = ReduceIterator::new(base_iter, aggregation);
                handle_reverse(reducer, is_reverse, count)
            }
            (Some(aggregation), None) => {
                let aggr_iter = create_aggregate_iterator(base_iter, options, aggregation);
                handle_reverse(aggr_iter, is_reverse, count)
            }
            _ => handle_reverse(base_iter, is_reverse, count),
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
            convert_inner(filtered, options, grouping, is_reverse)
        }
        (Some(ts_filter), None) => {
            let timestamps = copy_ts_filter(ts_filter);
            let filtered = base_iter.filter(move |sample| timestamps.contains(&sample.timestamp));
            convert_inner(filtered, options, grouping, is_reverse)
        }
        (None, Some(val_filter)) => {
            let filter = *val_filter;
            let filtered = base_iter.filter(move |sample| filter.is_match(sample.value));
            convert_inner(filtered, options, grouping, is_reverse)
        }
        (None, None) => convert_inner(base_iter, options, grouping, is_reverse),
    }
}

struct ReverseSampleIter<I>
where
    I: Iterator<Item = Sample>,
{
    inner: I,
    buf: Vec<Sample>,
    loaded: bool,
}

impl<I: Iterator<Item = Sample>> ReverseSampleIter<I> {
    pub fn new(inner: I) -> Self {
        let buf = Vec::new();
        Self {
            inner,
            buf,
            loaded: false,
        }
    }

    fn load_items(&mut self) {
        // todo: determine the length of the iterator if possible to pre-allocate the buffer
        for item in self.inner.by_ref() {
            self.buf.push(item);
        }
    }
}

impl<I: Iterator<Item = Sample>> Iterator for ReverseSampleIter<I> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.loaded {
            self.loaded = true;
            self.load_items();
        }
        self.buf.pop()
    }
}
