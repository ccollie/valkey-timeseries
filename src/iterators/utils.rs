use crate::aggregators::AggregateIterator;
use crate::common::{Sample, Timestamp};
use crate::iterators::{ReduceIterator, TimestampFilterIterator};
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::series::{SeriesSampleIterator, TimeSeries};
use smallvec::SmallVec;
use std::collections::BTreeSet;

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

/// Create an optimized range iterator for the given series and options
pub fn create_range_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
    latest_sample: Option<Sample>,
    is_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let has_aggregation = options.aggregation.is_some();

    // Aggregation requires ascending input order from base iterators
    // Without aggregation, base iterators can directly provide the requested order
    let should_reverse_iter = !has_aggregation && is_reverse;

    // Apply reversal after aggregation if needed
    let should_reverse_aggr = has_aggregation && is_reverse;

    if let Some(ts_filter) = options.timestamp_filter.as_ref() {
        let base_iter = TimestampFilterIterator::new(series, ts_filter, should_reverse_iter);
        // Remove the timestamp filter from options to avoid double filtering
        let opts = RangeOptions {
            date_range: options.date_range,
            count: options.count,
            latest: false,
            aggregation: options.aggregation,
            value_filter: options.value_filter,
            timestamp_filter: None,
        };
        if let Some(sample) = latest_sample {
            let latest_iter = std::iter::once(sample);
            return if is_reverse && options.aggregation.is_none() {
                // Only chain latest first for non-aggregation reverse queries
                let chained = latest_iter.chain(base_iter);
                create_sample_iterator_adapter(chained, &opts, grouping, should_reverse_aggr)
            } else {
                let chained = base_iter.chain(latest_iter);
                create_sample_iterator_adapter(chained, &opts, grouping, should_reverse_aggr)
            };
        }
        return create_sample_iterator_adapter(base_iter, &opts, grouping, should_reverse_aggr);
    }

    // No special handling required, use the standard sample iterator.
    let base_iter = SeriesSampleIterator::from_range_options(series, options, should_reverse_iter);

    // duplication is necessary to avoid boxing
    if let Some(sample) = latest_sample {
        let latest_iter = std::iter::once(sample);
        return if is_reverse && options.aggregation.is_none() {
            // Only chain latest first for non-aggregation reverse queries
            let chained = latest_iter.chain(base_iter);
            create_sample_iterator_adapter(chained, options, grouping, should_reverse_aggr)
        } else {
            let chained = base_iter.chain(latest_iter);
            create_sample_iterator_adapter(chained, options, grouping, should_reverse_aggr)
        };
    }

    create_sample_iterator_adapter(base_iter, options, grouping, should_reverse_aggr)
}

/// Create a sample iterator adapter that applies filtering, aggregation, grouping, and limits
/// based on the provided options. The resulting iterator yields samples according to the specified
/// criteria.
/// Boxing is delayed to the last possible moment to allow for compiler optimizations.
pub fn create_sample_iterator_adapter<'a, T: Iterator<Item = Sample> + 'a>(
    base_iter: T,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
    is_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
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

    fn create_inner<'a>(
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
            let timestamps = TimestampFilter::new(ts_filter);
            let filter = *val_filter;
            let filtered = base_iter.filter(move |sample| {
                timestamps.contains(sample.timestamp) && filter.is_match(sample.value)
            });
            create_inner(filtered, options, grouping, is_reverse)
        }
        (Some(ts_filter), None) => {
            let timestamps = TimestampFilter::new(ts_filter);
            let filtered = base_iter.filter(move |sample| timestamps.contains(sample.timestamp));
            create_inner(filtered, options, grouping, is_reverse)
        }
        (None, Some(val_filter)) => {
            let filter = *val_filter;
            let filtered = base_iter.filter(move |sample| filter.is_match(sample.value));
            create_inner(filtered, options, grouping, is_reverse)
        }
        (None, None) => create_inner(base_iter, options, grouping, is_reverse),
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

// this may be overkill, but we'll try optimizing memory for a
// very common case of a very small number of timestamps
enum TimestampFilterStorage {
    Set(BTreeSet<Timestamp>),
    List(SmallVec<Timestamp, 16>),
}

struct TimestampFilter {
    storage: TimestampFilterStorage,
}

impl TimestampFilter {
    pub fn new(timestamps: &[Timestamp]) -> Self {
        let storage = if timestamps.len() > 16 {
            let set = BTreeSet::from_iter(timestamps.iter().cloned());
            TimestampFilterStorage::Set(set)
        } else {
            TimestampFilterStorage::List(SmallVec::from_slice_copy(timestamps))
        };
        Self { storage }
    }

    pub fn contains(&self, ts: Timestamp) -> bool {
        match &self.storage {
            TimestampFilterStorage::Set(set) => set.contains(&ts),
            TimestampFilterStorage::List(list) => list.contains(&ts),
        }
    }
}
