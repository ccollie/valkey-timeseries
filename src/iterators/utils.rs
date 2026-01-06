use crate::aggregators::AggregateIterator;
use crate::common::hash::IntSet;
use crate::common::{Sample, Timestamp};
use crate::iterators::{ReduceIterator, TimestampFilterIterator};
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::series::{SeriesSampleIterator, TimeSeries};
use smallvec::SmallVec;

macro_rules! apply_iter_limit {
    ($iter:expr, $limit:expr) => {
        if let Some(limit) = $limit {
            Box::new($iter.take(limit as usize)) as Box<dyn Iterator<Item = _> + '_>
        } else {
            Box::new($iter) as Box<dyn Iterator<Item = _> + '_>
        }
    };
}

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

/// Create an optimized range iterator for the given series and options
pub fn create_range_iterator<'a>(
    series: &'a TimeSeries,
    options: &RangeOptions,
    grouping: &Option<RangeGroupingOptions>,
    latest_sample: Option<Sample>,
    is_reverse: bool,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    let has_aggregation = options.aggregation.is_some();
    let should_reverse_iter = !has_aggregation && is_reverse;
    let should_reverse_aggr = has_aggregation && is_reverse;

    // Helper to handle the "latest sample" chaining logic which depends on direction
    // and avoids boxing by using generics.
    fn chain_latest<'a, I>(
        base: I,
        latest: Option<Sample>,
        opts: &RangeOptions,
        grp: &Option<RangeGroupingOptions>,
        reverse_aggr: bool,
        should_reverse_iter: bool,
    ) -> Box<dyn Iterator<Item = Sample> + 'a>
    where
        I: Iterator<Item = Sample> + 'a,
    {
        if let Some(sample) = latest {
            let latest_iter = std::iter::once(sample);
            if should_reverse_iter && opts.aggregation.is_none() {
                create_sample_iterator_adapter(latest_iter.chain(base), opts, grp, reverse_aggr)
            } else {
                create_sample_iterator_adapter(base.chain(latest_iter), opts, grp, reverse_aggr)
            }
        } else {
            create_sample_iterator_adapter(base, opts, grp, reverse_aggr)
        }
    }

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
        chain_latest(
            base_iter,
            latest_sample,
            &opts,
            grouping,
            should_reverse_aggr,
            is_reverse,
        )
    } else {
        let base_iter =
            SeriesSampleIterator::from_range_options(series, options, should_reverse_iter);
        chain_latest(
            base_iter,
            latest_sample,
            options,
            grouping,
            should_reverse_aggr,
            is_reverse,
        )
    }
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
    // Apply Filters (Timestamp & Value)
    let ts_filter = options
        .timestamp_filter
        .as_ref()
        .map(|f| TimestampFilter::new(f));
    let val_filter = options.value_filter;

    let filtered = base_iter.filter(move |sample| {
        if let Some(ts) = &ts_filter {
            if !ts.matches(sample.timestamp) {
                return false;
            }
        }
        if let Some(val) = &val_filter {
            if !val.is_match(sample.value) {
                return false;
            }
        }
        true
    });

    let count = options.count;

    // Helper to apply reversal and limits, then box.
    // This ensures we only box once at the very end of the chain.
    fn finalize<'a, I: Iterator<Item = Sample> + 'a>(
        iter: I,
        is_reverse: bool,
        count: Option<usize>,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        if is_reverse {
            let rev = ReverseSampleIter::new(iter);
            apply_iter_limit!(rev, count)
        } else {
            apply_iter_limit!(iter, count)
        }
    }

    match (&options.aggregation, grouping) {
        (Some(agg), Some(grp)) => {
            let aggr_iter = create_aggregate_iterator(filtered, options, agg);
            let reducer = ReduceIterator::new(aggr_iter, grp.aggregation);
            finalize(reducer, is_reverse, count)
        }
        (None, Some(grp)) => {
            let reducer = ReduceIterator::new(filtered, grp.aggregation);
            finalize(reducer, is_reverse, count)
        }
        (Some(agg), None) => {
            let aggr_iter = create_aggregate_iterator(filtered, options, agg);
            finalize(aggr_iter, is_reverse, count)
        }
        (None, None) => finalize(filtered, is_reverse, count),
    }
}

pub(crate) struct ReverseSampleIter<I>
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
        // determine the length of the iterator if possible to pre-allocate the buffer
        let (lower, _) = self.inner.size_hint();
        if lower > 0 {
            self.buf.reserve(lower);
        }

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

const TIMESTAMP_FILTER_INLINE_THRESHOLD: usize = 16;

// this may be overkill, but we'll try optimizing memory for a
// very common case of a very small number of timestamps
pub enum TimestampFilter {
    Set(IntSet<Timestamp>),
    List(SmallVec<Timestamp, TIMESTAMP_FILTER_INLINE_THRESHOLD>),
}

impl TimestampFilter {
    pub fn new(timestamps: &[Timestamp]) -> Self {
        if timestamps.len() > 16 {
            Self::Set(IntSet::from_iter(timestamps.iter().copied()))
        } else {
            Self::List(SmallVec::from_slice_copy(timestamps))
        }
    }

    pub fn matches(&self, ts: Timestamp) -> bool {
        match self {
            TimestampFilter::Set(set) => set.contains(&ts),
            TimestampFilter::List(list) => list.contains(&ts),
        }
    }
}
