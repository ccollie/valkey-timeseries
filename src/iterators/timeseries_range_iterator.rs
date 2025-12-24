use crate::aggregators::AggregateIterator;
use crate::common::{Sample, Timestamp};
use crate::series::request_types::{AggregationOptions, RangeOptions};
use crate::series::{SeriesSampleIterator, TimeSeries, get_latest_compaction_sample};
use smallvec::SmallVec;
use valkey_module::Context;

/// An iterator over a TimeSeries based on RangeOptions.
/// This iterator handles various filters (timestamp, value) and aggregations, as well as the
/// special "LATEST" option for compaction series.
pub struct TimeSeriesRangeIterator<'a> {
    inner: Box<dyn Iterator<Item = Sample> + 'a>,
    size_hint: (usize, Option<usize>),
}

impl<'a> TimeSeriesRangeIterator<'a> {
    /// Creates a new TimeSeriesRangeIterator based on the provided options.
    /// This is slightly complex due to the various combinations of filters and aggregations, and
    /// the desire to minimize boxing.
    /// ctx is made optional to allow for easier unit testing without a full Valkey context.
    /// Note that reversing is left to the caller to handle, as not all iterators are double-ended.
    pub fn new(
        ctx: Option<&'a Context>,
        series: &'a TimeSeries,
        options: &RangeOptions,
        check_retention: bool,
    ) -> Self {
        let (mut start_ts, mut end_ts) = options.date_range.get_timestamps(None);

        // 1. Handle Retention
        if check_retention && !series.retention.is_zero() {
            let min_ts = series.get_min_timestamp();
            start_ts = start_ts.max(min_ts);
            end_ts = start_ts.max(end_ts);
        }

        // make sure we're a compaction, and that ends_ts > series.last_timestamp
        // (the latest sample would be in range if it exists)
        if options.latest && series.is_compaction() && end_ts > series.last_timestamp() {
            let Some(context) = ctx else {
                panic!("Context is required for LATEST option");
            };
            let iter = Self::create_latest_iterator(context, options, series, start_ts, end_ts);
            return Self {
                inner: iter,
                size_hint: (0, Some(1)),
            };
        }

        let mut size_hint = (0usize, options.count);

        let iter = SeriesSampleIterator::from_range_options(series, options, check_retention);

        let inner = if let Some(count) = options.count {
            let iter = iter.take(count);
            size_hint = (0, Some(count));
            Self::maybe_create_aggregated_iterator(iter, &options.aggregation, start_ts, end_ts)
        } else {
            Self::maybe_create_aggregated_iterator(iter, &options.aggregation, start_ts, end_ts)
        };

        Self { inner, size_hint }
    }

    fn maybe_create_aggregated_iterator<T: Iterator<Item = Sample> + 'a>(
        base_iter: T,
        aggr_options: &Option<AggregationOptions>,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        if let Some(aggr_options) = aggr_options {
            let aligned_start = aggr_options
                .alignment
                .get_aligned_timestamp(start_ts, end_ts);
            let iter = AggregateIterator::new(base_iter, aggr_options, aligned_start);
            return Box::new(iter);
        }
        Box::new(base_iter)
    }

    fn create_latest_iterator(
        ctx: &'a Context,
        options: &RangeOptions,
        series: &'a TimeSeries,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Box<dyn Iterator<Item = Sample> + 'a> {
        let timestamps: Option<SmallVec<Timestamp, 8>> =
            if let Some(v) = options.timestamp_filter.as_ref() {
                Some(SmallVec::from_slice_copy(v))
            } else {
                None
            };

        let value_filter = options.value_filter;

        let mut done = false;
        let iter = std::iter::from_fn(move || {
            if done {
                return None;
            } else {
                done = true;
            }

            let value = get_latest_compaction_sample(ctx, series)?;

            let ts = value.timestamp;
            if ts < start_ts || ts > end_ts {
                return None;
            }
            if !value_filter.is_none_or(|vf| vf.is_match(value.value)) {
                return None;
            }
            if !timestamps
                .as_ref()
                .is_none_or(|ts_vec| ts_vec.contains(&ts))
            {
                return None;
            }

            Some(value)
        });

        Box::new(iter)
    }
}

impl<'a> TimeSeriesRangeIterator<'a> {
    fn len_hint(&self) -> (usize, Option<usize>) {
        self.size_hint
    }
}

impl<'a> Iterator for TimeSeriesRangeIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
