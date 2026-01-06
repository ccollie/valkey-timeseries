use crate::aggregators::aggregate;
use crate::common::Sample;
use crate::common::binop::BinopFunc;
use crate::common::threads::join;
use crate::join::{JoinOptions, JoinType, JoinValue, create_join_iter};
use crate::series::TimeSeries;
use joinkit::EitherOrBoth;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

// naming is hard :-)
/// The result of a join operation, which can be either samples (if reduced) or raw join values
pub enum JoinResultType {
    Samples(Vec<Sample>),
    Values(Vec<JoinValue>),
}

impl From<JoinResultType> for ValkeyValue {
    fn from(value: JoinResultType) -> Self {
        let arr = match value {
            JoinResultType::Samples(samples) => samples.iter().map(|x| x.into()).collect(),
            JoinResultType::Values(values) => values.into_iter().map(|x| x.into()).collect(),
        };
        ValkeyValue::Array(arr)
    }
}

pub fn process_join(
    left_series: &TimeSeries,
    right_series: &TimeSeries,
    options: &JoinOptions,
) -> ValkeyResult<JoinResultType> {
    // TODO: use iterators instead of collecting samples up front
    let (left_samples, right_samples) = join(
        || fetch_samples(left_series, options),
        || fetch_samples(right_series, options),
    );
    join_internal(left_samples, right_samples, options)
}

pub(super) fn join_internal<L, R, IR, IL>(
    left: IL,
    right: IR,
    options: &JoinOptions,
) -> ValkeyResult<JoinResultType>
where
    L: Iterator<Item = Sample> + 'static,
    R: Iterator<Item = Sample> + 'static,
    IL: IntoIterator<IntoIter = L, Item = Sample>,
    IR: IntoIterator<IntoIter = R, Item = Sample>,
{
    // Disallow reducer for ANTI and SEMI joins (they return a single value per timestamp,
    // so REDUCE does not make sense)
    if options.reducer.is_some()
        && (options.join_type == JoinType::Semi || options.join_type == JoinType::Anti)
    {
        return Err(ValkeyError::Str(
            "TSDB: Reducer cannot be used with SEMI or ANTI joins",
        ));
    }

    let join_iter = create_join_iter(left, right, options.join_type);

    let count = options.count.unwrap_or(usize::MAX);
    if let Some(op) = options.reducer {
        let transform = op.get_handler();
        let iter = join_iter.map(|x| transform_join_value_to_sample(&x, transform));

        if let Some(aggr_options) = &options.aggregation {
            let (start, end) = options.date_range.get_timestamps(None);
            let aligned_timestamp = aggr_options.alignment.get_aligned_timestamp(start, end);
            let result = aggregate(aggr_options, aligned_timestamp, iter)
                .into_iter()
                .take(count)
                .collect();
            return Ok(JoinResultType::Samples(result));
        }

        return Ok(JoinResultType::Samples(iter.collect()));
    } else if options.join_type == JoinType::Semi || options.join_type == JoinType::Anti {
        // note that ANTI and SEMI joins return single values per timestamp, so we can use aggregation
        if let Some(aggr_options) = &options.aggregation {
            let sample_iter = join_iter.map(|x| transform_join_value_to_sample(&x, |l, _r| l));
            let (start, end) = options.date_range.get_timestamps(None);
            let aligned_timestamp = aggr_options.alignment.get_aligned_timestamp(start, end);
            let result = aggregate(aggr_options, aligned_timestamp, sample_iter)
                .into_iter()
                .take(count)
                .collect();
            return Ok(JoinResultType::Samples(result));
        }
    }

    Ok(JoinResultType::Values(join_iter.take(count).collect()))
}

pub(super) fn transform_join_value_to_sample(item: &JoinValue, f: BinopFunc) -> Sample {
    match item.0 {
        EitherOrBoth::Both(l, r) => Sample::new(l.timestamp, f(l.value, r.value)),
        EitherOrBoth::Left(l) => Sample::new(l.timestamp, f(l.value, f64::NAN)),
        EitherOrBoth::Right(r) => Sample::new(r.timestamp, f(f64::NAN, r.value)),
    }
}

fn fetch_samples(ts: &TimeSeries, options: &JoinOptions) -> Vec<Sample> {
    let (start, end) = options.date_range.get_series_range(ts, None, true);
    let mut samples = ts.get_range_filtered(
        start,
        end,
        options.timestamp_filter.as_deref(),
        options.value_filter,
    );
    if let Some(count) = &options.count {
        samples.truncate(*count);
    }
    samples
}
