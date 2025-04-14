use crate::aggregators::aggregate;
use crate::common::binop::BinopFunc;
use crate::common::parallel::join;
use crate::common::Sample;
use crate::join::{create_join_iter, JoinOptions, JoinValue};
use crate::series::TimeSeries;
use joinkit::EitherOrBoth;
use valkey_module::ValkeyValue;

// naming is hard :-)
/// Result of a join operation
pub enum JoinResultType {
    Samples(Vec<Sample>),
    Values(Vec<JoinValue>),
}

impl JoinResultType {
    pub fn to_valkey_value(&self, is_transform: bool) -> ValkeyValue {
        let arr = match self {
            JoinResultType::Samples(samples) => {
                samples.iter().map(|x| sample_to_value(*x)).collect()
            }
            JoinResultType::Values(values) => values
                .iter()
                .map(|x| join_value_to_valkey_value(x, is_transform))
                .collect(),
        };

        ValkeyValue::Array(arr)
    }
}

pub fn process_join(
    left_series: &TimeSeries,
    right_series: &TimeSeries,
    options: &JoinOptions,
) -> JoinResultType {
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
) -> JoinResultType
where
    L: Iterator<Item = Sample> + 'static, // icky, but this is internal
    R: Iterator<Item = Sample> + 'static, // icky, but this is internal
    IL: IntoIterator<IntoIter = L, Item = Sample>,
    IR: IntoIterator<IntoIter = R, Item = Sample>,
{
    let join_iter = create_join_iter(left, right, options.join_type);

    if let Some(op) = options.reducer {
        let transform = op.get_handler();

        let iter = join_iter.map(|x| transform_join_value_to_sample(&x, transform));

        return if let Some(aggr_options) = &options.aggregation {
            // Aggregation is valid only for transforms (all other options return multiple values per row)
            let (start_timestamp, end_timestamp) = options.date_range.get_timestamps(None);

            let aligned_timestamp = aggr_options
                .alignment
                .get_aligned_timestamp(start_timestamp, end_timestamp);

            let result = aggregate(aggr_options, aligned_timestamp, iter)
                .into_iter()
                .collect::<Vec<_>>();
            JoinResultType::Samples(result)
        } else {
            let result = iter.collect::<Vec<_>>();
            JoinResultType::Samples(result)
        };
    }

    let count = options.count.unwrap_or(usize::MAX);

    JoinResultType::Values(join_iter.take(count).collect::<Vec<_>>())
}

pub(super) fn transform_join_value_to_sample(item: &JoinValue, f: BinopFunc) -> Sample {
    match item.value {
        EitherOrBoth::Both(l, r) => Sample::new(item.timestamp, f(l, r)),
        EitherOrBoth::Left(l) => Sample::new(item.timestamp, f(l, f64::NAN)),
        EitherOrBoth::Right(r) => Sample::new(item.timestamp, f(f64::NAN, r)),
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

fn join_value_to_valkey_value(row: &JoinValue, is_transform: bool) -> ValkeyValue {
    let timestamp = ValkeyValue::from(row.timestamp);

    match row.value {
        EitherOrBoth::Both(left, right) => {
            let r_value = ValkeyValue::from(right);
            let l_value = ValkeyValue::from(left);
            let res = if let Some(other_timestamp) = row.other_timestamp {
                vec![
                    timestamp,
                    ValkeyValue::from(other_timestamp),
                    l_value,
                    r_value,
                ]
            } else {
                vec![timestamp, l_value, r_value]
            };
            ValkeyValue::Array(res)
        }
        EitherOrBoth::Left(left) => {
            let value = ValkeyValue::from(left);
            if is_transform {
                ValkeyValue::Array(vec![timestamp, value])
            } else {
                ValkeyValue::Array(vec![timestamp, value, ValkeyValue::Null])
            }
        }
        EitherOrBoth::Right(right) => ValkeyValue::Array(vec![
            timestamp,
            ValkeyValue::Null,
            ValkeyValue::Float(right),
        ]),
    }
}

fn sample_to_value(sample: Sample) -> ValkeyValue {
    let row = vec![
        ValkeyValue::from(sample.timestamp),
        ValkeyValue::from(sample.value),
    ];
    ValkeyValue::from(row)
}
