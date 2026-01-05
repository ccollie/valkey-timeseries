use crate::commands::command_args::parse_range_options;
use crate::iterators::TimeSeriesRangeIterator;
use crate::series::with_timeseries;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.RANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
pub fn range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    range_internal(ctx, args, false)
}

/// TS.REVRANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucket_duration [BUCKETTIMESTAMP bt] [EMPTY]]
pub fn rev_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    range_internal(ctx, args, true)
}

fn range_internal(ctx: &Context, args: Vec<ValkeyString>, is_reverse: bool) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries(ctx, &key, true, |series| {
        let iter = TimeSeriesRangeIterator::new(Some(ctx), series, &options, is_reverse);
        let samples = iter
            .into_iter()
            .map(|x| x.into())
            .collect::<Vec<ValkeyValue>>();

        Ok(ValkeyValue::from(samples))
    })
}
