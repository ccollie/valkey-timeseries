use crate::commands::arg_parse::parse_range_options;
use crate::series::range_utils::get_range;
use crate::series::with_timeseries;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.REVRANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucket_duration [BUCKETTIMESTAMP bt] [EMPTY]]
pub fn rev_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries(ctx, &key, true, |series| {
        let samples = get_range(series, &options, false)
            .into_iter()
            .rev()
            .map(|x| x.into())
            .collect::<Vec<ValkeyValue>>();

        Ok(ValkeyValue::from(samples))
    })
}
