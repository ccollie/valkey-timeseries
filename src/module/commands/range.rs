use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::get_range;
use crate::module::result::sample_to_value;
use crate::module::with_timeseries;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.RANGE key fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
pub fn range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let options = parse_range_options(&mut args)?;

    args.done()?;

    with_timeseries(ctx, &key, true, |series| {
        let samples = get_range(series, &options, false);
        let result = samples
            .into_iter()
            .map(sample_to_value)
            .collect::<Vec<ValkeyValue>>();

        Ok(ValkeyValue::from(result))
    })
}
