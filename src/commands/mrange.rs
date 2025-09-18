use crate::commands::mrange_fanout_operation::execute_mrange_fanout_operation;
use crate::commands::mrange_impl::process_mrange_query;
use crate::commands::parse_mrange_options;
use crate::error_consts;
use crate::fanout::is_clustered;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.MRANGE fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [WITHLABELS | <SELECTED_LABELS label...>]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
//   FILTER filterExpr...
//   [GROUPBY label REDUCE reducer]
pub fn mrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, false)
}

pub fn mrevrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, true)
}

fn mrange_internal(ctx: &Context, args: Vec<ValkeyString>, reverse: bool) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_mrange_options(&mut args)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    args.done()?;

    if is_clustered(ctx) {
        return execute_mrange_fanout_operation(ctx, options);
    }

    let result_rows = process_mrange_query(ctx, options, reverse)?;
    let result = result_rows
        .into_iter()
        .map(|series| series.into())
        .collect::<Vec<ValkeyValue>>();

    Ok(ValkeyValue::from(result))
}
