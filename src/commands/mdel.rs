use crate::commands::command_args::{
    CommandArgIterator, CommandArgToken, parse_command_arg_token, parse_series_selector_list,
    parse_timestamp_range_value,
};
use crate::commands::mdel_fanout_operation::MDelFanoutOperation;
use crate::error_consts;
use crate::fanout::{FanoutOperation, is_clustered};
use crate::labels::filters::SeriesSelector;
use crate::series::{TimestampRange, delete_series_by_selectors};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.MDEL [fromTimestamp toTimestamp] FILTER label=value [label=value ...]
///
/// Two modes:
/// 1. Range deletion: TS.MDEL fromTimestamp toTimestamp FILTER label=value
///    - Removes samples in [fromTimestamp, toTimestamp] for matching series
/// 2. Series deletion: TS.MDEL FILTER label=value
///    - Removes entire time series matching the filter
pub fn mdel(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    // Check if we have a timestamp range or just FILTER
    let (date_range, filters) = parse_mdel_args(&mut args)?;

    if is_clustered(ctx) {
        let operation = MDelFanoutOperation::new(filters, date_range);
        return operation.exec(ctx);
    }

    let total_deleted = delete_series_by_selectors(ctx, &filters, date_range)?;

    ctx.replicate_verbatim();

    Ok(ValkeyValue::BigNumber(total_deleted.to_string()))
}

fn parse_mdel_args(
    args: &mut CommandArgIterator,
) -> ValkeyResult<(Option<TimestampRange>, Vec<SeriesSelector>)> {
    const STOP_TOKENS: [CommandArgToken; 1] = [CommandArgToken::Filter];

    // Peek at first arg to determine mode
    let first_arg = args
        .peek()
        .ok_or(ValkeyError::Str(error_consts::MISSING_FILTER))?;

    let token = parse_command_arg_token(first_arg.as_slice());

    if token == Some(CommandArgToken::Filter) {
        // Series deletion mode: FILTER is first
        args.next(); // consume FILTER token
        let filters = parse_series_selector_list(args, &[])?;
        return Ok((None, filters));
    }

    // Range deletion mode: parse timestamps first
    let start_str = args
        .next()
        .ok_or(ValkeyError::Str(error_consts::INVALID_START_TIMESTAMP))?;
    let start = parse_timestamp_range_value(
        start_str
            .try_as_str()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_START_TIMESTAMP))?,
    )
    .map_err(|_| ValkeyError::Str(error_consts::INVALID_START_TIMESTAMP))?;

    let end_str = args
        .next()
        .ok_or(ValkeyError::Str(error_consts::INVALID_END_TIMESTAMP))?;
    let end = parse_timestamp_range_value(
        end_str
            .try_as_str()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_END_TIMESTAMP))?,
    )
    .map_err(|_| ValkeyError::Str(error_consts::INVALID_END_TIMESTAMP))?;

    let date_range = TimestampRange::new(start, end)?;

    // Now expect FILTER keyword
    let filter_arg = args
        .next()
        .ok_or(ValkeyError::Str(error_consts::MISSING_FILTER))?;

    if parse_command_arg_token(filter_arg.as_slice()) != Some(CommandArgToken::Filter) {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    let filters = parse_series_selector_list(args, &[])?;

    Ok((Some(date_range), filters))
}
