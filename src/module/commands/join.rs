use crate::error_consts;
use crate::join::asof::AsOfJoinStrategy;
use crate::join::{process_join, JoinOptions, JoinType};
use crate::module::arg_parse::{
    advance_if_next_token, advance_if_next_token_one_of, parse_aggregation_options,
    parse_command_arg_token, parse_count, parse_duration_ms, parse_join_operator,
    parse_timestamp_filter, parse_timestamp_range, parse_value_filter, CommandArgIterator,
    CommandArgToken,
};
use crate::module::{invalid_series_key_error, VK_TIME_SERIES_TYPE};
use crate::series::TimeSeries;
use std::time::Duration;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.JOIN key1 key2 fromTimestamp toTimestamp
///   [[INNER] | [FULL] | [LEFT [EXCLUSIVE]] | [RIGHT [EXCLUSIVE]] | [ASOF [PRIOR | NEXT] tolerance]]
///   [FILTER_BY_TS ts...]
///   [FILTER_BY_VALUE min max]
///   [COUNT count]
///   [REDUCE op]
///   [AGGREGATION aggregator bucketDuration [ALIGN align] [BUCKETTIMESTAMP timestamp] [EMPTY]]
pub fn join(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();

    let left_key = args.next_arg()?;
    let right_key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    let mut options = JoinOptions {
        date_range,
        ..Default::default()
    };

    parse_join_args(&mut args, &mut options)?;

    if left_key == right_key {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_JOIN_KEY));
    }

    let left_db_key = ctx.open_key(&left_key);
    let right_db_key = ctx.open_key(&right_key);

    let left_series = left_db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)?;
    let right_series = right_db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)?;

    match (left_series, right_series) {
        (Some(left_series), Some(right_series)) => {
            Ok(join_internal(left_series, right_series, &options))
        }
        (Some(_), None) => Err(invalid_series_key_error(&right_key)),
        (None, Some(_)) => Err(invalid_series_key_error(&left_key)),
        _ => Err(ValkeyError::Str(error_consts::INVALID_JOIN_KEY)),
    }
}

fn parse_asof(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    use CommandArgToken::*;

    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut direction = AsOfJoinStrategy::Prior;

    // ASOF [PRIOR | NEXT] [tolerance]
    if let Some(next) = advance_if_next_token_one_of(args, &[Prior, Next]) {
        if next == Prior {
            direction = AsOfJoinStrategy::Prior;
        } else if next == Next {
            direction = AsOfJoinStrategy::Next;
        }
    }

    if let Some(next_arg) = args.peek() {
        if let Ok(arg_str) = next_arg.try_as_str() {
            // see if we have a duration expression
            // durations in all cases start with an ascii digit, e.g. 1000 or 40ms
            let ch = arg_str.chars().next().unwrap();
            if ch.is_ascii_digit() {
                let tolerance_ms = parse_duration_ms(arg_str)?;
                if tolerance_ms < 0 {
                    return Err(ValkeyError::Str(error_consts::INVALID_ASOF_TOLERANCE));
                }
                tolerance = Duration::from_millis(tolerance_ms as u64);
                let _ = args.next_arg()?;
            }
        }
    }

    Ok(JoinType::AsOf(direction, tolerance))
}

fn possibly_parse_exclusive(args: &mut CommandArgIterator) -> bool {
    advance_if_next_token(args, CommandArgToken::Exclusive)
}

fn parse_join_args(args: &mut CommandArgIterator, options: &mut JoinOptions) -> ValkeyResult<()> {
    use CommandArgToken::*;
    let mut join_type_set = false;

    fn check_join_type_set(is_set: &mut bool) -> ValkeyResult<()> {
        if *is_set {
            Err(ValkeyError::Str("ERR join type already set"))
        } else {
            *is_set = true;
            Ok(())
        }
    }

    const VALID_TOKEN_ARGS: &[CommandArgToken] = &[
        AsOf,
        FilterByValue,
        FilterByTs,
        Count,
        Full,
        Left,
        Right,
        Inner,
        Reduce,
    ];

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            Aggregation => options.aggregation = Some(parse_aggregation_options(args)?),
            AsOf => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = parse_asof(args)?;
            }
            Count => {
                options.count = Some(parse_count(args)?);
            }
            FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            FilterByTs => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, VALID_TOKEN_ARGS)?);
            }
            Full => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Full;
            }
            Inner => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Inner;
            }
            Left => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Left(exclusive);
            }
            Right => {
                check_join_type_set(&mut join_type_set)?;
                let exclusive = possibly_parse_exclusive(args);
                options.join_type = JoinType::Right(exclusive);
            }
            Reduce => {
                let arg = args.next_str()?;
                options.reducer = Some(parse_join_operator(arg)?);
            }
            _ => return Err(ValkeyError::Str("TSDB: invalid JOIN command argument")),
        }
    }

    // aggregations are only valid when there is a transform
    if options.aggregation.is_some() && options.reducer.is_none() {
        return Err(ValkeyError::Str(error_consts::MISSING_JOIN_REDUCER));
    }

    Ok(())
}

fn join_internal(left: &TimeSeries, right: &TimeSeries, options: &JoinOptions) -> ValkeyValue {
    let result = process_join(left, right, options);
    result.to_valkey_value(false)
}

#[cfg(test)]
mod tests {}
