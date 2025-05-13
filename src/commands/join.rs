use crate::commands::arg_parse::{
    advance_if_next_token_one_of, parse_aggregation_options, parse_command_arg_token,
    parse_count_arg, parse_duration_ms, parse_join_operator, parse_timestamp_filter,
    parse_timestamp_range, parse_value_filter, CommandArgIterator, CommandArgToken,
};
use crate::error_consts;
use crate::join::{process_join, AsOfJoinOptions, AsofJoinStrategy, JoinOptions, JoinType};
use crate::series::{get_timeseries, invalid_series_key_error, TimeSeries};
use std::time::Duration;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

/// TS.JOIN key1 key2 fromTimestamp toTimestamp
///   [INNER | FULL | LEFT | RIGHT | ANTI | SEMI | ASOF [PREVIOUS | NEXT | NEAREST] tolerance [ALLOW_EXACT_MATCH [true|false]]]
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

    let left_series = get_timeseries(ctx, left_key, Some(AclPermissions::ACCESS), true)?;
    let right_series = get_timeseries(ctx, right_key, Some(AclPermissions::ACCESS), true)?;

    match (left_series, right_series) {
        (Some(left_series), Some(right_series)) => {
            Ok(join_internal(&left_series, &right_series, &options))
        }
        (Some(_), None) => Err(invalid_series_key_error()),
        (None, Some(_)) => Err(invalid_series_key_error()),
        _ => Err(ValkeyError::Str(error_consts::INVALID_JOIN_KEY)),
    }
}

fn parse_asof_join_options(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    use CommandArgToken::*;

    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut strategy = AsofJoinStrategy::Backward;

    // ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] [ALLOW_EXACT_MATCH [true|false]]
    if let Some(next) = advance_if_next_token_one_of(args, &[Previous, Next, Nearest]) {
        strategy = match next {
            Previous => AsofJoinStrategy::Backward,
            Next => AsofJoinStrategy::Forward,
            Nearest => AsofJoinStrategy::Nearest,
            _ => unreachable!("BUG: invalid match arm for AsofJoinStrategy"),
        };
    }

    let mut allow_exact_match = true;
    if let Some(next_arg) = args.peek() {
        if let Ok(arg_str) = next_arg.try_as_str() {
            // see if we have a duration expression
            // durations in all cases start with an ascii digit, e.g., 1000 or 40 ms
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
        if advance_if_next_token_one_of(args, &[AllowExactMatch]).is_some() {
            match advance_if_next_token_one_of(args, &[True, False]) {
                Some(True) => allow_exact_match = true,
                Some(False) => allow_exact_match = false,
                _ => {}
            }
        }
    }

    Ok(JoinType::AsOf(AsOfJoinOptions {
        strategy,
        tolerance,
        allow_exact_match,
    }))
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
        Aggregation,
        Anti,
        AsOf,
        Count,
        FilterByValue,
        FilterByTs,
        Full,
        Inner,
        Left,
        Reduce,
        Right,
        Semi,
    ];

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            Aggregation => options.aggregation = Some(parse_aggregation_options(args)?),
            Anti => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Anti;
            }
            AsOf => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = parse_asof_join_options(args)?;
            }
            Count => {
                options.count = Some(parse_count_arg(args)?);
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
                options.join_type = JoinType::Left;
            }
            Right => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Right;
            }
            Reduce => {
                let arg = args.next_str()?;
                options.reducer = Some(parse_join_operator(arg)?);
            }
            Semi => {
                check_join_type_set(&mut join_type_set)?;
                options.join_type = JoinType::Semi;
            }
            _ => return Err(ValkeyError::Str("TSDB: invalid JOIN command argument")),
        }
    }

    // aggregations are only valid when the resulting join returns a single value per timestamp, i.e.,
    // SEMI, ANTI, or when there is a transform
    if options.aggregation.is_some()
        && options.reducer.is_none()
        && options.join_type != JoinType::Semi
        && options.join_type != JoinType::Anti
    {
        // todo: better error message
        return Err(ValkeyError::Str(error_consts::MISSING_JOIN_REDUCER));
    }

    Ok(())
}

fn join_internal(left: &TimeSeries, right: &TimeSeries, options: &JoinOptions) -> ValkeyValue {
    let result = process_join(left, right, options);
    result.to_valkey_value()
}

#[cfg(test)]
mod tests {}
