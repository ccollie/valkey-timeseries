use crate::arg_types::MatchFilterOptions;
use crate::error_consts;
use crate::module::arg_parse::{parse_command_arg_token, parse_series_selector_list, parse_timestamp_arg, CommandArgToken};
use crate::series::index::{get_cardinality_by_matchers_list, with_matched_series, with_timeseries_index};
use crate::series::{TimestampRange, TimestampValue};
use std::collections::BTreeSet;
use valkey_module::{
    Context as RedisContext, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};


pub fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let options = parse_metadata_command_args(ctx, args, true)?;
    let mut counter: usize = 0;
    
    // if we don't have a date range, we can simply count postings...
    if options.date_range.is_none() {
        counter = with_timeseries_index(ctx, |index| {
            get_cardinality_by_matchers_list(index, &options.matchers)
        })? as usize;
    } else {
        with_matched_series(ctx, &mut counter, &options, |count, _, _| { *count += 1; })?;
    }
    Ok(ValkeyValue::from(counter as i64))
}

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
pub fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let options = parse_metadata_command_args(ctx, args, false)?;
    let limit = options.limit.unwrap_or(usize::MAX);

    let mut names: BTreeSet<String> = BTreeSet::new();

    with_matched_series(ctx, &mut names, &options, |acc, ts, _| {
        for label in ts.labels.iter() {
            acc.insert(label.name.into());
        }
    })?;

    let labels = names
        .into_iter()
        .take(limit)
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();
    
    Ok(ValkeyValue::Array(labels))
}

// TS.LABEL_VALUES label [FILTER seriesMatcher] [START fromTimestamp] [END fromTimestamp]
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub(crate) fn label_values(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(ctx, args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let mut names: BTreeSet<String> = BTreeSet::new();
    with_matched_series(ctx, &mut names, &label_args, |acc, ts, _| {
        for label in ts.labels.iter() {
            acc.insert(label.value.into());
        }
    })?;

    let label_values = names
        .into_iter()
        .take(limit)
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(label_values))
}

fn parse_metadata_command_args(
    _ctx: &RedisContext,
    args: Vec<ValkeyString>,
    require_matchers: bool,
) -> ValkeyResult<MatchFilterOptions> {
    const ARG_TOKENS: [CommandArgToken; 3] = [
        CommandArgToken::End,
        CommandArgToken::Start,
        CommandArgToken::Limit,
    ];

    let mut args = args.into_iter().skip(1).peekable();
    let mut matchers = Vec::with_capacity(4);
    let mut start_value: Option<TimestampValue> = None;
    let mut end_value: Option<TimestampValue> = None;
    let mut limit: Option<usize> = None;

    fn is_cmd_token(arg: CommandArgToken) -> bool {
        ARG_TOKENS.contains(&arg)
    }

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Start => {
                let next = args.next_str()?;
                start_value = Some(parse_timestamp_arg(next, "START")?);
            }
            CommandArgToken::End => {
                let next = args.next_str()?;
                end_value = Some(parse_timestamp_arg(next, "END")?);
            }
            CommandArgToken::Match => {
                let m = parse_series_selector_list(&mut args, is_cmd_token)?;
                matchers.extend(m);
            }
            CommandArgToken::Limit => {
                let next = args.next_u64()?;
                if next > usize::MAX as u64 {
                    return Err(ValkeyError::Str("ERR LIMIT too large"));
                }
                limit = Some(next as usize);
            }
            _ => {
                let msg = format!("ERR invalid argument '{}'", arg);
                return Err(ValkeyError::String(msg));
            }
        };
    }


    if require_matchers && matchers.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }


    let mut options = MatchFilterOptions {
        matchers,
        limit,
        ..Default::default()
    };

    if start_value.is_some() || end_value.is_some() {
        let range = TimestampRange {
            start: start_value.unwrap_or(TimestampValue::Earliest),
            end: end_value.unwrap_or(TimestampValue::Latest),
        };
        if !range.is_empty() {
            options.date_range = Some(range);
        }
    }

    Ok(options)
}
