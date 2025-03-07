use crate::aggregators::Aggregator;
use crate::arg_types::{MatchFilterOptions, RangeGroupingOptions};
use crate::common::rounding::{RoundingStrategy, MAX_DECIMAL_DIGITS, MAX_SIGNIFICANT_DIGITS};
use crate::common::time::current_time_millis;
use crate::common::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::aggregator::{AggregationOptions, BucketAlignment, BucketTimestamp};
use crate::join::join_reducer::JoinReducer;
use crate::labels::matchers::Matchers;
use crate::labels::{parse_series_selector, Label};
use crate::parser::number::parse_number;
use crate::parser::{
    metric_name::parse_metric_name as parse_metric, number::parse_number as parse_number_internal,
    parse_duration_value, timestamp::parse_timestamp as parse_timestamp_internal,
};
use crate::series::chunks::{ChunkCompression, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::series::types::*;
use crate::series::{TimestampRange, TimestampValue};
use ahash::AHashMap;
use std::collections::BTreeSet;
use std::iter::{Peekable, Skip};
use std::time::Duration;
use std::vec::IntoIter;
use strum_macros::EnumIter;
use valkey_module::{NextArg, ValkeyError, ValkeyResult, ValkeyString};

const MAX_TS_VALUES_FILTER: usize = 128;
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_ALIGN: &str = "ALIGN";
const CMD_ARG_ASOF: &str = "ASOF";
const CMD_ARG_BUCKET_TIMESTAMP: &str = "BUCKETTIMESTAMP";
const CMD_ARG_CHUNK_SIZE: &str = "CHUNK_SIZE";
const CMD_ARG_COMPRESSION: &str = "COMPRESSION";
const CMD_ARG_COMPRESSED: &str = "COMPRESSED";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_DECIMAL_DIGITS: &str = "DECIMAL_DIGITS";
const CMD_ARG_DUPLICATE_POLICY: &str = "DUPLICATE_POLICY";
const CMD_ARG_EMPTY: &str = "EMPTY";
const CMD_ARG_ENCODING: &str = "ENCODING";
const CMD_ARG_END: &str = "END";
const CMD_ARG_EXCLUSIVE: &str = "EXCLUSIVE";
const CMD_ARG_FILTER: &str = "FILTER";
const CMD_ARG_FILTER_BY_TS: &str = "FILTER_BY_TS";
const CMD_ARG_FILTER_BY_VALUE: &str = "FILTER_BY_VALUE";
const CMD_ARG_FULL: &str = "FULL";
const CMD_ARG_GROUP_BY: &str = "GROUPBY";
const CMD_ARG_IGNORE: &str = "IGNORE";
const CMD_ARG_INNER: &str = "INNER";
const CMD_ARG_LABELS: &str = "LABELS";
const CMD_ARG_LATEST: &str = "LATEST";
const CMD_ARG_LEFT: &str = "LEFT";
const CMD_ARG_LIMIT: &str = "LIMIT";
const CMD_ARG_MATCH: &str = "MATCH";
const CMD_ARG_METRIC: &str = "METRIC";
const CMD_ARG_NAME: &str = "NAME";
const CMD_ARG_NEXT: &str = "NEXT";
const CMD_ARG_ON_DUPLICATE: &str = "ON_DUPLICATE";
const CMD_ARG_PRIOR: &str = "PRIOR";
const CMD_ARG_REDUCE: &str = "REDUCE";
const CMD_ARG_RETENTION: &str = "RETENTION";
const CMD_ARG_RIGHT: &str = "RIGHT";
const CMD_ARG_ROUNDING: &str = "ROUNDING";
const CMD_ARG_SELECTED_LABELS: &str = "SELECTED_LABELS";
const CMD_ARG_STEP: &str = "STEP";
const CMD_ARG_SIGNIFICANT_DIGITS: &str = "SIGNIFICANT_DIGITS";
const CMD_ARG_START: &str = "START";
const CMD_ARG_TIMESTAMP: &str = "TIMESTAMP";
const CMD_ARG_UNCOMPRESSED: &str = "UNCOMPRESSED";
const CMD_ARG_WITH_LABELS: &str = "WITHLABELS";

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default, EnumIter)]
pub enum CommandArgToken {
    AsOf,
    Aggregation,
    Align,
    BucketTimestamp,
    ChunkSize,
    Compressed,
    Compression,
    Count,
    DecimalDigits,
    DuplicatePolicy,
    Empty,
    Encoding,
    End,
    Exclusive,
    Filter,
    FilterByTs,
    FilterByValue,
    Full,
    GroupBy,
    Ignore,
    Inner,
    Labels,
    Latest,
    Left,
    Limit,
    Match,
    Metric,
    Name,
    Next,
    OnDuplicate,
    Prior,
    Reduce,
    Retention,
    Right,
    Rounding,
    SelectedLabels,
    SignificantDigits,
    Start,
    Step,
    Timestamp,
    Uncompressed,
    WithLabels,
    #[default]
    Invalid
}

impl CommandArgToken {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            CommandArgToken::Aggregation => CMD_ARG_AGGREGATION,
            CommandArgToken::Align => CMD_ARG_ALIGN,
            CommandArgToken::AsOf => CMD_ARG_ASOF,
            CommandArgToken::ChunkSize => CMD_ARG_CHUNK_SIZE,
            CommandArgToken::Compressed => CMD_ARG_COMPRESSED,
            CommandArgToken::Compression => CMD_ARG_COMPRESSION,
            CommandArgToken::Count => CMD_ARG_COUNT,
            CommandArgToken::DecimalDigits => CMD_ARG_DECIMAL_DIGITS,
            CommandArgToken::DuplicatePolicy => CMD_ARG_DUPLICATE_POLICY,
            CommandArgToken::Empty => CMD_ARG_EMPTY,
            CommandArgToken::Encoding => CMD_ARG_ENCODING,
            CommandArgToken::End => CMD_ARG_END,
            CommandArgToken::Exclusive => CMD_ARG_EXCLUSIVE,
            CommandArgToken::Filter => CMD_ARG_FILTER,
            CommandArgToken::FilterByTs => CMD_ARG_FILTER_BY_TS,
            CommandArgToken::FilterByValue => CMD_ARG_FILTER_BY_VALUE,
            CommandArgToken::Full => CMD_ARG_FULL,
            CommandArgToken::GroupBy => CMD_ARG_GROUP_BY,
            CommandArgToken::Ignore => CMD_ARG_IGNORE,
            CommandArgToken::Inner => CMD_ARG_INNER,
            CommandArgToken::Latest => CMD_ARG_LATEST,
            CommandArgToken::Labels => CMD_ARG_LABELS,
            CommandArgToken::Left => CMD_ARG_LEFT,
            CommandArgToken::Limit => CMD_ARG_LIMIT,
            CommandArgToken::Match => CMD_ARG_MATCH,
            CommandArgToken::Metric => CMD_ARG_METRIC,
            CommandArgToken::Name => CMD_ARG_NAME,
            CommandArgToken::OnDuplicate => CMD_ARG_ON_DUPLICATE,
            CommandArgToken::Retention => CMD_ARG_RETENTION,
            CommandArgToken::Right => CMD_ARG_RIGHT,
            CommandArgToken::Rounding => CMD_ARG_ROUNDING,
            CommandArgToken::SignificantDigits => CMD_ARG_SIGNIFICANT_DIGITS,
            CommandArgToken::Start => CMD_ARG_START,
            CommandArgToken::Step => CMD_ARG_STEP,
            CommandArgToken::WithLabels => CMD_ARG_WITH_LABELS,
            CommandArgToken::BucketTimestamp => CMD_ARG_BUCKET_TIMESTAMP,
            CommandArgToken::Next => CMD_ARG_NEXT,
            CommandArgToken::Prior => CMD_ARG_PRIOR,
            CommandArgToken::Reduce => CMD_ARG_REDUCE,
            CommandArgToken::Uncompressed => CMD_ARG_UNCOMPRESSED,
            CommandArgToken::SelectedLabels => CMD_ARG_SELECTED_LABELS,
            CommandArgToken::Timestamp => CMD_ARG_TIMESTAMP,
            CommandArgToken::Invalid => "INVALID",
        }
    }
}

pub(crate) fn parse_command_arg_token(arg: &[u8]) -> Option<CommandArgToken> {
    hashify::tiny_map_ignore_case! {
        arg,
        "AGGREGATION" => CommandArgToken::Aggregation,
        "ALIGN" => CommandArgToken::Align,
        "ASOF" => CommandArgToken::AsOf,
        "BUCKETTIMESTAMP" => CommandArgToken::BucketTimestamp,
        "CHUNK_SIZE" => CommandArgToken::ChunkSize,
        "COMPRESSED" => CommandArgToken::Compressed,
        "COMPRESSION" => CommandArgToken::Compression,
        "COUNT" => CommandArgToken::Count,
        "DECIMAL_DIGITS" => CommandArgToken::DecimalDigits,
        "DUPLICATE_POLICY" => CommandArgToken::DuplicatePolicy,
        "EMPTY" => CommandArgToken::Empty,
        "END" => CommandArgToken::End,
        "ENCODING" => CommandArgToken::Encoding,
        "EXCLUSIVE" => CommandArgToken::Exclusive,
        "FILTER" => CommandArgToken::Filter,
        "FILTER_BY_TS" => CommandArgToken::FilterByTs,
        "FILTER_BY_VALUE" => CommandArgToken::FilterByValue,
        "FULL" => CommandArgToken::Full,
        "GROUP_BY" => CommandArgToken::GroupBy,
        "IGNORE" => CommandArgToken::Ignore,
        "INNER" => CommandArgToken::Inner,
        "LABELS" => CommandArgToken::Labels,
        "LATEST" => CommandArgToken::Latest,
        "LEFT" => CommandArgToken::Left,
        "LIMIT" => CommandArgToken::Limit,
        "MATCH" => CommandArgToken::Match,
        "METRIC" => CommandArgToken::Metric,
        "NAME" => CommandArgToken::Name,
        "NEXT" => CommandArgToken::Next,
        "ON_DUPLICATE" => CommandArgToken::OnDuplicate,
        "PRIOR" => CommandArgToken::Prior,
        "REDUCE" => CommandArgToken::Reduce,
        "RETENTION" => CommandArgToken::Retention,
        "RIGHT" => CommandArgToken::Right,
        "ROUNDING" => CommandArgToken::Rounding,
        "SELECTED_LABELS" => CommandArgToken::SelectedLabels,
        "SIGNIFICANT_DIGITS" => CommandArgToken::SignificantDigits,
        "START" => CommandArgToken::Start,
        "STEP" => CommandArgToken::Step,
        "TIMESTAMP" => CommandArgToken::Timestamp,
        "UNCOMPRESSED" => CommandArgToken::Uncompressed,
        "WITHLABELS" => CommandArgToken::WithLabels,
    }
}

pub type CommandArgIterator = Peekable<Skip<IntoIter<ValkeyString>>>;

pub fn parse_number_arg(arg: &ValkeyString, name: &str) -> ValkeyResult<f64> {
    if let Ok(value) = arg.parse_float() {
        return Ok(value);
    }
    let arg_str = arg.to_string_lossy();
    parse_number_with_unit(&arg_str).map_err(|_| {
        let msg = format!("ERR invalid number parsing {name}");
        ValkeyError::String(msg)
    })
}

pub fn parse_integer_arg(
    arg: &ValkeyString,
    name: &str,
    allow_negative: bool,
) -> ValkeyResult<i64> {
    let value = if let Ok(val) = arg.parse_integer() {
        val
    } else {
        let num = parse_number_arg(arg, name)?;
        if num != num.floor() {
            return Err(ValkeyError::Str(error_consts::INVALID_INTEGER));
        }
        if num > i64::MAX as f64 {
            return Err(ValkeyError::Str("ERR: value is too large"));
        }
        num as i64
    };
    if !allow_negative && value < 0 {
        let msg = format!("ERR: {name} must be a non-negative integer");
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

pub fn parse_timestamp(arg: &str) -> ValkeyResult<Timestamp> {
    if arg == "*" {
        return Ok(current_time_millis());
    }
    parse_timestamp_internal(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))
}

pub fn parse_timestamp_arg(arg: &str, name: &str) -> Result<TimestampValue, ValkeyError> {
    parse_timestamp_range_value(arg).map_err(|_e| {
        let msg = format!("TSDB: invalid {name} timestamp");
        ValkeyError::String(msg)
    })
}

pub fn parse_timestamp_range_value(arg: &str) -> ValkeyResult<TimestampValue> {
    TimestampValue::try_from(arg)
}

pub fn parse_duration_arg(arg: &ValkeyString) -> ValkeyResult<Duration> {
    if let Ok(value) = arg.parse_integer() {
        if value < 0 {
            return Err(ValkeyError::Str(
                "ERR: invalid duration, must be a non-negative integer",
            ));
        }
        return Ok(Duration::from_millis(value as u64));
    }
    let value_str = arg.to_string_lossy();
    parse_duration(&value_str)
}

pub fn parse_duration(arg: &str) -> ValkeyResult<Duration> {
    parse_duration_ms(arg).map(|d| Duration::from_millis(d as u64))
}

pub fn parse_duration_ms(arg: &str) -> ValkeyResult<i64> {
    parse_duration_value(arg, 1).map_err(|_| ValkeyError::Str(error_consts::INVALID_DURATION))
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    parse_number_internal(arg).map_err(|_e| TsdbError::InvalidNumber(arg.to_string()))
}

pub fn parse_metric_name(arg: &str) -> TsdbResult<Vec<Label>> {
    parse_metric(arg).map_err(|_e| TsdbError::InvalidMetric(arg.to_string()))
}

pub fn parse_join_operator(arg: &str) -> ValkeyResult<JoinReducer> {
    JoinReducer::try_from(arg)
}

pub fn parse_chunk_size(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let arg = args.next_str()?;
    fn get_error_result() -> ValkeyResult<usize> {
        let msg = format!("TSDB: CHUNK_SIZE value must be an integer multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(ValkeyError::String(msg))
    }

    let chunk_size = parse_number_with_unit(arg)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_CHUNK_SIZE))?;

    if chunk_size != chunk_size.floor() {
        return get_error_result();
    }
    if chunk_size < MIN_CHUNK_SIZE as f64 || chunk_size > MAX_CHUNK_SIZE as f64 {
        return get_error_result();
    }
    let chunk_size = chunk_size as usize;
    if chunk_size % 2 != 0 {
        return get_error_result();
    }
    Ok(chunk_size)
}

pub fn parse_chunk_compression(args: &mut CommandArgIterator) -> ValkeyResult<ChunkCompression> {
    args.next_str().and_then(|next| {
        ChunkCompression::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_COMPRESSION))
    })
}

pub fn parse_duplicate_policy(args: &mut CommandArgIterator) -> ValkeyResult<DuplicatePolicy> {
    args.next_str().and_then(|next| {
        DuplicatePolicy::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
    })
}

pub fn parse_timestamp_range(args: &mut CommandArgIterator) -> ValkeyResult<TimestampRange> {
    let first_arg = args.next_str()?;
    let start = parse_timestamp_range_value(first_arg)?;
    let end_value = if let Ok(arg) = args.next_str() {
        parse_timestamp_range_value(arg)
            .map_err(|_e| ValkeyError::Str("TSDB: invalid end timestamp"))?
    } else {
        TimestampValue::Latest
    };
    TimestampRange::new(start, end_value)
}

pub fn parse_retention(args: &mut CommandArgIterator) -> ValkeyResult<Duration> {
    if let Ok(next) = args.next_str() {
        parse_duration(next).map_err(|_e| ValkeyError::Str(error_consts::INVALID_DURATION))
    } else {
        Err(ValkeyError::Str("ERR missing RETENTION value"))
    }
}

pub fn parse_timestamp_filter(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<Timestamp>> {
    // FILTER_BY_TS already seen
    let mut values: Vec<Timestamp> = Vec::new();
    loop {
        if is_stop_token_or_end(args, stop_tokens) {
            break;
        }
        let arg = args.next_str()?;
        if let Ok(timestamp) = parse_timestamp(arg) {
            values.push(timestamp);
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP));
        }
        if values.len() == MAX_TS_VALUES_FILTER {
            break;
        }
    }
    if values.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: FILTER_BY_TS one or more arguments are missing",
        ));
    }
    values.sort();
    values.dedup();
    Ok(values)
}

pub fn parse_value_filter(args: &mut CommandArgIterator) -> ValkeyResult<ValueFilter> {
    let min = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("ERR cannot parse filter min parameter"))?;
    let max = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str("ERR cannot parse filter max parameter"))?;
    if max < min {
        return Err(ValkeyError::Str(
            "ERR filter min parameter is greater than max",
        ));
    }
    ValueFilter::new(min, max)
}

pub fn parse_count(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let next = args.next_arg()?;
    let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
        .map_err(|_| ValkeyError::Str(error_consts::NEGATIVE_COUNT))?;
    if count > usize::MAX as i64 {
        return Err(ValkeyError::Str("ERR COUNT value is too large"));
    }
    Ok(count as usize)
}

pub(crate) fn advance_if_next_token(args: &mut CommandArgIterator, token: CommandArgToken) -> bool {
    if let Some(next) = args.peek() {
        if let Some(tok) = parse_command_arg_token(next.as_slice()) {
            if tok == token {
                args.next();
                return true;
            }
        }
    }
    false
}

pub(crate) fn advance_if_next_token_one_of(
    args: &mut CommandArgIterator,
    tokens: &[CommandArgToken],
) -> Option<CommandArgToken> {
    if let Some(next) = args.peek() {
        if let Some(token) = parse_command_arg_token(next.as_slice()) {
            if tokens.contains(&token) {
                args.next();
                return Some(token);
            }
        }
    }
    None
}

pub(crate) fn expect_one_of(
    args: &mut CommandArgIterator,
    tokens: &[CommandArgToken],
) -> ValkeyResult<CommandArgToken> {
    if let Some(next) = args.next() {
        if let Some(token) = parse_command_arg_token(next.as_slice()) {
            if tokens.contains(&token) {
                return Ok(token);
            }
        }
    }
    let msg = format!("ERR: expected one of: {:?}", tokens);
    Err(ValkeyError::String(msg))
}

fn is_stop_token_or_end(args: &mut CommandArgIterator, stop_tokens: &[CommandArgToken]) -> bool {
    if let Some(next) = args.peek() {
        match parse_command_arg_token(next.as_slice()) {
            Some(token) => {
                args.next();
                stop_tokens.contains(&token)
            }
            None => false,
        }
    } else {
        false
    }
}

pub fn parse_label_list(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<String>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();

    loop {
        if is_stop_token_or_end(args, stop_tokens) {
            break;
        }

        let label = args.next_str()?;
        if labels.contains(label) {
            let msg = format!("ERR: duplicate label: {label}");
            return Err(ValkeyError::String(msg));
        }
        labels.insert(label.to_string());
    }

    let temp = labels.into_iter().collect();
    Ok(temp)
}

pub fn parse_label_value_pairs(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<AHashMap<String, String>> {
    let mut labels: AHashMap<String, String> = AHashMap::new();

    loop {
        let label = args.next_string()?;

        if label.is_empty() {
            return Err(ValkeyError::Str("ERR invalid label key"));
        }

        if labels.contains_key(&label) {
            let msg = format!("ERR: duplicate label: {label}");
            return Err(ValkeyError::String(msg));
        }

        // todo: regex validation
        let value = args
            .next_string()
            .map_err(|_| ValkeyError::Str("ERR invalid label value"))?;

        labels.insert(label, value);

        if is_stop_token_or_end(args, stop_tokens) {
            break;
        }
    }

    Ok(labels)
}

pub fn parse_aggregation_options(
    args: &mut CommandArgIterator,
) -> ValkeyResult<AggregationOptions> {
    // AGGREGATION token already seen
    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("ERR: Error parsing AGGREGATION"))?;
    let aggregator = Aggregator::try_from(agg_str)?;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| ValkeyError::Str("Error parsing bucketDuration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        aggregator,
        bucket_duration,
        timestamp_output: BucketTimestamp::Start,
        alignment: BucketAlignment::default(),
        time_delta: 0,
        empty: false,
    };

    let mut arg_count: usize = 0;

    let valid_tokens = [
        CommandArgToken::Align,
        CommandArgToken::Empty,
        CommandArgToken::BucketTimestamp,
    ];

    while let Some(token) = advance_if_next_token_one_of(args, &valid_tokens) {
        match token {
            CommandArgToken::Empty => {
                aggr.empty = true;
                arg_count += 1;
            }
            CommandArgToken::BucketTimestamp => {
                let next = args.next_str()?;
                arg_count += 1;
                aggr.timestamp_output = BucketTimestamp::try_from(next)?;
            }
            CommandArgToken::Align => {
                let next = args.next_str()?;
                aggr.alignment = next.try_into()?;
            }
            _ => break,
        }
        if arg_count == 3 {
            break;
        }
    }

    Ok(aggr)
}

pub fn parse_grouping_params(args: &mut CommandArgIterator) -> ValkeyResult<RangeGroupingOptions> {
    // GROUPBY token already seen
    let label = args.next_str()?;
    let token = args
        .next_str()
        .map_err(|_| ValkeyError::Str("ERR: missing REDUCE"))?;
    if !token.eq_ignore_ascii_case(CMD_ARG_REDUCE) {
        let msg = format!("ERR: expected \"{CMD_ARG_REDUCE}\", found \"{token}\"");
        return Err(ValkeyError::String(msg));
    }
    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("ERR: Error parsing grouping reducer"))?;

    let aggregator = Aggregator::try_from(agg_str).map_err(|_| {
        let msg = format!("ERR: invalid grouping aggregator \"{}\"", agg_str);
        ValkeyError::String(msg)
    })?;

    Ok(RangeGroupingOptions {
        group_label: label.to_string(),
        aggregator,
    })
}

pub fn parse_significant_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_SIGNIFICANT_DIGITS as u64 {
        let msg = format!("ERR SIGNIFICANT_DIGITS must be between 0 and {MAX_SIGNIFICANT_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::SignificantDigits(next as i32))
}

pub fn parse_decimal_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_DECIMAL_DIGITS as u64 {
        let msg = format!("ERR DECIMAL_DIGITS must be between 0 and {MAX_DECIMAL_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::DecimalDigits(next as i32))
}

pub(crate) fn parse_ignore_options(args: &mut CommandArgIterator) -> ValkeyResult<(i64, f64)> {
    // ignoreMaxTimediff
    let mut str = args.next_str()?;
    let ignore_max_timediff =
        parse_duration_ms(str).map_err(|_| ValkeyError::Str("Invalid ignoreMaxTimediff"))?;
    // ignoreMaxValDiff
    str = args.next_str()?;
    let ignore_max_val_diff =
        parse_number(str).map_err(|_| ValkeyError::Str("Invalid ignoreMaxValDiff"))?;
    Ok((ignore_max_timediff, ignore_max_val_diff))
}

pub fn parse_series_selector_list(
    args: &mut CommandArgIterator,
    is_cmd_token: fn(CommandArgToken) -> bool,
) -> ValkeyResult<Vec<Matchers>> {
    let mut matchers = vec![];

    while let Some(next) = args.peek() {
        if let Some(token) = parse_command_arg_token(next.as_slice()) {
            if is_cmd_token(token) {
                break;
            }
        } else {
            return Err(ValkeyError::Str("ERR: Invalid series selector"));
        }
        let arg = next.try_as_str()?;

        if let Ok(selector) = parse_series_selector(arg) {
            matchers.push(selector);
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_SERIES_SELECTOR));
        }
    }

    Ok(matchers)
}

pub(crate) fn parse_metadata_command_args(
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

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_parse_command_arg_token_case_insensitive() {
        let input = b"aGgReGaTiOn";
        let result = parse_command_arg_token(input);
        assert_eq!(result, Some(CommandArgToken::Aggregation));
    }

    #[test]
    fn test_parse_command_arg_token_exhaustive() {
        for token in CommandArgToken::iter() {
            if token == CommandArgToken::Invalid {
                continue;
            }
            let parsed = parse_command_arg_token(token.as_str().as_bytes());
            assert_eq!(parsed, Some(token));
            let lower = token.as_str().to_lowercase();
            let parsed_lowercase = parse_command_arg_token(lower.as_bytes());
            assert_eq!(parsed_lowercase, Some(token));
        }

    }
}