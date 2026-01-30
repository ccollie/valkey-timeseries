use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
use crate::common::Timestamp;
use crate::common::binop::ComparisonOperator;
use crate::common::rounding::{MAX_DECIMAL_DIGITS, MAX_SIGNIFICANT_DIGITS, RoundingStrategy};
use crate::common::time::current_time_millis;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::join::join_reducer::JoinReducer;
use crate::join::{AsOfJoinOptions, AsOfJoinStrategy, JoinOptions, JoinType};
use crate::labels::filters::SeriesSelector;
use crate::labels::{Label, parse_series_selector};
use crate::parser::number::parse_number;
use crate::parser::{
    metric_name::parse_metric_name as parse_metric, number::parse_number as parse_number_internal,
    parse_positive_duration_value, timestamp::parse_timestamp as parse_timestamp_internal,
};
use crate::series::chunks::{ChunkEncoding, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::series::request_types::{
    AggregationOptions, AggregatorConfig, MRangeOptions, MatchFilterOptions, MetaDateRangeFilter,
    RangeGroupingOptions, RangeOptions, ValueComparisonFilter,
};
use crate::series::types::{DuplicatePolicy, ValueFilter};
use crate::series::{TimestampRange, TimestampValue};
use ahash::AHashMap;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::iter::{Peekable, Skip};
use std::time::Duration;
use std::vec::IntoIter;
use strum_macros::EnumIter;
use valkey_module::{NextArg, ValkeyError, ValkeyResult, ValkeyString};

pub const MAX_TS_VALUES_FILTER: usize = 128;

// Kept because these are referenced directly in the parsing logic below.
const CMD_ARG_AGGREGATION: &str = "AGGREGATION";
const CMD_ARG_COUNT: &str = "COUNT";
const CMD_ARG_REDUCE: &str = "REDUCE";

macro_rules! command_arg_tokens {
    ( $( $variant:ident => $lit:literal ),+ $(,)? ) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default, EnumIter)]
        pub enum CommandArgToken {
            $(
                $variant,
            )+
            #[default]
            Invalid,
        }

        impl CommandArgToken {
            #[allow(dead_code)]
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(
                        CommandArgToken::$variant => $lit,
                    )+
                    CommandArgToken::Invalid => "INVALID",
                }
            }
        }

        pub(crate) fn parse_command_arg_token(arg: &[u8]) -> Option<CommandArgToken> {
            hashify::tiny_map_ignore_case! {
                arg,
                $(
                    $lit => CommandArgToken::$variant,
                )+
            }
        }
    };
}

command_arg_tokens! {
    Aggregation => "AGGREGATION",
    Align => "ALIGN",
    AllowExactMatch => "ALLOW_EXACT_MATCH",
    Anti => "ANTI",
    AsOf => "ASOF",
    BucketTimestamp => "BUCKETTIMESTAMP",
    ChunkSize => "CHUNK_SIZE",
    Compressed => "COMPRESSED",
    Compression => "COMPRESSION",
    Condition => "CONDITION",
    Count => "COUNT",
    DecimalDigits => "DECIMAL_DIGITS",
    DuplicatePolicy => "DUPLICATE_POLICY",
    Empty => "EMPTY",
    Encoding => "ENCODING",
    End => "END",
    False => "FALSE",
    Filter => "FILTER",
    FilterByTs => "FILTER_BY_TS",
    FilterByValue => "FILTER_BY_VALUE",
    FilterByRange => "FILTER_BY_RANGE",
    Full => "FULL",
    GroupBy => "GROUPBY",
    Ignore => "IGNORE",
    Inner => "INNER",
    Labels => "LABELS",
    Latest => "LATEST",
    Left => "LEFT",
    Limit => "LIMIT",
    Match => "MATCH",
    Metric => "METRIC",
    Name => "NAME",
    Nearest => "NEAREST",
    Next => "NEXT",
    Not => "NOT",
    OnDuplicate => "ON_DUPLICATE",
    Previous => "PREVIOUS",
    Prior => "PRIOR",
    Reduce => "REDUCE",
    Retention => "RETENTION",
    Right => "RIGHT",
    Rounding => "ROUNDING",
    SelectedLabels => "SELECTED_LABELS",
    Semi => "SEMI",
    SignificantDigits => "SIGNIFICANT_DIGITS",
    Start => "START",
    Step => "STEP",
    Timestamp => "TIMESTAMP",
    True => "TRUE",
    Uncompressed => "UNCOMPRESSED",
    WithLabels => "WITHLABELS",
}

impl Display for CommandArgToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
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
            return Err(ValkeyError::Str("TSDB: value is too large"));
        }
        num as i64
    };
    if !allow_negative && value < 0 {
        let msg = format!("TSDB: {name} must be a non-negative integer");
        return Err(ValkeyError::String(msg));
    }
    Ok(value)
}

pub fn parse_timestamp(arg: &str) -> ValkeyResult<Timestamp> {
    if arg == "*" {
        return Ok(current_time_millis());
    }
    parse_timestamp_internal(arg, false)
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))
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
                "TSDB: invalid duration, must be a non-negative integer",
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
    parse_positive_duration_value(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_DURATION))
}

pub fn parse_number_with_unit(arg: &str) -> TsdbResult<f64> {
    if arg.is_empty() {
        return Err(TsdbError::InvalidNumber(arg.to_string()));
    }
    parse_number_internal(arg).map_err(|_e| TsdbError::InvalidNumber(arg.to_string()))
}

pub fn parse_metric_name(arg: &str) -> ValkeyResult<Vec<Label>> {
    parse_metric(arg).map_err(|_e| ValkeyError::Str(error_consts::INVALID_METRIC_NAME))
}

/// Parse a float value for use in a command argument, specifically ADD, MADD, and INCRBY/DECRBY.
pub fn parse_value_arg(arg: &ValkeyString) -> ValkeyResult<f64> {
    let value = arg
        .try_as_str()
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_VALUE))?
        .parse::<f64>()
        .map_err(|_| ValkeyError::Str(error_consts::INVALID_VALUE))?;

    if value.is_nan() || value.is_infinite() {
        return Err(ValkeyError::Str(error_consts::INVALID_VALUE));
    }

    Ok(value)
}

pub fn parse_join_operator(arg: &str) -> ValkeyResult<JoinReducer> {
    JoinReducer::try_from(arg)
}

pub fn parse_chunk_size(arg: &str) -> ValkeyResult<usize> {
    fn get_error_result() -> ValkeyResult<usize> {
        let msg = format!(
            "TSDB: CHUNK_SIZE value must be an integer multiple of 8 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]"
        );
        Err(ValkeyError::String(msg))
    }

    if arg.is_empty() {
        return Err(ValkeyError::Str(error_consts::CANNOT_PARSE_CHUNK_SIZE));
    }

    let chunk_size = parse_number_internal(arg)
        .map_err(|_e| ValkeyError::Str(error_consts::CANNOT_PARSE_CHUNK_SIZE))?;

    if chunk_size != chunk_size.floor() {
        return get_error_result();
    }
    let chunk_size = chunk_size as usize;

    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) || !chunk_size.is_multiple_of(8) {
        return get_error_result();
    }

    Ok(chunk_size)
}

pub fn parse_chunk_compression(args: &mut CommandArgIterator) -> ValkeyResult<ChunkEncoding> {
    if let Ok(next) = args.next_str() {
        ChunkEncoding::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING))
    } else {
        Err(ValkeyError::Str(error_consts::MISSING_CHUNK_ENCODING))
    }
}

pub fn parse_duplicate_policy(args: &mut CommandArgIterator) -> ValkeyResult<DuplicatePolicy> {
    if let Ok(next) = args.next_str() {
        DuplicatePolicy::try_from(next)
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_DUPLICATE_POLICY))
    } else {
        Err(ValkeyError::Str(error_consts::MISSING_DUPLICATE_POLICY))
    }
}

pub fn parse_timestamp_range(args: &mut CommandArgIterator) -> ValkeyResult<TimestampRange> {
    let first_arg = args.next_str()?;
    let start = parse_timestamp_range_value(first_arg)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_START_TIMESTAMP))?;
    let end_value = if let Ok(arg) = args.next_str() {
        parse_timestamp_range_value(arg)
            .map_err(|_e| ValkeyError::Str(error_consts::INVALID_END_TIMESTAMP))?
    } else {
        TimestampValue::Latest
    };
    TimestampRange::new(start, end_value)
}

pub fn parse_retention(args: &mut CommandArgIterator) -> ValkeyResult<Duration> {
    if let Ok(next) = args.next_str() {
        parse_duration(next).map_err(|_e| ValkeyError::Str(error_consts::CANNOT_PARSE_RETENTION))
    } else {
        Err(ValkeyError::Str(error_consts::CANNOT_PARSE_RETENTION))
    }
}

pub fn parse_timestamp_filter(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<Timestamp>> {
    // FILTER_BY_TS already seen
    let mut values: Vec<Timestamp> = Vec::new();

    while !is_stop_token_or_end(args, stop_tokens) {
        let arg = args
            .next_str()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP_FILTER))?;

        let timestamp =
            parse_timestamp(arg).map_err(|_| ValkeyError::Str(error_consts::INVALID_TIMESTAMP))?;

        values.push(timestamp);

        if values.len() > MAX_TS_VALUES_FILTER {
            return Err(ValkeyError::Str(
                error_consts::TOO_MANY_TIMESTAMP_FILTER_VALUES,
            ));
        }
    }

    if values.is_empty() {
        return Err(ValkeyError::Str(
            error_consts::MISSING_TIMESTAMP_FILTER_VALUE,
        ));
    }

    Ok(values)
}

pub fn parse_value_filter(args: &mut CommandArgIterator) -> ValkeyResult<ValueFilter> {
    let min = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str(error_consts::CANNOT_PARSE_MIN))?;
    let max = parse_number_with_unit(args.next_str()?)
        .map_err(|_| ValkeyError::Str(error_consts::CANNOT_PARSE_MAX))?;
    if max < min {
        return Err(ValkeyError::Str(
            "TSDB filter min parameter is greater than max",
        ));
    }
    ValueFilter::new(min, max)
}

pub fn parse_count_arg(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let next = args
        .next_arg()
        .map_err(|_| ValkeyError::Str(error_consts::MISSING_COUNT_VALUE))?;
    let count = parse_integer_arg(&next, CMD_ARG_COUNT, false)
        .map_err(|_| ValkeyError::Str(error_consts::NEGATIVE_COUNT))?;
    Ok(count as usize)
}

fn expect_next_token(args: &mut CommandArgIterator, expected: CommandArgToken) -> ValkeyResult<()> {
    let found = args.next_str()?;
    let Some(found_token) = parse_command_arg_token(found.as_bytes()) else {
        let msg = format!(
            "TSDB: expected \"{}\", found \"{found}\"",
            expected.as_str()
        );
        return Err(ValkeyError::String(msg));
    };

    if found_token != expected {
        let msg = format!(
            "TSDB: expected \"{}\", found \"{found}\"",
            expected.as_str()
        );
        return Err(ValkeyError::String(msg));
    }

    Ok(())
}

pub(crate) fn parse_next_token(
    args: &mut CommandArgIterator,
    tokens: Option<&[CommandArgToken]>,
) -> ValkeyResult<Option<CommandArgToken>> {
    let arg = args.next_str()?;
    let Some(token) = parse_command_arg_token(arg.as_bytes()) else {
        return Ok(None);
    };
    let Some(valid_tokens) = tokens else {
        return Ok(Some(token));
    };
    if valid_tokens.contains(&token) {
        return Ok(Some(token));
    }
    let msg = if valid_tokens.len() == 1 {
        format!(
            "TSDB: expected \"{}\", found \"{arg}\"",
            valid_tokens[0].as_str()
        )
    } else {
        format!(
            "TSDB: expected one of {:?}, found \"{arg}\"",
            valid_tokens.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
        )
    };
    Err(ValkeyError::String(msg))
}

pub(crate) fn advance_if_next_token_one_of(
    args: &mut CommandArgIterator,
    tokens: &[CommandArgToken],
) -> Option<CommandArgToken> {
    if let Some(next) = args.peek()
        && let Some(token) = parse_command_arg_token(next)
        && tokens.contains(&token)
    {
        args.next();
        return Some(token);
    }
    None
}

pub(super) fn peek_token(args: &mut CommandArgIterator) -> Option<CommandArgToken> {
    args.peek().and_then(|next| parse_command_arg_token(next))
}

fn next_token(args: &mut CommandArgIterator) -> ValkeyResult<Option<CommandArgToken>> {
    let arg = match args.next_str() {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    Ok(parse_command_arg_token(arg.as_bytes()))
}

fn parse_optional_token_block(
    args: &mut CommandArgIterator,
    valid_tokens: &[CommandArgToken],
    max_tokens: usize,
    mut handle: impl FnMut(CommandArgToken, &mut CommandArgIterator) -> ValkeyResult<()>,
) -> ValkeyResult<()> {
    let mut consumed = 0usize;
    while consumed < max_tokens {
        let Some(token) = advance_if_next_token_one_of(args, valid_tokens) else {
            break;
        };
        handle(token, args)?;
        consumed += 1;
    }
    Ok(())
}

fn is_stop_token_or_end(args: &mut CommandArgIterator, stop_tokens: &[CommandArgToken]) -> bool {
    let Some(token) = peek_token(args) else {
        return args.peek().is_none(); // end => true, non-token => false
    };
    stop_tokens.contains(&token)
}

fn for_each_arg_until_stop(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
    mut f: impl FnMut(&str) -> ValkeyResult<()>,
) -> ValkeyResult<()> {
    while !is_stop_token_or_end(args, stop_tokens) {
        let arg = args.next_str()?;
        f(arg)?;
    }
    Ok(())
}

pub fn parse_label_list(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<String>> {
    let mut labels: BTreeSet<String> = BTreeSet::new();

    for_each_arg_until_stop(args, stop_tokens, |label| {
        if labels.contains(label) {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_LABEL));
        }
        labels.insert(label.to_string());
        Ok(())
    })?;

    Ok(labels.into_iter().collect())
}

pub fn parse_label_value_pairs(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<AHashMap<String, String>> {
    let mut labels: AHashMap<String, String> = AHashMap::new();

    while !is_stop_token_or_end(args, stop_tokens) {
        let name = args
            .next_str()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_LABEL_NAME))?;

        if name.is_empty() {
            return Err(ValkeyError::Str(error_consts::INVALID_LABEL_NAME));
        }

        // Must have a value next; if we hit stop/end here, it's an odd number of args.
        if is_stop_token_or_end(args, stop_tokens) || args.peek().is_none() {
            return Err(ValkeyError::Str(error_consts::INVALID_LABEL_VALUE));
        }

        let value = args
            .next_str()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_LABEL_VALUE))?;

        if labels.insert(name.to_string(), value.to_string()).is_some() {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_LABEL));
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
        .map_err(|_e| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?;
    let aggregator = AggregationType::try_from(agg_str)?;
    let mut value_filter: Option<ValueComparisonFilter> = None;
    let bucket_duration = parse_duration_arg(&args.next_arg()?)
        .map_err(|_e| ValkeyError::Str("TSDB: Couldn't parse bucket duration"))?;

    let mut aggr: AggregationOptions = AggregationOptions {
        bucket_duration: bucket_duration.as_millis() as u64,
        timestamp_output: BucketTimestamp::Start,
        ..Default::default()
    };

    let valid_tokens = [
        CommandArgToken::Align,
        CommandArgToken::Empty,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::Condition,
    ];

    parse_optional_token_block(args, &valid_tokens, 3, |token, args| match token {
        CommandArgToken::Empty => {
            aggr.report_empty = true;
            Ok(())
        }
        CommandArgToken::BucketTimestamp => {
            let next = args.next_str()?;
            aggr.timestamp_output = BucketTimestamp::try_from(next)?;
            Ok(())
        }
        CommandArgToken::Align => {
            let next = args.next_str()?;
            aggr.alignment = next.try_into()?;
            Ok(())
        }
        CommandArgToken::Condition => {
            value_filter = Some(parse_aggregator_value_filter(args)?);
            Ok(())
        }
        _ => Ok(()),
    })?;

    aggr.aggregation = AggregatorConfig::new(aggregator, value_filter)?;

    Ok(aggr)
}

pub(super) fn parse_aggregator_value_filter(
    args: &mut CommandArgIterator,
) -> ValkeyResult<ValueComparisonFilter> {
    // get the comparison operator
    let comp_str = args.next_str()?;
    let operator = ComparisonOperator::try_from(comp_str)
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_COMPARISON_OPERATOR))?;
    // get the comparison value
    let value = args.next_f64()?;
    Ok(ValueComparisonFilter { operator, value })
}

pub(super) fn parse_grouping_params(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RangeGroupingOptions> {
    // GROUPBY token already seen
    let label = args.next_str()?;

    expect_next_token(args, CommandArgToken::Reduce)
        .map_err(|_| ValkeyError::Str("TSDB: missing REDUCE"))?;

    let agg_str = args
        .next_str()
        .map_err(|_e| ValkeyError::Str("TSDB: error parsing grouping reducer"))?;

    let aggregator = AggregationType::try_from(agg_str).map_err(|_| {
        let msg = format!("TSDB: invalid grouping aggregator \"{agg_str}\"");
        ValkeyError::String(msg)
    })?;

    // Rate requires a time range, so it is not valid for grouping.
    if aggregator == AggregationType::Rate {
        let msg = "TSDB: aggregator not supported for GROUPBY reducer";
        return Err(ValkeyError::Str(msg));
    }

    let mut value_filter: Option<ValueComparisonFilter> = None;

    // see if we have a filter condition
    if let Some(token) = peek_token(args)
        && token == CommandArgToken::Condition
    {
        args.next(); // consume CONDITION
        value_filter = Some(parse_aggregator_value_filter(args)?);
    }

    let aggregation = AggregatorConfig::new(aggregator, value_filter)?;

    Ok(RangeGroupingOptions {
        group_label: label.to_string(),
        aggregation,
    })
}

pub fn parse_significant_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_SIGNIFICANT_DIGITS as u64 {
        let msg =
            format!("TSDB: SIGNIFICANT_DIGITS must be between 0 and {MAX_SIGNIFICANT_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::SignificantDigits(next as u8))
}

pub fn parse_decimal_digit_rounding(
    args: &mut CommandArgIterator,
) -> ValkeyResult<RoundingStrategy> {
    let next = args.next_u64()?;
    if next > MAX_DECIMAL_DIGITS as u64 {
        let msg = format!("TSDB: DECIMAL_DIGITS must be between 0 and {MAX_DECIMAL_DIGITS}");
        return Err(ValkeyError::String(msg));
    }
    Ok(RoundingStrategy::DecimalDigits(next as u8))
}

pub(crate) fn parse_ignore_options(args: &mut CommandArgIterator) -> ValkeyResult<(i64, f64)> {
    // ignoreMaxTimediff
    let mut str = args.next_str()?;
    let ignore_max_timediff =
        parse_duration_ms(str).map_err(|_| ValkeyError::Str(error_consts::CANNOT_PARSE_IGNORE))?;
    // ignoreMaxValDiff
    str = args.next_str()?;
    let ignore_max_val_diff =
        parse_number(str).map_err(|_| ValkeyError::Str(error_consts::CANNOT_PARSE_IGNORE))?;
    if ignore_max_timediff < 0 || ignore_max_val_diff < 0.0 {
        return Err(ValkeyError::Str(error_consts::NEGATIVE_IGNORE_VALUES));
    }
    Ok((ignore_max_timediff, ignore_max_val_diff))
}

pub fn parse_series_selector_list(
    args: &mut CommandArgIterator,
    stop_tokens: &[CommandArgToken],
) -> ValkeyResult<Vec<SeriesSelector>> {
    let mut matchers = vec![];

    while args.peek().is_some() {
        if let Some(token) = peek_token(args)
            && stop_tokens.contains(&token)
        {
            break;
        }

        let arg = args.next_str()?;
        if arg.is_empty() {
            return Err(ValkeyError::Str(error_consts::INVALID_SERIES_SELECTOR));
        }

        if let Ok(selector) = parse_series_selector(arg) {
            matchers.push(selector);
        } else {
            return Err(ValkeyError::Str(error_consts::INVALID_SERIES_SELECTOR));
        }
    }

    if matchers.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    Ok(matchers)
}

fn parse_align_for_aggregation(args: &mut CommandArgIterator) -> ValkeyResult<AggregationOptions> {
    // ALIGN token already seen
    let alignment_str = args.next_str()?;

    expect_next_token(args, CommandArgToken::Aggregation)
        .map_err(|_| ValkeyError::Str("TSDB: missing AGGREGATION"))?;

    let mut aggregation = parse_aggregation_options(args)?;
    aggregation.alignment = BucketAlignment::try_from(alignment_str)?;
    Ok(aggregation)
}

pub fn parse_range_options(args: &mut CommandArgIterator) -> ValkeyResult<RangeOptions> {
    const RANGE_OPTION_ARGS: [CommandArgToken; 7] = [
        CommandArgToken::Align,
        CommandArgToken::Aggregation,
        CommandArgToken::Count,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::FilterByTs,
        CommandArgToken::FilterByValue,
        CommandArgToken::Latest,
    ];

    let date_range = parse_timestamp_range(args)?;

    let mut options = RangeOptions {
        date_range,
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Align => {
                options.aggregation = Some(parse_align_for_aggregation(args)?);
            }
            CommandArgToken::Aggregation => {
                options.aggregation = Some(parse_aggregation_options(args)?);
            }
            CommandArgToken::Count => {
                options.count = Some(parse_count_arg(args)?);
            }
            CommandArgToken::FilterByValue => {
                options.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.timestamp_filter = Some(parse_timestamp_filter(args, &RANGE_OPTION_ARGS)?);
            }
            CommandArgToken::Latest => {
                options.latest = true;
            }
            _ => {
                return if token == CommandArgToken::Invalid {
                    Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT))
                } else {
                    let msg = format!("TSDB: invalid argument '{token}'");
                    Err(ValkeyError::String(msg))
                };
            }
        }
    }

    // according to docs, align cannot be Start if start time is Earliest, or End if end time is Latest
    if let Some(aggregation) = &options.aggregation {
        if options.date_range.start == TimestampValue::Earliest
            && aggregation.alignment == BucketAlignment::Start
        {
            return Err(ValkeyError::Str(
                "TSDB: cannot use 'start' align with '-' range start timestamp",
            ));
        }
        if options.date_range.end == TimestampValue::Latest
            && aggregation.alignment == BucketAlignment::End
        {
            return Err(ValkeyError::Str(
                "TSDB: cannot use 'end' align with '+' range end timestamp",
            ));
        }
    }

    // filter out timestamp filters that are outside the range
    if let Some(ts_filter) = options.timestamp_filter.as_mut() {
        let (start_ts, end_ts) = options.date_range.get_timestamps(None);
        ts_filter.retain(|&ts| ts >= start_ts && ts <= end_ts);
    }

    Ok(options)
}

pub fn parse_filter_by_range_options(
    args: &mut CommandArgIterator,
) -> ValkeyResult<MetaDateRangeFilter> {
    let mut exclude_range = false;
    // see if we want to negate the condition
    if let Some(peek) = peek_token(args)
        && peek == CommandArgToken::Not
    {
        exclude_range = true;
        args.next();
    }

    let range = parse_timestamp_range(args)?.resolve(None);
    match exclude_range {
        true => Ok(MetaDateRangeFilter::Excludes(range)),
        false => Ok(MetaDateRangeFilter::Includes(range)),
    }
}

pub fn parse_mrange_options(args: &mut CommandArgIterator) -> ValkeyResult<MRangeOptions> {
    const RANGE_OPTION_ARGS: [CommandArgToken; 12] = [
        CommandArgToken::Align,
        CommandArgToken::Aggregation,
        CommandArgToken::Count,
        CommandArgToken::BucketTimestamp,
        CommandArgToken::Filter,
        CommandArgToken::FilterByTs,
        CommandArgToken::FilterByValue,
        CommandArgToken::Latest,
        CommandArgToken::GroupBy,
        CommandArgToken::Reduce,
        CommandArgToken::SelectedLabels,
        CommandArgToken::WithLabels,
    ];

    let date_range = parse_timestamp_range(args)?;

    let mut options = MRangeOptions {
        range: RangeOptions {
            date_range,
            ..Default::default()
        },
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Align => {
                options.range.aggregation = Some(parse_align_for_aggregation(args)?);
            }
            CommandArgToken::Aggregation => {
                options.range.aggregation = Some(parse_aggregation_options(args)?);
            }
            CommandArgToken::Count => {
                options.range.count = Some(parse_count_arg(args)?);
            }
            CommandArgToken::Filter => {
                options.filters = parse_series_selector_list(args, &RANGE_OPTION_ARGS)?;
            }
            CommandArgToken::FilterByValue => {
                options.range.value_filter = Some(parse_value_filter(args)?);
            }
            CommandArgToken::FilterByTs => {
                options.range.timestamp_filter =
                    Some(parse_timestamp_filter(args, &RANGE_OPTION_ARGS)?);
            }
            CommandArgToken::GroupBy => {
                options.grouping = Some(parse_grouping_params(args)?);
            }
            CommandArgToken::Latest => {
                options.range.latest = true;
            }
            CommandArgToken::SelectedLabels => {
                options.selected_labels = parse_label_list(args, &RANGE_OPTION_ARGS)?;
            }
            CommandArgToken::WithLabels => {
                options.with_labels = true;
            }
            _ => {}
        }
    }

    if options.filters.is_empty() {
        return Err(ValkeyError::Str("TSDB: no FILTER given"));
    }

    // filter out timestamp filters that are outside the range
    if let Some(ts_filter) = options.range.timestamp_filter.as_mut() {
        let (start_ts, end_ts) = options.range.date_range.get_timestamps(None);
        ts_filter.retain(|&ts| ts >= start_ts && ts <= end_ts);
    }

    if !options.selected_labels.is_empty() && options.with_labels {
        return Err(ValkeyError::Str(
            error_consts::WITH_LABELS_AND_SELECTED_LABELS_SPECIFIED,
        ));
    }

    Ok(options)
}

fn parse_asof_join_options(args: &mut CommandArgIterator) -> ValkeyResult<JoinType> {
    use CommandArgToken::*;

    // ASOF already seen
    let mut tolerance = Duration::default();
    let mut strategy = AsOfJoinStrategy::Backward;

    // ASOF [PREVIOUS | NEXT | NEAREST] [tolerance] [ALLOW_EXACT_MATCH [true|false]]
    if let Some(next) = advance_if_next_token_one_of(args, &[Previous, Next, Nearest]) {
        strategy = match next {
            Previous => AsOfJoinStrategy::Backward,
            Next => AsOfJoinStrategy::Forward,
            Nearest => AsOfJoinStrategy::Nearest,
            _ => unreachable!("BUG: invalid match arm for AsofJoinStrategy"),
        };
    }

    let mut allow_exact_match = true;
    if args.peek().is_some() {
        if let Some(next_token) = peek_token(args) {
            // If the next thing is a known token, it's not a duration.
            if next_token != AllowExactMatch {
                // no-op; duration parsing below will handle only digit-starting strings
            }
        }

        if let Some(next_arg) = args.peek()
            && let Ok(arg_str) = next_arg.try_as_str()
        {
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

pub(super) fn parse_join_args(
    args: &mut CommandArgIterator,
    options: &mut JoinOptions,
) -> ValkeyResult<()> {
    use CommandArgToken::*;
    let mut join_type_set = false;

    fn check_join_type_set(is_set: &mut bool) -> ValkeyResult<()> {
        if *is_set {
            Err(ValkeyError::Str("TSDB: join type already set"))
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

    // Disallow REDUCE for joins that only return single values per timestamp
    if options.reducer.is_some()
        && (options.join_type == JoinType::Semi || options.join_type == JoinType::Anti)
    {
        return Err(ValkeyError::Str(
            "TSDB: cannot use REDUCE with SEMI or ANTI joins",
        ));
    }
    Ok(())
}

pub(crate) fn parse_metadata_command_args(
    args: &mut CommandArgIterator,
    require_matchers: bool,
) -> ValkeyResult<MatchFilterOptions> {
    const ARG_TOKENS: [CommandArgToken; 2] =
        [CommandArgToken::FilterByRange, CommandArgToken::Limit];

    let mut matchers = Vec::with_capacity(4);
    let mut limit: Option<usize> = None;
    let mut date_range: Option<MetaDateRangeFilter> = None;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::FilterByRange => {
                date_range = Some(parse_filter_by_range_options(args)?);
            }
            CommandArgToken::Filter => {
                let m = parse_series_selector_list(args, &ARG_TOKENS)?;
                matchers.extend(m);
            }
            CommandArgToken::Limit => {
                let next = args
                    .next_str()
                    .map_err(|_| ValkeyError::Str(error_consts::MISSING_LIMIT_VALUE))?;
                limit = parse_limit_value(next)?;
            }
            _ => {
                let msg = "TSDB: invalid argument";
                return Err(ValkeyError::Str(msg));
            }
        };
    }

    if require_matchers && matchers.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    Ok(MatchFilterOptions {
        matchers,
        limit,
        date_range,
    })
}

pub(super) fn parse_query_index_command_args(
    args: &mut CommandArgIterator,
) -> ValkeyResult<MatchFilterOptions> {
    let mut date_range: Option<MetaDateRangeFilter> = None;

    if let Some(token) = peek_token(args)
        && token == CommandArgToken::FilterByRange
    {
        // FILTER_BY_RANGE [NOT] <from> <to>
        args.next(); // consume token
        date_range = Some(parse_filter_by_range_options(args)?);
    };

    // everything else are filters

    let mut matchers = Vec::with_capacity(4);
    while let Ok(arg) = args.next_str() {
        let selector = parse_series_selector(arg)?;
        matchers.push(selector);
    }

    if matchers.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    Ok(MatchFilterOptions {
        date_range,
        matchers,
        limit: None,
    })
}

pub const DEFAULT_STATS_RESULTS_LIMIT: usize = 10;
pub const MAX_STATS_RESULTS_LIMIT: usize = 1000;

pub(super) fn parse_stats_command_args(args: &mut CommandArgIterator) -> ValkeyResult<usize> {
    let limit = if args.peek().is_none() {
        DEFAULT_STATS_RESULTS_LIMIT // No args, use default limit
    } else {
        parse_next_token(args, Some(&[CommandArgToken::Limit]))?;
        let next = args
            .next_str()
            .map_err(|_| ValkeyError::Str(error_consts::MISSING_LIMIT_VALUE))?;
        parse_limit_value(next)?.unwrap_or(DEFAULT_STATS_RESULTS_LIMIT)
    };

    Ok(limit)
}

fn parse_limit_value(val: &str) -> ValkeyResult<Option<usize>> {
    let limit = val
        .parse::<i64>()
        .map_err(|_e| ValkeyError::Str(error_consts::INVALID_LIMIT_VALUE))?;
    if limit < 1 {
        return Err(ValkeyError::Str("TSDB: LIMIT must be greater than 0"));
    }
    if limit > MAX_STATS_RESULTS_LIMIT as i64 {
        let msg = format!("TSDB: limit cannot be greater than {MAX_STATS_RESULTS_LIMIT}");
        return Err(ValkeyError::String(msg));
    }
    Ok(Some(limit as usize))
}

pub(super) fn find_last_token_instance(
    args: &[ValkeyString],
    cmd_tokens: &[CommandArgToken],
) -> Option<(CommandArgToken, usize)> {
    let mut i = args.len() - 1;
    for arg in args.iter().rev() {
        if let Some(token) = parse_command_arg_token(arg)
            && cmd_tokens.contains(&token)
        {
            return Some((token, i));
        }
        i -= 1;
    }
    None
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

    #[test]
    fn test_parse_chunk_size_valid_chunk_sizes() {
        // Test valid minimum size
        assert_eq!(
            parse_chunk_size(&MIN_CHUNK_SIZE.to_string()).unwrap(),
            MIN_CHUNK_SIZE
        );

        // Test valid maximum size
        assert_eq!(
            parse_chunk_size(&MAX_CHUNK_SIZE.to_string()).unwrap(),
            MAX_CHUNK_SIZE
        );

        // Test some typical values (multiples of 8)
        assert_eq!(parse_chunk_size("1024").unwrap(), 1024);
        assert_eq!(parse_chunk_size("4096").unwrap(), 4096);
        assert_eq!(parse_chunk_size("8192").unwrap(), 8192);
    }

    #[test]
    fn test_parse_chunk_size_valid_chunk_sizes_with_units() {
        // Test with units
        assert_eq!(parse_chunk_size("1kb").unwrap(), 1000);
        assert_eq!(parse_chunk_size("2Ki").unwrap(), 2048);
        assert_eq!(parse_chunk_size("1mb").unwrap(), 1000 * 1000);
        assert_eq!(parse_chunk_size("0.5Mi").unwrap(), 524288); // 0.5 * 1024 * 1024
    }

    #[test]
    fn test_parse_chunk_size_invalid_non_integer_chunk_sizes() {
        // Test non-integer values
        assert!(parse_chunk_size("123.5").is_err());
        assert!(parse_chunk_size("1.7kb").is_err());
        assert!(parse_chunk_size("invalid").is_err());
    }

    #[test]
    fn test_parse_chunk_size_invalid_chunk_sizes_below_minimum() {
        // Test values below minimum
        let too_small = MIN_CHUNK_SIZE - 8;
        assert!(parse_chunk_size(&too_small.to_string()).is_err());
        assert!(parse_chunk_size("8").is_err()); // Very small value
    }

    #[test]
    fn test_parse_chunk_size_invalid_chunk_sizes_above_maximum() {
        // Test values above maximum
        let too_large = MAX_CHUNK_SIZE + 8;
        assert!(parse_chunk_size(&too_large.to_string()).is_err());
        assert!(parse_chunk_size("1tb").is_err()); // Very large value
    }

    #[test]
    fn test_parse_chunk_size_invalid_non_multiple_of_eight() {
        // Test values are not multiple of 8
        assert!(parse_chunk_size("1025").is_err());
        assert!(parse_chunk_size("4097").is_err());
        assert!(parse_chunk_size("1023").is_err());
    }

    #[test]
    fn test_parse_chunk_size_invalid_format() {
        // Test invalid formatting
        assert!(parse_chunk_size("abc").is_err());
        assert!(parse_chunk_size("").is_err());
        assert!(parse_chunk_size("-1024").is_err());
        assert!(parse_chunk_size("1024kb!").is_err());
        assert!(parse_chunk_size("1024 kb").is_err());
    }

    #[test]
    fn test_parse_chunk_size_edge_cases() {
        // Exactly at the boundaries of multiples of 8
        let valid_near_min = MIN_CHUNK_SIZE;
        let invalid_near_min = MIN_CHUNK_SIZE + 4;

        assert_eq!(
            parse_chunk_size(&valid_near_min.to_string()).unwrap(),
            valid_near_min
        );
        assert!(parse_chunk_size(&invalid_near_min.to_string()).is_err());

        // Test a value that's very close to max but valid
        let valid_near_max = MAX_CHUNK_SIZE - 8;
        assert_eq!(
            parse_chunk_size(&valid_near_max.to_string()).unwrap(),
            valid_near_max
        );
    }
}
