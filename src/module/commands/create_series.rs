use crate::error_consts;
use crate::labels::Label;
use crate::module::arg_parse::{
    parse_chunk_compression, parse_chunk_size, parse_command_arg_token,
    parse_decimal_digit_rounding, parse_duplicate_policy, parse_ignore_options,
    parse_label_value_pairs, parse_metric_name, parse_retention, parse_significant_digit_rounding,
    CommandArgIterator, CommandArgToken,
};
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::index::{next_timeseries_id, with_timeseries_index};
use crate::series::{TimeSeries, TimeSeriesOptions};
use smallvec::SmallVec;
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{
    Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK,
};

/// Create a new time series
///
/// TS.CREATE-SERIES key
///   [METRIC metric]
///   [RETENTION retentionPeriod]
///   [ENCODING <pco|gorilla|uncompressed|compressed>]
///   [CHUNK_SIZE chunkSize]
///   [DUPLICATE_POLICY duplicatePolicy]
///   [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///   [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///   [LABELS label1=value1 label2=value2 ...]
pub fn create(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let (parsed_key, options) = parse_create_options(args)?;

    create_and_store_series(ctx, &parsed_key, options)?;

    VALKEY_OK
}

pub fn parse_create_options(
    args: Vec<ValkeyString>,
) -> ValkeyResult<(ValkeyString, TimeSeriesOptions)> {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args
        .next()
        .ok_or(ValkeyError::Str("Err missing key argument"))?;

    let options = parse_series_options(
        &mut args,
        TimeSeriesOptions::default(),
        &[CommandArgToken::OnDuplicate],
    )?;

    // if options.labels.is_empty() {
    //     return Err(ValkeyError::Str(
    //         error_consts::INVALID_OR_MISSING_METRIC_NAME,
    //     ));
    // }

    Ok((key, options))
}

pub fn parse_series_options(
    args: &mut CommandArgIterator,
    options: TimeSeriesOptions,
    invalid_args: &[CommandArgToken],
) -> ValkeyResult<TimeSeriesOptions> {
    let metric_set = false;

    let mut options = options;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        if invalid_args.contains(&token) {
            return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
        }
        match token {
            CommandArgToken::ChunkSize => options.chunk_size = Some(parse_chunk_size(args)?),
            CommandArgToken::Encoding => {
                options.chunk_compression = parse_chunk_compression(args)?;
            }
            CommandArgToken::DecimalDigits => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                let rounding = parse_decimal_digit_rounding(args)?;
                options.rounding = Some(rounding);
            }
            CommandArgToken::DuplicatePolicy => {
                options.sample_duplicate_policy.policy = parse_duplicate_policy(args)?
            }
            CommandArgToken::OnDuplicate => {
                options.on_duplicate = Some(parse_duplicate_policy(args)?);
            }
            CommandArgToken::Metric => {
                if metric_set {
                    return Err(ValkeyError::Str(error_consts::METRIC_ALREADY_SET));
                }
                let metric = args.next_string()?;
                options.labels = parse_metric_name(&metric)?;
            }
            CommandArgToken::Labels => {
                if metric_set {
                    return Err(ValkeyError::Str(error_consts::METRIC_ALREADY_SET));
                }
                options.labels = parse_labels(args, invalid_args)?;
            }
            CommandArgToken::Ignore => {
                let (ignore_max_timediff, ignore_max_val_diff) = parse_ignore_options(args)?;
                if ignore_max_timediff < 0 || ignore_max_val_diff < 0.0 {
                    return Err(ValkeyError::Str(error_consts::INVALID_IGNORE_OPTIONS));
                }
                options.sample_duplicate_policy.max_time_delta = ignore_max_timediff as u64;
                options.sample_duplicate_policy.max_value_delta = ignore_max_val_diff;
            }
            CommandArgToken::Retention => options.retention(parse_retention(args)?),
            CommandArgToken::SignificantDigits => {
                if options.rounding.is_some() {
                    return Err(ValkeyError::Str(error_consts::ROUNDING_ALREADY_SET));
                }
                options.rounding = Some(parse_significant_digit_rounding(args)?);
            }
            _ => {
                return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT));
            }
        };
    }

    Ok(options)
}

const VALID_SERIES_OPTIONS_ARGS: [CommandArgToken; 9] = [
    CommandArgToken::Metric,
    CommandArgToken::Labels,
    CommandArgToken::Retention,
    CommandArgToken::Encoding,
    CommandArgToken::ChunkSize,
    CommandArgToken::Ignore,
    CommandArgToken::DuplicatePolicy,
    CommandArgToken::SignificantDigits,
    CommandArgToken::DecimalDigits,
];

fn parse_labels(
    args: &mut CommandArgIterator,
    invalid_args: &[CommandArgToken],
) -> ValkeyResult<Vec<Label>> {
    let mut stop_tokens: SmallVec<CommandArgToken, 16> = SmallVec::from(VALID_SERIES_OPTIONS_ARGS);
    if !invalid_args.is_empty() {
        stop_tokens.retain(|token| !invalid_args.contains(token));
    }
    let label_map = parse_label_value_pairs(args, &stop_tokens)?;

    let mut labels = Vec::new();
    for (label_name, label_value) in label_map {
        labels.push(Label::new(label_name, label_value));
    }
    Ok(labels)
}

pub(crate) fn create_series(
    key: &ValkeyString,
    options: TimeSeriesOptions,
    ctx: &Context,
) -> ValkeyResult<TimeSeries> {
    let mut ts = TimeSeries::with_options(options)?;
    if ts.id == 0 {
        ts.id = next_timeseries_id();
    }
    with_timeseries_index(ctx, |index| {
        let labels = ts.labels.to_label_vec();
        // will return an error if the series already exists
        let existing_id = index.posting_by_labels(&labels)?;
        if let Some(_id) = existing_id {
            return Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES));
        }

        index.index_timeseries(&ts, key.iter().as_slice());
        Ok(ts)
    })
}

pub(crate) fn create_and_store_series(
    ctx: &Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_KEY));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VK_TIME_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.CREATE", key);
    ctx.log_verbose("series created");

    Ok(())
}
