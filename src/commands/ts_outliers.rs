use crate::analysis::outliers::{
    Anomaly, AnomalyDetectionMethodOptions, AnomalyDirection, AnomalyMethod, AnomalyOptions,
    AnomalyResult, ESDOutlierOptions, MADAnomalyOptions, MethodInfo, RCFOptions, RCFThreshold,
    SmoothedZScoreOptions, detect_anomalies,
};
use crate::analysis::seasonality::Seasonality;
use crate::commands::{
    CommandArgIterator, CommandArgToken, parse_command_arg_token, parse_duration_ms,
    parse_timestamp_range,
};
use crate::common::Sample;
use crate::common::hash::IntSet;
use crate::common::threads::spawn;
use crate::error_consts;
use crate::series::{TimestampRange, get_timeseries};
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    AclPermissions, Context, MODULE_CONTEXT, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

const MAX_SEASONALITY_PERIODS: usize = 4;

static COMMAND_OPTIONS: [CommandArgToken; 4] = [
    CommandArgToken::Direction,
    CommandArgToken::Output,
    CommandArgToken::Method,
    CommandArgToken::Seasonality,
];

enum OutputFormat {
    Full,
    Simple,
    Cleaned,
}

#[derive(PartialEq, Eq)]
enum ZScoreType {
    Standard,
    Modified,
    Smoothed,
}

/// TS.OUTLIERS key fromTimestamp toTimestamp
///     METHOD <method> [method-specific-options]
///     [OUTPUT <full|simple|cleaned>]
///     [DIRECTION <positive|negative|both>]
///     [SEASONALITY <period1> [period2] ...]
pub fn ts_outliers_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_string()?;
    // Parse timestamps
    let date_range = parse_timestamp_range(&mut args)?;

    let mut anomaly_direction = AnomalyDirection::Both;
    let mut output_format = OutputFormat::Simple;
    let mut options: Option<AnomalyOptions> = None;
    let mut seasonality: Option<Seasonality> = None;

    while let Some(arg) = args.next() {
        let token = parse_command_arg_token(arg.as_slice()).unwrap_or_default();
        match token {
            CommandArgToken::Output => {
                let format_str = args.next_str()?;
                output_format = parse_output_format(format_str)?;
            }
            CommandArgToken::Direction => {
                let dir_str = args.next_str()?;
                anomaly_direction = dir_str.parse()?;
            }
            CommandArgToken::Seasonality => {
                seasonality = Some(parse_seasonality(&mut args)?);
            }
            CommandArgToken::Method => {
                if options.is_some() {
                    return Err(ValkeyError::Str("TSDB: outliers METHOD already specified"));
                }

                let method: AnomalyMethod = args.next_str()?.parse()?;
                options = Some(parse_method_options(method, &mut args)?);
            }
            _ => {
                return Err(ValkeyError::String(format!(
                    "TSDB: unknown option OUTLIERS {arg}"
                )));
            }
        }
    }

    let mut options = options.unwrap_or_default();
    options.seasonality = seasonality;

    process_request(
        ctx,
        key,
        date_range,
        options,
        anomaly_direction,
        output_format,
    )
}

fn process_request(
    ctx: &Context,
    key: String,
    date_range: TimestampRange,
    options: AnomalyOptions,
    anomaly_direction: AnomalyDirection,
    output_format: OutputFormat,
) -> ValkeyResult {
    let blocked_client = ctx.block_client();
    spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);

        // Acquire module context and fetch samples inside a small scope so we don't hold the lock
        // longer than necessary.
        let samples_res = {
            let ctx = MODULE_CONTEXT.lock();
            let key = ctx.create_string(key.as_bytes());
            match get_timeseries(&ctx, &key, Some(AclPermissions::ACCESS), false) {
                Ok(Some(series)) => {
                    let (start, end) = date_range.get_series_range(&series, None, false);
                    Ok(series.get_range(start, end))
                }
                Ok(None) => Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
                Err(e) => Err(e),
            }
        };

        match samples_res {
            Err(e) => {
                thread_ctx.reply(Err(e));
            }
            Ok(samples) => {
                let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
                match detect_anomalies(&values, options) {
                    Err(err) => {
                        thread_ctx.reply(Err(ValkeyError::String(format!(
                            "TSDB: outlier detection failed: {err}"
                        ))));
                    }
                    Ok(result) => {
                        let reply =
                            format_output(result, samples, anomaly_direction, output_format);
                        thread_ctx.reply(reply);
                    }
                }
            }
        }
    });

    // Reply will be sent from the background thread
    Ok(ValkeyValue::NoReply)
}

fn parse_method_options(
    method: AnomalyMethod,
    args: &mut CommandArgIterator,
) -> ValkeyResult<AnomalyOptions> {
    match method {
        AnomalyMethod::Ewma => parse_ewma_options(args),
        AnomalyMethod::Cusum => {
            // Cusum has no additional options
            Ok(AnomalyOptions {
                options: AnomalyDetectionMethodOptions::Cusum,
                ..Default::default()
            })
        }
        AnomalyMethod::ZScore => parse_zscore_options_ex(args),
        AnomalyMethod::ModifiedZScore => parse_modified_zscore_options(args),
        AnomalyMethod::SmoothedZScore => parse_smoothed_zscore_options(args),
        AnomalyMethod::Mad => parse_mad_options(args),
        AnomalyMethod::DoubleMAD => parse_double_mad_options(args),
        AnomalyMethod::InterquartileRange => parse_iqr_options(args),
        AnomalyMethod::RandomCutForest => parse_rcf_options(args),
        AnomalyMethod::Esd => parse_esd_options(args),
    }
}

fn parse_seasonality(args: &mut CommandArgIterator) -> ValkeyResult<Seasonality> {
    if let Some(arg) = args.peek() {
        // check for auto
        if arg.as_slice().eq_ignore_ascii_case(b"auto") {
            args.next(); // consume AUTO
            return Ok(Seasonality::Auto);
        }
    }
    let mut periods: Vec<usize> = Vec::with_capacity(4);

    // loop while the next token is a number
    while let Some(v) = args.peek() {
        if let Ok(value) = v.parse_unsigned_integer() {
            periods.push(value as usize);
            args.next();
            continue;
        }
        break;
    }
    if periods.is_empty() || periods.len() > MAX_SEASONALITY_PERIODS {
        return Err(ValkeyError::Str("TSDB: invalid SEASONALITY periods"));
    }
    // periods should be unique and sorted
    periods.sort_unstable();
    if !periods.windows(2).all(|w| w[0] != w[1]) {
        return Err(ValkeyError::Str("TSDB: SEASONALITY periods must be unique"));
    }

    Ok(Seasonality::Periods(periods))
}

fn parse_output_format(arg: &str) -> ValkeyResult<OutputFormat> {
    match arg.len() {
        4 if arg.eq_ignore_ascii_case("full") => {
            return Ok(OutputFormat::Full);
        }
        6 if arg.eq_ignore_ascii_case("simple") => {
            return Ok(OutputFormat::Simple);
        }
        7 if arg.eq_ignore_ascii_case("cleaned") => {
            return Ok(OutputFormat::Cleaned);
        }
        _ => {}
    }
    Err(ValkeyError::String(format!(
        "TSDB: unknown output format option {arg}"
    )))
}

fn parse_ewma_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    // EWMA [ALPHA <value>]
    let alpha = if let Some(arg) = args.peek() {
        let slice = arg.as_slice();
        if slice.len() == 5 && slice.eq_ignore_ascii_case(b"alpha") {
            args.next(); // consume ALPHA
            Some(parse_single_value(args, "ALPHA")?)
        } else {
            None
        }
    } else {
        None
    };

    let mut options = AnomalyOptions {
        ..Default::default()
    };

    options.options = AnomalyDetectionMethodOptions::Ewma(alpha);

    Ok(options)
}

fn parse_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = parse_optional_threshold_option(args)?;
    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::ZScore(threshold),
        ..Default::default()
    })
}

fn parse_modified_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = parse_optional_threshold_option(args)?;

    let options = AnomalyOptions {
        options: AnomalyDetectionMethodOptions::ModifiedZScore(threshold),
        ..Default::default()
    };

    Ok(options)
}

#[inline]
fn is_command_option(arg: &[u8]) -> bool {
    parse_command_arg_token(arg).is_some_and(|token| COMMAND_OPTIONS.contains(&token))
}

fn parse_zscore_options_ex(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut threshold: Option<f64> = None;
    let mut influence: Option<f64> = None;
    let mut lag: Option<usize> = None;
    let mut zscore_type: Option<ZScoreType> = None;

    if let Some(arg) = args.peek() {
        zscore_type = hashify::tiny_map_ignore_case!(arg.as_slice(),
            "STANDARD" => ZScoreType::Standard,
            "MODIFIED" => ZScoreType::Modified,
            "SMOOTHED" => ZScoreType::Smoothed
        );
        if zscore_type.is_some() {
            args.next();
        }
    }

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "THRESHOLD" => {
                threshold = Some(parse_single_value(args, "THRESHOLD")?);
            },
            "INFLUENCE" => {
                influence = Some(parse_single_value(args, "INFLUENCE")?);
            },
            "LAG" => {
                lag = Some(parse_single_value(args, "LAG")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown zscore option {arg}")));
            }
        );
    }

    if lag.is_some() || influence.is_some() {
        if zscore_type.is_none() {
            zscore_type = Some(ZScoreType::Smoothed);
        } else if zscore_type != Some(ZScoreType::Smoothed) {
            return Err(ValkeyError::String(
                "TSDB: LAG and INFLUENCE options only apply to smoothed z-score".to_string(),
            ));
        }
    }

    let zscore_type = zscore_type.unwrap_or(ZScoreType::Standard);
    let options = match zscore_type {
        ZScoreType::Standard => AnomalyDetectionMethodOptions::ZScore(threshold),
        ZScoreType::Modified => AnomalyDetectionMethodOptions::ModifiedZScore(threshold),
        ZScoreType::Smoothed => {
            let mut smoothed_options = SmoothedZScoreOptions::default();
            if let Some(threshold) = threshold {
                smoothed_options.threshold = threshold;
            }
            if let Some(influence) = influence {
                smoothed_options.influence = influence;
            }
            if let Some(lag) = lag {
                smoothed_options.lag = lag;
            }
            AnomalyDetectionMethodOptions::SmoothedZScore(smoothed_options)
        }
    };

    Ok(AnomalyOptions {
        options,
        ..Default::default()
    })
}

fn parse_smoothed_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut smoothed_options = SmoothedZScoreOptions::default();

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "THRESHOLD" => {
                smoothed_options.threshold = parse_single_value(args, "THRESHOLD")?;
            },
            "INFLUENCE" => {
                smoothed_options.influence = parse_single_value(args, "INFLUENCE")?;
            },
            "LAG" => {
                smoothed_options.lag = parse_single_value(args, "LAG")? as usize;
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown smoothed zscore option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::SmoothedZScore(smoothed_options),
        ..Default::default()
    })
}

// Mad [ESTIMATOR <mad-estimator>] [<value>], e.g. Mad ESTIMATOR HarrellDavis THRESHOLD 3.0
fn parse_mad_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut mad_options = MADAnomalyOptions::default();
    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "ESTIMATOR" => {
                 let estimator_arg = args
                    .next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing Mad estimator type"))?;
                 mad_options.estimator = estimator_arg.parse()?;
            },
            "THRESHOLD" => {
                 mad_options.k = parse_single_value(args, "THRESHOLD")?;
            },
            _ => {
                 return Err(ValkeyError::String(format!("TSDB: unknown Mad option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::Mad(mad_options),
        ..Default::default()
    })
}

/// doubleMAD [ESTIMATOR <mad-estimator>] [THRESHOLD <value>]
/// e.g. doubleMAD ESTIMATOR HarrellDavis THRESHOLD 3.0
fn parse_double_mad_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut double_mad_options = MADAnomalyOptions::default();

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "ESTIMATOR" => {
                let estimator_arg = args
                    .next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing Double Mad estimator type"))?;
                double_mad_options.estimator = estimator_arg.parse()?;
            },
            "THRESHOLD" => {
                double_mad_options.k = parse_single_value(args, "THRESHOLD")?;
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown Double Mad option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::DoubleMAD(double_mad_options),
        ..Default::default()
    })
}

fn parse_iqr_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = parse_optional_threshold_option(args)?;

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::InterQuartileRange(threshold),
        ..Default::default()
    })
}

fn parse_rcf_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut rcf_options = RCFOptions::default();

    while let Some(arg) = args.peek() {
        if is_command_option(arg.as_slice()) {
            break;
        }
        let arg = args.next().unwrap();
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(arg_slice,
            "NUM_TREES" => {
                rcf_options.num_trees = Some(parse_single_value(args, "NUM_TREES")? as usize);
            },
            "SAMPLE_SIZE" => {
                rcf_options.sample_size = Some(parse_single_value(args, "SAMPLE_SIZE")? as usize);
            },
            "THRESHOLD" => {
                let val = parse_single_value(args, "THRESHOLD")?;
                rcf_options.threshold = Some(
                    RCFThreshold::std_dev(val)
                        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?
                );
            },
            "CONTAMINATION" => {
                let val = parse_single_value(args, "CONTAMINATION")?;
                rcf_options.threshold = Some(
                    RCFThreshold::contamination(val)
                        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?
                );
            },
            "DECAY" => {
                rcf_options.time_decay = Some(parse_single_value(args, "DECAY")?);
            },
            "SHINGLE_SIZE" => {
                rcf_options.shingle_size = Some(parse_single_value(args, "SHINGLE_SIZE")? as usize);
            },
            "OUTPUT_AFTER" => {
                rcf_options.output_after = Some(parse_single_value(args, "OUTPUT_AFTER")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown RCF option {arg}")));
            }
        );
    }

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::Rcf(rcf_options),
        ..Default::default()
    })
}

fn parse_esd_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        ..Default::default()
    };

    let mut esd_options = ESDOutlierOptions::default();

    while let Some(arg) = args.next() {
        let arg_slice = arg.as_slice();

        if is_command_option(arg_slice) {
            break;
        }

        hashify::fnc_map_ignore_case!(arg_slice,
           "ALPHA" => {
                esd_options.alpha = parse_single_value(args, "ALPHA")?;
            },
            "HYBRID" => {
                esd_options.hybrid = true;
            },
            "MAX_OUTLIERS" => {
                esd_options.max_outliers = Some(parse_single_value(args, "MAX_OUTLIERS")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown ESD option {arg}")));
            }
        );
    }

    options.options = AnomalyDetectionMethodOptions::Esd(Option::from(esd_options));

    Ok(options)
}

fn parse_optional_threshold_option(args: &mut CommandArgIterator) -> ValkeyResult<Option<f64>> {
    if let Some(arg) = args.peek()
        && arg.eq_ignore_ascii_case(b"threshold")
    {
        let _ = args.next();
        Ok(Some(parse_single_value(args, "THRESHOLD")?))
    } else {
        Ok(None)
    }
}

fn parse_single_option_value(
    iter: &mut CommandArgIterator,
    option_name: &str,
) -> ValkeyResult<f64> {
    if let Some(arg) = iter.next()
        && arg.as_slice().eq_ignore_ascii_case(option_name.as_bytes())
    {
        return parse_single_value(iter, option_name);
    }
    Err(ValkeyError::String(format!(
        "TSDB: Missing or invalid {option_name}"
    )))
}

fn parse_single_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let Ok(value_str) = iter.next_str() else {
        return Err(ValkeyError::String(format!(
            "TSDB: Missing value for {option_name}"
        )));
    };

    value_str.parse().map_err(|_e| {
        ValkeyError::String(format!(
            "TSDB: invalid value for {option_name}: {value_str}"
        ))
    })
}

fn parse_single_duration(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<i64> {
    let Ok(value_str) = iter.next_str() else {
        return Err(ValkeyError::String(format!(
            "TSDB: Missing value for {option_name}"
        )));
    };
    let duration = parse_duration_ms(value_str)?;
    if duration < 0 {
        return Err(ValkeyError::String(format!(
            "TSDB: invalid duration for {option_name}"
        )));
    }
    Ok(duration)
}

fn format_output(
    result: AnomalyResult,
    samples: Vec<Sample>,
    direction: AnomalyDirection,
    output_format: OutputFormat,
) -> ValkeyResult {
    match output_format {
        OutputFormat::Full => format_output_full(result, &samples, direction),
        OutputFormat::Simple => format_output_simple(result, &samples, direction),
        OutputFormat::Cleaned => format_output_cleaned(result, samples, direction),
    }
}

/// Returns anomalies only as a list of tuples (timestamp, value, anomaly_direction, anomaly score)
fn format_output_simple(
    result: AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) -> ValkeyResult {
    Ok(format_anomalies(&result, samples, direction))
}

/// Returns all samples excluding those that are anomalies in the specified direction, as well as anomalies
fn format_output_cleaned(
    result: AnomalyResult,
    samples: Vec<Sample>,
    direction: AnomalyDirection,
) -> ValkeyResult {
    let cleaned_samples = format_cleaned_samples(&samples, &result.anomalies, direction);
    let anomalies = format_anomalies(&result, &samples, direction);
    let result: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::from([
        ("samples".into(), ValkeyValue::Array(cleaned_samples)),
        ("outliers".into(), anomalies),
    ]);
    Ok(ValkeyValue::Map(result))
}

fn format_cleaned_samples(
    samples: &[Sample],
    outliers: &[Anomaly],
    direction: AnomalyDirection,
) -> Vec<ValkeyValue> {
    if outliers.is_empty() {
        return samples
            .iter()
            .map(|sample| sample.into())
            .collect::<Vec<ValkeyValue>>();
    }

    let indices: IntSet<usize> = outliers
        .iter()
        .filter(|anomaly| anomaly.signal.matches_direction(direction))
        .map(|anomaly| anomaly.index)
        .collect();

    samples
        .iter()
        .enumerate()
        .filter(|(index, _x)| !indices.contains(index))
        .map(|(_, sample)| sample.into())
        .collect::<Vec<ValkeyValue>>()
}

/// Returns anomalies only as a list of tuples (timestamp, value, anomaly_direction, score)
fn format_anomalies(
    result: &AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) -> ValkeyValue {
    // Collect only the anomalies
    let anomalies: Vec<ValkeyValue> = result
        .anomalies
        .iter()
        .filter_map(|outlier| {
            if !outlier.signal.matches_direction(direction) {
                return None;
            }
            let sample = samples.get(outlier.index)?;
            let anomaly: ValkeyValue = outlier.signal.into();
            let timestamp = ValkeyValue::Integer(sample.timestamp);
            let sample_value = ValkeyValue::Float(sample.value);
            let anomaly_score = ValkeyValue::Float(outlier.score);
            Some(ValkeyValue::Array(vec![
                timestamp,
                sample_value,
                anomaly,
                anomaly_score,
            ]))
        })
        .collect();

    ValkeyValue::Array(anomalies)
}

fn format_output_full(
    result: AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) -> ValkeyResult {
    let mut res: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();

    // Add method info
    res.insert(
        "method".into(),
        ValkeyValue::SimpleString(format!("{:?}", result.method)),
    );

    // Add the threshold
    res.insert("threshold".into(), ValkeyValue::Float(result.threshold));

    // Add samples
    let sample_values: Vec<ValkeyValue> = samples
        .iter()
        .zip(result.scores.iter())
        .map(|(sample, score)| {
            ValkeyValue::Array(vec![
                ValkeyValue::Integer(sample.timestamp),
                ValkeyValue::Float(sample.value),
                ValkeyValue::Float(*score),
            ])
        })
        .collect();
    res.insert("samples".into(), ValkeyValue::Array(sample_values));

    // Add scores
    let scores: Vec<ValkeyValue> = result
        .scores
        .iter()
        .map(|&x| ValkeyValue::Float(x))
        .collect();
    res.insert("scores".into(), ValkeyValue::Array(scores));

    // Add anomalies
    let anomalies = format_anomalies(&result, samples, direction);

    res.insert("outliers".into(), anomalies);

    // Add method-specific info if available
    if let Some(method_info) = result.method_info {
        match method_info {
            MethodInfo::Fenced {
                lower_fence,
                upper_fence,
                center_line,
            } => {
                let mut fenced_info = HashMap::new();
                fenced_info.insert("lower_fence".into(), ValkeyValue::Float(lower_fence));
                fenced_info.insert("upper_fence".into(), ValkeyValue::Float(upper_fence));
                if let Some(center) = center_line {
                    fenced_info.insert("center_line".into(), ValkeyValue::Float(center));
                }
                res.insert("method_info".into(), ValkeyValue::Map(fenced_info));
            }
            MethodInfo::Spc {
                control_limits,
                center_line,
            } => {
                let mut spc_info = HashMap::new();
                spc_info.insert(
                    "control_limits".into(),
                    ValkeyValue::Array(vec![
                        ValkeyValue::Float(control_limits.0),
                        ValkeyValue::Float(control_limits.1),
                    ]),
                );
                spc_info.insert("center_line".into(), ValkeyValue::Float(center_line));
                res.insert("method_info".into(), ValkeyValue::Map(spc_info));
            }
        }
    }

    Ok(ValkeyValue::Map(res))
}
