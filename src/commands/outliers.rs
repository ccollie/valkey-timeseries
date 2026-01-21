use crate::analysis::outliers::{
    Anomaly, AnomalyDetectionMethodOptions, AnomalyDirection, AnomalyMethod, AnomalyOptions,
    AnomalyResult, MADAnomalyOptions, MethodInfo, RCFOptions, SPCMethod, SPCMethodOptions,
    SmoothedZScoreOptions, detect_anomalies,
};
use crate::commands::{CommandArgIterator, parse_duration_ms, parse_timestamp_range};
use crate::common::Sample;
use crate::common::hash::IntSet;
use crate::error_consts;
use crate::series::get_timeseries;
use crate::series::range_utils::get_range;
use crate::series::request_types::RangeOptions;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

enum OutputFormat {
    Full,
    Simple,
    Cleaned,
}

/// TS.OUTLIERS key fromTimestamp toTimestamp
/// [FORMAT <full|simple|cleaned>]
/// [DIRECTION <positive|negative|both>]
/// METHOD <method> [method-specific-options]
pub fn outliers(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    // Parse timestamps
    let date_range = parse_timestamp_range(&mut args)?;

    let mut anomaly_direction = AnomalyDirection::Both;
    let mut output_format = OutputFormat::Simple;
    let mut options: Option<AnomalyOptions> = None;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
            "FORMAT" => {
                let format_str = args.next_str()?;
                output_format = parse_output_format(format_str)?;
            },
            "DIRECTION" => {
                let dir_str = args.next_str()?;
                anomaly_direction = dir_str.parse()?;
            },
            "METHOD" => {
                if options.is_some() {
                    return Err(ValkeyError::Str("TSDB: outliers METHOD already specified"));
                }

                // see if we have an spc method
                if let Some(peek) = args.peek() {
                    let candidate = peek.as_slice();
                    if SPCMethod::try_from(candidate).is_ok() {
                        options = Some(parse_spc_options(&mut args)?);
                        continue;
                    }
                }

                let method: AnomalyMethod = args.next_str()?.parse()?;
                options = Some(parse_method_options(method, &mut args)?);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown option OUTLIERS {arg}")));
            }
        );
    }

    let options = options.unwrap_or_default();

    let Some(series) = get_timeseries(ctx, key, Some(AclPermissions::ACCESS), true)? else {
        return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
    };

    // Get the time series data for the specified range
    let range_options = RangeOptions {
        date_range,
        ..Default::default()
    };

    let samples = get_range(&series, &range_options, true);
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Perform analysis detection
    let result = detect_anomalies(&values, &options)
        .map_err(|e| ValkeyError::String(format!("TSDB: outlier detection failed: {e}")))?;

    // Format result based on the requested format
    format_output(result, samples, anomaly_direction, output_format)
}

fn parse_method_options(
    method: AnomalyMethod,
    args: &mut CommandArgIterator,
) -> ValkeyResult<AnomalyOptions> {
    match method {
        AnomalyMethod::StatisticalProcessControl => parse_spc_options(args),
        AnomalyMethod::ZScore => parse_zscore_options(args),
        AnomalyMethod::ModifiedZScore => parse_modified_zscore_options(args),
        AnomalyMethod::SmoothedZScore => parse_smoothed_zscore_options(args),
        AnomalyMethod::Mad => parse_mad_options(args),
        AnomalyMethod::DoubleMAD => parse_double_mad_options(args),
        AnomalyMethod::InterquartileRange => parse_iqr_options(args),
        AnomalyMethod::RandomCutForest => parse_rcf_options(args),
    }
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

// Sub-command parsers for each analysis detection method
fn parse_spc_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        ..Default::default()
    };

    let Some(arg) = args.next() else {
        return Ok(options);
    };

    let spc_method: SPCMethod = arg.as_slice().try_into()?;

    if spc_method == SPCMethod::Ewma {
        if args.peek().is_some() {
            args.next(); // consume ALPHA
            let ewma_alpha = parse_single_value(args, "ALPHA")?;
            options.options = AnomalyDetectionMethodOptions::Spc(SPCMethodOptions {
                spc_method,
                ewma_alpha: Some(ewma_alpha),
            });
        } else {
            options.options = AnomalyDetectionMethodOptions::Spc(SPCMethodOptions {
                spc_method,
                ewma_alpha: None,
            });
        }
    }
    args.done()?;

    Ok(options)
}

fn parse_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = if args.peek().is_some() {
        // Expect: THRESHOLD <value>
        parse_single_option_value(args, "THRESHOLD").map(Some)?
    } else {
        None
    };
    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::ZScore(threshold),
        ..Default::default()
    })
}

fn parse_modified_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let threshold = if args.peek().is_some() {
        // Expect: THRESHOLD <value>
        parse_single_option_value(args, "THRESHOLD").map(Some)?
    } else {
        None
    };

    let options = AnomalyOptions {
        options: AnomalyDetectionMethodOptions::ModifiedZScore(threshold),
        ..Default::default()
    };

    Ok(options)
}

fn parse_smoothed_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut smoothed_options = SmoothedZScoreOptions::default();

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
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

    let options = AnomalyOptions {
        options: AnomalyDetectionMethodOptions::SmoothedZScore(smoothed_options),
        ..Default::default()
    };

    Ok(options)
}

// Mad [ESTIMATOR <mad-estimator>] [<value>], e.g. Mad ESTIMATOR HarrellDavis THRESHOLD 3.0
fn parse_mad_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut mad_options = MADAnomalyOptions::default();
    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
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
    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
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
    let threshold = if args.peek().is_some() {
        // Expect: THRESHOLD <value>
        Some(parse_single_option_value(args, "THRESHOLD")?)
    } else {
        None
    };

    Ok(AnomalyOptions {
        options: AnomalyDetectionMethodOptions::InterQuartileRange(threshold),
        ..Default::default()
    })
}

fn parse_rcf_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        ..Default::default()
    };

    let mut rcf_options = RCFOptions::default();
    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
           "NUM_TREES" => {
                rcf_options.num_trees = Some(parse_single_value(args, "NUM_TREES")? as usize);
            },
            "SAMPLE_SIZE" => {
                rcf_options.sample_size = Some(parse_single_value(args, "SAMPLE_SIZE")? as usize);
            },
            "THRESHOLD" => {
                rcf_options.threshold = parse_single_value(args, "THRESHOLD")?;
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
                return Err(ValkeyError::String(format!("TSDB: unknown Rcf option {arg}")));
            }
        );
    }

    options.options = AnomalyDetectionMethodOptions::Rcf(rcf_options);

    Ok(options)
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
            } => {
                let mut fenced_info = HashMap::new();
                fenced_info.insert("lower_fence".into(), ValkeyValue::Float(lower_fence));
                fenced_info.insert("upper_fence".into(), ValkeyValue::Float(upper_fence));
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
