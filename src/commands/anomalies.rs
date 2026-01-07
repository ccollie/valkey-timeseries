// In src/commands/analysis.rs
use crate::analysis::outliers::{
    AnomalyDirection, AnomalyMADEstimator, AnomalyMethod, AnomalyOptions, AnomalyResult,
    AnomalySignal, DistanceMetric, MethodInfo, SPCMethod, SmoothedZScoreOptions, detect_anomalies,
};
use crate::commands::{CommandArgIterator, parse_timestamp_range};
use crate::common::Sample;
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
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    // Parse timestamps
    let date_range = parse_timestamp_range(&mut args)?;

    let mut anomaly_direction = AnomalyDirection::Both;
    let mut method = AnomalyMethod::SmoothedZScore;
    let mut output_format = OutputFormat::Simple;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
            "DIRECTION" => {
                let dir_str = args.next_str()?;
                anomaly_direction = dir_str.parse()?;
            },
            "FORMAT" => {
                let format_str = args.next_str()?;
                output_format = parse_output_format(format_str)?;
            },
            "METHOD" => {
                let method_str = args.next_str()?;
                method = method_str.parse()?;
                break;
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown option {arg}")));
            }
        );
    }

    let options = parse_method_options(method, &mut args)?;

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
        .map_err(|e| ValkeyError::String(format!("TSDB: analysis detection failed: {e}")))?;

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
        AnomalyMethod::DoubleMAD => parse_double_mad_options(args),
        AnomalyMethod::InterquartileRange => parse_iqr_options(args),
        AnomalyMethod::IsolationForest => parse_isolation_forest_options(args),
        AnomalyMethod::DistanceBased => parse_distance_based_options(args),
        AnomalyMethod::PredictionBased => parse_prediction_based_options(args),
        AnomalyMethod::RandomCutForest => todo!(),
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
        method: AnomalyMethod::StatisticalProcessControl,
        ..Default::default()
    };

    let mut iter = args.into_iter().peekable();
    while let Some(arg) = iter.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
            "SHEWHART" => {
                options.spc_method = SPCMethod::Shewhart;
            },
            "CUSUM" => {
                options.spc_method = SPCMethod::Cusum;
            },
            "EWMA" => {
                options.spc_method = SPCMethod::Ewma;
                // Parse alpha parameter
                if let Some(alpha_arg) = iter.peek() {
                    options.ewma_alpha = alpha_arg.parse_float()?;
                    iter.next();
                }
            },
            "THRESHOLD" => {
                let arg = iter.next().ok_or(ValkeyError::Str("TSDB: Missing THRESHOLD value"))?;
                let threshold = arg.parse_float()
                    .map_err(|_| ValkeyError::Str("TSDB: Invalid THRESHOLD value"))?;
                options.threshold = Some(threshold);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown option {arg}")));
            }
        );
    }

    Ok(options)
}

fn parse_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::ZScore,
        threshold: Some(3.0), // Default Z-score threshold
        ..Default::default()
    };

    if args.len() == 0 {
        return Ok(options);
    }

    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }
    let iter = args.into_iter();
    options.threshold = parse_single_option_value(iter, "threshold").map(Some)?;

    Ok(options)
}

fn parse_modified_zscore_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::ModifiedZScore,
        threshold: Some(3.5), // Default modified Z-score threshold
        ..Default::default()
    };

    if args.peek().is_none() {
        return Ok(options);
    }

    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let iter = args.into_iter();
    options.threshold = parse_single_option_value(iter, "threshold").map(Some)?;

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
        method: AnomalyMethod::SmoothedZScore,
        threshold: Some(3.5), // Default modified Z-score threshold
        smoothed_zscore_options: Some(smoothed_options),
        ..Default::default()
    };
    Ok(options)
}

// doubleMAD [<mad-estimator>] [<value>], e.g. doubleMAD HarrellDavis 3.0
fn parse_double_mad_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::DoubleMAD,
        threshold: Some(3.0), // Default threshold
        mad_estimator: Some(AnomalyMADEstimator::Simple),
        ..Default::default()
    };

    if args.peek().is_none() {
        return Ok(options);
    }

    // [estimator][threshold]
    // get estimator type
    let estimator_arg = args
        .next_str()
        .map_err(|_| ValkeyError::Str("TSDB: Missing double MAD estimator type"))?;
    options.mad_estimator = Some(estimator_arg.parse()?);

    if args.peek().is_none() {
        return Ok(options);
    }

    options.threshold = parse_single_option_value(args, "threshold").map(Some)?;

    Ok(options)
}

fn parse_iqr_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::InterquartileRange,
        threshold: Some(1.5), // Default IQR multiplier
        ..Default::default()
    };

    options.threshold = parse_single_option_value(args, "multiplier").map(Some)?;

    Ok(options)
}

fn parse_isolation_forest_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::IsolationForest,
        contamination: 0.1,
        n_trees: 100,
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
           "NTREES" => {
                options.n_trees = parse_single_value(args, "NTREES")? as usize;
            },
            "CONTAMINATION" => {
                options.contamination = parse_single_value(args, "CONTAMINATION")?;
            },
            "WINDOW_SIZE" => {
                options.window_size = Some(parse_single_value(args, "WINDOW_SIZE")? as usize);
            },
            "SUBSAMPLE_SIZE" => {
                options.subsample_size = Some(parse_single_value(args, "SUBSAMPLE_SIZE")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown option {arg}")));
            }
        );
    }

    Ok(options)
}

fn parse_distance_based_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::DistanceBased,
        contamination: 0.1,
        k_neighbors: 5,
        distance_metric: DistanceMetric::Euclidean,
        ..Default::default()
    };

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
            "K_NEIGHBORS" => {
                options.k_neighbors = parse_single_value(args, "K_NEIGHBORS")? as usize;
            },
            "CONTAMINATION" => {
                options.contamination = parse_single_value(args, "CONTAMINATION")?;
            },
            "DISTANCE_METRIC" => {
                let Some(metric_val) = args.next() else {
                    return Err(ValkeyError::Str("TSDB: Missing DISTANCE_METRIC value"));
                };
                options.distance_metric = (&metric_val).try_into()?;
            },
            "WINDOW_SIZE" => {
                options.window_size = Some(parse_single_value(args, "WINDOW_SIZE")? as usize);
            },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: unknown option {arg}")));
            }
        );
    }

    Ok(options)
}

fn parse_prediction_based_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::PredictionBased,
        ..Default::default()
    };

    options.window_size = Some(parse_single_option_value(args, "WINDOW_SIZE")? as usize);
    Ok(options)
}

fn parse_single_option_value(
    iter: &mut CommandArgIterator,
    option_name: &str,
) -> ValkeyResult<f64> {
    if let Some(arg) = iter.next() {
        if arg.as_slice().eq_ignore_ascii_case(option_name.as_bytes()) {
            return parse_single_value(iter, option_name);
        }
    }
    Err(ValkeyError::String(format!(
        "TSDB: Missing or invalid {option_name}"
    )))
}

fn parse_single_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let Some(value_str) = iter.next() else {
        return Err(ValkeyError::String(format!(
            "TSDB: Missing value for {option_name}"
        )));
    };
    value_str
        .parse_float()
        .map_err(|_| ValkeyError::String(format!("TSDB: invalid value for {option_name}")))
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
    Ok(format_anomalies(result, samples, direction))
}

/// Returns all samples excluding those that are anomalies in the specified direction, as well anomalies
fn format_output_cleaned(
    result: AnomalyResult,
    samples: Vec<Sample>,
    direction: AnomalyDirection,
) -> ValkeyResult {
    let cleaned_samples = format_cleaned_samples(&samples, &result.anomalies, direction);
    let anomalies = format_anomalies(result, &samples, direction);
    let result: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::from([
        ("samples".into(), ValkeyValue::Array(cleaned_samples)),
        ("anomalies".into(), anomalies),
    ]);
    Ok(ValkeyValue::Map(result))
}

/// Returns anomalies only as a list of tuples (timestamp, value, anomaly_direction, score)
fn format_anomalies(
    result: AnomalyResult,
    samples: &[Sample],
    direction: AnomalyDirection,
) -> ValkeyValue {
    // Collect only the anomalies
    let anomalies: Vec<ValkeyValue> = result
        .anomalies
        .into_iter()
        .zip(samples.iter())
        .zip(result.scores)
        .filter(|((signal, _), _)| signal.matches_direction(direction))
        .map(|((signal, sample), score)| {
            let anomaly: ValkeyValue = signal.into();
            let timestamp = ValkeyValue::Integer(sample.timestamp);
            let sample_value = ValkeyValue::Float(sample.value);
            let anomaly_score = ValkeyValue::Float(score);
            ValkeyValue::Array(vec![timestamp, sample_value, anomaly, anomaly_score])
        })
        .collect();

    ValkeyValue::Array(anomalies)
}

fn format_cleaned_samples(
    samples: &[Sample],
    anomalies: &[AnomalySignal],
    direction: AnomalyDirection,
) -> Vec<ValkeyValue> {
    // filter out samples that are anomalies in the specified direction
    samples
        .iter()
        .zip(anomalies.iter())
        .filter(|(_, dir)| !dir.matches_direction(direction))
        .map(|(s, _)| s.into())
        .collect()
}

fn format_output_full(
    result: AnomalyResult,
    samples: &[Sample],
    _direction: AnomalyDirection,
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
    let sample_values: Vec<ValkeyValue> = samples.iter().map(|s| s.into()).collect();
    res.insert("samples".into(), ValkeyValue::Array(sample_values));

    // Add scores
    let scores: Vec<ValkeyValue> = result.scores.into_iter().map(ValkeyValue::Float).collect();
    res.insert("scores".into(), ValkeyValue::Array(scores));

    // Add anomalies
    let anomalies: Vec<ValkeyValue> = result
        .anomalies
        .into_iter()
        .map(|signal| signal.into())
        .collect();

    res.insert("anomalies".into(), ValkeyValue::Array(anomalies.clone()));

    // Add method-specific info if available
    if let Some(method_info) = result.method_info {
        match method_info {
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
            MethodInfo::IsolationForest {
                average_path_length,
            } => {
                let mut if_info = HashMap::new();
                if_info.insert(
                    "average_path_length".into(),
                    ValkeyValue::Float(average_path_length),
                );
                res.insert("method_info".into(), ValkeyValue::Map(if_info));
            }
            MethodInfo::DistanceBased { distances } => {
                let mut db_info = HashMap::new();
                db_info.insert(
                    "distances".into(),
                    ValkeyValue::Array(distances.into_iter().map(ValkeyValue::Float).collect()),
                );
                res.insert("method_info".into(), ValkeyValue::Map(db_info));
            }
        }
    }

    Ok(ValkeyValue::Map(res))
}
