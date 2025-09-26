// In src/commands/anomaly.rs
use crate::anomaly::anomalies::{detect_anomalies, AnomalyDirection, AnomalyDirectionResult, AnomalyMethod, AnomalyOptions, AnomalyResult, DistanceMetric, MethodInfo, SPCMethod};
use crate::commands::{parse_timestamp_range, CommandArgIterator};
use crate::common::hash::IntSet;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::get_timeseries;
use crate::series::range_utils::get_range;
use crate::series::request_types::RangeOptions;
use ahash::HashSetExt;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};
use std::collections::{HashMap, HashSet};

/// TS.ANOMALY key fromTimestamp toTimestamp [REMOVE] [DIRECTION <above|below|both>] <method> [method-specific-options]
pub fn anomaly(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    // Parse timestamps
    let date_range = parse_timestamp_range(&mut args)?;

    let possible_remove = args.next_str()?.to_lowercase();
    let mut remove = false;
    let mut anomaly_direction = AnomalyDirection::Both;

    let method_str = if possible_remove == "remove" {
        // We will handle removal after detection
        remove = true;
        args.next_str()?
    } else if possible_remove == "direction" {
        anomaly_direction = args.next_str()?.parse()?;
        args.next_str()?
    } else {
        &possible_remove
    };

    // Parse the method and dispatch to the appropriate sub-command handler
    let method: AnomalyMethod = method_str.parse()?;

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

    // Perform anomaly detection
    let result = detect_anomalies(&values, &options)
        .map_err(|e| ValkeyError::String(format!("TSDB: anomaly detection failed: {e}")))?;

    // Format result based on the requested format
    format_anomaly_result(result, samples, anomaly_direction, remove)
}

fn remove_anomalies(samples: &mut Vec<Sample>, result: &AnomalyResult, direction: AnomalyDirection) -> ValkeyResult<()> {
    let mut timestamps: IntSet<Timestamp> = IntSet::with_capacity(16); // need better estimate?

    for (&anomaly, sample) in
        result.anomalies.iter().zip(samples.iter()) {
        if anomaly.matches_direction(direction) {
            timestamps.insert(sample.timestamp);
        }
    }
    samples.retain(|s| !timestamps.contains(&s.timestamp));
    Ok(())
}

fn parse_method_options(method: AnomalyMethod, args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    match method {
        AnomalyMethod::StatisticalProcessControl => parse_spc_options(args),
        AnomalyMethod::ZScore => parse_zscore_options(args),
        AnomalyMethod::ModifiedZScore => parse_modified_zscore_options(args),
        AnomalyMethod::InterquartileRange => parse_iqr_options(args),
        AnomalyMethod::IsolationForest => parse_isolation_forest_options(args),
        AnomalyMethod::DistanceBased => parse_distance_based_options(args),
        AnomalyMethod::OneClassSVM => parse_one_class_svm_options(args),
        AnomalyMethod::PredictionBased => parse_prediction_based_options(args),
    }
}

// Sub-command parsers for each anomaly detection method
fn parse_spc_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::StatisticalProcessControl,
        ..Default::default()
    };

    let mut iter = args.into_iter().peekable();
    while let Some(arg) = iter .next() {
        hashify::fnc_map_ignore_case!(arg.as_slice(),
            "SHEWHART" => {
                options.spc_method = SPCMethod::Shewhart;
            },
            "Cusum" => {
                options.spc_method = SPCMethod::Cusum;
            },
            "Ewma" => {
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
                return Err(ValkeyError::String(format!("TSDB: Unknown option {arg}")));
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
                return Err(ValkeyError::String(format!("TSDB: Unknown option {arg}")));
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
                return Err(ValkeyError::String(format!("TSDB: Unknown option {arg}")));
            }
        );
    }

    Ok(options)
}

fn parse_one_class_svm_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    // Placeholder - uses distance-based as fallback
    parse_distance_based_options(args)
}

fn parse_prediction_based_options(args: &mut CommandArgIterator) -> ValkeyResult<AnomalyOptions> {
    let mut options = AnomalyOptions {
        method: AnomalyMethod::PredictionBased,
        ..Default::default()
    };

    options.window_size = Some(parse_single_option_value(args, "WINDOW_SIZE")? as usize);
    Ok(options)
}

fn parse_single_option_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    if let Some(arg) = iter.next() {
        if arg.as_slice().eq_ignore_ascii_case(option_name.as_bytes()) {
            return parse_single_value(iter, option_name);
        }
    }
    Err(ValkeyError::String(format!("TSDB: Missing or invalid {option_name}")))
}

fn parse_single_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let Some(value_str) = iter.next() else {
        return Err(ValkeyError::String(format!("TSDB: Missing value for {option_name}")));
    };
    value_str.parse_float()
        .map_err(|_| ValkeyError::String(format!("TSDB: Invalid value for {option_name}")))
}


fn format_anomaly_result(result: AnomalyResult, samples: Vec<Sample>, direction: AnomalyDirection, remove: bool) -> ValkeyResult {
    let mut res: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::new();

    // Add method info
    res.insert("method".into(), ValkeyValue::SimpleString(format!("{:?}", result.method)));

    // Add the threshold
    res.insert("threshold".into(), ValkeyValue::Float(result.threshold));

    let mut scores = result.scores;
    let mut anomalies = result.anomalies;
    let mut samples = samples;

    // If remove is set, filter out anomalies from scores and anomalies
    if remove {
        let mut indices_to_remove: Vec<usize> = Vec::new();
        let mut timestamps: HashSet<i64> = HashSet::with_capacity(16); // need better estimate?
        // Find the indices of the anomalies to remove
        for (i, (&anomaly, sample)) in anomalies.iter().zip(samples.iter()).enumerate() {
            if anomaly.matches_direction(direction) {
                timestamps.insert(sample.timestamp);
            } else {
                indices_to_remove.push(i);
            }   
        }

        samples.retain(|s| !timestamps.contains(&s.timestamp));
        // Remove in reverse order to maintain correct indices
        for &index in indices_to_remove.iter().rev() {
            scores.remove(index);
            anomalies.remove(index);
        }
    }

    // Add cleaned samples
    let sample_values: Vec<ValkeyValue> = samples.iter().map(|s| s.into()).collect();
    res.insert("samples".into(), ValkeyValue::Array(sample_values));

    // Add scores
    let scores: Vec<ValkeyValue> = scores
        .into_iter()
        .map(ValkeyValue::Float)
        .collect();
    res.insert("scores".into(), ValkeyValue::Array(scores));

    // Add anomaly indicators
    let anomalies: Vec<ValkeyValue> = anomalies
        .into_iter()
        .map(|dir| {
            let dir_as_int = match dir {
                AnomalyDirectionResult::Above => 1,
                AnomalyDirectionResult::Below => -1,
                AnomalyDirectionResult::None => 0,
            };
            ValkeyValue::Integer(dir_as_int)
        })
        .collect();
    res.insert("anomalies".into(), ValkeyValue::Array(anomalies.clone()));

    // Add method-specific info if available
    if let Some(method_info) = result.method_info {
        match method_info {
            MethodInfo::Spc { control_limits, center_line } => {
                let mut spc_info = HashMap::new();
                spc_info.insert("control_limits".into(), ValkeyValue::Array(vec![
                    ValkeyValue::Float(control_limits.0),
                    ValkeyValue::Float(control_limits.1),
                ]));
                spc_info.insert("center_line".into(), ValkeyValue::Float(center_line));
                res.insert("method_info".into(), ValkeyValue::Map(spc_info));
            },
            MethodInfo::IsolationForest { average_path_length } => {
                let mut if_info = HashMap::new();
                if_info.insert("average_path_length".into(), ValkeyValue::Float(average_path_length));
                res.insert("method_info".into(), ValkeyValue::Map(if_info));
            },
            MethodInfo::DistanceBased { distances } => {
                let mut db_info = HashMap::new();
                db_info.insert("distances".into(), ValkeyValue::Array(distances.into_iter().map(ValkeyValue::Float).collect()));
                res.insert("method_info".into(), ValkeyValue::Map(db_info));
            },
        }
    }

    Ok(ValkeyValue::Map(res))
}