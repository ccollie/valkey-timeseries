//! Anomaly detection algorithms for time series
//!
//! This module provides various algorithms for detecting anomalies and outliers
//! in time series data, including statistical process control, isolation forest,
//! one-class SVM, distance-based, and prediction-based approaches.

use super::smoothed_zscores::SmoothedZScoreAnomalyDetector;
use crate::analysis::common::Array2D;
use crate::analysis::common::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use crate::analysis::outliers::{DoubleMadOutlierDetector, OutlierDetector};
use crate::analysis::quantile_estimators::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, Samples, SimpleNormalizedEstimator,
};
use rand::prelude::*;
use rand::seq::SliceRandom;
use std::cmp::PartialEq;
use std::fmt::Debug;
use std::str::FromStr;
use krcf::rcf;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::analysis::outliers::rcf_outlier_detector::{RCFOptions, RcfOutlierDetector};

/// Method for analysis detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyMethod {
    /// Statistical process control (Spc)
    StatisticalProcessControl,
    /// Isolation forest for time series
    IsolationForest,
    /// One-class SVM for time series
    DistanceBased,
    /// Prediction-based analysis detection
    PredictionBased,
    /// Z-score based detection
    ZScore,
    /// Modified Z-score using median absolute deviation
    ModifiedZScore,
    /// The Smoothed Z-Score algorithm
    SmoothedZScore,
    /// Double median absolute deviation (MAD) method
    DoubleMAD,
    /// Interquartile range (IQR) method
    InterquartileRange,
    /// Random Cut Forest (RCF) method
    RandomCutForest,
}

impl AnomalyMethod {
    /// Returns a human-readable name for the method
    pub fn name(&self) -> &'static str {
        match self {
            AnomalyMethod::StatisticalProcessControl => "Statistical Process Control",
            AnomalyMethod::IsolationForest => "Isolation Forest",
            AnomalyMethod::DistanceBased => "Distance-Based",
            AnomalyMethod::PredictionBased => "Prediction-Based",
            AnomalyMethod::ZScore => "Z-Score",
            AnomalyMethod::ModifiedZScore => "Modified Z-Score",
            AnomalyMethod::SmoothedZScore => "Smoothed Z-Score",
            AnomalyMethod::DoubleMAD => "Double MAD",
            AnomalyMethod::InterquartileRange => "Interquartile Range (IQR)",
            AnomalyMethod::RandomCutForest => "Random Cut Forest",
        }
    }
}

impl FromStr for AnomalyMethod {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "spc" => Ok(AnomalyMethod::StatisticalProcessControl),
            "isolationforest" => Ok(AnomalyMethod::IsolationForest),
            "isolation-forest" => Ok(AnomalyMethod::IsolationForest),
            "distance" => Ok(AnomalyMethod::DistanceBased),
            "prediction" => Ok(AnomalyMethod::PredictionBased),
            "zscore" => Ok(AnomalyMethod::ZScore),
            "z-score" => Ok(AnomalyMethod::ZScore),
            "modifiedzscore" => Ok(AnomalyMethod::ModifiedZScore),
            "smoothed-zscore" => Ok(AnomalyMethod::SmoothedZScore),
            "smoothedzscore" => Ok(AnomalyMethod::SmoothedZScore),
            "mad" => Ok(AnomalyMethod::ModifiedZScore),
            "doublemad" => Ok(AnomalyMethod::DoubleMAD),
            "interquartilerange" => Ok(AnomalyMethod::InterquartileRange),
            "iqr" => Ok(AnomalyMethod::InterquartileRange),
            "rcf" => Ok(AnomalyMethod::RandomCutForest),
            "randomcutforest" => Ok(AnomalyMethod::RandomCutForest)
        };
        res.unwrap_or(Err(ValkeyError::String(format!(
            "TSDB: unknown analysis detection method: {s}"
        ))))
    }
}

impl TryFrom<&ValkeyString> for AnomalyMethod {
    type Error = ValkeyError;

    fn try_from(s: &ValkeyString) -> ValkeyResult<Self> {
        let str = s.to_string_lossy();
        Self::from_str(&str)
    }
}

/// Statistical process control method
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SPCMethod {
    /// Shewhart control charts
    Shewhart,
    /// Cusum control charts
    Cusum,
    /// Exponentially weighted moving average (Ewma)
    Ewma,
}

impl FromStr for SPCMethod {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "shewhart" => SPCMethod::Shewhart,
            "cusum" => SPCMethod::Cusum,
            "ewma" => SPCMethod::Ewma
        };
        match res {
            Some(method) => Ok(method),
            None => {
                let msg = format!("Unknown Spc method: {s}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

/// Distance metric for distance-based analysis detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean distance
    Euclidean,
    /// Manhattan distance
    Manhattan,
    /// Mahalanobis distance
    Mahalanobis,
    /// Dynamic time warping distance
    Dtw,
}

impl FromStr for DistanceMetric {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "euclidean" => DistanceMetric::Euclidean,
            "manhattan" => DistanceMetric::Manhattan,
            "mahalanobis" => DistanceMetric::Mahalanobis,
            "dtw" => DistanceMetric::Dtw,
            "dynamic-time-warping" => DistanceMetric::Dtw
        };
        match res {
            Some(metric) => Ok(metric),
            None => {
                let msg = format!("Unknown distance metric: {s}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for DistanceMetric {
    type Error = ValkeyError;

    fn try_from(s: &ValkeyString) -> ValkeyResult<Self> {
        let str = s.to_string_lossy();
        Self::from_str(&str)
    }
}

/// Options for configuring the Smoothed Z-Score algorithm.
///
/// The Smoothed Z-Score algorithm is useful for detecting signals, such as anomalies or outliers, in time-series
/// data by comparing new datapoints to a continually adjusted moving average and standard deviation. The following
/// parameters influence its sensitivity and adaptability.
///
/// # Fields
///
/// * `threshold` - The number of standard deviations from the moving mean required to classify a new datapoint as
///   a signal. A larger threshold reduces sensitivity to outliers, while a smaller threshold makes the algorithm
///   more sensitive.
///
/// * `influence` - A value between 0 and 1 that determines how much detected signals influence the dataset's
///   moving mean and standard deviation. A lower influence makes the algorithm less affected by signals, while a
///   higher influence allows signals to have a greater impact.
///
/// * `lag` - The number of previous datapoints used to calculate the moving mean and standard deviation. Higher
///   values result in a smoother long-term average, making the algorithm less responsive to short-term fluctuations
///   but more robust to changes in the long-term trend.
///
/// # Examples
///
/// ```rust
/// let options = SmoothedZScoreOptions {
///     threshold: 3.5,
///     influence: 0.5,
///     lag: 10,
/// };
/// ```
///
/// This example initializes the `SmoothedZScoreOptions` structure with a threshold of 3.5 standard deviations,
/// an influence of 0.5, and a lag of 10, providing a balanced configuration for detecting outliers in moderately
/// stationary data.
///
/// # Notes
///
/// Adjusting these parameters requires an understanding of your dataset's characteristics. For highly
/// non-stationary data, consider decreasing `lag` to improve adaptability. For datasets with frequent
/// noise or minor fluctuations, increasing `threshold` can improve robustness, while tuning `influence`
/// helps control the trade-off between reactivity and noise sensitivity.
#[derive(Clone, Copy, Debug)]
pub struct SmoothedZScoreOptions {
    /// `threshold` is the number of standard deviations from the moving mean above which the algorithm will classify a new
    /// datapoint as being a signal.
    pub threshold: f64,
    /// `influence` is the influence of signals on the algorithm's detection threshold.
    pub influence: f64,
    /// `lag` determines how much your data will be smoothed and how adaptive the algorithm is to change in the long-term
    /// average of the data. The more stationary your data is, the more lags you should include to improve the
    /// robustness of the algorithm.
    pub lag: usize,
}

impl Default for SmoothedZScoreOptions {
    fn default() -> Self {
        Self {
            threshold: 3.5,
            influence: 0.0,
            lag: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyMADEstimator {
    Simple,
    HarrellDavis,
    Invariant,
}

impl AnomalyMADEstimator {
    pub fn alias(&self) -> &'static str {
        match self {
            AnomalyMADEstimator::Simple => "Simple",
            AnomalyMADEstimator::HarrellDavis => "Harrell-Davis",
            AnomalyMADEstimator::Invariant => "Invariant",
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            AnomalyMADEstimator::Simple => "simple",
            AnomalyMADEstimator::HarrellDavis => "harrelldavis",
            AnomalyMADEstimator::Invariant => "invariant",
        }
    }
}

impl FromStr for AnomalyMADEstimator {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "simple" => AnomalyMADEstimator::Simple,
            "harrelldavis" => AnomalyMADEstimator::HarrellDavis,
            "invariant" => AnomalyMADEstimator::Invariant,
            "hd" => AnomalyMADEstimator::HarrellDavis,
        };
        match res {
            Some(estimator) => Ok(estimator),
            None => {
                let msg = format!("Unknown quantile estimator: {s}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DoubleMADOptions {}

/// Options for seasonal adjustment
#[derive(Debug, Clone, Copy)]
pub struct SeasonalAdjustment {
    /// Seasonal period for adjustment
    pub seasonal_period: usize,
}

/// Options for analysis detection
#[derive(Debug, Clone)]
pub struct AnomalyOptions {
    /// Detection method to use
    pub method: AnomalyMethod,
    /// Threshold for analysis detection (interpretation depends on method)
    pub threshold: Option<f64>,
    /// Window size for local analysis detection
    pub window_size: Option<usize>,
    /// Number of trees for isolation forest
    pub n_trees: usize,
    /// Subsampling size for isolation forest
    pub subsample_size: Option<usize>,
    /// Spc method (if using Spc)
    pub spc_method: SPCMethod,
    /// Alpha for Ewma (if using Ewma Spc)
    pub ewma_alpha: f64,
    /// Distance metric (if using a distance-based method)
    pub distance_metric: DistanceMetric,
    /// Number of nearest neighbors (for distance-based methods)
    pub k_neighbors: usize,
    /// Contamination rate (expected fraction of anomalies)
    pub contamination: f64,
    /// Seasonal adjustment options
    pub seasonal_adjustment: Option<SeasonalAdjustment>,
    pub smoothed_zscore_options: Option<SmoothedZScoreOptions>,
    pub mad_estimator: Option<AnomalyMADEstimator>,
    /// Random Cut Forest options
    pub rcf_options: Option<RCFOptions>,
}

impl Default for AnomalyOptions {
    fn default() -> Self {
        Self {
            method: AnomalyMethod::StatisticalProcessControl,
            threshold: None,
            window_size: None,
            n_trees: 100,
            subsample_size: None,
            spc_method: SPCMethod::Shewhart,
            ewma_alpha: 0.3,
            distance_metric: DistanceMetric::Euclidean,
            k_neighbors: 5,
            contamination: 0.1,
            seasonal_adjustment: None,
            smoothed_zscore_options: None,
            mad_estimator: None,
            rcf_options: None,
        }
    }
}

/// Direction of anomalies to detect
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyDirection {
    /// Detect anomalies in both directions (high and low)
    Both,
    /// Detect only high anomalies
    Positive,
    /// Detect only low anomalies
    Negative,
}

impl AnomalyDirection {
    /// Returns a human-readable name for the direction
    pub fn name(&self) -> &'static str {
        match self {
            AnomalyDirection::Both => "both",
            AnomalyDirection::Positive => "positive",
            AnomalyDirection::Negative => "negative",
        }
    }
}

impl FromStr for AnomalyDirection {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "+" => AnomalyDirection::Positive,
            "-" => AnomalyDirection::Negative,
            "both" => AnomalyDirection::Both,
            "positive" => AnomalyDirection::Positive,
            "negative" => AnomalyDirection::Negative
        };
        match res {
            Some(direction) => Ok(direction),
            None => {
                let msg = format!("TSDB: unknown analysis direction: {s}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

impl TryFrom<&ValkeyString> for AnomalyDirection {
    type Error = ValkeyError;

    fn try_from(s: &ValkeyString) -> ValkeyResult<Self> {
        let str = s.to_string_lossy();
        Self::from_str(&str)
    }
}

/// Result of analysis direction detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalySignal {
    /// No anomalies detected
    None = 0,
    /// Indicates that a particular value is a positive peak.
    Positive = 1,
    /// Indicates that a particular value is a negative peak.
    Negative = -1,
}

impl AnomalySignal {
    pub fn is_anomaly(&self) -> bool {
        *self != AnomalySignal::None
    }

    pub fn matches_direction(&self, direction: AnomalyDirection) -> bool {
        match direction {
            AnomalyDirection::Both => self.is_anomaly(),
            AnomalyDirection::Positive => *self == AnomalySignal::Positive,
            AnomalyDirection::Negative => *self == AnomalySignal::Negative,
        }
    }
}

impl From<AnomalySignal> for ValkeyValue {
    fn from(signal: AnomalySignal) -> Self {
        ValkeyValue::from(signal as i64)
    }
}

/// Result of analysis detection
#[derive(Debug, Clone)]
pub struct AnomalyResult {
    /// Anomaly scores for each point (higher scores indicate more anomalous)
    pub scores: Vec<f64>,
    /// Direction of detected anomalies
    pub anomalies: Vec<AnomalySignal>,
    /// Threshold used for binary classification
    pub threshold: f64,
    /// Method used for detection
    pub method: AnomalyMethod,
    /// Additional information specific to the method
    pub method_info: Option<MethodInfo>,
}

/// Method-specific information
#[derive(Debug, Clone)]
pub enum MethodInfo {
    /// Spc-specific information
    Spc {
        /// Control limits (lower, upper)
        control_limits: (f64, f64),
        /// Center line value
        center_line: f64,
    },
    /// Isolation Forest-specific information
    IsolationForest {
        /// Average path length for normal points
        average_path_length: f64,
    },
    /// Distance-based information
    DistanceBased {
        /// Distance scores for each point
        distances: Vec<f64>,
    },
}

/// Detects anomalies in a time series
///
/// This function applies various analysis detection algorithms to identify
/// points in the time series that deviate significantly from normal behavior.
///
/// # Arguments
///
/// * `ts` - The time series to analyze
/// * `options` - Options controlling the analysis detection
///
/// # Returns
///
/// * A result containing analysis scores and binary classifications
///
/// # Example
///
/// ```
/// use std::collections::Vec;
/// use crate::analysis::{detect_anomalies, AnomalyOptions, AnomalyMethod};
///
/// // Create a time series with some anomalies
/// let mut ts = Vec::from_vec((0..100).map(|i| (i as f64 / 10.0).sin()).collect());
/// ts[25] = 5.0; // Anomaly
/// ts[75] = -5.0; // Anomaly
///
/// let options = AnomalyOptions {
///     method: AnomalyMethod::ZScore,
///     threshold: Some(3.0),
///     ..Default::default()
/// };
///
/// let result = detect_anomalies(&ts, &options).unwrap();
/// println!("Anomalies detected: {}", result.is_anomaly.iter().filter(|&&x| x).count());
/// ```
pub fn detect_anomalies(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();

    if n < 3 {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: "Time series too short for analysis detection".to_string(),
            required: 3,
            actual: n,
        });
    }

    // Apply seasonal adjustment if requested
    if let Some(adjustment) = options.seasonal_adjustment {
        let adjusted = seasonally_adjust(ts, adjustment.seasonal_period)?;
        return handle_dispatch(&adjusted, options);
    };

    handle_dispatch(ts, options)
}

fn handle_dispatch(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    // Apply the selected analysis detection method
    match options.method {
        AnomalyMethod::StatisticalProcessControl => detect_anomalies_spc(ts, options),
        AnomalyMethod::IsolationForest => detect_anomalies_isolation_forest(ts, options),
        AnomalyMethod::DistanceBased => detect_anomalies_distance_based(ts, options),
        AnomalyMethod::PredictionBased => detect_anomalies_prediction_based(ts, options),
        AnomalyMethod::ZScore => detect_anomalies_zscore(ts, options),
        AnomalyMethod::ModifiedZScore => detect_anomalies_modified_zscore(ts, options),
        AnomalyMethod::DoubleMAD => detect_anomalies_double_mad(ts, options),
        AnomalyMethod::SmoothedZScore => detect_anomalies_smoothed_zscore(ts, options),
        AnomalyMethod::InterquartileRange => detect_anomalies_iqr(ts, options),
        AnomalyMethod::RandomCutForest => detect_anomalies_rcf(ts, options),
    }
}

/// Statistical Process Control (Spc) analysis detection
fn detect_anomalies_spc(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let mut scores = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    match options.spc_method {
        SPCMethod::Shewhart => {
            // Calculate control limits using the first portion of data
            let training_size = (n as f64 * 0.5).min(100.0) as usize;
            let training_data = &ts[0..training_size];

            let mean = calculate_mean(training_data);
            let std_dev = calculate_std_dev(training_data);

            let multiplier = 3.0; // 3-sigma control limits
            let ucl = mean + multiplier * std_dev; // Upper control limit
            let lcl = mean - multiplier * std_dev; // Lower control limit

            for &value in ts {
                let distance_from_center = (value - mean).abs();
                scores.push(distance_from_center / std_dev);

                let anomaly_direction = get_anomaly_direction(lcl, ucl, value);
                anomalies.push(anomaly_direction);
            }

            Ok(AnomalyResult {
                scores,
                anomalies,
                threshold: multiplier,
                method: AnomalyMethod::StatisticalProcessControl,
                method_info: Some(MethodInfo::Spc {
                    control_limits: (lcl, ucl),
                    center_line: mean,
                }),
            })
        }
        SPCMethod::Cusum => {
            // Cusum control chart implementation
            let training_size = (n as f64 * 0.5).min(100.0) as usize;
            let training_data = &ts[0..training_size];

            let target = calculate_mean(training_data);
            let std_dev = calculate_std_dev(training_data);

            let k = 0.5 * std_dev; // Reference value
            let h = 5.0 * std_dev; // Decision interval

            let mut cusum_pos = 0.0;
            let mut cusum_neg = 0.0;
            let threshold = h / std_dev;

            for &v in ts {
                let value = normalize_value(v);
                cusum_pos = f64::max(0.0, cusum_pos + (value - target) - k);
                cusum_neg = f64::max(0.0, cusum_neg - (value - target) - k);

                let cusum_max = f64::max(cusum_pos, cusum_neg);
                let score = cusum_max / std_dev;
                scores.push(score);

                let anomaly_direction = if cusum_pos > threshold {
                    AnomalySignal::Positive
                } else if cusum_neg > threshold {
                    AnomalySignal::Negative
                } else {
                    AnomalySignal::None
                };

                anomalies.push(anomaly_direction);
            }

            Ok(AnomalyResult {
                scores,
                anomalies,
                threshold,
                method: AnomalyMethod::StatisticalProcessControl,
                method_info: Some(MethodInfo::Spc {
                    control_limits: (-h, h),
                    center_line: target,
                }),
            })
        }
        SPCMethod::Ewma => {
            // Ewma control chart implementation
            let alpha = options.ewma_alpha;
            let training_size = (n as f64 * 0.5).min(100.0) as usize;
            let training_data = &ts[0..training_size];

            let target = calculate_mean(training_data);
            let sigma = calculate_std_dev(training_data);

            let mut ewma = target;
            let l = 3.0; // Control limit multiplier

            for (i, &v) in ts.iter().enumerate() {
                let value = normalize_value(v);
                ewma = alpha * value + (1.0 - alpha) * ewma;

                let ewma_variance = sigma * sigma * alpha / (2.0 - alpha)
                    * (1.0 - (1.0 - alpha).powi(2 * (i as i32 + 1)));
                let ewma_std = ewma_variance.sqrt();

                let ucl = target + l * ewma_std;
                let lcl = target - l * ewma_std;

                let score = (ewma - target).abs() / ewma_std;

                let anomaly_direction = get_anomaly_direction(lcl, ucl, value);
                anomalies.push(anomaly_direction);

                scores.push(score);
            }

            Ok(AnomalyResult {
                scores,
                anomalies,
                threshold: l,
                method: AnomalyMethod::StatisticalProcessControl,
                method_info: Some(MethodInfo::Spc {
                    control_limits: (target - l * sigma, target + l * sigma),
                    center_line: target,
                }),
            })
        }
    }
}

/// Isolation Forest analysis detection (simplified version)
fn detect_anomalies_isolation_forest(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let n_trees = options.n_trees;
    let subsample_size = options
        .subsample_size
        .unwrap_or((n as f64 * 0.5).min(256.0) as usize);

    // Convert to sliding windows for multivariate representation
    let window_size = options.window_size.unwrap_or(10.min(n / 4));
    let windowed_data = create_sliding_windows(ts, window_size)?;
    let n_windows = windowed_data.rows();

    let mut path_lengths: Vec<f64> = vec![0.0; n_windows];
    let mut rng = rand::rng();

    // Build isolation trees
    for _ in 0..n_trees {
        let tree_path_lengths = build_isolation_tree(&windowed_data, subsample_size, &mut rng)?;
        for i in 0..n_windows {
            path_lengths[i] += tree_path_lengths[i];
        }
    }

    // Average path lengths
    for len in path_lengths.iter_mut() {
        *len /= n_trees as f64;
    }

    // Calculate expected path length for normal data
    let c_n = if subsample_size > 2 {
        2.0 * (subsample_size as f64 - 1.0).ln() + 0.5772156649
            - 2.0 * (subsample_size - 1) as f64 / subsample_size as f64
    } else {
        1.0
    };

    // Calculate analysis scores (higher for anomalies)
    let mut scores: Vec<f64> = Vec::with_capacity(n);
    let mut anomaly_scores = Vec::with_capacity(n_windows);

    for i in 0..n_windows {
        let score = 2.0_f64.powf(-path_lengths[i] / c_n);
        anomaly_scores[i] = score;
    }

    // Map window scores back to time series
    for i in 0..n {
        let window_idx = if i >= window_size {
            i - window_size + 1
        } else {
            0
        }
        .min(n_windows - 1);
        scores.push(anomaly_scores[window_idx]);
    }

    // Determine a threshold and anomalies
    let threshold = determine_threshold(&scores, options.contamination);

    // The analysis score reflects how easily the point was isolated, not if the value itself is
    // numerically "above" or "below" the bulk of the data.
    let anomalies: Vec<AnomalySignal> = scores
        .iter()
        .map(|&score| {
            if score > threshold {
                AnomalySignal::Positive
            } else {
                AnomalySignal::None
            }
        })
        .collect();

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::IsolationForest,
        method_info: Some(MethodInfo::IsolationForest {
            average_path_length: calculate_mean(&path_lengths),
        }),
    })
}

/// Z-score based analysis detection
fn detect_anomalies_zscore(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let mean = calculate_mean(ts);
    let std_dev = calculate_std_dev(ts); // todo: empty result should be (1.0);

    let threshold = options.threshold.unwrap_or(3.0);

    let mut scores = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    for &value in ts {
        let value = if value.is_nan() { 0.0 } else { value };
        let zscore = (value - mean) / std_dev;
        let z_abs = zscore.abs();

        scores.push(z_abs); // todo: use zscore instead of abs?
        let anomaly_direction = if z_abs > threshold {
            if zscore > 0.0 {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        };
        anomalies.push(anomaly_direction);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::ZScore,
        method_info: None,
    })
}

/// Modified Z-score using median absolute deviation
fn detect_anomalies_modified_zscore(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let threshold = options.threshold.unwrap_or(3.5);

    // Calculate median
    let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let median = if n % 2 == 0 {
        (sorted_values[n / 2 - 1] + sorted_values[n / 2]) / 2.0
    } else {
        sorted_values[n / 2]
    };

    // Calculate MAD (Median Absolute Deviation)
    let mut abs_deviations: Vec<f64> = ts
        .iter()
        .map(|&x| (normalize_value(x) - median).abs())
        .collect();
    abs_deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let half = n / 2;
    let mad = if n % 2 == 0 {
        (abs_deviations[half - 1] + abs_deviations[half]) / 2.0
    } else {
        abs_deviations[half]
    };

    // Scale MAD for consistency with normal distribution
    let mad_scaled = mad / 0.6745;

    let mut scores = Vec::with_capacity(n);
    let mut anomalies = Vec::with_capacity(n);

    for &v in ts {
        let value = normalize_value(v);
        let modified_zscore = if mad_scaled > 1e-10 {
            0.6745 * (value - median) / mad
        } else {
            0.0
        };
        let score = modified_zscore.abs();
        scores.push(score);

        let anomaly_direction = if score > threshold {
            // use value > median instead of modified_zscore > 0.0 to avoid issues when mad_scaled is very small ?????
            if modified_zscore > 0.0 {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        };
        anomalies.push(anomaly_direction);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::ModifiedZScore,
        method_info: None,
    })
}

fn detect_anomalies_smoothed_zscore(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let SmoothedZScoreOptions {
        lag,
        influence,
        threshold,
    } = options.smoothed_zscore_options.unwrap_or_default();

    if lag == 0 {
        return Err(TimeSeriesAnalysisError::InvalidInput(
            "the length of the initial values is zero, the length is used as the lag for the algorithm".to_string()
        ));
    }

    let n = ts.len();
    let (initial, rest) = ts.split_at(lag);

    let mut detector = SmoothedZScoreAnomalyDetector::new(influence, threshold, initial)?;
    let mut scores: Vec<f64> = Vec::with_capacity(n);

    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    for &value in rest {
        let signal = detector.next(value);
        anomalies.push(signal);
        scores.push(detector.prev_score)
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::InterquartileRange,
        method_info: None,
    })
}

fn detect_anomalies_double_mad(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let threshold = options.threshold.unwrap_or(3.0);

    // Calculate median
    let sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
    let sample: Samples = Samples::new_unweighted(sorted_values);

    let detector = match options
        .mad_estimator
        .unwrap_or(AnomalyMADEstimator::Invariant)
    {
        AnomalyMADEstimator::Simple => DoubleMadOutlierDetector::from_sample_with_k(
            &sample,
            threshold,
            Some(SimpleNormalizedEstimator::default()),
        ),
        AnomalyMADEstimator::HarrellDavis => DoubleMadOutlierDetector::from_sample_with_k(
            &sample,
            threshold,
            Some(HarrellDavisNormalizedEstimator),
        ),
        AnomalyMADEstimator::Invariant => DoubleMadOutlierDetector::from_sample_with_k(
            &sample,
            threshold,
            Some(InvariantMADEstimator::default()),
        ),
    };

    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);
    let mut scores: Vec<f64> = Vec::with_capacity(n);
    for &v in ts {
        let value = normalize_value(v);
        let score = value; // detector.score(value, &sample);
        scores.push(score);

        let anomaly_direction = if detector.is_upper_outlier(score) {
            AnomalySignal::Positive
        } else if detector.is_lower_outlier(score) {
            AnomalySignal::Negative
        } else {
            AnomalySignal::None
        };
        anomalies.push(anomaly_direction);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::DoubleMAD,
        method_info: None,
    })
}

/// Interquartile Range (IQR) analysis detection
fn detect_anomalies_iqr(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let multiplier = options.threshold.unwrap_or(1.5);

    // Calculate quartiles
    let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let q1_idx = n / 4;
    let q3_idx = 3 * n / 4;
    let q1 = sorted_values[q1_idx];
    let q3 = sorted_values[q3_idx];
    let iqr = q3 - q1;

    let lower_bound = q1 - multiplier * iqr;
    let upper_bound = q3 + multiplier * iqr;

    let mut scores: Vec<f64> = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    for &v in ts {
        let value = if v.is_nan() { 0.0 } else { v };
        let score = if value < lower_bound {
            (lower_bound - value) / iqr
        } else if value > upper_bound {
            (value - upper_bound) / iqr
        } else {
            0.0
        };

        let anomaly_direction = get_anomaly_direction(lower_bound, upper_bound, value);
        anomalies.push(anomaly_direction);
        scores.push(score);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold: multiplier,
        method: AnomalyMethod::InterquartileRange,
        method_info: None,
    })
}

/// Placeholder implementations for complex methods
fn detect_anomalies_one_class_svm(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    // Simplified implementation using a distance-based approach as a substitute
    detect_anomalies_distance_based(ts, options)
}

fn detect_anomalies_distance_based(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let window_size = options.window_size.unwrap_or(10.min(n / 4));
    let k = options.k_neighbors;

    // Create sliding windows
    let windowed_data = create_sliding_windows(ts, window_size)?;
    let n_windows = windowed_data.rows();

    let mut distances = Vec::with_capacity(n_windows);

    // Calculate the average distance to k nearest neighbors for each window
    for i in 0..n_windows {
        let mut window_distances = Vec::new();
        let current_window = windowed_data.get_row(i).expect("Row index out of bounds");

        for j in 0..n_windows {
            if i != j {
                let other_window = windowed_data.get_row(j).expect("Row index out of bounds");
                let dist = euclidean_distance(current_window, other_window);
                window_distances.push(dist);
            }
        }

        window_distances.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let avg_k_distance: f64 = window_distances.iter().take(k).sum::<f64>() / k as f64;
        distances.push(avg_k_distance);
    }

    // Map back to time series
    let mut scores = Vec::with_capacity(n);
    for i in 0..n {
        let window_idx = if i >= window_size {
            i - window_size + 1
        } else {
            0
        }
        .min(n_windows - 1);
        let d = distances[window_idx];
        scores.push(d);
    }

    let threshold = determine_threshold(&scores, options.contamination);

    // The analysis score reflects distance, not if the value itself is numerically "above" or "below"
    let anomalies = scores
        .iter()
        .map(|&score| {
            if score > threshold {
                AnomalySignal::Positive
            } else {
                AnomalySignal::None
            }
        })
        .collect();

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::DistanceBased,
        method_info: Some(MethodInfo::DistanceBased {
            distances: distances.to_owned(),
        }),
    })
}

/// Prediction-based analysis detection
fn detect_anomalies_prediction_based(
    ts: &[f64],
    _options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    // Simplified prediction-based approach using moving average prediction
    let n = ts.len();
    let window_size = 10.min(n / 4);

    let mut scores: Vec<f64> = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    for i in window_size..n {
        // Calculate moving average as a prediction
        let window = &ts[i - window_size..i];
        let prediction = calculate_mean(window);
        let actual = normalize_value(ts[i]);

        // Calculate prediction error
        let error = (actual - prediction).abs();
        scores.push(error);
    }

    // Calculate a threshold based on prediction errors
    let valid_scores: Vec<f64> = scores.iter().skip(window_size).copied().collect();

    if !valid_scores.is_empty() {
        let mean_error = valid_scores.iter().sum::<f64>() / valid_scores.len() as f64;
        let std_error = {
            let variance = valid_scores
                .iter()
                .map(|&x| (x - mean_error).powi(2))
                .sum::<f64>()
                / valid_scores.len() as f64;
            variance.sqrt()
        };

        let lower_bound = mean_error - 3.0 * std_error;
        let upper_bound = mean_error + 3.0 * std_error;
        let threshold = upper_bound; // Use the upper bound as the threshold

        for &score in scores.iter().take(n).skip(window_size) {
            let anomaly_direction = get_anomaly_direction(lower_bound, upper_bound, score);
            anomalies.push(anomaly_direction);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold,
            method: AnomalyMethod::PredictionBased,
            method_info: None,
        })
    } else {
        Err(TimeSeriesAnalysisError::InsufficientData {
            message: "Not enough data for prediction-based analysis detection".to_string(),
            required: window_size + 1,
            actual: n,
        })
    }
}

fn detect_anomalies_rcf(
    ts: &[f64],
    options: &AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    // Placeholder for Random Cut Forest implementation
    let kcf_options = options
        .rcf_options
        .unwrap_or(RCFOptions::default());
    let mut detector = RcfOutlierDetector::new(kcf_options)
        .map_err(|e| TimeSeriesAnalysisError::InvalidModel(format!("{:?}", e)))?;
    let mut scores: Vec<f64> = Vec::with_capacity(ts.len());
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(ts.len());
    for &value in ts {
        let score = detector.score(&[value])
            .map_err(|e| TimeSeriesAnalysisError::InvalidModel(format!("{:?}", e)))?;
        scores.push(score);

        let anomaly_direction = if score > kcf_options.threshold {
            AnomalySignal::Positive
        } else {
            AnomalySignal::None
        };
        anomalies.push(anomaly_direction);
    }
}

// Helper functions
fn seasonally_adjust(ts: &[f64], period: usize) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let n = ts.len();
    if n < period * 2 {
        return Ok(ts.to_vec());
    }

    let mut adjusted = ts.to_vec();

    // Simple seasonal adjustment using period-wise detrending
    for season in 0..period {
        let mut seasonal_values = Vec::new();
        let mut indices = Vec::new();

        for i in (season..n).step_by(period) {
            seasonal_values.push(ts[i]);
            indices.push(i);
        }

        if seasonal_values.len() > 1 {
            let seasonal_mean = calculate_mean(&seasonal_values);

            for &idx in &indices {
                adjusted[idx] -= seasonal_mean;
            }
        }
    }

    Ok(adjusted)
}

fn create_sliding_windows(
    _ts: &[f64],
    window_size: usize,
) -> TimeSeriesAnalysisResult<Array2D<f64>> {
    let n = _ts.len();
    if n < window_size {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: "Time series too short for windowing".to_string(),
            required: window_size,
            actual: n,
        });
    }

    let n_windows = n - window_size + 1;
    let mut windows = Array2D::new(n_windows, window_size);

    for i in 0..n_windows {
        let mut row = Vec::with_capacity(window_size);
        for j in 0..window_size {
            let val = normalize_value(_ts[i + j]);
            row.push(val);
        }
        windows.set_row(i, row);
    }

    Ok(windows)
}

fn build_isolation_tree(
    data: &Array2D<f64>,
    subsample_size: usize,
    rng: &mut ThreadRng,
) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let n_samples = data.rows();
    let _n_features = data.cols();

    // Subsample data
    let actual_subsample_size = subsample_size.min(n_samples);
    let mut indices: Vec<usize> = (0..n_samples).collect();
    indices.shuffle(rng);
    let _subsample_indices = &indices[0..actual_subsample_size];

    let mut path_lengths = Vec::with_capacity(n_samples);

    // Build tree using subsample, but calculate path lengths for all points
    for idx in 0..n_samples {
        let point = data.get_row(idx).expect("Row index out of bounds");
        path_lengths.push(calculate_isolation_path_length(point, data, 0, rng));
    }

    Ok(path_lengths)
}

fn calculate_isolation_path_length(
    point: &[f64],
    data: &Array2D<f64>,
    depth: usize,
    rng: &mut ThreadRng,
) -> f64 {
    const MAX_DEPTH: usize = 20; // Prevent infinite recursion

    if depth >= MAX_DEPTH || data.rows() <= 1 {
        return depth as f64;
    }

    // Randomly select a feature and split value
    let feature_idx = rng.random_range(0..data.cols());
    let feature_values: Vec<f64> = data.get_column(feature_idx).unwrap();
    let min_val = feature_values.iter().copied().fold(f64::INFINITY, f64::min);
    let max_val = feature_values
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    if (max_val - min_val).abs() < 1e-10 {
        return depth as f64; // No variation in this feature
    }

    // Simplified: return path length based on how far the point is from the mean
    let mean_val = feature_values.iter().sum::<f64>() / feature_values.len() as f64;
    let deviation = (point[feature_idx] - mean_val).abs();
    let max_deviation = (max_val - min_val) / 2.0;

    // Anomalies (points far from mean) should have shorter path lengths
    if max_deviation > 0.0 {
        let normalized_deviation = deviation / max_deviation;
        // Points far from mean get shorter paths (are isolated faster)
        depth as f64 + (1.0 - normalized_deviation.min(1.0))
    } else {
        depth as f64 + 1.0
    }
}

fn euclidean_distance(a: &[f64], b: &[f64]) -> f64 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}

fn determine_threshold(scores: &[f64], contamination: f64) -> f64 {
    let mut sorted_scores: Vec<f64> = scores.to_vec();
    sorted_scores.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let threshold_idx = ((1.0 - contamination) * sorted_scores.len() as f64) as usize;
    sorted_scores[threshold_idx.min(sorted_scores.len() - 1)]
}

fn calculate_std_dev(data: &[f64]) -> f64 {
    let n = data.len();
    if n <= 1 {
        return 0.0;
    }

    let mean = calculate_mean(data);
    let variance = data.iter().map(|&x| (x - mean) * (x - mean)).sum::<f64>() / (n - 1) as f64;

    variance.sqrt()
}

fn calculate_mean(data: &[f64]) -> f64 {
    let n = data.len();
    if n == 0 {
        return 0.0;
    }
    data.iter().sum::<f64>() / n as f64
}

#[inline]
fn normalize_value(v: f64) -> f64 {
    if v.is_nan() { 0.0 } else { v }
}

fn get_anomaly_direction(low_threshold: f64, hi_threshold: f64, value: f64) -> AnomalySignal {
    if value < low_threshold {
        AnomalySignal::Negative
    } else if value > hi_threshold {
        AnomalySignal::Positive
    } else {
        AnomalySignal::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zscore_anomaly_detection() {
        // Create a time series with clear anomalies
        let mut ts: Vec<f64> = (0..100).map(|i| (i as f64 / 10.0).sin()).collect();
        ts[25] = 5.0; // Clear analysis
        ts[75] = -5.0; // Clear analysis

        let options = AnomalyOptions {
            method: AnomalyMethod::ZScore,
            threshold: Some(3.0),
            ..Default::default()
        };

        let result = detect_anomalies(&ts, &options).unwrap();

        // Should detect the two anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert!(
            anomaly_count >= 2,
            "Should detect at least 2 anomalies, found {anomaly_count}"
        );

        // Anomalies should have high scores
        assert!(result.scores[25] > 3.0);
        assert!(result.scores[75] > 3.0);
    }

    #[test]
    fn test_modified_zscore() {
        let ts = vec![1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]; // 10.0 is an outlier

        let options = AnomalyOptions {
            method: AnomalyMethod::ModifiedZScore,
            threshold: Some(3.5),
            ..Default::default()
        };

        let result = detect_anomalies(&ts, &options).unwrap();

        // Should detect the outlier at index 5
        assert!(
            result.anomalies[5].is_anomaly(),
            "Should detect anomaly at index 5"
        );
        assert!(result.scores[5] > 3.5);
    }

    #[test]
    fn test_iqr_anomaly_detection() {
        let mut ts = vec![1.0; 100];
        ts[50] = 10.0; // Clear outlier

        let options = AnomalyOptions {
            method: AnomalyMethod::InterquartileRange,
            threshold: Some(1.5),
            ..Default::default()
        };

        let result = detect_anomalies(&ts, &options).unwrap();

        // Should detect the outlier
        assert!(
            result.anomalies[50].is_anomaly(),
            "Should detect anomaly at index 50"
        );
    }

    #[test]
    fn test_spc_shewhart() {
        // Create a time series with a shift in mean
        let mut ts = Vec::with_capacity(100);
        for i in 0..50 {
            let v = 1.0 + 0.1 * (i as f64 * 0.1).sin();
            ts.push(v);
        }
        for i in 50..100 {
            ts[i] = 5.0 + 0.1 * (i as f64 * 0.1).sin(); // Shift in mean
        }

        let options = AnomalyOptions {
            method: AnomalyMethod::StatisticalProcessControl,
            spc_method: SPCMethod::Shewhart,
            ..Default::default()
        };

        let result = detect_anomalies(&ts, &options).unwrap();

        // Should detect anomalies in the second half
        let anomalies_second_half = result.anomalies[50..]
            .iter()
            .filter(|&&x| x.is_anomaly())
            .count();
        assert!(
            anomalies_second_half > 10,
            "Should detect many anomalies in second half"
        );
    }

    #[test]
    fn test_isolation_forest() {
        let mut ts: Vec<f64> = (0..50).map(|i| (i as f64 / 5.0).sin()).collect();
        ts[25] = 10.0; // Anomaly

        let options = AnomalyOptions {
            method: AnomalyMethod::IsolationForest,
            contamination: 0.1,
            window_size: Some(5),
            n_trees: 10, // Fewer trees for faster testing
            ..Default::default()
        };

        let result = detect_anomalies(&ts, &options).unwrap();

        // Should detect some anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert!(anomaly_count > 0, "Should detect at least one anomaly");
    }

    #[test]
    fn test_edge_cases() {
        // Test with a very short time series
        let ts = vec![1.0, 2.0];
        let options = AnomalyOptions::default();

        let result = detect_anomalies(&ts, &options);
        assert!(result.is_err());

        // Test with constant time series
        let ts = vec![1.0; 50];
        let options = AnomalyOptions {
            method: AnomalyMethod::ZScore,
            threshold: Some(3.0),
            ..Default::default()
        };

        let result = detect_anomalies(&ts, &options).unwrap();
        // Should detect no anomalies in constant series
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert_eq!(
            anomaly_count, 0,
            "Should detect no anomalies in constant series"
        );
    }
}
