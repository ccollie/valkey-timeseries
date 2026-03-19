mod anomalies;
mod cusum_outlier_detector;
mod double_mad_outlier_detector;
#[cfg(test)]
mod double_mad_outlier_detector_tests;
mod esd_outlier_detector;
mod ewma_outlier_detector;
mod iqr_outlier_detector;
pub mod mad_estimator;
mod mad_outlier_detector;
#[cfg(test)]
mod mad_outlier_detector_tests;
mod modified_zscore_outlier_detector;
#[cfg(test)]
mod outlier_test_data;
mod rcf_outlier_detector;
mod smoothed_zscores;
mod utils;
mod zscore_outlier_detector;

pub use anomalies::*;
pub use double_mad_outlier_detector::*;
pub use esd_outlier_detector::ESDOutlierOptions;
pub use rcf_outlier_detector::*;
pub use smoothed_zscores::*;
use std::fmt::Display;

use std::ops::Deref;
use std::str::FromStr;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Method for outlier detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyMethod {
    /// Ewma-based statistical process control
    Ewma,
    /// CUSUM-based statistical process control
    Cusum,
    /// Z-score based detection
    ZScore,
    /// Modified Z-score using median absolute deviation
    ModifiedZScore,
    /// The Smoothed Z-Score algorithm
    SmoothedZScore,
    /// Mean Absolute Deviation (Mad) method
    Mad,
    /// Double median absolute deviation (Mad) method
    DoubleMAD,
    /// Interquartile range (IQR) method
    InterquartileRange,
    /// Random Cut Forest (Rcf) method
    RandomCutForest,
    /// ESD (Extreme Studentized Deviate) method
    Esd,
}

impl AnomalyMethod {
    /// Returns a human-readable name for the method
    pub fn name(&self) -> &'static str {
        match self {
            AnomalyMethod::Ewma => "EWMA",
            AnomalyMethod::Cusum => "CUSUM",
            AnomalyMethod::ZScore => "Z-Score",
            AnomalyMethod::ModifiedZScore => "Modified Z-Score",
            AnomalyMethod::SmoothedZScore => "Smoothed Z-Score",
            AnomalyMethod::Mad => "Median Absolute Deviation (MAD)",
            AnomalyMethod::DoubleMAD => "Double MAD",
            AnomalyMethod::InterquartileRange => "Interquartile Range (IQR)",
            AnomalyMethod::RandomCutForest => "Random Cut Forest",
            AnomalyMethod::Esd => "Extreme Studentized Deviate (ESD)",
        }
    }
}

impl FromStr for AnomalyMethod {
    type Err = ValkeyError;

    fn from_str(s: &str) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s.as_bytes(),
            "esd" => Ok(AnomalyMethod::Esd),
            "ewma" => Ok(AnomalyMethod::Ewma),
            "cusum" => Ok(AnomalyMethod::Cusum),
            "zscore" => Ok(AnomalyMethod::ZScore),
            "modified-zscore" => Ok(AnomalyMethod::ModifiedZScore),
            "smoothed-zscore" => Ok(AnomalyMethod::SmoothedZScore),
            "mad" => Ok(AnomalyMethod::Mad),
            "double-mad" => Ok(AnomalyMethod::DoubleMAD),
            "iqr" => Ok(AnomalyMethod::InterquartileRange),
            "rcf" => Ok(AnomalyMethod::RandomCutForest),
        };
        res.unwrap_or(Err(ValkeyError::Str(
            "TSDB: unknown anomaly detection method",
        )))
    }
}

impl TryFrom<&ValkeyString> for AnomalyMethod {
    type Error = ValkeyError;

    fn try_from(s: &ValkeyString) -> ValkeyResult<Self> {
        let str = s.to_string_lossy();
        Self::from_str(&str)
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

impl Display for AnomalyDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
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

    pub fn is_positive(&self) -> bool {
        *self == AnomalySignal::Positive
    }

    pub fn is_negative(&self) -> bool {
        *self == AnomalySignal::Negative
    }
}

impl From<AnomalySignal> for ValkeyValue {
    fn from(signal: AnomalySignal) -> Self {
        ValkeyValue::from(signal as i64)
    }
}

/// Method-specific information
#[derive(Debug, Clone, Copy)]
pub enum MethodInfo {
    /// For methods like ZScore, MAD and IQR
    Fenced {
        lower_fence: f64,
        upper_fence: f64,
        center_line: Option<f64>,
    },
    /// Spc-specific information
    Spc {
        /// Control limits (lower, upper)
        control_limits: (f64, f64),
        /// Center line value
        center_line: f64,
    },
}

#[derive(Debug, Copy, Clone)]
pub struct Anomaly {
    pub signal: AnomalySignal,
    pub value: f64,
    pub score: f64,
    pub index: usize,
}

impl Anomaly {
    pub fn is_anomaly(&self) -> bool {
        self.signal.is_anomaly()
    }

    pub fn is_negative(&self) -> bool {
        self.signal.is_negative()
    }

    pub fn is_positive(&self) -> bool {
        self.signal.is_positive()
    }
}

/// Result of anomaly detection
#[derive(Debug, Clone)]
pub struct AnomalyResult {
    /// Anomaly scores for each point (higher scores indicate more anomalous)
    pub scores: Vec<f64>,
    /// Detected anomalies
    pub anomalies: Vec<Anomaly>,
    /// Threshold used for binary classification
    pub threshold: f64,
    /// Method used for detection
    pub method: AnomalyMethod,
    /// Additional information specific to the method
    pub method_info: Option<MethodInfo>,
}

impl AnomalyResult {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            scores: Vec::with_capacity(n),
            anomalies: Vec::with_capacity(4),
            threshold: 0.0,
            method: AnomalyMethod::ZScore,
            method_info: None,
        }
    }

    /// Count the number of detected anomalies
    pub fn count_anomalies(&self) -> usize {
        self.anomalies.len()
    }

    /// Get outlier percentage.
    pub fn outlier_percentage(&self) -> f64 {
        let count = self.count_anomalies();
        if count == 0 {
            0.0
        } else {
            100.0 * count as f64 / self.scores.len() as f64
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
            "harrell-davis" => AnomalyMADEstimator::HarrellDavis,
            "harrelldavis" => AnomalyMADEstimator::HarrellDavis,
            "hd" => AnomalyMADEstimator::HarrellDavis,
            "invariant" => AnomalyMADEstimator::Invariant,
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

/// Options for MAD-based anomaly detection
#[derive(Debug, Clone, Copy)]
pub struct MADAnomalyOptions {
    /// Multiplier for MAD to set thresholds
    pub k: f64,
    /// Estimator to use for MAD calculation
    pub estimator: AnomalyMADEstimator,
}

impl Default for MADAnomalyOptions {
    fn default() -> Self {
        Self {
            k: 3.0,
            estimator: AnomalyMADEstimator::Invariant,
        }
    }
}

/// Trait for outlier detection
pub trait OutlierDetector {
    fn get_anomaly_score(&self, value: f64) -> f64;

    fn classify(&self, x: f64) -> AnomalySignal;
}

impl<T> OutlierDetector for Box<T>
where
    T: OutlierDetector,
{
    fn get_anomaly_score(&self, value: f64) -> f64 {
        self.deref().get_anomaly_score(value)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        self.deref().classify(x)
    }
}
