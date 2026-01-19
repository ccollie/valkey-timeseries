mod anomalies;
mod double_mad_outlier_detector;
#[cfg(test)]
mod double_mad_outlier_detector_tests;
mod iqr;
mod isolation_forest;
pub mod mad_estimator;
mod mad_outlier_detector;
#[cfg(test)]
mod mad_outlier_detector_tests;
mod modified_zscore;
#[cfg(test)]
mod outlier_test_data;
mod rcf_outlier_detector;
mod smoothed_zscores;
mod spc_cusum;
mod spc_ewma;
mod spc_shewart;
mod zscore;

pub use anomalies::*;
pub use double_mad_outlier_detector::*;
pub use isolation_forest::*;
pub use rcf_outlier_detector::*;

use std::ops::Deref;
use std::str::FromStr;
use valkey_module::{ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Method for outlier detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnomalyMethod {
    /// Statistical process control (Spc)
    StatisticalProcessControl,
    /// Isolation forest for time series
    IsolationForest,
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
}

impl AnomalyMethod {
    /// Returns a human-readable name for the method
    pub fn name(&self) -> &'static str {
        match self {
            AnomalyMethod::StatisticalProcessControl => "Statistical Process Control",
            AnomalyMethod::IsolationForest => "Isolation Forest",
            AnomalyMethod::ZScore => "Z-Score",
            AnomalyMethod::ModifiedZScore => "Modified Z-Score",
            AnomalyMethod::SmoothedZScore => "Smoothed Z-Score",
            AnomalyMethod::Mad => "Median Absolute Deviation (Mad)",
            AnomalyMethod::DoubleMAD => "Double Mad",
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
            "zscore" => Ok(AnomalyMethod::ZScore),
            "z-score" => Ok(AnomalyMethod::ZScore),
            "modified-zscore" => Ok(AnomalyMethod::ModifiedZScore),
            "modifiedzscore" => Ok(AnomalyMethod::ModifiedZScore),
            "smoothed-zscore" => Ok(AnomalyMethod::SmoothedZScore),
            "smoothedzscore" => Ok(AnomalyMethod::SmoothedZScore),
            "mad" => Ok(AnomalyMethod::Mad),
            "double-mad" => Ok(AnomalyMethod::DoubleMAD),
            "doublemad" => Ok(AnomalyMethod::DoubleMAD),
            "interquartile-range" => Ok(AnomalyMethod::InterquartileRange),
            "interquartilerange" => Ok(AnomalyMethod::InterquartileRange),
            "iqr" => Ok(AnomalyMethod::InterquartileRange),
            "rcf" => Ok(AnomalyMethod::RandomCutForest),
            "random-cut-forest" => Ok(AnomalyMethod::RandomCutForest),
            "randomcutforest" => Ok(AnomalyMethod::RandomCutForest)
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

impl TryFrom<&str> for SPCMethod {
    type Error = ValkeyError;

    fn try_from(s: &str) -> ValkeyResult<Self> {
        SPCMethod::try_from(s.as_bytes())
    }
}

impl TryFrom<&[u8]> for SPCMethod {
    type Error = ValkeyError;

    fn try_from(s: &[u8]) -> ValkeyResult<Self> {
        let res = hashify::tiny_map_ignore_case! {
            s,
            "shewhart" => SPCMethod::Shewhart,
            "cusum" => SPCMethod::Cusum,
            "ewma" => SPCMethod::Ewma
        };
        match res {
            Some(method) => Ok(method),
            None => {
                let invalid = String::from_utf8_lossy(s);
                let msg = format!("TSDB: unknown Spc method: {invalid}");
                Err(ValkeyError::String(msg))
            }
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

pub trait OutlierDetector {
    // Check if the value is a lower outlier
    fn is_lower_outlier(&self, x: f64) -> bool;

    // Check if the value is an upper outlier
    fn is_upper_outlier(&self, x: f64) -> bool;
}

impl<T> OutlierDetector for Box<T>
where
    T: OutlierDetector,
{
    fn is_lower_outlier(&self, x: f64) -> bool {
        self.deref().is_lower_outlier(x)
    }

    fn is_upper_outlier(&self, x: f64) -> bool {
        self.deref().is_upper_outlier(x)
    }
}

pub(super) fn get_anomaly_direction(
    low_threshold: f64,
    hi_threshold: f64,
    value: f64,
) -> AnomalySignal {
    if value < low_threshold {
        AnomalySignal::Negative
    } else if value > hi_threshold {
        AnomalySignal::Positive
    } else {
        AnomalySignal::None
    }
}
