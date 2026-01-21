//! Anomaly detection algorithms for time series
//!
//! This module provides various algorithms for detecting anomalies and outliers
//! in time series data, including statistical process control, isolation forest,
//! Mad, Double Mad, and Random Cut Forest approaches.

use super::cusum_outlier_detector::detect_anomalies_spc_cusum;
use super::iqr::detect_anomalies_iqr;
use super::isolation_forest::{IsolationForestOptions, detect_anomalies_isolation_forest};
use super::mad_outlier_detector::MadOutlierDetector;
use super::modified_zscore_outlier_detector::detect_anomalies_modified_zscore;
use super::rcf_outlier_detector::{RCFOptions, detect_anomalies_rcf};
use super::smoothed_zscores::SmoothedZScoreOptions;
use super::spc_ewma_outlier_detector::{EWMA_DEFAULT_ALPHA, detect_anomalies_spc_ewma};
use super::spc_shewhart_outlier_detector::detect_anomalies_spc_shewhart;
use super::zscore_outlier_detector::{ZScoreOutlierDetector, detect_anomalies_zscore};
use super::{
    AnomalyMethod, AnomalyResult, MADAnomalyOptions, SPCMethod, detect_anomalies_double_mad,
};
use crate::analysis::math::calculate_mean;
use crate::analysis::outliers::smoothed_zscores::detect_anomalies_smoothed_zscore;
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use std::fmt::Debug;

/// Options for seasonal adjustment
#[derive(Debug, Clone, Copy)]
pub struct SeasonalAdjustment {
    /// Seasonal period for adjustment
    pub seasonal_period: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct SPCMethodOptions {
    /// Spc method (if using Spc)
    pub spc_method: SPCMethod,
    /// Alpha for Ewma (if using Ewma Spc)
    pub ewma_alpha: Option<f64>,
}

impl Default for SPCMethodOptions {
    fn default() -> Self {
        Self {
            spc_method: SPCMethod::Shewhart,
            ewma_alpha: Some(EWMA_DEFAULT_ALPHA),
        }
    }
}

#[derive(Debug, Clone)]
pub enum AnomalyDetectionMethodOptions {
    Spc(SPCMethodOptions),
    InterQuartileRange(Option<f64>),
    IsolationForest(IsolationForestOptions),
    ZScore(Option<f64>),
    SmoothedZScore(SmoothedZScoreOptions),
    ModifiedZScore(Option<f64>),
    Mad(MADAnomalyOptions),
    DoubleMAD(MADAnomalyOptions),
    Rcf(RCFOptions),
}

impl Default for AnomalyDetectionMethodOptions {
    fn default() -> Self {
        AnomalyDetectionMethodOptions::ZScore(Some(ZScoreOutlierDetector::DEFAULT_THRESHOLD))
    }
}

impl AnomalyDetectionMethodOptions {
    pub fn method(&self) -> AnomalyMethod {
        match self {
            AnomalyDetectionMethodOptions::Spc(_) => AnomalyMethod::StatisticalProcessControl,
            AnomalyDetectionMethodOptions::InterQuartileRange(_) => {
                AnomalyMethod::InterquartileRange
            }
            AnomalyDetectionMethodOptions::IsolationForest(_) => AnomalyMethod::IsolationForest,
            AnomalyDetectionMethodOptions::ZScore(_) => AnomalyMethod::ZScore,
            AnomalyDetectionMethodOptions::SmoothedZScore(_) => AnomalyMethod::SmoothedZScore,
            AnomalyDetectionMethodOptions::ModifiedZScore(_) => AnomalyMethod::ModifiedZScore,
            AnomalyDetectionMethodOptions::Mad(_) => AnomalyMethod::Mad,
            AnomalyDetectionMethodOptions::DoubleMAD(_) => AnomalyMethod::DoubleMAD,
            AnomalyDetectionMethodOptions::Rcf(_) => AnomalyMethod::RandomCutForest,
        }
    }

    pub fn for_ewma(alpha: f64) -> Self {
        AnomalyDetectionMethodOptions::Spc(SPCMethodOptions {
            spc_method: SPCMethod::Ewma,
            ewma_alpha: Some(alpha),
        })
    }
}

/// Options for analysis detection
#[derive(Debug, Clone, Default)]
pub struct AnomalyOptions {
    /// Seasonal adjustment options
    pub seasonal_adjustment: Option<SeasonalAdjustment>,
    /// Analysis detection method options
    pub options: AnomalyDetectionMethodOptions,
}

impl AnomalyOptions {
    pub fn method(&self) -> AnomalyMethod {
        self.options.method()
    }
}

/// Detects anomalies in a time series
///
/// This function applies various anomaly detection algorithms to identify
/// points in the time series that deviate significantly from normal behavior.
///
/// # Arguments
///
/// * `ts` - The time series to analyze
/// * `options` - Options controlling the anomaly detection
///
/// # Returns
///
/// * A result containing anomaly scores and binary classifications
///
/// # Example
///
/// ```
/// use std::collections::Vec;
/// use super::{detect_anomalies, AnomalyOptions, AnomalyMethod};
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
            message: "TSDB: insufficient samples for anomaly detection".to_string(),
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
    match options.options {
        AnomalyDetectionMethodOptions::Spc(opts) => detect_anomalies_spc(ts, opts),
        AnomalyDetectionMethodOptions::InterQuartileRange(threshold) => {
            detect_anomalies_iqr(ts, threshold)
        }
        AnomalyDetectionMethodOptions::IsolationForest(options) => {
            detect_anomalies_isolation_forest(ts, options)
        }
        AnomalyDetectionMethodOptions::ZScore(threshold) => detect_anomalies_zscore(ts, threshold),
        AnomalyDetectionMethodOptions::SmoothedZScore(opts) => {
            detect_anomalies_smoothed_zscore(ts, opts)
        }
        AnomalyDetectionMethodOptions::ModifiedZScore(threshold) => {
            detect_anomalies_modified_zscore(ts, threshold)
        }
        AnomalyDetectionMethodOptions::Mad(options) => detect_anomalies_mad(ts, options),
        AnomalyDetectionMethodOptions::DoubleMAD(options) => {
            detect_anomalies_double_mad(ts, options)
        }
        AnomalyDetectionMethodOptions::Rcf(opts) => detect_anomalies_rcf(ts, opts),
    }
}

/// Statistical Process Control (Spc) anomaly detection
fn detect_anomalies_spc(
    ts: &[f64],
    options: SPCMethodOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    match options.spc_method {
        SPCMethod::Shewhart => detect_anomalies_spc_shewhart(ts),
        SPCMethod::Cusum => {
            // Cusum control chart implementation
            detect_anomalies_spc_cusum(ts)
        }
        SPCMethod::Ewma => {
            // Ewma control chart implementation
            detect_anomalies_spc_ewma(ts, options.ewma_alpha)
        }
    }
}

fn detect_anomalies_mad(
    ts: &[f64],
    options: MADAnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = MadOutlierDetector::create_with_options(ts, options);
    detector.detect(ts)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_cases() {
        // Test with a very short time series
        let ts = vec![1.0, 2.0];
        let options = AnomalyOptions::default();

        let result = detect_anomalies(&ts, &options);
        assert!(result.is_err());

        // Test with constant time series
        let ts = vec![1.0; 50];

        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();
        // Should detect no anomalies in constant series
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert_eq!(
            anomaly_count, 0,
            "Should detect no anomalies in constant series"
        );
    }
}
