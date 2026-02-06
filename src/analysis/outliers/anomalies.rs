//! Anomaly detection algorithms for time series
//!
//! This module provides various algorithms for detecting anomalies and outliers
//! in time series data, including statistical process control, isolation forest,
//! Mad, Double Mad, and Random Cut Forest approaches.

use super::{
    AnomalyMethod, AnomalyResult, MADAnomalyOptions,
    cusum_outlier_detector::detect_anomalies_spc_cusum,
    detect_anomalies_double_mad,
    ewma_outlier_detector::detect_anomalies_spc_ewma,
    iqr_outlier_detector::detect_anomalies_iqr,
    mad_outlier_detector::MadOutlierDetector,
    modified_zscore_outlier_detector::detect_anomalies_modified_zscore,
    rcf_outlier_detector::{RCFOptions, detect_anomalies_rcf},
    sesd_outlier_detector::{SESDOutlierOptions, detect_anomalies_sesd},
    smoothed_zscores::{SmoothedZScoreOptions, detect_anomalies_smoothed_zscore},
    zscore_outlier_detector::{ZScoreOutlierDetector, detect_anomalies_zscore},
};
use crate::analysis::{
    INSUFFICIENT_DATA_ERROR, TimeSeriesAnalysisError, TimeSeriesAnalysisResult,
    seasonality::{mstl::Mstl, stl::Stl},
};

#[derive(Debug, Clone)]
pub struct SeasonalAdjustment {
    /// Seasonal period for adjustment
    pub seasonal_periods: Vec<usize>,
}

#[derive(Debug, Clone)]
pub enum AnomalyDetectionMethodOptions {
    Cusum,
    Ewma(Option<f64>),
    InterQuartileRange(Option<f64>),
    ZScore(Option<f64>),
    SmoothedZScore(SmoothedZScoreOptions),
    ModifiedZScore(Option<f64>),
    Mad(MADAnomalyOptions),
    DoubleMAD(MADAnomalyOptions),
    Rcf(RCFOptions),
    SESD(Option<SESDOutlierOptions>),
}

impl Default for AnomalyDetectionMethodOptions {
    fn default() -> Self {
        AnomalyDetectionMethodOptions::ZScore(Some(ZScoreOutlierDetector::DEFAULT_THRESHOLD))
    }
}

impl AnomalyDetectionMethodOptions {
    pub fn method(&self) -> AnomalyMethod {
        match self {
            Self::Cusum => AnomalyMethod::Cusum,
            Self::Ewma(_) => AnomalyMethod::Ewma,
            Self::InterQuartileRange(_) => AnomalyMethod::InterquartileRange,
            Self::ZScore(_) => AnomalyMethod::ZScore,
            Self::SmoothedZScore(_) => AnomalyMethod::SmoothedZScore,
            Self::ModifiedZScore(_) => AnomalyMethod::ModifiedZScore,
            Self::Mad(_) => AnomalyMethod::Mad,
            Self::DoubleMAD(_) => AnomalyMethod::DoubleMAD,
            Self::Rcf(_) => AnomalyMethod::RandomCutForest,
            Self::SESD(_) => AnomalyMethod::SESD,
        }
    }

    pub fn for_ewma(alpha: f64) -> Self {
        AnomalyDetectionMethodOptions::Ewma(Some(alpha))
    }
}

/// Options for anomaly detection
#[derive(Debug, Clone, Default)]
pub struct AnomalyOptions {
    /// Seasonal adjustment options
    pub seasonal_adjustment: Option<SeasonalAdjustment>,
    /// Anomaly detection method options
    pub options: AnomalyDetectionMethodOptions,
}

impl AnomalyOptions {
    pub fn method(&self) -> AnomalyMethod {
        self.options.method()
    }

    pub fn set_seasonal_periods(&mut self, periods: Vec<usize>) {
        self.seasonal_adjustment = Some(SeasonalAdjustment {
            seasonal_periods: periods,
        });
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
/// println!("Anomalies detected: {}", result.anomalies.len());
/// ```
pub fn detect_anomalies(
    ts: &[f64],
    options: AnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();

    if n < 3 {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: INSUFFICIENT_DATA_ERROR.to_string(),
            required: 3,
            actual: n,
        });
    }

    // Apply seasonal adjustment if requested
    if let Some(adjustment) = &options.seasonal_adjustment {
        let adjusted = seasonality_adjust(ts, &adjustment.seasonal_periods)?;
        return handle_dispatch(&adjusted, options);
    };

    handle_dispatch(ts, options)
}

fn handle_dispatch(ts: &[f64], options: AnomalyOptions) -> TimeSeriesAnalysisResult<AnomalyResult> {
    match options.options {
        AnomalyDetectionMethodOptions::Cusum => detect_anomalies_spc_cusum(ts),
        AnomalyDetectionMethodOptions::Ewma(alpha) => detect_anomalies_spc_ewma(ts, alpha),
        AnomalyDetectionMethodOptions::InterQuartileRange(threshold) => {
            detect_anomalies_iqr(ts, threshold)
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
        AnomalyDetectionMethodOptions::SESD(opts) => detect_anomalies_sesd(ts, opts),
    }
}

fn detect_anomalies_mad(
    ts: &[f64],
    options: MADAnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = MadOutlierDetector::create_with_options(ts, options);
    detector.detect(ts)
}

fn validate_insufficient_data<T: Default>(
    required: usize,
    actual: usize,
) -> TimeSeriesAnalysisResult<T> {
    if actual >= required {
        return Ok(T::default());
    }
    Err(TimeSeriesAnalysisError::InsufficientData {
        message: "TSDB: insufficient samples for anomaly detection".to_string(),
        required,
        actual,
    })
}

/// Seasonal adjustment using (M)Stl decomposition
fn seasonality_adjust(ts: &[f64], periods: &[usize]) -> TimeSeriesAnalysisResult<Vec<f64>> {
    if periods.is_empty() {
        return Ok(ts.to_vec());
    }

    let n = ts.len();
    if periods.len() == 1 {
        let required = 2 * periods[0];
        validate_insufficient_data::<Vec<f64>>(required, n)?;

        Stl::new(periods[0])
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("STL decomposition failed".to_string())
            })
    } else {
        let max_period = periods[periods.len() - 1];
        let required = 2 * max_period;
        validate_insufficient_data::<Vec<f64>>(required, n)?;

        Mstl::new(periods.to_vec())
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("MSTL decomposition failed".to_string())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_cases() {
        // Test with a very short time series
        let ts = vec![1.0, 2.0];
        let options = AnomalyOptions::default();

        let result = detect_anomalies(&ts, options);
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
