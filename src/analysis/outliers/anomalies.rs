//! Anomaly detection algorithms for time series
//!
//! This module provides various algorithms for detecting anomalies and outliers
//! in time series data, including statistical process control, isolation forest,
//! Mad, Double Mad, and Random Cut Forest approaches.

use super::{
    AnomalyMethod, AnomalyResult, BatchOutlierDetector, MADAnomalyOptions,
    SmoothedZScoreAnomalyDetector,
    cusum_outlier_detector::CusumOutlierDetector,
    double_mad_outlier_detector::DoubleMadOutlierDetector,
    ewma_outlier_detector::{EWMA_DEFAULT_ALPHA, EwmaOutlierDetector},
    iqr_outlier_detector::{IQR_DEFAULT_THRESHOLD, IQROutlierDetector},
    mad_outlier_detector::MadOutlierDetector,
    modified_zscore_outlier_detector::{
        MODIFIED_ZSCORE_DEFAULT_THRESHOLD, ModifiedZScoreOutlierDetector,
    },
    rcf_outlier_detector::{RCFOptions, RcfOutlierDetector},
    smoothed_zscores::SmoothedZScoreOptions,
    zscore_outlier_detector::ZScoreOutlierDetector,
};
use crate::analysis::outliers::esd_outlier_detector::{ESDOutlierDetector, ESDOutlierOptions};
use crate::analysis::seasonality::{Seasonality, seasonally_adjust};
use crate::analysis::{INSUFFICIENT_DATA_ERROR, TimeSeriesAnalysisError, TimeSeriesAnalysisResult};

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
    Esd(Option<ESDOutlierOptions>),
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
            Self::Esd(_) => AnomalyMethod::Esd,
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
    pub seasonality: Option<Seasonality>,
    /// Anomaly detection method options
    pub options: AnomalyDetectionMethodOptions,
}

impl AnomalyOptions {
    pub fn method(&self) -> AnomalyMethod {
        self.options.method()
    }

    pub fn set_seasonal_periods(&mut self, periods: Vec<usize>) {
        self.seasonality = Some(Seasonality::Periods(periods));
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
/// use crate::analysis::outliers::anomalies::{detect_anomalies, AnomalyOptions, AnomalyDetectionMethodOptions};
///
/// // Create a time series with some anomalies
/// let mut ts = vec![0.0; 100];
/// for i in 0..100 {
///     ts[i] = (i as f64 / 10.0).sin();
/// }
/// ts[25] = 5.0; // Anomaly
/// ts[75] = -5.0; // Anomaly
///
/// let options = AnomalyOptions {
///     options: AnomalyDetectionMethodOptions::ZScore(Some(3.0)),
///     ..Default::default()
/// };
///
/// let result = detect_anomalies(&ts, options).unwrap();
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
    if let Some(adjustment) = &options.seasonality {
        let adjusted = seasonally_adjust(ts, adjustment)?;
        let mut result = handle_dispatch(&adjusted, options)?;
        // we calculate anomalies on the seasonally adjusted data, but we want to report the original values in the result
        for anomaly in &mut result.anomalies {
            anomaly.value = ts[anomaly.index];
        }
        return Ok(result);
    };

    handle_dispatch(ts, options)
}

fn handle_dispatch(ts: &[f64], options: AnomalyOptions) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = build_detector(ts, options.options)?;
    detector.train(ts)?;
    detector.detect(ts)
}

fn build_detector(
    ts: &[f64],
    method_options: AnomalyDetectionMethodOptions,
) -> TimeSeriesAnalysisResult<Box<dyn BatchOutlierDetector>> {
    let detector: Box<dyn BatchOutlierDetector> = match method_options {
        AnomalyDetectionMethodOptions::Cusum => Box::new(CusumOutlierDetector::default()),
        AnomalyDetectionMethodOptions::Ewma(alpha) => Box::new(EwmaOutlierDetector::from_series(
            ts,
            alpha.unwrap_or(EWMA_DEFAULT_ALPHA),
        )),
        AnomalyDetectionMethodOptions::InterQuartileRange(threshold) => Box::new(
            IQROutlierDetector::new(ts, threshold.unwrap_or(IQR_DEFAULT_THRESHOLD)),
        ),
        AnomalyDetectionMethodOptions::ZScore(threshold) => Box::new(ZScoreOutlierDetector::new(
            threshold.unwrap_or(ZScoreOutlierDetector::DEFAULT_THRESHOLD),
        )),
        AnomalyDetectionMethodOptions::SmoothedZScore(options) => {
            let detector = SmoothedZScoreAnomalyDetector::new(
                options.influence,
                options.threshold,
                options.lag,
            )?;
            Box::new(detector)
        }
        AnomalyDetectionMethodOptions::ModifiedZScore(threshold) => {
            Box::new(ModifiedZScoreOutlierDetector::new(
                threshold.unwrap_or(MODIFIED_ZSCORE_DEFAULT_THRESHOLD),
            ))
        }
        AnomalyDetectionMethodOptions::Mad(options) => {
            Box::new(MadOutlierDetector::new(options.k, options.estimator))
        }
        AnomalyDetectionMethodOptions::DoubleMAD(options) => {
            Box::new(DoubleMadOutlierDetector::with_options(options))
        }
        AnomalyDetectionMethodOptions::Rcf(opts) => {
            let detector = RcfOutlierDetector::new(opts)
                .map_err(|e| TimeSeriesAnalysisError::InvalidModel(format!("{e:?}")))?;
            Box::new(detector)
        }
        AnomalyDetectionMethodOptions::Esd(options) => {
            let detector = if let Some(opts) = options {
                ESDOutlierDetector::new(opts.alpha, opts.hybrid, opts.max_outliers)
            } else {
                ESDOutlierDetector::default()
            };
            Box::new(detector)
        }
    };

    Ok(detector)
}
