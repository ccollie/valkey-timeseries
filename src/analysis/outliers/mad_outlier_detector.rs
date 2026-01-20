use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::mad_estimator::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, MedianAbsoluteDeviationEstimator,
    SimpleNormalizedEstimator,
};
use crate::analysis::outliers::{
    AnomalyMADEstimator, AnomalyMethod, AnomalyResult, AnomalySignal, MADAnomalyOptions,
    MethodInfo, OutlierDetector,
};
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::Samples;

/// Outlier detector based on the median absolute deviation.
/// Considers all values outside [median - k * Mad, median + k * Mad] as outliers.
#[derive(Debug)]
pub struct MadOutlierDetector {
    lower_fence: f64,
    upper_fence: f64,
    pub mad: f64,
    median: f64,
    k: f64,
}

impl MadOutlierDetector {
    const DEFAULT_K: f64 = 3.0;

    /// Create a new Mad outlier detector for a slice of f64, given k.
    fn create(data: &[f64], k: f64, estimator: &impl MedianAbsoluteDeviationEstimator) -> Self {
        assert!(!data.is_empty(), "Sample cannot be empty");
        let samples = Samples::from(data.to_vec());
        let median = estimator.quantile_estimator().median(&samples);
        let mad = estimator.mad(&samples);
        let lower_fence = median - k * mad;
        let upper_fence = median + k * mad;
        MadOutlierDetector {
            k,
            lower_fence,
            upper_fence,
            median,
            mad,
        }
    }

    pub fn create_with_options(data: &[f64], options: MADAnomalyOptions) -> Self {
        let k = options.k;
        match options.estimator {
            AnomalyMADEstimator::Simple => MadOutlierDetector::with_k_and_estimator(
                data,
                k,
                &SimpleNormalizedEstimator::default(),
            ),
            AnomalyMADEstimator::HarrellDavis => {
                MadOutlierDetector::with_k_and_estimator(data, k, &HarrellDavisNormalizedEstimator)
            }
            AnomalyMADEstimator::Invariant => {
                MadOutlierDetector::with_k_and_estimator(data, k, &InvariantMADEstimator::default())
            }
        }
    }

    /// Create a new Mad outlier detector for a slice of f64, given k.
    pub fn with_k_and_estimator(
        data: &[f64],
        k: f64,
        estimator: &impl MedianAbsoluteDeviationEstimator,
    ) -> Self {
        Self::create(data, k, estimator)
    }

    pub fn with_estimator(data: &[f64], estimator: &impl MedianAbsoluteDeviationEstimator) -> Self {
        Self::create(data, Self::DEFAULT_K, estimator)
    }

    /// Create a new Mad outlier detector with default k.
    pub fn new(data: &[f64]) -> Self {
        let estimator = SimpleNormalizedEstimator::new();
        Self::create(data, Self::DEFAULT_K, &estimator)
    }

    /// Returns whether a value is an outlier, according to the detector.
    pub fn is_outlier(&self, value: f64) -> bool {
        value < self.lower_fence || value > self.upper_fence
    }

    /// Returns the lower fence.
    pub fn lower_fence(&self) -> f64 {
        self.lower_fence
    }

    /// Returns the upper fence.
    pub fn upper_fence(&self) -> f64 {
        self.upper_fence
    }

    /// Returns a normalized anomaly score in `[0.0..=1.0]` describing how "anomalous" `value` is.
    ///
    /// Interpretation:
    /// - `0.0` means "at the median" (no deviation).
    /// - `1.0` means "at or beyond the configured MAD fence" (i.e., `k * mad` away from the median).
    ///
    /// This is computed as:
    /// `score = clamp(|value - median| / (k * mad), 0..1)`
    /// where `k` is inferred from the detector's fences.
    pub fn get_anomaly_score(&self, value: f64) -> f64 {
        if !value.is_finite() {
            return 0.0;
        }
        if !self.mad.is_finite() || self.mad <= 0.0 {
            return 0.0;
        }

        let k = self.k;
        if !k.is_finite() || k <= 0.0 {
            return 0.0;
        }

        let denom = k * self.mad;
        if !denom.is_finite() || denom <= 0.0 {
            return 0.0;
        }

        let raw = (value - self.median).abs() / denom;
        raw.clamp(0.0, 1.0)
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

        for &value in ts {
            let score = self.get_anomaly_score(value);
            let anomaly = self.classify(value);
            scores.push(score);
            anomalies.push(anomaly);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.k,
            method: AnomalyMethod::Mad,
            method_info: Some(MethodInfo::Fenced {
                lower_fence: self.lower_fence,
                upper_fence: self.upper_fence,
            }),
        })
    }
}

impl OutlierDetector for MadOutlierDetector {
    fn get_anomaly_score(&self, value: f64) -> f64 {
        self.get_anomaly_score(value)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        if x < self.lower_fence {
            AnomalySignal::Negative
        } else if x > self.upper_fence {
            AnomalySignal::Positive
        } else {
            AnomalySignal::None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mad_outlier_detector() {
        let data = [1.0, 2.0, 2.0, 2.0, 3.0, 14.0];
        let detector = MadOutlierDetector::new(&data);
        // 14.0 is an outlier
        assert!(detector.is_outlier(14.0));
        // 1.0 is not an outlier for k=3
        assert!(!detector.is_outlier(1.0));
        // 2.0 is not an outlier
        assert!(!detector.is_outlier(2.0));
    }

    #[test]
    fn test_get_anomaly_score_is_normalized() {
        let data = [1.0, 2.0, 2.0, 2.0, 3.0, 14.0];
        let detector = MadOutlierDetector::new(&data);

        let score_at_median = detector.get_anomaly_score(2.0);
        assert_eq!(score_at_median, 0.0);

        // Exactly at the upper fence should be 1.0
        let score_at_upper_fence = detector.get_anomaly_score(detector.upper_fence());
        assert!((score_at_upper_fence - 1.0).abs() < 1e-12);

        // Beyond the fence clamps to 1.0
        let score_beyond = detector.get_anomaly_score(1e9);
        assert!((score_beyond - 1.0).abs() < 1e-12);

        // Non-finite values are treated as non-anomalous for scoring purposes
        let score_nan = detector.get_anomaly_score(f64::NAN);
        assert_eq!(score_nan, 0.0);
    }
}
