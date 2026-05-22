use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::mad_estimator::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, MedianAbsoluteDeviationEstimator,
    SimpleNormalizedEstimator,
};
use crate::analysis::outliers::{
    Anomaly, AnomalyMADEstimator, AnomalyMethod, AnomalyResult, AnomalySignal,
    BatchOutlierDetector, MethodInfo,
};
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::Samples;

/// Outlier detector based on the median absolute deviation.
/// Considers all values outside [median - k * Mad, median + k * Mad] as outliers.
#[derive(Debug)]
pub struct MadOutlierDetector {
    is_trained: bool,
    estimator: AnomalyMADEstimator,
    lower_fence: f64,
    upper_fence: f64,
    mad: f64,
    median: f64,
    k: f64,
}

impl Default for MadOutlierDetector {
    fn default() -> Self {
        MadOutlierDetector {
            is_trained: false,
            estimator: AnomalyMADEstimator::Simple,
            lower_fence: f64::NAN,
            upper_fence: f64::NAN,
            mad: f64::NAN,
            median: f64::NAN,
            k: Self::DEFAULT_K,
        }
    }
}

impl MadOutlierDetector {
    pub const DEFAULT_K: f64 = 3.0;

    pub fn new(k: f64, estimator: AnomalyMADEstimator) -> Self {
        Self {
            k,
            estimator,
            ..Default::default()
        }
    }

    pub fn with_estimator(estimator: AnomalyMADEstimator) -> Self {
        Self::new(Self::DEFAULT_K, estimator)
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

    /// Returns a normalized anomaly score in `[0..1]` describing how "anomalous" `value` is.
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
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        for (index, &value) in ts.iter().enumerate() {
            let score = self.get_anomaly_score(value);
            let signal = self.classify(value);
            if signal.is_anomaly() {
                let outlier = Anomaly {
                    index,
                    signal,
                    value,
                    score,
                };
                anomalies.push(outlier);
            }
            scores.push(score);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.k,
            method: AnomalyMethod::Mad,
            method_info: Some(MethodInfo::Fenced {
                lower_fence: self.lower_fence,
                upper_fence: self.upper_fence,
                center_line: None,
            }),
        })
    }
}

impl BatchOutlierDetector for MadOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::Mad
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        debug_assert!(!data.is_empty(), "Sample cannot be empty");
        fn get_mad_median(
            data: &[f64],
            estimator: impl MedianAbsoluteDeviationEstimator,
        ) -> (f64, f64) {
            let samples = Samples::from(data.to_vec());
            let median = estimator.quantile_estimator().median(&samples);
            let mad = estimator.mad(&samples);
            (median, mad)
        }

        let (median, mad) = match self.estimator {
            AnomalyMADEstimator::Simple => {
                let estimator = SimpleNormalizedEstimator::new();
                get_mad_median(data, estimator)
            }
            AnomalyMADEstimator::Invariant => {
                let estimator = InvariantMADEstimator::new();
                get_mad_median(data, estimator)
            }
            AnomalyMADEstimator::HarrellDavis => {
                let estimator = HarrellDavisNormalizedEstimator;
                get_mad_median(data, estimator)
            }
        };

        let k = self.k;
        self.median = median;
        self.mad = mad;
        self.lower_fence = median - k * mad;
        self.upper_fence = median + k * mad;
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        MadOutlierDetector::detect(self, ts)
    }

    fn get_anomaly_score(&self, value: f64) -> f64 {
        MadOutlierDetector::get_anomaly_score(self, value)
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
        let mut detector = MadOutlierDetector::default();
        detector.train(&data).unwrap();

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
        let mut detector = MadOutlierDetector::default();
        detector.train(&data).unwrap();

        let score_at_median = detector.get_anomaly_score(2.0);
        assert_eq!(score_at_median, 0.0);

        // Exactly at the upper fence should be 1.0
        let score_at_upper_fence = detector.get_anomaly_score(detector.upper_fence());
        assert!((score_at_upper_fence - 1.0).abs() < f64::EPSILON);

        // Beyond the fence clamps to 1.0
        let score_beyond = detector.get_anomaly_score(1e9);
        assert!((score_beyond - 1.0).abs() < f64::EPSILON);

        // Non-finite values are treated as non-anomalous for scoring purposes
        let score_nan = detector.get_anomaly_score(f64::NAN);
        assert_eq!(score_nan, 0.0);
    }
}
