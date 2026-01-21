use super::utils::get_anomaly_direction;
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::mad_estimator::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, MedianAbsoluteDeviationEstimator,
    SimpleNormalizedEstimator,
};
use crate::analysis::outliers::{
    Anomaly, AnomalyMADEstimator, AnomalyMethod, AnomalyResult, AnomalySignal, MADAnomalyOptions,
    MethodInfo, OutlierDetector,
};
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::Samples;

/// Outlier detector based on the double median absolute deviation.
/// Consider all values outside [median - k * LowerMAD, median + k * UpperMAD] as outliers.
///
/// https://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
/// https://aakinshin.net/posts/harrell-davis-double-mad-outlier-detector/
#[derive(Debug)]
pub struct DoubleMadOutlierDetector {
    lower_fence: f64,
    upper_fence: f64,
    threshold: f64,
}

impl DoubleMadOutlierDetector {
    const DEFAULT_K: f64 = 3.0;

    pub fn new(samples: &Samples) -> Self {
        let estimator = SimpleNormalizedEstimator::new();
        Self::init_from_estimator(estimator, samples, Self::DEFAULT_K)
    }

    pub fn with_data_and_options(data: &[f64], options: MADAnomalyOptions) -> Self {
        let threshold = options.k;
        let sample: Samples = Samples::new_unweighted(data.to_vec());

        match options.estimator {
            AnomalyMADEstimator::Simple => DoubleMadOutlierDetector::with_k_and_estimator(
                &sample,
                threshold,
                Some(SimpleNormalizedEstimator::default()),
            ),
            AnomalyMADEstimator::HarrellDavis => DoubleMadOutlierDetector::with_k_and_estimator(
                &sample,
                threshold,
                Some(HarrellDavisNormalizedEstimator),
            ),
            AnomalyMADEstimator::Invariant => DoubleMadOutlierDetector::with_k_and_estimator(
                &sample,
                threshold,
                Some(InvariantMADEstimator::default()),
            ),
        }
    }

    pub fn with_estimator(
        samples: &Samples,
        estimator: impl MedianAbsoluteDeviationEstimator,
    ) -> Self {
        Self::init_from_estimator(estimator, samples, Self::DEFAULT_K)
    }

    pub fn with_k_and_estimator(
        sample: &Samples,
        k: f64,
        estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        match estimator {
            Some(estimator) => Self::init_from_estimator(estimator, sample, k),
            None => {
                let estimator = SimpleNormalizedEstimator::new();
                Self::init_from_estimator(estimator, sample, k)
            }
        }
    }

    fn init_from_estimator(
        estimator: impl MedianAbsoluteDeviationEstimator,
        sample: &Samples,
        k: f64,
    ) -> Self {
        let median = estimator.quantile_estimator().median(sample);
        let lower_mad = estimator.lower_mad(sample);
        let upper_mad = estimator.upper_mad(sample);
        Self {
            lower_fence: median - k * lower_mad,
            upper_fence: median + k * upper_mad,
            threshold: k,
        }
    }

    /// Calculates a normalized anomaly score in [0, 1].
    ///
    /// - Returns 0.0 when the value equals the midpoint between fences (least anomalous).
    /// - Returns values approaching 1.0 as the value moves further beyond the fences.
    /// - Values within fences return scores < 0.5, values outside return scores >= 0.5.
    pub fn get_anomaly_score(&self, value: f64) -> f64 {
        let midpoint = (self.lower_fence + self.upper_fence) / 2.0;
        let half_range = (self.upper_fence - self.lower_fence) / 2.0;

        // Guard against zero range (all values identical)
        if half_range <= 0.0 {
            return if (value - midpoint).abs() < f64::EPSILON {
                0.0
            } else {
                1.0
            };
        }

        // Calculate the distance from midpoint, normalized by half_range
        let normalized_distance = (value - midpoint).abs() / half_range;

        // Map to [0, 1] using a sigmoid-like transformation:
        // - distance = 0 → score = 0
        // - distance = 1 (at fence) → score = 0.5
        // - distance → ∞ → score → 1
        normalized_distance / (1.0 + normalized_distance)
    }

    pub fn is_outlier(&self, value: f64) -> bool {
        value < self.lower_fence || value > self.upper_fence
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(n);

        for (index, &value) in ts.iter().enumerate() {
            let score = self.get_anomaly_score(value);
            let anomaly = self.classify(value);
            if anomaly.is_anomaly() {
                let outlier = Anomaly {
                    index,
                    value,
                    signal: anomaly,
                    score,
                };
                anomalies.push(outlier);
            }
            scores.push(score);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.threshold,
            method: AnomalyMethod::DoubleMAD,
            method_info: Some(MethodInfo::Fenced {
                lower_fence: self.lower_fence,
                upper_fence: self.upper_fence,
            }),
        })
    }
}

impl OutlierDetector for DoubleMadOutlierDetector {
    fn get_anomaly_score(&self, value: f64) -> f64 {
        self.get_anomaly_score(value)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        get_anomaly_direction(self.lower_fence, self.upper_fence, x)
    }
}

/// Double Median Absolute Deviation (Mad) anomaly detection
/// https://aakinshin.net/posts/harrell-davis-double-mad-outlier-detector/
/// https://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
pub fn detect_anomalies_double_mad(
    ts: &[f64],
    options: MADAnomalyOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = DoubleMadOutlierDetector::with_data_and_options(ts, options);
    detector.detect(ts)
}
