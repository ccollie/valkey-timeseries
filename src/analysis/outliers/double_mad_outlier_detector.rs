use super::utils::get_anomaly_direction;
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::mad_estimator::{
    HarrellDavisNormalizedEstimator, InvariantMADEstimator, MedianAbsoluteDeviationEstimator,
    SimpleNormalizedEstimator,
};
use crate::analysis::outliers::{
    Anomaly, AnomalyMADEstimator, AnomalyMethod, AnomalyResult, AnomalySignal, MADAnomalyOptions,
    MethodInfo, BatchOutlierDetector,
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
    /// Optional computed fences. They may be None when the detector is constructed
    /// without data and will be computed when `train` is called (or on first detect).
    lower_fence: Option<f64>,
    upper_fence: Option<f64>,
    /// Configured threshold (k)
    threshold: f64,
    /// Midpoint between fences (optional until trained)
    midpoint: Option<f64>,
    /// Half range between fences (optional until trained)
    half_range: Option<f64>,
    /// Options describing estimator and k (kept so the detector can be trained later)
    options: MADAnomalyOptions,
}

impl DoubleMadOutlierDetector {
    const DEFAULT_K: f64 = 3.0;

    pub fn new(samples: &Samples) -> Self {
        // Create detector from sample using default options
        let mut det = Self::with_options(MADAnomalyOptions::default());
        det.train_from_samples(samples, Self::DEFAULT_K, Some(SimpleNormalizedEstimator::new()));
        det
    }

    /// Construct an untrained detector with the provided options.
    /// Fences will be computed when `train` is called (or on-demand in `detect`).
    pub fn with_options(options: MADAnomalyOptions) -> Self {
        DoubleMadOutlierDetector {
            lower_fence: None,
            upper_fence: None,
            threshold: options.k,
            midpoint: None,
            half_range: None,
            options,
        }
    }

    /// Returns true when the detector has been trained and fences have been computed.
    /// Callers may use this to decide whether to call `train` prior to scoring/classifying.
    pub fn is_trained(&self) -> bool {
        self.lower_fence.is_some()
            && self.upper_fence.is_some()
            && self.midpoint.is_some()
            && self.half_range.is_some()
    }

    pub fn with_data_and_options(data: &[f64], options: MADAnomalyOptions) -> Self {
        let threshold = options.k;
        let sample: Samples = Samples::new_unweighted(data.to_vec());
        let mut det = Self::with_options(options);
        // compute fences immediately using provided data
        det.train_from_samples(&sample, threshold, None::<SimpleNormalizedEstimator>);
        det
    }

    pub fn with_estimator(
        samples: &Samples,
        estimator: impl MedianAbsoluteDeviationEstimator,
    ) -> Self {
        let mut det = Self::with_options(MADAnomalyOptions::default());
        det.train_from_samples(samples, Self::DEFAULT_K, Some(estimator));
        det
    }

    pub fn with_k_and_estimator(
        sample: &Samples,
        k: f64,
        estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        let mut det = Self::with_options(MADAnomalyOptions { k, estimator: AnomalyMADEstimator::Invariant });
        // Use provided estimator or default to Simple
        det.train_from_samples(sample, k, estimator);
        det
    }

    fn compute_fences(
        estimator: impl MedianAbsoluteDeviationEstimator,
        sample: &Samples,
        k: f64,
    ) -> (f64, f64, f64, f64) {
        let median = estimator.quantile_estimator().median(sample);
        let lower_mad = estimator.lower_mad(sample);
        let upper_mad = estimator.upper_mad(sample);
        let lower_fence = median - k * lower_mad;
        let upper_fence = median + k * upper_mad;
        let midpoint = (lower_fence + upper_fence) / 2.0;
        let half_range = (upper_fence - lower_fence) / 2.0;
        (lower_fence, upper_fence, midpoint, half_range)
    }

    /// Train the detector using explicit Samples and optional estimator.
    fn train_from_samples<E: MedianAbsoluteDeviationEstimator>(&mut self, samples: &Samples, k: f64, estimator: Option<E>) {
        let mut apply_fences = |(l, u, m, h): (f64, f64, f64, f64)| {
            self.lower_fence = Some(l);
            self.upper_fence = Some(u);
            self.midpoint = Some(m);
            self.half_range = Some(h);
            self.threshold = k;
        };

        match estimator {
            Some(est) => {
                apply_fences(Self::compute_fences(est, samples, k));
            }
            None => {
                match self.options.estimator {
                    AnomalyMADEstimator::Simple => {
                        let est = SimpleNormalizedEstimator::default();
                        apply_fences(Self::compute_fences(est, samples, k));
                    }
                    AnomalyMADEstimator::HarrellDavis => {
                        let est = HarrellDavisNormalizedEstimator;
                        apply_fences(Self::compute_fences(est, samples, k));
                    }
                    AnomalyMADEstimator::Invariant => {
                        let est = InvariantMADEstimator::default();
                        apply_fences(Self::compute_fences(est, samples, k));
                    }
                }
            }
        }
    }

    /// Calculates a normalized anomaly score in [0, 1].
    ///
    /// - Returns 0.0 when the value equals the midpoint between fences (least anomalous).
    /// - Returns values approaching 1.0 as the value moves further beyond the fences.
    /// - Values within fences return scores < 0.5, values outside return scores >= 0.5.
    pub fn get_anomaly_score(&self, value: f64) -> f64 {
        // If not trained (no fences), we cannot compute a meaningful score.
        // Caller should ensure `train` is called prior to scoring. As a
        // fallback, treat everything as non-anomalous (score 0.0).
        let midpoint = match self.midpoint {
            Some(m) => m,
            None => return 0.0,
        };

        let half_range = match self.half_range {
            Some(h) => h,
            None => return 0.0,
        };

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
        match (self.lower_fence, self.upper_fence) {
            (Some(l), Some(u)) => value < l || value > u,
            _ => false,
        }
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        // Ensure detector is trained: compute fences if needed
        if !self.is_trained() {
            // build Samples and train using stored options
            let samples = Samples::new_unweighted(ts.to_vec());
            self.train_from_samples(&samples, self.options.k, None::<SimpleNormalizedEstimator>);
        }

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
                lower_fence: self.lower_fence.unwrap_or(f64::NAN),
                upper_fence: self.upper_fence.unwrap_or(f64::NAN),
                center_line: None,
            }),
        })
    }
}

impl BatchOutlierDetector for DoubleMadOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::DoubleMAD
    }

    fn train(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<()> {
        if ts.is_empty() {
            return Ok(());
        }
        let samples = Samples::new_unweighted(ts.to_vec());
        self.train_from_samples(&samples, self.options.k, None::<SimpleNormalizedEstimator>);
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        DoubleMadOutlierDetector::detect(self, ts)
    }

    fn get_anomaly_score(&self, value: f64) -> f64 {
        DoubleMadOutlierDetector::get_anomaly_score(self, value)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        // If fences are not computed yet, treat everything as non-anomalous
        match (self.lower_fence, self.upper_fence) {
            (Some(l), Some(u)) => get_anomaly_direction(l, u, x),
            _ => AnomalySignal::None,
        }
    }
}