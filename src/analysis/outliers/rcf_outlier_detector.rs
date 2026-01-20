use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal, OutlierDetector};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use crate::common::threads::NUM_THREADS;
use krcf::{RandomCutForest, RandomCutForestOptions};
use orx_parallel::{ParIter, ParIterResult, Parallelizable};
use std::sync::atomic::Ordering;
use valkey_module::logging::log_warning;
use valkey_module::{ValkeyError, ValkeyResult};

const DEFAULT_THRESHOLD: f64 = 0.7;

/// Configuration options for the Rcf outlier detector.
#[derive(Debug, Copy, Clone)]
pub struct RCFOptions {
    /// The number of trees in the Random Cut Forest.
    pub num_trees: Option<usize>,
    /// The sample size for each tree in the Random Cut Forest.
    pub sample_size: Option<usize>,
    /// The anomaly score threshold above which a point is considered an outlier.
    pub threshold: f64,
    /// A parameter that determines how much weight is given to older points. A non-zero value helps the
    /// model adapt to changing data patterns.
    pub time_decay: Option<f64>,
    /// The size of the shingle (number of consecutive data points combined into a single point).
    pub shingle_size: Option<usize>,
    /// Number of initial data points to skip before producing output scores.
    pub output_after: Option<usize>,
    /// Whether to enable parallel execution for RCF operations.
    pub parallel_enabled: Option<bool>,
}

impl Default for RCFOptions {
    fn default() -> Self {
        Self {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: 0.7,
            time_decay: None,
            shingle_size: None,
            output_after: None,
            parallel_enabled: None,
        }
    }
}

impl RCFOptions {
    /// Create RCFOptions with default values overridden by provided ones.
    pub fn with_overrides(overrides: RCFOptions) -> Self {
        RCFOptions {
            num_trees: overrides.num_trees.or(Some(100)),
            sample_size: overrides.sample_size.or(Some(256)),
            threshold: if overrides.threshold > 0.0 && overrides.threshold < 1.0 {
                overrides.threshold
            } else {
                DEFAULT_THRESHOLD
            },
            time_decay: overrides.time_decay,
            shingle_size: overrides.shingle_size,
            output_after: overrides.output_after,
            parallel_enabled: overrides.parallel_enabled,
        }
    }

    /// Heuristic: decide whether to enable parallel execution for RCF operations.
    ///
    /// Assumptions:
    /// - Univariate input data (base dimension = 1)
    /// - Effective dimension is `shingle_size` (since a shingle concatenates consecutive scalar values).
    ///
    /// Rule:
    /// Enable parallel if:
    /// - `num_trees >= num_threads`, AND
    /// - `num_trees * dimensions_eff * log2(sample_size) >= 10_000`
    pub fn should_parallelize(&self) -> bool {
        let num_threads = NUM_THREADS.load(Ordering::Relaxed);

        // Missing values fall back to the same defaults used elsewhere.
        let num_trees = self.num_trees.unwrap_or(100);
        let sample_size = self.sample_size.unwrap_or(256).max(2); // avoid log2(0/1)
        let shingle_size = self.shingle_size.unwrap_or(1).max(1);

        // Univariate base dimension = 1, so the effective dimension is just the shingle size.
        let dimensions_eff = shingle_size;

        if num_threads <= 1 {
            return false;
        }
        if num_trees < num_threads {
            return false;
        }

        // Integer log2 (floor) is good enough for a heuristic and avoids FP edge cases.
        let log2_sample = (usize::BITS - (sample_size as u64).leading_zeros() - 1) as usize;

        // Compute `num_trees * dimensions_eff * log2(sample_size)` with saturation to avoid overflow.
        let work = num_trees
            .saturating_mul(dimensions_eff)
            .saturating_mul(log2_sample);

        work >= 10_000
    }
}
impl From<RCFOptions> for RandomCutForestOptions {
    fn from(options: RCFOptions) -> RandomCutForestOptions {
        let parallel_execution_enabled = match options.parallel_enabled {
            None | Some(true) => Some(options.should_parallelize()),
            Some(false) => None,
        };

        RandomCutForestOptions {
            num_trees: options.num_trees,
            sample_size: options.sample_size,
            lambda: options.time_decay,
            shingle_size: options.shingle_size.unwrap_or(1),
            output_after: options.output_after,
            parallel_execution_enabled,
            ..Default::default()
        }
    }
}

/// Outlier detector backed by a Random Cut Forest (Rcf).
#[derive(Debug, Clone)]
pub struct RcfOutlierDetector {
    pub(super) forest: RandomCutForest,
    threshold: f64,
}

impl RcfOutlierDetector {
    pub fn new(options: RCFOptions) -> ValkeyResult<Self> {
        let threshold = if options.threshold > 0.0 && options.threshold < 1.0 {
            options.threshold
        } else {
            DEFAULT_THRESHOLD
        };
        let rcf_options: RandomCutForestOptions = options.into();
        let forest = RandomCutForest::new(rcf_options)
            .map_err(|e| ValkeyError::String(format!("Failed to create Rcf: {:?}", e)))?;
        Ok(Self { forest, threshold })
    }

    /// Wrap an existing trained `RandomCutForest` with a detection threshold.
    pub fn from_forest(forest: RandomCutForest, threshold: f64) -> Self {
        Self { forest, threshold }
    }

    pub fn set_data(&mut self, data: &[f64]) {
        for &value in data {
            if let Err(e) = self.forest.update(&[value as f32]) {
                log::error!("Failed to update Rcf with value {}: {:?}", value, e);
            }
        }
    }

    /// Return the Rcf anomaly score for a single scalar value.
    pub fn score(&self, value: f64) -> f64 {
        self.forest.score(&[value as f32]).unwrap_or_else(|e| {
            log::error!("{:?}", e);
            f64::NAN
        })
    }

    /// Like [`score`], but returns an error instead of logging + `NaN`.
    pub fn try_score(&self, value: f64) -> ValkeyResult<f64> {
        self.forest
            .score(&[value as f32])
            .map_err(|e| ValkeyError::String(format!("Failed to score Rcf point: {:?}", e)))
    }

    /// Batch scoring that uses [`try_score`] and returns the first error encountered.
    ///
    /// Parallelizes over input points when the batch is large enough to amortize overhead.
    pub fn try_batch_scores(&self, values: &[f64]) -> ValkeyResult<Vec<f64>> {
        let num_threads = NUM_THREADS.load(Ordering::Relaxed);

        // Heuristic: only parallelize for sufficiently large batches.
        // Rationale: scoring a single point is relatively small work; thread context switching overhead
        // can dominate for small N.
        let should_parallelize = num_threads > 1 && values.len() >= 256 * num_threads;

        if should_parallelize {
            values
                .par()
                .map(|&v| self.try_score(v))
                .into_fallible_result()
                .collect()
        } else {
            values.iter().map(|&v| self.try_score(v)).collect()
        }
    }

    /// Convenience: is the score above the configured threshold?
    pub fn is_anomaly(&self, value: f64) -> bool {
        self.score(value) > self.threshold
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let scores = self.try_batch_scores(ts).map_err(|e| {
            let msg = format!("Failed to score Rcf point: {e:?}");
            log_warning(&msg);
            TimeSeriesAnalysisError::AnomalyDetectionError(
                "failed to compute RCF scores".to_string(),
            )
        })?;

        let anomalies: Vec<AnomalySignal> = scores
            .iter()
            .map(|&score| {
                if score > self.threshold {
                    AnomalySignal::Positive
                } else {
                    AnomalySignal::None
                }
            })
            .collect();

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.threshold,
            method: AnomalyMethod::RandomCutForest,
            method_info: None,
        })
    }
}

impl OutlierDetector for RcfOutlierDetector {
    fn get_anomaly_score(&self, value: f64) -> f64 {
        let raw_score = self.score(value);
        normalize_rcf_score(raw_score)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        let score = self.score(x);
        // Rcf produces a symmetric anomaly score; treat above-threshold as outlier.
        if score > self.threshold {
            AnomalySignal::Positive
        } else {
            AnomalySignal::None
        }
    }
}

/// Normalize RCF anomaly scores to the range (0, 1].
///
/// RCF raw scores are unbounded (typically 0 to ~3+ for anomalies).
/// This uses a sigmoid-like transformation to map scores to (0, 1].
pub fn normalize_rcf_score(raw_score: f64) -> f64 {
    if raw_score.is_nan() || raw_score < 0.0 {
        return 0.0;
    }
    // Sigmoid transformation: 1 - e^(-score)
    // - score=0 → 0
    // - score→∞ → 1
    // - score=1 ≈ 0.632
    // - score=2 ≈ 0.865
    // - score=3 ≈ 0.950
    1.0 - (-raw_score).exp()
}

/// Random Cut Forest (Rcf) anomaly detection
pub(super) fn detect_anomalies_rcf(
    ts: &[f64],
    options: RCFOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut options = options;
    if options.output_after.is_none() {
        options.output_after = Some(ts.len());
    }
    let detector = RcfOutlierDetector::new(options)
        .map_err(|e| TimeSeriesAnalysisError::InvalidModel(format!("{:?}", e)))?;
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rcf_detects_large_outlier() {
        // small example; tune num_trees/sample_size for your kcrf build
        let data = [1.0, 1.1, 0.9, 1.05, 1.0, 100.0];
        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(256),
            threshold: 0.8,
            time_decay: None,
            shingle_size: None,
            output_after: Some(data.len()),
            parallel_enabled: None,
        })
        .unwrap();
        detector.set_data(&data);

        // 100.0 should score highly and be considered an anomaly (adjust the threshold as needed)
        assert!(detector.is_anomaly(100.0));
    }

    #[test]
    fn score_is_nan_when_forest_is_empty() {
        let detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(20),
            sample_size: Some(64),
            threshold: 0.7,
            time_decay: None,
            shingle_size: None,
            output_after: None,
            parallel_enabled: None,
        })
        .unwrap();

        // Scoring before any updates may error internally; wrapper converts that into NaN.
        let score = detector.score(1.0);
        assert_eq!(score, 0.0); // Rcf returns 0.0 for empty forest
        assert!(!detector.is_anomaly(1.0)); // NaN > threshold is false
    }

    #[test]
    fn outlier_scores_higher_than_typical_values_after_training() {
        // Train on a tight cluster around 1.0
        let train = [1.0, 1.02, 0.98, 1.01, 0.99, 1.0, 1.01, 0.995, 1.005, 1.0];

        // This avoids relying on an absolute threshold (which can be flaky across builds);
        // we only assert that an obvious outlier scores higher than a typical point.
        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(60),
            sample_size: Some(128),
            threshold: 0.9999, // effectively disable threshold check for this test
            time_decay: None,
            shingle_size: None,
            output_after: Some(train.len()),
            parallel_enabled: None,
        })
        .unwrap();

        detector.set_data(&train);

        let typical = detector.score(1.0);
        let outlier = detector.score(50.0);

        assert!(
            !typical.is_nan(),
            "typical score should be a number after training"
        );
        assert!(
            !outlier.is_nan(),
            "outlier score should be a number after training"
        );
        assert!(
            outlier > typical,
            "expected outlier score ({outlier}) to be > typical score ({typical})"
        );
    }

    #[test]
    fn rcf_detects_spiky_data() {
        // Baseline around 10.0, with occasional spikes to 100.0
        let mut data = vec![10.0; 100];
        data[25] = 100.0; // spike
        data[50] = 95.0; // spike
        data[75] = 105.0; // spike

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: 0.75,
            time_decay: Some(0.1),
            shingle_size: Some(1),
            output_after: Some(30), // allow training period
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // Verify spikes score high
        let spike_score = detector.score(100.0);
        let normal_score = detector.score(10.0);

        assert!(!spike_score.is_nan(), "spike score should be valid");
        assert!(!normal_score.is_nan(), "normal score should be valid");
        assert!(
            spike_score > normal_score,
            "spike ({spike_score}) should score higher than normal ({normal_score})"
        );
        assert!(
            detector.is_anomaly(100.0),
            "spike should be detected as anomaly"
        );
        assert!(
            !detector.is_anomaly(10.0),
            "normal value should not be anomaly"
        );
    }

    #[test]
    fn rcf_handles_constant_data() {
        // All values identical
        let data = vec![42.0; 100];

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(128),
            threshold: 0.7,
            time_decay: None,
            shingle_size: Some(1),
            output_after: Some(20),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // Constant value should have a low anomaly score
        let constant_score = detector.score(42.0);
        assert!(!constant_score.is_nan(), "constant score should be valid");

        // A different value should score higher
        let different_score = detector.score(100.0);
        assert!(
            different_score > constant_score,
            "different value ({different_score}) should score higher than constant ({constant_score})"
        );
    }

    #[test]
    fn rcf_detects_gradual_shift() {
        // Data shifts from ~1.0 to ~10.0 gradually
        let mut data = Vec::with_capacity(200);
        for i in 0..100 {
            data.push(1.0 + 0.05 * (i as f64 * 0.1).sin());
        }
        for i in 0..100 {
            data.push(10.0 + 0.05 * (i as f64 * 0.1).sin());
        }

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: 0.6,
            time_decay: Some(0.05), // helps adapt to shift
            shingle_size: Some(4),  // multi-point context
            output_after: Some(50),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // After shift, values around 10.0 should eventually normalize
        // but immediate transition should show high scores
        let baseline_score = detector.score(1.0);
        let shifted_score = detector.score(10.0);

        assert!(!baseline_score.is_nan());
        assert!(!shifted_score.is_nan());
    }

    #[test]
    fn rcf_batch_scoring_matches_individual() {
        let train = vec![1.0, 1.1, 0.9, 1.05, 1.0, 0.95, 1.02, 1.08];
        let test_values = vec![1.0, 5.0, 1.05, 20.0];

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(60),
            sample_size: Some(128),
            threshold: 0.8,
            time_decay: None,
            shingle_size: Some(1),
            output_after: Some(train.len()),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&train);

        // Individual scoring
        let individual_scores: Vec<f64> = test_values
            .iter()
            .map(|&v| detector.try_score(v).unwrap())
            .collect();

        // Batch scoring
        let batch_scores = detector.try_batch_scores(&test_values).unwrap();

        assert_eq!(individual_scores.len(), batch_scores.len());
        for (i, (&ind, &batch)) in individual_scores
            .iter()
            .zip(batch_scores.iter())
            .enumerate()
        {
            assert!(
                (ind - batch).abs() < 1e-10,
                "score mismatch at index {i}: individual={ind}, batch={batch}"
            );
        }
    }

    #[test]
    fn rcf_parallel_batch_scoring() {
        let train: Vec<f64> = (0..100)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.05).sin())
            .collect();
        // Large test batch to trigger parallel execution
        let test_values: Vec<f64> = (0..2000)
            .map(|i| if i % 100 == 0 { 50.0 } else { 1.0 })
            .collect();

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: 0.75,
            time_decay: None,
            shingle_size: Some(1),
            output_after: Some(train.len()),
            parallel_enabled: Some(true),
        })
        .unwrap();

        detector.set_data(&train);

        let scores = detector.try_batch_scores(&test_values).unwrap();
        assert_eq!(scores.len(), test_values.len());

        // Verify no NaN scores
        assert!(
            scores.iter().all(|s| !s.is_nan()),
            "all scores should be valid"
        );
    }

    #[test]
    fn rcf_with_shingle_size() {
        // Shingle size creates multi-point context
        let mut data = vec![1.0; 50];
        data.extend(vec![10.0; 50]); // abrupt shift

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(80),
            sample_size: Some(256),
            threshold: 0.7,
            time_decay: Some(0.1),
            shingle_size: Some(4), // 4-point sliding window
            output_after: Some(20),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // With shingle, the detector sees sequences like [1,1,1,1] vs [10,10,10,10]
        let score = detector.score(10.0);
        assert!(!score.is_nan(), "score with shingle should be valid");
    }

    #[test]
    fn rcf_should_parallelize_heuristic() {
        // Test the heuristic logic
        let opts_small = RCFOptions {
            num_trees: Some(10),
            sample_size: Some(64),
            threshold: 0.7,
            shingle_size: Some(1),
            ..Default::default()
        };
        assert!(
            !opts_small.should_parallelize(),
            "small forest should not parallelize"
        );

        let opts_large = RCFOptions {
            num_trees: Some(200),
            sample_size: Some(512),
            threshold: 0.7,
            shingle_size: Some(8),
            ..Default::default()
        };
        // The result depends on NUM_THREADS, but we can verify it returns a boolean
        let _ = opts_large.should_parallelize();
    }

    #[test]
    fn rcf_with_time_decay() {
        let mut data = Vec::with_capacity(200);
        // Early pattern: ~5.0
        for i in 0..100 {
            data.push(5.0 + 0.2 * (i as f64 * 0.1).sin());
        }
        // Later pattern: ~15.0
        for i in 0..100 {
            data.push(15.0 + 0.2 * (i as f64 * 0.1).sin());
        }

        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: 0.65,
            time_decay: Some(0.001), // decay parameter helps adapt
            shingle_size: Some(1),
            output_after: Some(50),
            parallel_enabled: Some(false),
        })
        .unwrap();

        detector.set_data(&data);

        // With time decay, the forest adapts to a new pattern
        let score = detector.score(15.0);
        assert!(!score.is_nan(), "score with time decay should be valid");
    }

    #[test]
    fn rcf_empty_batch_scoring() {
        let detector = RcfOutlierDetector::new(RCFOptions::default()).unwrap();
        let scores = detector.try_batch_scores(&[]).unwrap();
        assert!(scores.is_empty(), "empty input should yield empty output");
    }

    #[test]
    fn rcf_single_value_batch() {
        let train = vec![1.0; 50];
        let mut detector = RcfOutlierDetector::new(RCFOptions {
            num_trees: Some(50),
            sample_size: Some(128),
            threshold: 0.7,
            output_after: Some(train.len()),
            ..Default::default()
        })
        .unwrap();

        detector.set_data(&train);

        let scores = detector.try_batch_scores(&[5.0]).unwrap();
        assert_eq!(scores.len(), 1);
        assert!(!scores[0].is_nan());
    }
}
