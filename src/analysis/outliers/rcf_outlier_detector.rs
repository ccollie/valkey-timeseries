use krcf::{RandomCutForest, RandomCutForestOptions};
use valkey_module::{ValkeyError, ValkeyResult};
use crate::analysis::outliers::OutlierDetector;

const DEFAULT_THRESHOLD: f64 = 0.7;

#[derive(Debug, Clone)]
pub struct RCFOptions {
    pub num_trees: Option<usize>,
    pub sample_size: Option<usize>,
    pub threshold: f64,
    /// A parameter that determines how much weight is given to older points. A non-zero value helps the
    /// model adapt to changing data patterns.
    pub time_decay: Option<f64>,
}

impl Default for RCFOptions {
    fn default() -> Self {
        Self {
            num_trees: Some(100),
            sample_size: Some(256),
            threshold: 0.7,
            time_decay: None,
        }
    }
}

impl From<RCFOptions> for RandomCutForestOptions {
    fn from(options: RCFOptions) -> RandomCutForestOptions {
        let mut rcf_options = RandomCutForestOptions::default();
        rcf_options.num_trees = options.num_trees;
        rcf_options.sample_size = options.sample_size;
        rcf_options.lambda = options.time_decay;
        rcf_options
    }
}


/// Outlier detector backed by a Random Cut Forest (RCF).
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
            .map_err(|e| ValkeyError::String(format!("Failed to create RCF: {:?}", e)))?;
        Ok(Self {
            forest,
            threshold,
        })
    }
    /// Wrap an existing trained `RandomCutForest` with a detection threshold.
    pub fn from_forest(forest: RandomCutForest, threshold: f64) -> Self {
        Self { forest, threshold }
    }

    /// Return the RCF anomaly score for a single scalar value.
    pub fn score(&self, value: f64) -> f64 {
        self.forest.score(&[value as f32]).unwrap_or_else(|e| {
            log::error!("{:?}", e);
            f64::NAN
        })
    }

    /// Convenience: is the score above the configured threshold?
    pub fn is_anomaly(&self, value: f64) -> bool {
        self.score(value) > self.threshold
    }
}

impl OutlierDetector for RcfOutlierDetector {
    /// RCF produces a symmetric anomaly score; treat above-threshold as outlier.
    fn is_lower_outlier(&self, x: f64) -> bool {
        self.is_anomaly(x)
    }

    fn is_upper_outlier(&self, x: f64) -> bool {
        self.is_anomaly(x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rcf_detects_large_outlier() {
        // small example; tune num_trees/sample_size for your kcrf build
        let data = [1.0, 1.1, 0.9, 1.05, 1.0, 100.0];
        let detector = RcfOutlierDetector::from_univariate_data(&data[..5], 50, 256, 0.8);
        // 100.0 should score highly and be considered an anomaly (adjust threshold as needed)
        assert!(detector.is_anomaly(100.0));
    }
}
