use crate::analysis::outliers::OutlierDetector;
use crate::analysis::outliers::mad_estimator::{
    MedianAbsoluteDeviationEstimator, SimpleNormalizedEstimator,
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
            lower_fence,
            upper_fence,
            mad,
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
}

impl OutlierDetector for MadOutlierDetector {
    fn is_lower_outlier(&self, x: f64) -> bool {
        x < self.lower_fence
    }

    fn is_upper_outlier(&self, x: f64) -> bool {
        x > self.upper_fence
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
}
