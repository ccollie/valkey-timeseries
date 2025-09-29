use std::cmp::Ordering;
use crate::analysis::outliers::OutlierDetector;
use crate::analysis::quantile_estimators::{MedianAbsoluteDeviationEstimator, Samples};

/// Outlier detector based on the median absolute deviation.
/// Considers all values outside [median - k * MAD, median + k * MAD] as outliers.
#[derive(Debug)]
pub struct MadOutlierDetector {
    lower_fence: f64,
    upper_fence: f64,
    pub mad: f64,
}

impl MadOutlierDetector {
    const DEFAULT_K: f64 = 3.0;
    
    /// Create a new MAD outlier detector for a slice of f64, given k.
    pub fn new_with_k(data: &[f64], k: f64) -> Self {
        assert!(!data.is_empty(), "Sample cannot be empty");
        let median = get_median(data);
        let mad = get_mad(data, median);
        let lower_fence = median - k * mad;
        let upper_fence = median + k * mad;
        MadOutlierDetector { lower_fence, upper_fence, mad }
    }

    pub fn new_with_estimator(data: &[f64], estimator: &impl MedianAbsoluteDeviationEstimator) -> Self
    {
        assert!(!data.is_empty(), "Sample cannot be empty");
        let samples = Samples::from(data);
        let mad = estimator.mad(&samples);
        let lower_fence = estimator.lower_mad(&samples);
        let upper_fence = estimator.upper_mad(&samples);
        MadOutlierDetector { lower_fence, upper_fence, mad }
    }

    /// Computes the median absolute deviation (MAD) of a &[f64]
    fn mad(data: &[f64], median: f64) -> f64 {
        let deviations: Vec<f64> = data.iter().map(|v| (v - median).abs()).collect();
        get_median(&deviations)
    }

    /// Create a new MAD outlier detector with default k.
    pub fn new(data: &[f64]) -> Self {
        Self::new_with_k(data, Self::DEFAULT_K)
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

fn get_mad(data: &[f64], median: f64) -> f64 {
    let deviations: Vec<f64> = data.iter().map(|v| (v - median).abs()).collect();
    get_median(&deviations)
}

/// Computes the median of a &[f64]
fn get_median(data: &[f64]) -> f64 {
    let mut sorted = data.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let len = sorted.len();
    if len == 0 {
        return f64::NAN;
    }
    if len % 2 == 1 {
        sorted[len / 2]
    } else {
        (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
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