use crate::analysis::outliers::OutlierDetector;
use crate::analysis::outliers::mad_estimator::{
    MedianAbsoluteDeviationEstimator, SimpleNormalizedEstimator,
};
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::Samples;

/// Outlier detector based on the double median absolute deviation.
/// Consider all values outside [median - k * LowerMAD, median + k * UpperMAD] as outliers.
///
/// See also: https://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
/// https://aakinshin.net/posts/harrell-davis-double-mad-outlier-detector/
#[derive(Debug)]
pub struct DoubleMadOutlierDetector {
    pub lower_fence: f64,
    pub upper_fence: f64,
}

impl DoubleMadOutlierDetector {
    const DEFAULT_K: f64 = 3.0;

    pub fn new(samples: &Samples) -> Self {
        let estimator = SimpleNormalizedEstimator::new();
        Self::init_from_estimator(estimator, samples, Self::DEFAULT_K)
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
        }
    }

    pub fn is_outlier(&self, value: f64) -> bool {
        value < self.lower_fence || value > self.upper_fence
    }
}

impl OutlierDetector for DoubleMadOutlierDetector {
    fn is_lower_outlier(&self, x: f64) -> bool {
        x < self.lower_fence
    }

    fn is_upper_outlier(&self, x: f64) -> bool {
        x > self.upper_fence
    }
}
