use crate::analysis::outliers::OutlierDetector;
use crate::analysis::quantile_estimators::QuantileEstimator;
use crate::analysis::quantile_estimators::SimpleNormalizedEstimator;
use crate::analysis::quantile_estimators::{MedianAbsoluteDeviationEstimator, Samples};

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

    pub fn new_with_estimator(
        samples: &Samples,
        median_abs_dev_estimator: impl MedianAbsoluteDeviationEstimator,
    ) -> Self {
        Self::init_from_estimator(median_abs_dev_estimator, samples, Self::DEFAULT_K)
    }

    pub fn new_with_k_and_estimator(
        sample: &Samples,
        k: f64,
        median_abs_dev_estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        match median_abs_dev_estimator {
            Some(estimator) => Self::init_from_estimator(estimator, sample, k),
            None => {
                let estimator = SimpleNormalizedEstimator::new();
                Self::init_from_estimator(estimator, sample, k)
            },
        }
    }

    fn init_from_estimator(estimator: impl MedianAbsoluteDeviationEstimator, sample: &Samples, k: f64) -> Self {
        let median = estimator.quantile_estimator().median(sample);
        let lower_mad = estimator.lower_mad(sample);
        let upper_mad = estimator.upper_mad(sample);
        Self {
            lower_fence: median - k * lower_mad,
            upper_fence: median + k * upper_mad,
        }
    }

    pub fn from_sample(
        sample: &Samples,
        median_abs_dev_estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        Self::new_with_k_and_estimator(sample, Self::DEFAULT_K, median_abs_dev_estimator)
    }

    pub fn from_values(
        values: &[f64],
        median_abs_dev_estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        let sample = Samples::from(values);
        Self::from_sample(&sample, median_abs_dev_estimator)
    }

    pub fn from_sample_with_k(
        sample: &Samples,
        k: f64,
        median_abs_dev_estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        Self::new_with_k_and_estimator(sample, k, median_abs_dev_estimator)
    }

    pub fn from_values_with_k(
        values: &[f64],
        k: f64,
        median_abs_dev_estimator: Option<impl MedianAbsoluteDeviationEstimator>,
    ) -> Self {
        let sample = Samples::from(values);
        Self::new_with_k_and_estimator(&sample, k, median_abs_dev_estimator)
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