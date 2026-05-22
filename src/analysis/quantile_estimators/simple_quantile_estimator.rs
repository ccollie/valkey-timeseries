use super::{HyndmanFanQuantileEstimator, HyndmanFanType, QuantileEstimator, Samples};
use std::fmt;

/// The most common quantile estimator, also known as Type 7 (see [Hyndman 1996]).
/// Consistent with many other statistical packages like R, Julia, NumPy, Excel (`PERCENTILE`, `PERCENTILE.INC`), Python (`inclusive`).
///
/// Hyndman, Rob J., and Yanan Fan. "Sample quantiles in statistical packages." The American Statistician 50, no. 4 (1996): 361-365.
/// https://doi.org/10.2307/2684934
#[derive(Clone, Copy)]
pub struct SimpleQuantileEstimator {
    inner: HyndmanFanQuantileEstimator,
}

impl Default for SimpleQuantileEstimator {
    fn default() -> Self {
        Self {
            inner: HyndmanFanQuantileEstimator::new(HyndmanFanType::Type7),
        }
    }
}

impl SimpleQuantileEstimator {
    // this is `Copy`, so it's cheap to clone
    /// Returns a new instance of the SimpleQuantileEstimator (Hyndman-Fan Type 7).
    pub fn instance() -> Self {
        SimpleQuantileEstimator::default()
    }
}

impl QuantileEstimator for SimpleQuantileEstimator {
    /// Computes the quantile for the given sorted sample and probability (0..=1).
    ///
    /// This method implements the Hyndman & Fan Type 7 quantile estimator.
    fn quantile(&self, sample: &Samples, probability: f64) -> f64 {
        self.inner.quantile(sample, probability)
    }
    fn supports_weighted_samples(&self) -> bool {
        self.inner.supports_weighted_samples()
    }
}

impl fmt::Debug for SimpleQuantileEstimator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimpleQuantileEstimator(Type 7)")
    }
}

#[cfg(test)]
mod tests {
    use super::SimpleQuantileEstimator;
    use crate::analysis::quantile_estimators::QuantileEstimator;

    #[test]
    fn test_quantile_type7() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let data = super::Samples::new_unweighted(values);
        let estimator = SimpleQuantileEstimator::instance();
        assert_eq!(estimator.quantile(&data, 0.0), 1.0);
        assert_eq!(estimator.quantile(&data, 1.0), 5.0);
        assert_eq!(estimator.quantile(&data, 0.5), 3.0);
        assert_eq!(estimator.quantile(&data, 0.25), 2.0);
        assert_eq!(estimator.quantile(&data, 0.75), 4.0);
    }
}
