use crate::analysis::math::BetaDistribution;
use crate::analysis::quantile_estimators::{QuantileEstimator, Samples};
use core::f64;

/// Trimmed Harrell-Davis quantile estimator based on the highest density
/// interval of the given width.
///
/// See: <https://arxiv.org/abs/2111.11776>
pub struct TrimmedHarrellDavisQuantileEstimator {
    get_interval_width: Box<dyn Fn(f64) -> f64 + Send + Sync>,
}

impl TrimmedHarrellDavisQuantileEstimator {
    /// Creates a new TrimmedHarrellDavisQuantileEstimator
    pub fn new(get_interval_width: Box<dyn Fn(f64) -> f64 + Send + Sync>) -> Self {
        Self { get_interval_width }
    }

    /// Creates an estimator with sqrt-based interval width
    pub fn sqrt() -> Self {
        Self::new(Box::new(|n: f64| 1.0 / n.sqrt()))
    }

    /// Binary search implementation for finding roots
    fn binary_search<F>(f: F, mut left: f64, mut right: f64) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let mut fl = f(left);
        let fr = f(right);

        // Check if the root exists in the interval
        if (fl < 0.0 && fr < 0.0) || (fl > 0.0 && fr > 0.0) {
            return f64::NAN;
        }

        while right - left > 1e-9 {
            let m = (left + right) / 2.0;
            let fm = f(m);

            if (fl < 0.0 && fm < 0.0) || (fl > 0.0 && fm > 0.0) {
                fl = fm;
                left = m;
            } else {
                right = m;
            }
        }

        (left + right) / 2.0
    }

    /// Computes the Highest Density Interval (HDI) for a Beta distribution
    pub(crate) fn get_beta_hdi(a: f64, b: f64, width: f64) -> (f64, f64) {
        const EPS: f64 = 1e-9;

        // Handle edge cases
        if a < 1.0 + EPS && b < 1.0 + EPS {
            return (0.5 - width / 2.0, 0.5 + width / 2.0);
        }
        if a < 1.0 + EPS && b > 1.0 {
            return (0.0, width);
        }
        if a > 1.0 && b < 1.0 + EPS {
            return (1.0 - width, 1.0);
        }
        if width > 1.0 - EPS {
            return (0.0, 1.0);
        }
        if (a - b).abs() < EPS {
            return (0.5 - width / 2.0, 0.5 + width / 2.0);
        }

        // Find HDI by searching for equal density at boundaries
        let mode = (a - 1.0) / (a + b - 2.0);
        let l = Self::binary_search(
            |x| {
                Self::denormalized_log_beta_pdf(a, b, x)
                    - Self::denormalized_log_beta_pdf(a, b, x + width)
            },
            f64::max(0.0, mode - width),
            f64::min(mode, 1.0 - width),
        );
        let r = l + width;

        (l, r)
    }

    /// Computes the log of the denormalized Beta PDF
    fn denormalized_log_beta_pdf(a: f64, b: f64, x: f64) -> f64 {
        if !(0.0..=1.0).contains(&x) {
            return f64::NEG_INFINITY;
        }

        if x < 1e-9 {
            if a > 1.0 {
                return f64::NEG_INFINITY;
            }
            if (a - 1.0).abs() < 1e-9 {
                return b.ln();
            }
            return f64::INFINITY;
        }

        if x > 1.0 - 1e-9 {
            if b > 1.0 {
                return f64::NEG_INFINITY;
            }
            if (b - 1.0).abs() < 1e-9 {
                return a.ln();
            }
            return f64::INFINITY;
        }

        if a < 1e-9 || b < 1e-9 {
            return f64::NEG_INFINITY;
        }

        (a - 1.0) * x.ln() + (b - 1.0) * (1.0 - x).ln()
    }
}

impl QuantileEstimator for TrimmedHarrellDavisQuantileEstimator {
    fn quantile(&self, sample: &Samples, probability: f64) -> f64 {
        let n = sample.weighted_size();
        let a = (n + 1.0) * probability;
        let b = (n + 1.0) * (1.0 - probability);
        let distribution = BetaDistribution::new(a, b);
        let d = (self.get_interval_width)(n);
        let hdi = Self::get_beta_hdi(a, b, d);
        let hdi_cdf_l = distribution.cdf(hdi.0);
        let hdi_cdf_r = distribution.cdf(hdi.1);

        let cdf = |x: f64| -> f64 {
            if x <= hdi.0 {
                0.0
            } else if x > hdi.1 {
                1.0
            } else {
                (distribution.cdf(x) - hdi_cdf_l) / (hdi_cdf_r - hdi_cdf_l)
            }
        };

        let mut c1 = 0.0;
        let mut beta_cdf_right = 0.0;
        let mut current_probability;

        if sample.is_weighted() {
            current_probability = 0.0;
            let weights = sample.sorted_weights.as_ref().unwrap();
            for (&weight, &value) in weights.iter().zip(sample.values.iter()) {
                let beta_cdf_left = beta_cdf_right;
                current_probability += weight / sample.total_weight;

                beta_cdf_right = cdf(current_probability);
                let w = beta_cdf_right - beta_cdf_left;
                c1 += w * value;
            }
        } else {
            let j_l = (hdi.0 * sample.len() as f64).floor() as usize;
            let j_r = (hdi.1 * sample.len() as f64).ceil() as usize - 1;

            for j in j_l..=j_r {
                let beta_cdf_left = beta_cdf_right;
                current_probability = (j + 1) as f64 / sample.len() as f64;

                let cdf_value = cdf(current_probability);
                beta_cdf_right = cdf_value;
                let w = beta_cdf_right - beta_cdf_left;
                c1 += w * sample.values[j];
            }
        }

        c1
    }

    fn supports_weighted_samples(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;

    pub fn test_beta_hdi() {
        let cases = [
            [10.0, 0.0, 0.3, 0.7, 1.0],
            [0.0, 10.0, 0.3, 0.0, 0.3],
            [1.0, 1.0, 0.5, 0.25, 0.75],
            [3.0, 3.0, 0.3, 0.35, 0.65],
            [7.0, 3.0, 0.3, 0.5797299, 0.8797299],
            [7.0, 13.0, 0.3, 0.1947799, 0.4947799],
        ];

        for case in cases.iter() {
            let hdi = TrimmedHarrellDavisQuantileEstimator::get_beta_hdi(case[0], case[1], case[2]);
            assert!((case[3] - hdi.0).abs() < EPSILON);
            assert!((case[4] - hdi.1).abs() < EPSILON);
        }
    }

    #[test]
    fn test_get_beta_hdi_symmetric() {
        let (l, r) = TrimmedHarrellDavisQuantileEstimator::get_beta_hdi(2.0, 2.0, 0.5);
        assert!((l - 0.25).abs() < EPSILON);
        assert!((r - 0.75).abs() < EPSILON);
    }

    #[test]
    fn test_denormalized_log_beta_pdf() {
        let result = TrimmedHarrellDavisQuantileEstimator::denormalized_log_beta_pdf(2.0, 2.0, 0.5);
        assert!(result.is_finite());
    }

    #[test]
    fn test_estimation_01() {
        let values = vec![-3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0];
        let sample = Samples::new(values);
        let probabilities = vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
        let estimator = TrimmedHarrellDavisQuantileEstimator::sqrt();

        let actual_quantiles: Vec<f64> = probabilities
            .iter()
            .map(|&p| estimator.quantile(&sample, p))
            .collect();

        let expected_quantiles = vec![
            -3.0,
            -2.72276083590394,
            -2.30045481668633,
            -1.66479731161074,
            -0.877210708467137,
            2.22044604925031e-16,
            0.877210708467138,
            1.66479731161074,
            2.30045481668633,
            2.72276083590394,
            3.0,
        ];

        for (actual, expected) in actual_quantiles.iter().zip(expected_quantiles.iter()) {
            assert!(
                (actual - expected).abs() < 1e-6,
                "Expected {}, got {}",
                expected,
                actual
            );
        }
    }
}
