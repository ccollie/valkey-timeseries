use crate::analysis::math::beta_function::{beta_complete_log_value, beta_regularized_incomplete_inverse_value, beta_regularized_incomplete_value};
use std::fmt;

/// Probability type alias (for clarity)
pub type Probability = f64;

pub struct BetaDistribution {
    pub alpha: f64,
    pub beta: f64,
    median: Option<f64>,
}

impl BetaDistribution {
    pub fn new(alpha: f64, beta: f64) -> Self {
        debug_assert!(alpha >= 0.0, "Alpha must be non-negative");
        debug_assert!(beta >= 0.0, "Beta must be non-negative");
        BetaDistribution {
            alpha,
            beta,
            median: None,
        }
    }

    /// Probability density function
    pub fn pdf(&self, x: f64) -> f64 {
        if !(0.0..=1.0).contains(&x) {
            return 0.0;
        }

        if x < 1e-9 {
            if self.alpha > 1.0 {
                return 0.0;
            }
            if (self.alpha - 1.0).abs() < 1e-9 {
                return self.beta;
            }
            return f64::INFINITY;
        }

        if x > 1.0 - 1e-9 {
            if self.beta > 1.0 {
                return 0.0;
            }
            if (self.beta - 1.0).abs() < 1e-9 {
                return self.alpha;
            }
            return f64::INFINITY;
        }

        if self.alpha < 1e-9 || self.beta < 1e-9 {
            return 0.0;
        }

        ((self.alpha - 1.0) * x.ln()
            + (self.beta - 1.0) * (1.0 - x).ln()
            - beta_complete_log_value(self.alpha, self.beta))
            .exp()
    }

    /// Cumulative distribution function
    pub fn cdf(&self, x: f64) -> f64 {
        beta_regularized_incomplete_value(self.alpha, self.beta, x)
    }

    /// Quantile function (inverse CDF)
    pub fn quantile(&self, p: Probability) -> f64 {
        beta_regularized_incomplete_inverse_value(self.alpha, self.beta, p)
    }

    pub fn mean(&self) -> f64 {
        self.alpha / (self.alpha + self.beta)
    }

    pub fn median(&mut self) -> f64 {
        if let Some(med) = self.median {
            med
        } else {
            let med = self.quantile(0.5);
            self.median = Some(med);
            med
        }
    }

    pub fn variance(&self) -> f64 {
        self.alpha * self.beta
            / (self.alpha + self.beta).powi(2)
            / (self.alpha + self.beta + 1.0)
    }

    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }

    pub fn skewness(&self) -> f64 {
        2.0 * (self.beta - self.alpha)
            * (self.alpha + self.beta + 1.0).sqrt()
            / (self.alpha + self.beta + 2.0)
            / (self.alpha * self.beta).sqrt()
    }
}

impl fmt::Display for BetaDistribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Beta({},{})", self.alpha, self.beta)
    }
}