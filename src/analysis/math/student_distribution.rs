use crate::analysis::math::beta_function::{
    beta_regularized_incomplete_inverse_value, beta_regularized_incomplete_value,
};
use crate::analysis::math::gamma_function::log_gamma;
use std::f64::consts::PI;

#[derive(Debug, Clone, Copy)]
pub struct StudentDistribution {
    df: f64,
}

impl StudentDistribution {
    pub fn new(df: f64) -> Self {
        Self { df }
    }

    pub fn pdf(&self, x: f64) -> f64 {
        let df2 = (self.df + 1.0) / 2.0;
        let term1 = log_gamma(df2) - log_gamma(self.df / 2.0);
        let term2 = (1.0 + sqr(x) / self.df).ln() * -df2;
        (term1 + term2).exp() / (PI * self.df).sqrt()
    }

    pub fn cdf(&self, x: f64) -> f64 {
        let p = 0.5
            * beta_regularized_incomplete_value(0.5 * self.df, 0.5, self.df / (self.df + sqr(x)));
        if x > 0.0 { 1.0 - p } else { p }
    }

    pub fn quantile(&self, p: f64) -> f64 {
        let x = beta_regularized_incomplete_inverse_value(0.5 * self.df, 0.5, 2.0 * p.min(1.0 - p));
        let x = (self.df * (1.0 - x) / x).sqrt();
        if p >= 0.5 { x } else { -x }
    }

    pub fn mean(&self) -> f64 {
        if self.df > 1.0 { 0.0 } else { f64::NAN }
    }

    pub fn median(&self) -> f64 {
        0.0
    }

    pub fn variance(&self) -> f64 {
        if self.df > 2.0 {
            self.df / (self.df - 2.0)
        } else if self.df > 1.0 {
            f64::INFINITY
        } else {
            f64::NAN
        }
    }

    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }
}

fn regularized_incomplete_beta_inv(a: f64, b: f64, p: f64) -> f64 {
    beta_regularized_incomplete_value(a, b, p)
}

#[inline]
fn sqr(x: f64) -> f64 {
    x * x
}

impl std::fmt::Display for StudentDistribution {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Student({})", self.df)
    }
}
