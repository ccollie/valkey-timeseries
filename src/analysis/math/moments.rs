use std::f64;
use std::ops::Deref;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Moments {
    pub mean: f64,
    pub variance: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub standard_deviation: f64,
}

impl Moments {
    pub fn new(mean: f64, variance: f64, skewness: f64, kurtosis: f64) -> Moments {
        let standard_deviation = variance.sqrt();
        Moments {
            mean,
            variance,
            skewness,
            kurtosis,
            standard_deviation,
        }
    }

    pub fn create<T: Deref<Target = [f64]>>(values: T) -> Moments {
        debug_assert!(!values.is_empty(), "moments: values must not be empty");
        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;

        let variance = if n == 1.0 {
            0.0
        } else {
            values.iter().map(|&d| (d - mean).powi(2)).sum::<f64>() / (n - 1.0)
        };

        fn calc_central_moment(values: &[f64], mean: f64, k: i32) -> f64 {
            values.iter().map(|&x| (x - mean).powi(k)).sum::<f64>() / values.len() as f64
        }

        let skewness = if variance == 0.0 {
            0.0
        } else {
            calc_central_moment(&values, mean, 3) / variance.powf(1.5)
        };

        let kurtosis = if variance == 0.0 {
            0.0
        } else {
            calc_central_moment(&values, mean, 4) / variance.powi(2)
        };

        Moments::new(mean, variance, skewness, kurtosis)
    }
}
