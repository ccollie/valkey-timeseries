use crate::common::hash::IntSet;
use anofox_forecast::features::Feature;

/// All statistical measures computed for a time series.
#[derive(Debug, Default, Clone)]
pub struct SeriesStats {
    pub length: usize,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub max: f64,
    pub range: f64,
    pub median: f64,
    pub n_nans: usize,
    pub n_zeros: usize,
    pub n_positive: usize,
    pub n_negative: usize,
    pub n_unique_values: usize,
    pub is_constant: bool,
    pub plateau_size: usize,
    pub plateau_size_non_zero: usize,
    pub n_zeros_start: usize,
    pub n_zeros_end: usize,
    pub skewness: f64,
    pub kurtosis: f64,
}

/// Compute all statistical measures from the sample slice.
pub fn calculate_stats(values: &[f64]) -> SeriesStats {
    let mut stats = SeriesStats::default();

    if values.is_empty() {
        return stats;
    }

    let mut unique_values: IntSet<i64> = IntSet::default();

    let mut n_zeros: usize = 0;
    let mut n_positive: usize = 0;
    let mut n_negative: usize = 0;

    for &v in values {
        if v == 0.0 {
            n_zeros += 1;
        } else if v > 0.0 {
            n_positive += 1;
        } else {
            n_negative += 1;
        }
        unique_values.insert(v.to_bits() as i64);
    }

    stats.n_zeros = n_zeros;
    stats.n_positive = n_positive;
    stats.n_negative = n_negative;
    stats.n_unique_values = unique_values.len();
    stats.is_constant = unique_values.len() == 1;

    calc_central_tendency(values, &mut stats);
    calc_plateaus(values, &mut stats);
    calc_leading_trailing_zeros(values, &mut stats);

    stats.range = stats.max - stats.min;

    stats
}

/// Central tendency and dispersion: mean, std, min, max, median, skewness, kurtosis.
fn calc_central_tendency(values: &[f64], stats: &mut SeriesStats) {
    if values.is_empty() {
        return;
    }

    stats.mean = moments::mean(values);
    stats.std = moments::standard_deviation(values);
    stats.min = Feature::Minimum.compute(values);
    stats.max = Feature::Maximum.compute(values);
    stats.median = Feature::Median.compute(values);
    stats.skewness = moments::skewness(values);
    stats.kurtosis = moments::kurtosis(values);
}

/// Longest consecutive run of identical values:
/// plateauSize, plateauSizeNonZero.
fn calc_plateaus(samples: &[f64], stats: &mut SeriesStats) {
    let mut max_plateau: usize = 0;
    let mut max_nonzero_plateau: usize = 0;
    let mut run_start: usize = 0;

    for i in 1..samples.len() {
        if samples[i - 1] != samples[i] {
            let run_len = i - run_start;
            let run_val = samples[run_start];

            max_plateau = max_plateau.max(run_len);
            if run_val != 0.0 {
                max_nonzero_plateau = max_nonzero_plateau.max(run_len);
            }
            run_start = i;
        }
    }

    // Final run
    let run_len = samples.len() - run_start;
    let run_val = samples[run_start];
    max_plateau = max_plateau.max(run_len);
    if run_val != 0.0 {
        max_nonzero_plateau = max_nonzero_plateau.max(run_len);
    }

    stats.plateau_size = max_plateau;
    stats.plateau_size_non_zero = max_nonzero_plateau;
}

/// Leading and trailing zeros
fn calc_leading_trailing_zeros(samples: &[f64], stats: &mut SeriesStats) {
    stats.n_zeros_start = samples.iter().take_while(|&&v| v == 0.0).count();

    stats.n_zeros_end = samples.iter().rev().take_while(|&&v| v == 0.0).count();
}

/// Moment statistics computed in f64.
///
/// anofox computes the mean, variance, sums and energy through f32 SIMD kernels (a mean of 18.26
/// comes back as 18.259998…), and its skewness and kurtosis plug the population standard
/// deviation into the bias-adjusted formulas, which expect the sample one, inflating both.
/// Commands report these instead.
pub mod moments {
    pub fn sum(values: &[f64]) -> f64 {
        values.iter().sum()
    }

    pub fn mean(values: &[f64]) -> f64 {
        if values.is_empty() {
            return f64::NAN;
        }
        sum(values) / values.len() as f64
    }

    fn sum_of_squared_deviations(values: &[f64]) -> f64 {
        let m = mean(values);
        values.iter().map(|x| (x - m) * (x - m)).sum()
    }

    /// Population variance (divides by `n`).
    pub fn variance(values: &[f64]) -> f64 {
        match values.len() {
            0 => f64::NAN,
            1 => 0.0,
            n => sum_of_squared_deviations(values) / n as f64,
        }
    }

    /// Sample variance (divides by `n - 1`).
    pub fn variance_sample(values: &[f64]) -> f64 {
        match values.len() {
            0 | 1 => f64::NAN,
            n => sum_of_squared_deviations(values) / (n - 1) as f64,
        }
    }

    /// Population standard deviation.
    pub fn standard_deviation(values: &[f64]) -> f64 {
        variance(values).sqrt()
    }

    pub fn abs_energy(values: &[f64]) -> f64 {
        values.iter().map(|x| x * x).sum()
    }

    pub fn root_mean_square(values: &[f64]) -> f64 {
        if values.is_empty() {
            return f64::NAN;
        }
        (abs_energy(values) / values.len() as f64).sqrt()
    }

    /// Standard deviations below this count as a constant series.
    const CONSTANT_STD: f64 = 1e-10;

    /// Adjusted Fisher–Pearson skewness (G1, as Excel's `SKEW` and pandas report it); NaN below
    /// 3 values, 0 for a constant series.
    pub fn skewness(values: &[f64]) -> f64 {
        let n = values.len() as f64;
        if values.len() < 3 {
            return f64::NAN;
        }
        let s = variance_sample(values).sqrt();
        if s < CONSTANT_STD {
            return 0.0;
        }
        let m = mean(values);
        let cubes: f64 = values.iter().map(|x| ((x - m) / s).powi(3)).sum();
        n / ((n - 1.0) * (n - 2.0)) * cubes
    }

    /// Bias-adjusted excess kurtosis (G2, as Excel's `KURT` and pandas report it); NaN below 4
    /// values or for a constant series.
    pub fn kurtosis(values: &[f64]) -> f64 {
        let n = values.len() as f64;
        if values.len() < 4 {
            return f64::NAN;
        }
        let s = variance_sample(values).sqrt();
        if s < CONSTANT_STD {
            return f64::NAN;
        }
        let m = mean(values);
        let fourths: f64 = values.iter().map(|x| ((x - m) / s).powi(4)).sum();
        n * (n + 1.0) / ((n - 1.0) * (n - 2.0) * (n - 3.0)) * fourths
            - 3.0 * (n - 1.0).powi(2) / ((n - 2.0) * (n - 3.0))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn mean_is_exact_in_f64() {
            assert_eq!(mean(&[18.26, 18.26, 18.26]), 18.26);
        }

        #[test]
        fn skewness_and_kurtosis_match_the_adjusted_formulas() {
            // G1 and G2 of [1, 2, 3, 4, 10], computed by hand from the sample moments.
            let data = [1.0, 2.0, 3.0, 4.0, 10.0];
            assert!((skewness(&data) - 1.697056274847714).abs() < 1e-12);
            assert!((kurtosis(&data) - 3.152).abs() < 1e-12);
        }

        #[test]
        fn small_and_constant_inputs() {
            assert!(skewness(&[1.0, 2.0]).is_nan());
            assert!(kurtosis(&[1.0, 2.0, 3.0]).is_nan());
            assert_eq!(skewness(&[5.0; 10]), 0.0);
            assert!(kurtosis(&[5.0; 10]).is_nan());
            assert_eq!(variance(&[3.0]), 0.0);
            assert!(variance_sample(&[3.0]).is_nan());
        }
    }
}
