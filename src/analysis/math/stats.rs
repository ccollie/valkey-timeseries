/// Online mean/variance via Welford's method
/// Tracks distribution of m_t so we can z-score it.
#[derive(Debug, Clone)]
pub struct RunningStats {
    n: usize,
    mean: f64,
    mean_sq: f64, // sum of squares of deviations from the mean
}

impl RunningStats {
    pub fn new() -> Self {
        Self {
            n: 0,
            mean: 0.0,
            mean_sq: 0.0,
        }
    }

    pub fn update(&mut self, x: f64) {
        if x.is_nan() {
            return;
        }
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.mean_sq += delta * delta2;
    }

    pub fn count(&self) -> usize {
        self.n
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    pub fn variance(&self) -> f64 {
        if self.n > 1 {
            self.mean_sq / (self.n as f64 - 1.0)
        } else {
            0.0
        }
    }

    pub fn std(&self) -> f64 {
        self.variance().sqrt()
    }
}

fn sum_and_count_finite(data: &[f64]) -> (f64, usize) {
    data.iter().fold((0.0f64, 0usize), |(s, c), &x| {
        if x.is_finite() {
            (s + x, c + 1)
        } else {
            (s, c)
        }
    })
}

/// Utility function to calculate mean and standard deviation of a slice of f64 values.
pub fn calculate_mean_std_dev(data: &[f64]) -> (f64, f64) {
    let n = data.len();
    if n <= 1 {
        return (0.0, 0.0);
    }

    let mean = calculate_mean(data);
    let variance = calculate_variance(data);

    (mean, variance.sqrt())
}

pub fn calculate_std_dev(data: &[f64]) -> f64 {
    let (_mean, std_dev) = calculate_mean_std_dev(data);
    std_dev
}

pub fn calculate_mean(data: &[f64]) -> f64 {
    let (sum, count) = sum_and_count_finite(data);
    if count == 0 {
        0.0
    } else {
        sum / (count as f64)
    }
}

/// Compute variance.
pub fn calculate_variance(values: &[f64]) -> f64 {
    let finite: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    let n = finite.len();
    if n < 2 {
        return 0.0;
    }
    let mean = finite.iter().sum::<f64>() / (n as f64);
    finite.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / ((n - 1) as f64)
}

pub fn calculate_median(data: &[f64]) -> f64 {
    // Filter out NaN and infinities; keep existing sentinel behavior (0.0) for no valid data.
    let mut finite: Vec<f64> = data.iter().copied().filter(|x| x.is_finite()).collect();
    if finite.is_empty() {
        return 0.0;
    }
    // Safe to unwrap partial_cmp because all values are finite
    finite.sort_by(|a, b| a.partial_cmp(b).unwrap());
    calculate_median_sorted(&finite)
}

pub fn calculate_median_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return 0.0;
    }
    // Assumes `sorted` contains only finite values and is sorted ascending.
    let half = n / 2;
    if n.is_multiple_of(2) {
        (sorted[half - 1] + sorted[half]) / 2.0
    } else {
        sorted[half]
    }
}

pub fn quantile(data: &[f64], q: f64) -> f64 {
    // Validate q in [0.0, 1.0]; if invalid, keep sentinel behavior and return 0.0.
    if !(0.0..=1.0).contains(&q) {
        return 0.0;
    }
    // Filter finite values (ignore NaN and infinities)
    let mut finite: Vec<f64> = data.iter().copied().filter(|x| x.is_finite()).collect();
    if finite.is_empty() {
        return 0.0;
    }
    // Safe to unwrap partial_cmp because all values are finite
    finite.sort_by(|a, b| a.partial_cmp(b).unwrap());
    quantile_sorted(&finite, q)
}

pub fn quantile_sorted(sorted_data: &[f64], q: f64) -> f64 {
    let n = sorted_data.len();
    if n == 0 {
        return 0.0;
    }
    // Validate q in [0.0, 1.0]; if invalid, return sentinel 0.0
    if !(0.0..=1.0).contains(&q) {
        return 0.0;
    }

    // Use Hyndman & Fan type 7 interpolation (common default):
    // pos = (n - 1) * q
    // idx = floor(pos), frac = pos - idx
    let pos = (n - 1) as f64 * q;
    let lf = pos.floor();
    let idx = lf as usize;
    let frac = pos - lf;

    if idx + 1 < n {
        // interpolate between idx and idx+1
        sorted_data[idx] * (1.0 - frac) + sorted_data[idx + 1] * frac
    } else {
        // q == 1.0 -> return last element
        sorted_data[n - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_ignores_nan_and_inf() {
        let v = vec![1.0, 2.0, f64::NAN, f64::INFINITY, -f64::INFINITY, 3.0];
        assert_eq!(calculate_median(&v), 2.0);
    }

    #[test]
    fn median_only_nan_returns_zero() {
        let v = vec![f64::NAN, f64::NAN];
        assert_eq!(calculate_median(&v), 0.0);
    }

    #[test]
    fn quantile_basic_and_interpolate() {
        let v = vec![0.0, 10.0];
        assert_eq!(quantile(&v, 0.0), 0.0);
        assert_eq!(quantile(&v, 1.0), 10.0);
        assert_eq!(quantile(&v, 0.25), 2.5);
    }

    #[test]
    fn quantile_ignores_nan() {
        let v = vec![1.0, 2.0, f64::NAN, 3.0];
        assert_eq!(quantile(&v, 1.0), 3.0);
    }

    #[test]
    fn quantile_out_of_range_returns_zero() {
        let v = vec![1.0, 2.0, 3.0];
        assert_eq!(quantile(&v, -0.5), 0.0);
        assert_eq!(quantile(&v, 2.0), 0.0);
    }

    #[test]
    fn mean_ignores_nan() {
        let v = vec![1.0, f64::NAN, 3.0];
        assert_eq!(calculate_mean(&v), 2.0);
    }

    #[test]
    fn variance_sample() {
        let v = vec![1.0, 2.0, 3.0];
        assert_eq!(calculate_variance(&v), 1.0);
    }
}
