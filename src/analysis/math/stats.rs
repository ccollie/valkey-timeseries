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

/// Utility function to calculate mean and standard deviation of a slice of f64 values.
pub fn calculate_mean_std_dev(data: &[f64]) -> (f64, f64) {
    let n = data.len();
    if n <= 1 {
        return (0.0, 0.0);
    }

    let mean = calculate_mean(data);
    let variance = data.iter().map(|&x| (x - mean) * (x - mean)).sum::<f64>() / (n - 1) as f64;

    (mean, variance.sqrt())
}

pub fn calculate_std_dev(data: &[f64]) -> f64 {
    let (_mean, std_dev) = calculate_mean_std_dev(data);
    std_dev
}

pub fn calculate_mean(data: &[f64]) -> f64 {
    let n = data.len();
    if n == 0 {
        return 0.0;
    }
    data.iter().sum::<f64>() / n as f64
}

/// Compute variance.
pub fn calculate_variance(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let mean: f64 = values.iter().sum::<f64>() / n as f64;
    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1) as f64
}

pub fn calculate_median(data: &[f64]) -> f64 {
    let n = data.len();
    if n == 0 {
        return 0.0;
    }
    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    calculate_median_sorted(&sorted_data)
}

pub fn calculate_median_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return 0.0;
    }

    let half = n / 2;

    if n.is_multiple_of(2) {
        (sorted[half - 1] + sorted[half]) / 2.0
    } else {
        sorted[half]
    }
}

pub fn quantile(data: &[f64], q: f64) -> f64 {
    let n = data.len();
    if n == 0 {
        return 0.0;
    }
    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    quantile_sorted(&sorted_data, q)
}

pub fn quantile_sorted(sorted_data: &[f64], q: f64) -> f64 {
    let n = sorted_data.len();
    if n == 0 {
        return 0.0;
    }
    let rank = (n as f64 * q).floor() as usize;
    sorted_data[rank]
}
