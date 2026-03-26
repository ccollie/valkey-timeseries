use std::f64::consts::PI;

pub fn gamma(x: f64) -> f64 {
    debug_assert!(x > 1e-5, "gamma: x should be positive");

    // For small x, use recurrence: gamma(x) = gamma(x+1) / x
    if x < 1.0 {
        return stirling_approximation(x + 3.0) / x / (x + 1.0) / (x + 2.0);
    }
    if x < 2.0 {
        return stirling_approximation(x + 2.0) / x / (x + 1.0);
    }
    if x < 3.0 {
        return stirling_approximation(x + 1.0) / x;
    }

    stirling_approximation(x)
}

pub fn log_gamma(x: f64) -> f64 {
    debug_assert!(x > 1e-5, "log_gamma: x should be positive");

    if x < 1.0 {
        return stirling_approximation_log(x + 3.0) - (x * (x + 1.0) * (x + 2.0)).ln();
    }
    if x < 2.0 {
        return stirling_approximation_log(x + 2.0) - (x * (x + 1.0)).ln();
    }
    if x < 3.0 {
        return stirling_approximation_log(x + 1.0) - x.ln();
    }

    stirling_approximation_log(x)
}

// sum = sum(b[2*n] / (2n * (2n-1) * x^(2n-1)))
fn get_series_value(x: f64) -> f64 {
    // Bernoulli numbers
    const B2: f64 = 1.0 / 6.0;
    const B4: f64 = -1.0 / 30.0;
    const B6: f64 = 1.0 / 42.0;
    const B8: f64 = -1.0 / 30.0;
    const B10: f64 = 5.0 / 66.0;

    B2 / 2.0 / x
        + B4 / 12.0 / (x * x * x)
        + B6 / 30.0 / (x * x * x * x * x)
        + B8 / 56.0 / (x * x * x * x * x * x * x)
        + B10 / 90.0 / (x * x * x * x * x * x * x * x * x)
}

fn stirling_approximation(x: f64) -> f64 {
    let e = std::f64::consts::E;
    (2.0 * PI / x).sqrt() * (x / e).powf(x) * get_series_value(x).exp()
}

fn stirling_approximation_log(x: f64) -> f64 {
    x * x.ln() - x + 0.5 * (2.0 * PI / x).ln() + get_series_value(x)
}

#[cfg(test)]
mod tests {
    use super::{gamma, log_gamma};

    #[test]
    fn test_gamma_simple() {
        let gamma_5 = gamma(5.0);
        let gamma_4 = gamma(4.0);
        assert!((gamma_5 - 24.0).abs() < 1e-8);
        assert!((gamma_4 - 6.0).abs() < 1e-8);
    }

    #[test]
    fn test_gamma_log_simple() {
        let log_gamma_5 = log_gamma(5.0);
        assert!((log_gamma_5 - 24.0f64.ln()).abs() < 1e-8);
    }
}
