pub fn calculate_std_dev(data: &[f64]) -> f64 {
    let n = data.len();
    if n <= 1 {
        return 0.0;
    }

    let mean = calculate_mean(data);
    let variance = data.iter().map(|&x| (x - mean) * (x - mean)).sum::<f64>() / (n - 1) as f64;

    variance.sqrt()
}

pub fn calculate_mean(data: &[f64]) -> f64 {
    let n = data.len();
    if n == 0 {
        return 0.0;
    }
    data.iter().sum::<f64>() / n as f64
}
