use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::anomalies::normalize_value;
use crate::analysis::outliers::{
    AnomalyMethod, AnomalyResult, AnomalySignal, get_anomaly_direction,
};

pub const IQR_DEFAULT_THRESHOLD: f64 = 1.5;

/// Interquartile Range (IQR) anomaly detection
pub(super) fn detect_anomalies_iqr(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let multiplier = threshold.unwrap_or(IQR_DEFAULT_THRESHOLD);

    // Calculate quartiles
    let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
    sorted_values.sort_by(|&a, &b| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));

    let q1_idx = n / 4;
    let q3_idx = 3 * n / 4;
    let q1 = sorted_values[q1_idx];
    let q3 = sorted_values[q3_idx];
    let iqr = q3 - q1;

    let lower_bound = q1 - multiplier * iqr;
    let upper_bound = q3 + multiplier * iqr;

    let mut scores: Vec<f64> = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    for &v in ts {
        let value = if v.is_nan() { 0.0 } else { v };
        let score = if value < lower_bound {
            (lower_bound - value) / iqr
        } else if value > upper_bound {
            (value - upper_bound) / iqr
        } else {
            0.0
        };

        let anomaly_direction = get_anomaly_direction(lower_bound, upper_bound, value);
        anomalies.push(anomaly_direction);
        scores.push(score);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold: multiplier,
        method: AnomalyMethod::InterquartileRange,
        method_info: None,
    })
}

#[cfg(test)]
mod tests {
    use crate::analysis::outliers::iqr::detect_anomalies_iqr;

    #[test]
    fn test_iqr_anomaly_detection() {
        let mut ts = vec![1.0; 100];
        ts[50] = 10.0; // Clear outlier

        let result = detect_anomalies_iqr(&ts, Some(1.5)).unwrap();

        // Should detect the outlier
        assert!(
            result.anomalies[50].is_anomaly(),
            "Should detect anomaly at index 50"
        );
    }
}
