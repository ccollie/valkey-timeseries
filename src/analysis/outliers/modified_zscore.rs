use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::anomalies::normalize_value;
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal};

pub const MODIFIED_ZSCORE_DEFAULT_THRESHOLD: f64 = 3.5;

/// Modified Z-score using median absolute deviation
pub(super) fn detect_anomalies_modified_zscore(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let threshold = threshold.unwrap_or(MODIFIED_ZSCORE_DEFAULT_THRESHOLD);

    // Calculate median
    let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let median = if n.is_multiple_of(2) {
        (sorted_values[n / 2 - 1] + sorted_values[n / 2]) / 2.0
    } else {
        sorted_values[n / 2]
    };

    // Calculate Mad (Median Absolute Deviation)
    let mut abs_deviations: Vec<f64> = ts
        .iter()
        .map(|&x| (normalize_value(x) - median).abs())
        .collect();
    abs_deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let half = n / 2;
    let mad = if n.is_multiple_of(2) {
        (abs_deviations[half - 1] + abs_deviations[half]) / 2.0
    } else {
        abs_deviations[half]
    };

    // Scale Mad for consistency with normal distribution
    let mad_scaled = mad / 0.6745;

    let mut scores = Vec::with_capacity(n);
    let mut anomalies = Vec::with_capacity(n);

    for &v in ts {
        let value = normalize_value(v);
        let modified_zscore = if mad_scaled > 1e-10 {
            0.6745 * (value - median) / mad
        } else {
            0.0
        };
        let score = modified_zscore.abs();
        scores.push(score);

        let anomaly_direction = if score > threshold {
            // use value > median instead of modified_zscore > 0.0 to avoid issues when mad_scaled is very small ?????
            if modified_zscore > 0.0 {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        };
        anomalies.push(anomaly_direction);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::ModifiedZScore,
        method_info: None,
    })
}

#[cfg(test)]
mod tests {
    use super::detect_anomalies_modified_zscore;

    #[test]
    fn test_modified_zscore() {
        let ts = vec![1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]; // 10.0 is an outlier

        let result = detect_anomalies_modified_zscore(&ts, Some(3.5)).unwrap();

        // Should detect the outlier at index 5
        assert!(
            result.anomalies[5].is_anomaly(),
            "Should detect anomaly at index 5"
        );
        assert!(result.scores[5] > 3.5);
    }
}
