use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal};

pub const ZSCORE_DEFAULT_THRESHOLD: f64 = 3.0;

/// Z-score based analysis detection using sample standard deviation
pub(super) fn detect_anomalies_zscore(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let threshold = threshold.unwrap_or(ZSCORE_DEFAULT_THRESHOLD);
    let mean = calculate_mean(ts);
    let std_dev = calculate_std_dev(ts);

    if std_dev < f64::EPSILON {
        // All values are identical; no anomalies can be detected
        return Ok(AnomalyResult {
            scores: vec![0.0; n],
            anomalies: vec![AnomalySignal::None; n],
            threshold,
            method: AnomalyMethod::ZScore,
            method_info: None,
        });
    }

    let mut scores = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    for &value in ts {
        let value = if value.is_nan() { 0.0 } else { value };
        let zscore = (value - mean) / std_dev;
        let z_abs = zscore.abs();

        scores.push(z_abs);
        let anomaly_direction = if z_abs > threshold {
            if zscore > 0.0 {
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
        method: AnomalyMethod::ZScore,
        method_info: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zscore_anomaly_detection() {
        // Create a time series with clear anomalies
        let mut ts: Vec<f64> = (0..100).map(|i| (i as f64 / 10.0).sin()).collect();
        ts[25] = 5.0; // Clear analysis
        ts[75] = -5.0; // Clear analysis

        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();

        // Should detect the two anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert!(
            anomaly_count >= 2,
            "Should detect at least 2 anomalies, found {anomaly_count}"
        );

        // Anomalies should have high scores
        assert!(result.scores[25] > 3.0);
        assert!(result.scores[75] > 3.0);
    }

    #[test]
    fn test_zscore_anomaly_detection_strong() {
        // Most values are ~0..1; anomalies are far away.
        const STRONG_ANOMALIES: [f64; 32] = [
            0.10, 0.05, 0.12, 0.08, 0.11, 0.09, 0.07, 0.10, 0.06, 0.08, 0.09, 0.11, 0.10, 0.07,
            0.08, 0.12, 0.09, 0.10, 0.08, 0.07, 6.00, // strong positive anomaly
            0.09, 0.11, 0.08, 0.10, 0.07, 0.09, 0.10, -6.00, // strong negative anomaly
            0.08, 0.09, 0.10,
        ];

        let result = detect_anomalies_zscore(&STRONG_ANOMALIES, Some(3.0)).unwrap();
        let anomalies = result
            .anomalies
            .iter()
            .zip(STRONG_ANOMALIES.iter())
            .filter(|&(&x, _)| x.is_anomaly())
            .collect::<Vec<_>>();
        assert_eq!(anomalies.len(), 2);

        let first = anomalies[0];
        let second = anomalies[1];
        assert_eq!(first.0.is_positive(), true);
        assert_eq!(*first.1, 6.00);
        assert_eq!(second.0.is_negative(), true);
        assert_eq!(*second.1, -6.00);
    }

    #[test]
    fn test_zscore_anomaly_detection_constant() {
        // Constant series (std dev = 0). No anomalies.
        const CONSTANT: [f64; 40] = [1.0; 40];

        let result = detect_anomalies_zscore(&CONSTANT, Some(3.0)).unwrap();

        // Should detect no anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert_eq!(
            anomaly_count, 0,
            "Should detect no anomalies in constant series"
        );
    }

    #[test]
    fn test_zscore_anomaly_detection_noisy_spike() {
        // “Mostly normal” Gaussian-ish values with a single spike.
        const NOISY_SPIKE: [f64; 30] = [
            -0.30, 0.05, 0.12, -0.18, 0.22, 0.09, -0.11, 0.04, 0.15, -0.07, 0.08, -0.02, 0.10,
            0.01, -0.05, 0.06, 0.00, 0.11, -0.09, 0.03, 3.50, // spike
            0.07, -0.04, 0.02, 0.09, -0.08, 0.05, 0.01, -0.03, 0.04,
        ];

        let result = detect_anomalies_zscore(&NOISY_SPIKE, Some(3.0)).unwrap();
        let mut anomaly_count = 0;
        let mut anomaly_index = 0;

        // Should detect the spike
        for (i, &signal) in result.anomalies.iter().enumerate() {
            if signal.is_anomaly() {
                anomaly_index = i;
                anomaly_count += 1;
            }
        }
        assert_eq!(anomaly_count, 1, "Should detect exactly one anomaly");
        assert_eq!(anomaly_index, 20, "Anomaly should be at index 20");
    }

    #[test]
    fn test_zcore_anomaly_detection_small_sample_size() {
        // Small n but valid (n >= 3). One outlier.
        const SMALL_SAMPLE_SIZE: [f64; 4] = [0.0, 0.1, 0.05, 5.0];

        // because of a small sample size, use a lower threshold.
        let result = detect_anomalies_zscore(&SMALL_SAMPLE_SIZE, Some(1.3)).unwrap();
        let mut anomaly_count = 0;
        let mut anomaly_index = 0;
        // Should detect the outlier
        for (i, &signal) in result.anomalies.iter().enumerate() {
            if signal.is_anomaly() {
                anomaly_index = i;
                anomaly_count += 1;
            }
        }
        assert_eq!(anomaly_count, 1, "Should detect exactly one anomaly");
        assert_eq!(anomaly_index, 3, "Anomaly should be at index 3");
    }
}
