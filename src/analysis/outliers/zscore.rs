use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal};

pub const ZSCORE_DEFAULT_THRESHOLD: f64 = 3.0;

/// Squash an unbounded non-negative score into the range [0..1].
#[inline]
fn normalize_unbounded_score(score: f64) -> f64 {
    if !score.is_finite() || score <= 0.0 {
        0.0
    } else {
        score / (score + 1.0)
    }
}

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

        let normalized_score = normalize_unbounded_score(z_abs);
        scores.push(normalized_score);

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
        let score_25 = result.scores[25];
        let score_75 = result.scores[75];
        assert!(score_25 > 0.8);
        assert!(score_75 > 0.8);
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

    #[test]
    fn test_normalize_unbounded_score_range_and_edges() {
        // Range contract + edge handling.
        assert_eq!(normalize_unbounded_score(f64::NAN), 0.0);
        assert_eq!(normalize_unbounded_score(f64::INFINITY), 0.0);
        assert_eq!(normalize_unbounded_score(-1.0), 0.0);
        assert_eq!(normalize_unbounded_score(0.0), 0.0);

        let s = normalize_unbounded_score(1.0);
        assert!(s > 0.0 && s < 1.0, "Expected (0,1) for score=1.0, got {s}");
    }

    #[test]
    fn test_normalize_unbounded_score_monotonicity() {
        // Monotonic squashing: larger inputs must not produce smaller outputs.
        let a = normalize_unbounded_score(0.5);
        let b = normalize_unbounded_score(1.0);
        let c = normalize_unbounded_score(2.0);
        let d = normalize_unbounded_score(10.0);

        assert!(a < b, "Expected 0.5 < 1.0 mapping, got {a} vs {b}");
        assert!(b < c, "Expected 1.0 < 2.0 mapping, got {b} vs {c}");
        assert!(c < d, "Expected 2.0 < 10.0 mapping, got {c} vs {d}");
        assert!(
            d < 1.0,
            "Expected strict less than 1.0 for finite inputs, got {d}"
        );
    }

    #[test]
    fn test_scores_are_normalized_0_to_1() {
        // Any output score must be in [0..1], even for extreme values.
        let ts = [0.0, 0.1, 0.05, 1000.0, -1000.0, 0.02, 0.03];
        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();

        assert_eq!(result.scores.len(), ts.len());
        for (i, &s) in result.scores.iter().enumerate() {
            assert!(
                (0.0..=1.0).contains(&s),
                "Score out of range at index {i}: {s}"
            );
        }
    }

    #[test]
    fn test_anomalies_have_high_normalized_scores() {
        // Keep the original intent: anomalies should score "high" on the normalized scale.
        // Note: score cannot exceed 1.0 anymore, so check for closeness to 1 instead of > 3.
        let mut ts: Vec<f64> = (0..100).map(|i| (i as f64 / 10.0).sin()).collect();
        ts[25] = 5.0;
        ts[75] = -5.0;

        let result = detect_anomalies_zscore(&ts, Some(3.0)).unwrap();

        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert!(
            anomaly_count >= 2,
            "Should detect at least 2 anomalies, found {anomaly_count}"
        );

        assert!(
            result.scores[25] > 0.75,
            "Expected high normalized score at index 25, got {}",
            result.scores[25]
        );
        assert!(
            result.scores[75] > 0.75,
            "Expected high normalized score at index 75, got {}",
            result.scores[75]
        );
    }

    #[test]
    fn test_direction_and_score_zero_at_mean_like_points() {
        // Basic sanity: non-anomalous points near the mean should have low scores;
        // anomaly direction should still reflect the z-score sign.
        const TS: [f64; 6] = [0.0, 0.02, -0.01, 0.01, 6.0, -6.0];

        let result = detect_anomalies_zscore(&TS, Some(1.5)).unwrap();

        // Near-mean points should be low.
        for i in 0..4 {
            assert!(
                result.scores[i] < 0.5,
                "Expected low score near mean at index {i}, got {}",
                result.scores[i]
            );
        }

        // Extremes should be anomalies with directional signals.
        assert_eq!(
            result.anomalies[4],
            AnomalySignal::Positive,
            "Expected positive anomaly at index 4"
        );
        assert_eq!(
            result.anomalies[5],
            AnomalySignal::Negative,
            "Expected negative anomaly at index 5"
        );

        // And have "high" normalized scores.
        assert!(
            result.scores[4] > 0.6,
            "Expected high score at index 4, got {}",
            result.scores[4]
        );
        assert!(
            result.scores[5] > 0.6,
            "Expected high score at index 5, got {}",
            result.scores[5]
        );
    }
}
