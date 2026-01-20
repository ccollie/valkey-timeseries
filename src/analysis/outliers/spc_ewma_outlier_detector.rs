use super::utils::{get_anomaly_direction, normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo};

/// Default alpha for Ewma SPC
pub const EWMA_DEFAULT_ALPHA: f64 = 0.3;
pub const EWMA_DEFAULT_MULTIPLIER: f64 = 3.0;

/// SPC Exponentially Weighted Moving Average (EWMA) outlier detector
#[derive(Debug)]
pub struct EwmaOutlierDetector {
    /// smoothing factor
    alpha: f64,
    /// target (process mean)
    target: f64,
    /// standard deviation of the process
    sigma: f64,
    /// number of standard deviations for control limits
    multiplier: f64,
}

impl EwmaOutlierDetector {
    pub fn new(alpha: f64, target: f64, sigma: f64) -> Self {
        EwmaOutlierDetector {
            alpha,
            target,
            sigma,
            multiplier: EWMA_DEFAULT_MULTIPLIER,
        }
    }

    pub fn from_series(ts: &[f64], alpha: f64) -> Self {
        let training_size = (ts.len() as f64 * 0.5).min(100.0) as usize;
        let training_data = &ts[0..training_size];

        let target = calculate_mean(training_data);
        let sigma = calculate_std_dev(training_data);
        let multiplier = 3.0;

        EwmaOutlierDetector {
            alpha,
            target,
            sigma,
            multiplier,
        }
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

        let mut ewma = self.target;

        for (i, &v) in ts.iter().enumerate() {
            let value = normalize_value(v);
            ewma = self.alpha * value + (1.0 - self.alpha) * ewma;

            let ewma_variance = self.sigma * self.sigma * self.alpha / (2.0 - self.alpha)
                * (1.0 - (1.0 - self.alpha).powi(2 * (i as i32 + 1)));
            let ewma_std = ewma_variance.sqrt();

            let distance = self.multiplier * ewma_std;
            let ucl = self.target + distance;
            let lcl = self.target - distance;

            let raw_score = if !ewma_std.is_finite() || ewma_std <= f64::EPSILON {
                0.0
            } else {
                (ewma - self.target).abs() / ewma_std
            };
            let score = normalize_unbounded_score(raw_score);

            let anomaly_direction = get_anomaly_direction(lcl, ucl, value);
            anomalies.push(anomaly_direction);

            scores.push(score);
        }

        let distance = self.multiplier * self.sigma;
        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.multiplier,
            method: AnomalyMethod::StatisticalProcessControl,
            method_info: Some(MethodInfo::Spc {
                control_limits: (self.target - distance, self.target + distance),
                center_line: self.target,
            }),
        })
    }
}
/// Statistical Process Control (Spc) anomaly detection
pub(super) fn detect_anomalies_spc_ewma(
    ts: &[f64],
    alpha: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    // Ewma control chart implementation
    let alpha = alpha.unwrap_or(EWMA_DEFAULT_ALPHA);
    let detector = EwmaOutlierDetector::from_series(ts, alpha);
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_basic_anomaly_detection() {
        // Create baseline data with normal variation
        let mut ts: Vec<f64> = (0..50)
            .map(|i| 1.0 + 0.1 * (i as f64 * 0.1).sin())
            .collect();
        // Add clear anomalies
        ts[30] = 5.0; // Positive spike
        ts[40] = -3.0; // Negative spike

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Should detect both anomalies
        assert!(
            result.anomalies[30].is_positive(),
            "Should detect positive anomaly at index 30"
        );
        assert!(
            result.anomalies[40].is_negative(),
            "Should detect negative anomaly at index 40"
        );

        // Anomalies should have high scores
        assert!(
            result.scores[30] > 0.8,
            "Positive anomaly score should be > 3.0"
        );
        assert!(
            result.scores[40] > 0.8,
            "Negative anomaly score should be > 3.0"
        );
    }

    #[test]
    fn test_ewma_default_alpha() {
        // Test with the default alpha value
        let mut ts: Vec<f64> = vec![1.0; 30];
        ts[15] = 10.0; // Single outlier

        // Default alpha (0.3
        let result = detect_anomalies_spc_ewma(&ts, None).unwrap();

        assert!(
            result.anomalies[15].is_anomaly(),
            "Should detect anomaly with default alpha"
        );
    }

    #[test]
    fn test_ewma_varying_alpha_sensitivity() {
        // Test how different alpha values affect sensitivity
        let mut ts: Vec<f64> = (0..40)
            .map(|i| 2.0 + 0.2 * (i as f64 * 0.2).cos())
            .collect();
        ts[20] = 4.5; // Moderate outlier

        // Low alpha (more smoothing, less reactive)
        let low_alpha_result = detect_anomalies_spc_ewma(&ts, Some(0.1)).unwrap();

        // High alpha (less smoothing, more reactive)
        let high_alpha_result = detect_anomalies_spc_ewma(&ts, Some(0.8)).unwrap();

        // High alpha should be more sensitive (higher score)
        assert!(
            high_alpha_result.scores[20] >= low_alpha_result.scores[20],
            "High alpha should produce higher or equal anomaly score"
        );
    }

    #[test]
    fn test_ewma_gradual_shift() {
        // Create a time series with a gradual shift (process drift)
        let mut ts: Vec<f64> = Vec::new();

        // Normal baseline (0-29)
        for i in 0..30 {
            ts.push(1.0 + 0.05 * (i as f64).sin());
        }

        // Gradual shift (30-59)
        for i in 30..60 {
            let shift = (i - 30) as f64 * 0.1; // Gradual increase
            ts.push(1.0 + shift + 0.05 * (i as f64).sin());
        }

        let result = detect_anomalies_spc_ewma(&ts, Some(0.2)).unwrap();

        // Should detect anomalies in the shifted region
        let shifted_anomalies = result.anomalies[35..]
            .iter()
            .filter(|&&x| x.is_anomaly())
            .count();

        assert!(
            shifted_anomalies > 10,
            "Should detect anomalies in shifted region, found {shifted_anomalies}"
        );
    }

    #[test]
    fn test_ewma_step_change() {
        // Test with abrupt step change
        let mut ts: Vec<f64> = vec![5.0; 40]; // start with stable values

        // Step change (40-79)
        ts.extend(std::iter::repeat_n(8.0, 40));

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Should detect anomalies right after the step change
        let anomalies_after_step = result.anomalies[40..50]
            .iter()
            .filter(|&&x| x.is_anomaly())
            .count();

        assert!(
            anomalies_after_step > 0,
            "Should detect anomalies after step change"
        );
    }

    #[test]
    fn test_ewma_constant_series() {
        // Test with perfectly constant series (no variation)
        let ts = vec![3.0; 50];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Should detect no anomalies in constant series
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();
        assert_eq!(
            anomaly_count, 0,
            "Should detect no anomalies in constant series"
        );
    }

    #[test]
    fn test_ewma_small_sample_size() {
        // Test with minimum viable sample size
        let ts = vec![1.0, 1.1, 1.0, 5.0, 1.0, 1.1];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.5)).unwrap();

        // Should detect the spike at index 3
        assert!(
            result.anomalies[3].is_anomaly(),
            "Should detect anomaly at index 3"
        );
    }

    #[test]
    fn test_ewma_alternating_pattern() {
        // Test with an alternating high-low pattern
        let ts: Vec<f64> = (0usize..40usize)
            .map(|i| if i.is_multiple_of(2) { 1.0 } else { 1.5 })
            .collect();

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Regular alternating pattern should not be flagged as anomalous
        // after initial stabilization
        let late_anomalies = result.anomalies[20..]
            .iter()
            .filter(|&&x| x.is_anomaly())
            .count();

        assert!(
            late_anomalies < 5,
            "Regular pattern should not produce many anomalies after stabilization"
        );
    }

    #[test]
    fn test_ewma_with_trend() {
        // Test with a linear upward trend
        let ts: Vec<f64> = (0..60)
            .map(|i| i as f64 * 0.1 + 0.05 * (i as f64).sin())
            .collect();

        let result = detect_anomalies_spc_ewma(&ts, Some(0.2)).unwrap();

        // Gradual trend may cause some anomalies, but not excessive
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();

        assert!(
            anomaly_count < ts.len() / 2,
            "Gradual trend should not cause excessive anomalies"
        );
    }

    #[test]
    fn test_ewma_method_info() {
        let ts = vec![1.0, 1.1, 1.0, 1.2, 1.1, 1.0];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3)).unwrap();

        // Verify method info is populated
        assert!(
            result.method_info.is_some(),
            "Method info should be present"
        );

        if let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = result.method_info
        {
            assert!(
                control_limits.0 < control_limits.1,
                "Lower limit should be less than upper"
            );
            assert!(center_line > 0.0, "Center line should be positive");
        } else {
            panic!("Expected SPC method info");
        }
    }

    #[test]
    fn test_ewma_multiple_anomalies() {
        // Test with multiple scattered anomalies
        let mut ts: Vec<f64> = (0..100)
            .map(|i| 2.0 + 0.1 * (i as f64 * 0.1).sin())
            .collect();
        // Add multiple anomalies
        ts[10] = 6.0;
        ts[30] = -2.0;
        ts[50] = 7.0;
        ts[80] = -1.0;

        let result = detect_anomalies_spc_ewma(&ts, Some(0.25)).unwrap();

        // Count detected anomalies
        let anomaly_count = result.anomalies.iter().filter(|&&x| x.is_anomaly()).count();

        assert!(
            anomaly_count >= 4,
            "Should detect at least 4 anomalies, found {anomaly_count}"
        );
    }

    #[test]
    fn test_ewma_nan_handling() {
        // Test with NaN values (should be normalized to 0.0)
        let ts = vec![1.0, 1.1, f64::NAN, 1.2, 1.0, f64::NAN, 1.1];

        let result = detect_anomalies_spc_ewma(&ts, Some(0.3));

        // Should not panic and should complete successfully
        assert!(result.is_ok(), "Should handle NaN values gracefully");
    }

    #[test]
    fn test_ewma_extreme_alpha_values() {
        let ts: Vec<f64> = (0..20).map(|i| 1.0 + 0.1 * i as f64).collect();

        // Very low alpha (0.01) - highly smoothed
        let low_result = detect_anomalies_spc_ewma(&ts, Some(0.01));
        assert!(low_result.is_ok(), "Should handle very low alpha");

        // Very high alpha (0.99) - minimal smoothing
        let high_result = detect_anomalies_spc_ewma(&ts, Some(0.99));
        assert!(high_result.is_ok(), "Should handle very high alpha");
    }
}
