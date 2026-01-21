use super::utils::{normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{Anomaly, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo};

/// Statistical Process Control (Spc) cusum anomaly detection
pub struct CusumOutlierDetector {
    target: f64,
    std_dev: f64,
    k: f64,
    h: f64,
}

impl CusumOutlierDetector {
    pub fn new(mean: f64, std_dev: f64) -> Self {
        let k = 0.5 * std_dev; // Reference value
        let h = 5.0 * std_dev; // Decision interval

        CusumOutlierDetector {
            target: mean,
            k,
            h,
            std_dev,
        }
    }

    pub fn from_series(ts: &[f64]) -> Self {
        let training_size = (ts.len() as f64 * 0.5).min(100.0) as usize;
        let training_data = &ts[0..training_size];

        let target = calculate_mean(training_data);
        let std_dev = calculate_std_dev(training_data);

        Self::new(target, std_dev)
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        let mut cusum_pos = 0.0;
        let mut cusum_neg = 0.0;
        let threshold = self.h / self.std_dev;
        let valid_std_dev = self.std_dev.is_finite() && self.std_dev > f64::EPSILON;

        for (index, &v) in ts.iter().enumerate() {
            let value = normalize_value(v);
            cusum_pos = f64::max(0.0, cusum_pos + (value - self.target) - self.k);
            cusum_neg = f64::max(0.0, cusum_neg - (value - self.target) - self.k);

            let cusum_max = f64::max(cusum_pos, cusum_neg);
            let raw_score = if !valid_std_dev {
                0.0
            } else {
                cusum_max / self.std_dev
            };

            // normalize score to [0, 1].
            let score = normalize_unbounded_score(raw_score);
            scores.push(score);

            let signal = if cusum_pos > threshold {
                AnomalySignal::Positive
            } else if cusum_neg > threshold {
                AnomalySignal::Negative
            } else {
                AnomalySignal::None
            };

            if signal.is_anomaly() {
                let outlier = Anomaly {
                    index,
                    value: v,
                    signal,
                    score,
                };
                anomalies.push(outlier);
            }
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold,
            method: AnomalyMethod::StatisticalProcessControl,
            method_info: Some(MethodInfo::Spc {
                control_limits: (-self.h, self.h),
                center_line: self.target,
            }),
        })
    }
}

/// Statistical Process Control (Spc) cusum anomaly detection
pub(super) fn detect_anomalies_spc_cusum(ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let detector = CusumOutlierDetector::from_series(ts);
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_empty_series() {
        let detector = CusumOutlierDetector::new(0.0, 1.0);
        let anomaly_result = detector.detect(&[]).unwrap();

        assert!(anomaly_result.scores.is_empty());
        assert!(anomaly_result.anomalies.is_empty());
    }

    #[test]
    fn test_detect_constant_series() {
        let ts = vec![5.0; 10];
        let detector = CusumOutlierDetector::from_series(&ts);

        let anomaly_result = detector.detect(&ts).unwrap();

        assert_eq!(anomaly_result.scores.len(), 10);
        // All scores should be low (near zero) for constant values
        for score in &anomaly_result.scores {
            assert!(*score < 0.1);
        }

        // No anomalies should be detected
        assert_eq!(anomaly_result.anomalies.len(), 0);
    }

    #[test]
    fn test_detect_positive_shift() {
        let detector = CusumOutlierDetector::new(0.0, 1.0);
        let mut ts = vec![0.0; 10];
        ts.extend(vec![10.0; 10]); // Positive shift

        let anomaly_result = detector.detect(&ts).unwrap();

        // Should detect positive anomalies after the shift
        let positive_anomalies = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_positive())
            .count();

        assert!(positive_anomalies > 0);
    }

    #[test]
    fn test_detect_negative_shift() {
        let detector = CusumOutlierDetector::new(0.0, 1.0);
        let mut ts = vec![0.0; 10];
        ts.extend(vec![-10.0; 10]); // Negative shift

        let anomaly_result = detector.detect(&ts).unwrap();

        // Should detect negative anomalies after the shift
        let negative_anomalies = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_negative())
            .count();

        assert!(negative_anomalies > 0);
    }

    #[test]
    fn test_detect_anomalies_spc_cusum_with_positive_spike() {
        // Add variance to baseline to get valid std dev
        let mut ts = vec![10.0, 9.0, 11.0, 10.5, 9.5];
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 9.0]);
        ts.extend(vec![50.0; 10]); // Positive spike - much larger relative to baseline
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 8.0]);

        let result = detect_anomalies_spc_cusum(&ts);

        assert!(result.is_ok());
        let anomaly_result = result.unwrap();

        // Should detect positive anomalies during the spike
        let positive_count = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_positive())
            .count();

        assert!(
            positive_count > 0,
            "Expected positive anomalies to be detected"
        );
    }

    #[test]
    fn test_detect_anomalies_spc_cusum_with_negative_spike() {
        // Add variance to baseline
        let mut ts = vec![10.0, 9.0, 11.0, 10.5, 9.5];
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 9.0]);
        ts.extend(vec![-30.0; 10]); // Negative spike - much larger deviation
        ts.extend(vec![10.0, 9.0, 11.0, 10.5, 9.5, 8.0]);

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();

        // Should detect negative anomalies during the spike
        let negative_count = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_negative())
            .count();

        assert!(
            negative_count > 0,
            "Expected negative anomalies to be detected"
        );
    }

    #[test]
    fn test_detect_with_zero_std_dev() {
        let ts = vec![5.0, 5.0, 5.0, 5.0, 5.0, 4.9, 5.1, 5.0, 5.0];
        let detector = CusumOutlierDetector::from_series(&ts);

        let anomaly_result = detector.detect(&ts).unwrap();

        // All scores should be zero when std_dev is invalid
        for score in &anomaly_result.scores {
            assert_eq!(*score, 0.0);
        }
    }

    #[test]
    fn test_detect_with_nan_values() {
        let detector = CusumOutlierDetector::new(0.0, 1.0);
        let ts = vec![0.0, f64::NAN, 0.0];

        let anomaly_result = detector.detect(&ts).unwrap();

        // Should handle NaN values gracefully
        assert_eq!(anomaly_result.scores.len(), 3);
    }

    #[test]
    fn test_detect_scores_normalized() {
        let detector = CusumOutlierDetector::new(0.0, 1.0);
        let ts = vec![0.0, 5.0, 10.0, 15.0, 20.0];

        let anomaly_result = detector.detect(&ts).unwrap();

        // All scores should be in [0, 1] range
        for score in &anomaly_result.scores {
            assert!(*score >= 0.0 && *score <= 1.0);
        }
    }

    #[test]
    fn test_detect_method_metadata() {
        let detector = CusumOutlierDetector::new(10.0, 2.0);
        let ts = vec![10.0; 5];

        let anomaly_result = detector.detect(&ts).unwrap();

        assert_eq!(
            anomaly_result.method,
            AnomalyMethod::StatisticalProcessControl
        );
        assert!(anomaly_result.method_info.is_some());

        if let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = anomaly_result.method_info
        {
            assert_eq!(center_line, 10.0);
            assert_eq!(control_limits.0, -detector.h);
            assert_eq!(control_limits.1, detector.h);
        } else {
            panic!("Expected Spc method info");
        }
    }

    #[test]
    fn test_detect_threshold_calculation() {
        let std_dev = 2.0;
        let detector = CusumOutlierDetector::new(0.0, std_dev);
        let ts = vec![0.0; 5];

        let result = detector.detect(&ts);
        assert!(result.is_ok());
        let anomaly_result = result.unwrap();

        let expected_threshold = (5.0 * std_dev) / std_dev;
        assert_eq!(anomaly_result.threshold, expected_threshold);
    }

    #[test]
    fn test_detect_anomalies_spc_cusum_with_gradual_shift() {
        let mut ts: Vec<f64> = (0..50).map(|i| i as f64).collect();
        ts.extend((50..100).map(|i| (i + 50) as f64)); // Gradual upward shift

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();
        assert_eq!(anomaly_result.scores.len(), 100);

        // Should detect the shift
        let positive_count = anomaly_result
            .anomalies
            .iter()
            .filter(|&&a| a.is_positive())
            .count();

        assert!(positive_count > 0);
    }

    #[test]
    fn test_detect_anomalies_spc_cusum_normal_distribution() {
        // Test with normally distributed data
        let ts = vec![
            100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 103.0, 97.0, 100.0, 101.0, 99.0, 100.0, 102.0,
            98.0, 100.0, 101.0, 99.0, 100.0,
        ];

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();

        let anomaly_count = anomaly_result.anomalies.len();
        // Most values should not be anomalies in normal distribution
        let no_anomaly_count = ts.len() - anomaly_count;

        assert!(no_anomaly_count > ts.len() / 2);
    }

    #[test]
    fn test_detect_anomalies_spc_cusum_method_info() {
        let ts = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let anomaly_result = detect_anomalies_spc_cusum(&ts).unwrap();

        assert_eq!(
            anomaly_result.method,
            AnomalyMethod::StatisticalProcessControl
        );
        assert!(anomaly_result.method_info.is_some());

        if let Some(MethodInfo::Spc {
            control_limits,
            center_line,
        }) = anomaly_result.method_info
        {
            assert!(control_limits.0 < 0.0);
            assert!(control_limits.1 > 0.0);
            assert!(center_line.is_finite());
        } else {
            panic!("Expected Spc method info");
        }
    }
}
