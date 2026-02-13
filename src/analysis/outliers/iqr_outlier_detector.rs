use super::utils::{get_anomaly_direction, normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::{
    Anomaly, AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo, OutlierDetector,
};

pub const IQR_DEFAULT_THRESHOLD: f64 = 1.5;

/// Interquartile Range (IQR) outlier detector
#[derive(Debug)]
pub struct IQROutlierDetector {
    lower_fence: f64,
    upper_fence: f64,
    iqr: f64,
    threshold: f64,
}

impl IQROutlierDetector {
    pub fn new(ts: &[f64], threshold: f64) -> Self {
        let n = ts.len();

        // Calculate quartiles
        let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
        sorted_values.sort_by(|&a, &b| a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal));

        let q1_idx = n / 4;
        let q3_idx = 3 * n / 4;
        let q1 = sorted_values[q1_idx];
        let q3 = sorted_values[q3_idx];
        let iqr = q3 - q1;

        let lower_fence = q1 - threshold * iqr;
        let upper_fence = q3 + threshold * iqr;

        IQROutlierDetector {
            lower_fence,
            upper_fence,
            iqr,
            threshold,
        }
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores: Vec<f64> = Vec::with_capacity(n);
        let mut anomalies: Vec<Anomaly> = Vec::with_capacity(4);

        for (index, &v) in ts.iter().enumerate() {
            let value = if v.is_nan() { 0.0 } else { v };
            let score = self.get_anomaly_score(value);

            let signal = self.classify(value);
            if signal.is_anomaly() {
                anomalies.push(Anomaly {
                    index,
                    signal,
                    value: v,
                    score,
                });
            }
            scores.push(score);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.threshold,
            method: AnomalyMethod::InterquartileRange,
            method_info: Some(MethodInfo::Fenced {
                lower_fence: self.lower_fence,
                upper_fence: self.upper_fence,
                center_line: Some((self.lower_fence + self.upper_fence) / 2.0),
            }),
        })
    }
}

impl OutlierDetector for IQROutlierDetector {
    fn get_anomaly_score(&self, value: f64) -> f64 {
        // Guard against degenerate IQR to avoid division by zero / NaN.
        if !self.iqr.is_finite() || self.iqr <= f64::EPSILON {
            return 0.0;
        }

        let raw = if value < self.lower_fence {
            (self.lower_fence - value) / self.iqr
        } else if value > self.upper_fence {
            (value - self.upper_fence) / self.iqr
        } else {
            0.0
        };

        normalize_unbounded_score(raw)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        let value = if x.is_nan() { 0.0 } else { x };
        get_anomaly_direction(self.lower_fence, self.upper_fence, value)
    }
}

/// Interquartile Range (IQR) anomaly detection
pub(super) fn detect_anomalies_iqr(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector: IQROutlierDetector =
        IQROutlierDetector::new(ts, threshold.unwrap_or(IQR_DEFAULT_THRESHOLD));
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use crate::analysis::outliers::MethodInfo;
    use crate::analysis::outliers::iqr_outlier_detector::detect_anomalies_iqr;

    #[test]
    fn test_iqr_anomaly_detection() {
        let mut ts = vec![1.0; 100];
        ts[50] = 10.0; // Clear outlier

        let result = detect_anomalies_iqr(&ts, Some(1.5)).unwrap();

        assert_eq!(result.anomalies.len(), 1);
        assert!(
            result.anomalies[0].is_anomaly(),
            "Should detect anomaly at index 50"
        );
        assert_eq!(
            result.anomalies[0].index, 50,
            "Should detect anomaly at index 50"
        );
    }

    #[test]
    fn test_iqr_constant_series() {
        // All values are the same - no outliers
        let values = vec![5.0; 100];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert_eq!(
            result.anomalies.len(),
            0,
            "Constant series should have no outliers"
        );
    }

    #[test]
    fn test_iqr_score_normalization() {
        let values = vec![1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        // All scores should be non-negative and finite
        for score in &result.scores {
            assert!(score.is_finite(), "Score should be finite");
            assert!(*score >= 0.0, "Score should be non-negative");
        }
    }

    #[test]
    fn test_iqr_both_direction_outliers() {
        // Series with both high and low outliers
        let values = vec![
            50.0, 51.0, 49.0, 52.0, 48.0,  // Normal range
            100.0, // High outlier
            50.0, 49.0, 51.0, // Normal range
            5.0,  // Low outlier
            50.0, 52.0, // Normal range
        ];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert_eq!(result.anomalies.len(), 2, "Should detect two outliers");
        // Check high outlier
        assert!(
            result.anomalies[0].is_positive(),
            "Expected positive outlier at index 5"
        );
        assert_eq!(
            result.anomalies[0].index, 5,
            "Outlier should have occurred at index 5"
        );
        assert_eq!(
            result.anomalies[0].value, 100.0,
            "Should detect 100.0 as high outlier"
        );

        // Check low outlier
        assert!(
            result.anomalies[1].is_negative(),
            "Expected negative outlier at index 9"
        );
        assert_eq!(
            result.anomalies[1].index, 9,
            "Outlier should have occurred at index 9"
        );
        assert_eq!(
            result.anomalies[1].value, 5.0,
            "Should detect 5.0 as low outlier"
        );
    }

    #[test]
    fn test_iqr_edge_values_near_fences() {
        // Values just inside the fences should not be anomalies
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        if let Some(MethodInfo::Fenced {
            lower_fence,
            upper_fence,
            ..
        }) = result.method_info
        {
            // Check that extreme values in the dataset are classified correctly
            // relative to the fences
            for (i, &value) in values.iter().enumerate() {
                let is_outside = value < lower_fence || value > upper_fence;
                if is_outside {
                    assert!(
                        result.anomalies.iter().any(|a| a.index == i),
                        "Value {value} at index {i} should be an anomaly",
                    );
                    continue;
                } else {
                    assert!(
                        result.anomalies.iter().all(|a| a.index != i),
                        "Value {value} at index {i} should not be an anomaly"
                    );
                }
            }
        }
    }

    #[test]
    fn test_iqr_large_dataset() {
        // Test with a larger dataset (100+ points)
        let mut values: Vec<f64> = (0..100).map(|i| 50.0 + (i as f64 * 0.1).sin()).collect();

        // Add outliers
        values[25] = 100.0;
        values[75] = 0.0;

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        assert_eq!(result.anomalies.len(), 2);
        assert!(
            result.anomalies[0].is_positive(),
            "Expected anomaly at index 25"
        );
        assert_eq!(
            result.anomalies[0].index, 25,
            "Expected anomaly at index 25"
        );
        assert!(
            result.anomalies[1].is_negative(),
            "Expected anomaly at index 75"
        );
        assert_eq!(
            result.anomalies[1].index, 75,
            "Expected anomaly at index 75"
        );
    }

    #[test]
    fn test_iqr_zero_iqr_edge_case() {
        // When all values are the same, IQR = 0
        // This tests the degenerate case handling
        let values = vec![42.0; 50];

        let result = detect_anomalies_iqr(&values, Some(1.5)).unwrap();

        // Should handle gracefully without division by zero
        for score in &result.scores {
            assert!(score.is_finite());
            assert_eq!(*score, 0.0, "All scores should be zero for constant series");
        }
    }
}
