use super::utils::{get_anomaly_direction, normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::{
    AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo, OutlierDetector,
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
        let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

        for &v in ts {
            let value = if v.is_nan() { 0.0 } else { v };
            let direction = self.classify(value);
            let score = self.get_anomaly_score(value);
            anomalies.push(direction);
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
