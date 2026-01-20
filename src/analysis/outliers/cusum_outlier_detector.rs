use super::utils::{normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo};

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
        let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

        let mut cusum_pos = 0.0;
        let mut cusum_neg = 0.0;
        let threshold = self.h / self.std_dev;
        let valid_std_dev = self.std_dev.is_finite() && self.std_dev > f64::EPSILON;

        for &v in ts {
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

            let anomaly_direction = if cusum_pos > threshold {
                AnomalySignal::Positive
            } else if cusum_neg > threshold {
                AnomalySignal::Negative
            } else {
                AnomalySignal::None
            };

            anomalies.push(anomaly_direction);
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
