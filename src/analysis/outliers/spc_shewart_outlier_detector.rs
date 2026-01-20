use super::utils::{get_anomaly_direction, normalize_unbounded_score};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo};

/// Statistical Process Control (Spc) Shewhart outlier detector
pub struct SpcShewhartOutlierDetector {
    mean: f64,
    /// Standard Deviation
    std_dev: f64,
    /// Upper Control Limit
    ucl: f64,
    /// Lower Control Limit
    lcl: f64,
    /// Control limits multiplier (std deviations)
    multiplier: f64,
}

impl SpcShewhartOutlierDetector {
    pub const DEFAULT_THRESHOLD: f64 = 3.0;
    pub const DEFAULT_MULTIPLIER: f64 = 3.0;

    pub fn new(ts: &[f64], threshold: Option<f64>) -> Self {
        let mean = calculate_mean(ts);
        let std_dev = calculate_std_dev(ts);
        let multiplier = threshold.unwrap_or(Self::DEFAULT_MULTIPLIER);

        let ucl = mean + multiplier * std_dev;
        let lcl = mean - multiplier * std_dev;

        SpcShewhartOutlierDetector {
            mean,
            std_dev,
            ucl,
            lcl,
            multiplier,
        }
    }

    pub fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

        let is_std_dev_valid = self.std_dev.is_finite() && self.std_dev > f64::EPSILON;

        for &value in ts {
            let distance_from_center = (value - self.mean).abs();
            let raw_score = if is_std_dev_valid {
                distance_from_center / self.std_dev
            } else {
                0.0
            };
            let score = normalize_unbounded_score(raw_score);
            let anomaly = get_anomaly_direction(self.lcl, self.ucl, value);
            scores.push(score);
            anomalies.push(anomaly);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.multiplier,
            method: AnomalyMethod::StatisticalProcessControl,
            method_info: Some(MethodInfo::Spc {
                control_limits: (self.lcl, self.ucl),
                center_line: self.mean,
            }),
        })
    }
}

/// Statistical Process Control (Spc) Shewart anomaly detection
pub(super) fn detect_anomalies_spc_shewart(ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let mut detector = SpcShewhartOutlierDetector::new(ts, None);
    detector.detect(ts)
}
