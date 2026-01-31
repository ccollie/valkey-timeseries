use super::utils::{normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::outliers::{
    Anomaly, AnomalyMethod, AnomalyResult, AnomalySignal, OutlierDetector,
};

pub const MODIFIED_ZSCORE_DEFAULT_THRESHOLD: f64 = 3.5;

/// Modified Z-score outlier detector
#[derive(Debug)]
pub struct ModifiedZScoreOutlierDetector {
    median: f64,
    mad: f64,
    mad_scaled: f64,
    threshold: f64,
}

impl ModifiedZScoreOutlierDetector {
    pub fn new(ts: &[f64], threshold: f64) -> Self {
        let n = ts.len();

        // Calculate median
        let mut sorted_values: Vec<f64> = ts.iter().map(|&x| normalize_value(x)).collect();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let half = n / 2;

        let median = if n.is_multiple_of(2) {
            (sorted_values[half - 1] + sorted_values[half]) / 2.0
        } else {
            sorted_values[half]
        };

        // Calculate Mad (Median Absolute Deviation)
        let mut abs_deviations: Vec<f64> = ts
            .iter()
            .map(|&x| (normalize_value(x) - median).abs())
            .collect();
        abs_deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mad = if n.is_multiple_of(2) {
            (abs_deviations[half - 1] + abs_deviations[half]) / 2.0
        } else {
            abs_deviations[half]
        };
        // Scale Mad for consistency with normal distribution
        let mad_scaled = mad / 0.6745;

        ModifiedZScoreOutlierDetector {
            median,
            mad,
            mad_scaled,
            threshold,
        }
    }

    #[inline]
    fn get_modified_zscore(&self, value: f64) -> f64 {
        let value = normalize_value(value);
        if self.mad_scaled > 1e-10 {
            0.6745 * (value - self.median) / self.mad
        } else {
            0.0
        }
    }

    pub fn detect(&self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        let n = ts.len();
        let mut scores = Vec::with_capacity(n);
        let mut anomalies = Vec::with_capacity(4);

        for (index, &v) in ts.iter().enumerate() {
            let value = normalize_value(v);
            let score = self.get_anomaly_score(value);
            let anomaly_direction = self.classify(value);
            if anomaly_direction.is_anomaly() {
                let anomaly = Anomaly {
                    index,
                    signal: anomaly_direction,
                    value: v,
                    score,
                };
                anomalies.push(anomaly);
            }
            scores.push(score);
        }

        Ok(AnomalyResult {
            scores,
            anomalies,
            threshold: self.threshold,
            method: AnomalyMethod::ModifiedZScore,
            method_info: None,
        })
    }
}
impl OutlierDetector for ModifiedZScoreOutlierDetector {
    fn get_anomaly_score(&self, value: f64) -> f64 {
        let modified_zscore = self.get_modified_zscore(value).abs();
        normalize_unbounded_score(modified_zscore)
    }

    fn classify(&self, x: f64) -> AnomalySignal {
        let modified_zscore = self.get_modified_zscore(x);
        let z_abs = modified_zscore.abs();
        if z_abs > self.threshold {
            if modified_zscore > 0.0 {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        }
    }
}

/// Modified Z-score using median absolute deviation
pub(super) fn detect_anomalies_modified_zscore(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let threshold = threshold.unwrap_or(MODIFIED_ZSCORE_DEFAULT_THRESHOLD);
    let detector = ModifiedZScoreOutlierDetector::new(ts, threshold);
    detector.detect(ts)
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
        assert!(
            result.scores[5] > 0.9,
            "Anomaly score should be high for outlier"
        );
    }
}
