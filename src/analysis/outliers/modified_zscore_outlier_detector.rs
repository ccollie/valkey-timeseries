use super::utils::{normalize_unbounded_score, normalize_value};
use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::calculate_median_sorted;
use crate::analysis::outliers::{
    Anomaly, AnomalyMethod, AnomalyResult, AnomalySignal, BatchOutlierDetector, MethodInfo,
};

pub const MODIFIED_ZSCORE_DEFAULT_THRESHOLD: f64 = 3.5;

/// Modified Z-score outlier detector
#[derive(Debug, Clone, Copy)]
pub struct ModifiedZScoreOutlierDetector {
    median: f64,
    mad: f64,
    mad_scaled: f64,
    threshold: f64,
    is_trained: bool,
}

impl Default for ModifiedZScoreOutlierDetector {
    fn default() -> Self {
        ModifiedZScoreOutlierDetector {
            median: 0.0,
            mad: 0.0,
            mad_scaled: 0.0,
            threshold: MODIFIED_ZSCORE_DEFAULT_THRESHOLD,
            is_trained: false,
        }
    }
}
impl ModifiedZScoreOutlierDetector {
    pub fn new(threshold: f64) -> Self {
        ModifiedZScoreOutlierDetector {
            threshold,
            ..Default::default()
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

        // Modified Z-score threshold |z| > T translates to:
        // x < median - T * (MAD / 0.6745)  OR  x > median + T * (MAD / 0.6745)
        let delta = if self.mad_scaled > 1e-10 {
            self.threshold * self.mad_scaled
        } else {
            0.0
        };

        let lower_fence = self.median - delta;
        let upper_fence = self.median + delta;

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
            method_info: Some(MethodInfo::Fenced {
                lower_fence,
                upper_fence,
                center_line: None,
            }),
        })
    }
}
impl BatchOutlierDetector for ModifiedZScoreOutlierDetector {
    fn method(&self) -> AnomalyMethod {
        AnomalyMethod::ModifiedZScore
    }

    fn train(&mut self, data: &[f64]) -> TimeSeriesAnalysisResult<()> {
        // Calculate median
        let mut sorted_values: Vec<f64> = data.iter().map(|&x| normalize_value(x)).collect();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = calculate_median_sorted(&sorted_values);

        // Calculate Mad (Median Absolute Deviation)
        let mut abs_deviations: Vec<f64> = data
            .iter()
            .map(|&x| (normalize_value(x) - median).abs())
            .collect();
        abs_deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mad = calculate_median_sorted(&abs_deviations);

        // Scale Mad for consistency with normal distribution
        let mad_scaled = mad / 0.6745;
        self.mad_scaled = mad_scaled;
        self.median = median;
        self.mad = mad;
        self.is_trained = true;
        Ok(())
    }

    fn detect(&mut self, ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
        ModifiedZScoreOutlierDetector::detect(self, ts)
    }

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
fn detect_anomalies_modified_zscore(
    ts: &[f64],
    threshold: Option<f64>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let threshold = threshold.unwrap_or(MODIFIED_ZSCORE_DEFAULT_THRESHOLD);
    let mut detector = ModifiedZScoreOutlierDetector::new(threshold);
    detector.train(ts)?;
    detector.detect(ts)
}

#[cfg(test)]
mod tests {
    use super::detect_anomalies_modified_zscore;

    #[test]
    fn test_modified_zscore() {
        let ts = vec![1.0, 2.0, 1.5, 2.2, 1.8, 10.0, 2.1, 1.9]; // 10.0 is an outlier

        let result = detect_anomalies_modified_zscore(&ts, Some(3.5)).unwrap();

        assert_eq!(result.anomalies.len(), 1, "Should detect one anomaly");
        // Should detect the outlier at index 5
        assert_eq!(result.anomalies[0].value, 10.0, "Should detect one anomaly");
        assert!(
            result.anomalies[0].score > 0.9,
            "Anomaly score should be high for outlier"
        );
    }
}
