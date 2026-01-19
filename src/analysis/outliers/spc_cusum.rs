use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::anomalies::normalize_value;
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo};

/// Statistical Process Control (Spc) cusum anomaly detection
pub(super) fn detect_anomalies_spc_cusum(ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let mut scores = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    // Cusum control chart implementation
    let training_size = (n as f64 * 0.5).min(100.0) as usize;
    let training_data = &ts[0..training_size];

    let target = calculate_mean(training_data);
    let std_dev = calculate_std_dev(training_data);

    let k = 0.5 * std_dev; // Reference value
    let h = 5.0 * std_dev; // Decision interval

    let mut cusum_pos = 0.0;
    let mut cusum_neg = 0.0;
    let threshold = h / std_dev;

    for &v in ts {
        let value = normalize_value(v);
        cusum_pos = f64::max(0.0, cusum_pos + (value - target) - k);
        cusum_neg = f64::max(0.0, cusum_neg - (value - target) - k);

        let cusum_max = f64::max(cusum_pos, cusum_neg);
        let score = cusum_max / std_dev;
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
            control_limits: (-h, h),
            center_line: target,
        }),
    })
}
