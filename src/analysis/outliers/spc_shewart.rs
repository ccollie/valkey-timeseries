use crate::analysis::TimeSeriesAnalysisResult;
use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{
    AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo, get_anomaly_direction,
};

/// Statistical Process Control (Spc) Shewart anomaly detection
pub(super) fn detect_anomalies_spc_shewart(ts: &[f64]) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let mut scores = Vec::with_capacity(n);
    let mut anomalies: Vec<AnomalySignal> = Vec::with_capacity(n);

    // Calculate control limits using the first portion of data
    let training_size = (n as f64 * 0.5).min(100.0) as usize;
    let training_data = &ts[0..training_size];

    let mean = calculate_mean(training_data);
    let std_dev = calculate_std_dev(training_data);

    let multiplier = 3.0; // 3-sigma control limits
    let ucl = mean + multiplier * std_dev; // Upper control limit
    let lcl = mean - multiplier * std_dev; // Lower control limit

    for &value in ts {
        let distance_from_center = (value - mean).abs();
        scores.push(distance_from_center / std_dev);

        let anomaly_direction = get_anomaly_direction(lcl, ucl, value);
        anomalies.push(anomaly_direction);
    }

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold: multiplier,
        method: AnomalyMethod::StatisticalProcessControl,
        method_info: Some(MethodInfo::Spc {
            control_limits: (lcl, ucl),
            center_line: mean,
        }),
    })
}
