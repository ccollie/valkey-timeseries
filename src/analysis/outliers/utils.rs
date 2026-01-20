use crate::analysis::outliers::AnomalySignal;

/// Squash an unbounded non-negative score into the range [0..1].
#[inline]
pub(super) fn normalize_unbounded_score(score: f64) -> f64 {
    if !score.is_finite() || score <= 0.0 {
        0.0
    } else {
        score / (score + 1.0)
    }
}

#[inline]
pub(super) fn normalize_value(v: f64) -> f64 {
    if v.is_nan() { 0.0 } else { v }
}

pub(super) fn get_anomaly_direction(
    low_threshold: f64,
    hi_threshold: f64,
    value: f64,
) -> AnomalySignal {
    if value < low_threshold {
        AnomalySignal::Negative
    } else if value > hi_threshold {
        AnomalySignal::Positive
    } else {
        AnomalySignal::None
    }
}
