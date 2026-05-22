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

#[inline]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_unbounded_score_range_and_edges() {
        // Range contract + edge handling.
        assert_eq!(normalize_unbounded_score(f64::NAN), 0.0);
        assert_eq!(normalize_unbounded_score(f64::INFINITY), 0.0);
        assert_eq!(normalize_unbounded_score(-1.0), 0.0);
        assert_eq!(normalize_unbounded_score(0.0), 0.0);

        let s = normalize_unbounded_score(1.0);
        assert!(s > 0.0 && s < 1.0, "Expected (0,1) for score=1.0, got {s}");
    }

    #[test]
    fn test_normalize_unbounded_score_monotonicity() {
        // Monotonic squashing: larger inputs must not produce smaller outputs.
        let a = normalize_unbounded_score(0.5);
        let b = normalize_unbounded_score(1.0);
        let c = normalize_unbounded_score(2.0);
        let d = normalize_unbounded_score(10.0);

        assert!(a < b, "Expected 0.5 < 1.0 mapping, got {a} vs {b}");
        assert!(b < c, "Expected 1.0 < 2.0 mapping, got {b} vs {c}");
        assert!(c < d, "Expected 2.0 < 10.0 mapping, got {c} vs {d}");
        assert!(
            d < 1.0,
            "Expected strict less than 1.0 for finite inputs, got {d}"
        );
    }
}
