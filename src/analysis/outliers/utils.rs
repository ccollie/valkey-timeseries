use crate::analysis::outliers::AnomalySignal;
use crate::analysis::{INSUFFICIENT_DATA_ERROR, TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use crate::common::Timestamp;
use crate::common::constants::{MILLIS_PER_DAY, MILLIS_PER_HOUR, MILLIS_PER_MIN, MILLIS_PER_SEC};

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

/// Infer the period from datetime index
pub(crate) fn infer_period(timestamps: &[Timestamp]) -> TimeSeriesAnalysisResult<usize> {
    let n = timestamps.len();
    if n < 2 {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: INSUFFICIENT_DATA_ERROR.to_string(),
            required: 3,
            actual: n,
        });
    }

    // Compute positive deltas (expected ms). Reject non-increasing or duplicate timestamps.
    let mut deltas: Vec<u64> = Vec::with_capacity(n - 1);
    for w in timestamps.windows(2) {
        let dt: i64 = w[1] - w[0];
        if dt <= 0 {
            return Err(TimeSeriesAnalysisError::Other(
                "Timestamps must be strictly increasing to infer period".to_string(),
            ));
        }
        deltas.push(dt as u64);
    }

    // Robust central tendency: median delta.
    deltas.sort_unstable();
    let mid = deltas.len() / 2;
    let median_diff: u64 = if deltas.len() % 2 == 1 {
        deltas[mid]
    } else {
        // Avoid overflow and keep pure integer arithmetic.
        let a = deltas[mid - 1];
        let b = deltas[mid];
        a / 2 + b / 2 + (a % 2 + b % 2) / 2
    };

    // Pick the closest canonical sampling interval within a tolerance, rather than using `<=` buckets.
    const TOLERANCE_RATIO: f64 = 0.20; // +/- 20%
    let candidates: &[(u64, usize)] = &[
        (MILLIS_PER_SEC, 86_400), // 1s  -> daily seasonality
        (MILLIS_PER_MIN, 1_440),  // 1m  -> daily
        (MILLIS_PER_HOUR, 24),    // 1h  -> daily
        (MILLIS_PER_DAY, 7),      // 1d  -> weekly
    ];

    let mut best: Option<(f64, usize)> = None;
    for &(step_ms, period) in candidates {
        let rel_err = ((median_diff as f64) - (step_ms as f64)).abs() / (step_ms as f64);
        if rel_err <= TOLERANCE_RATIO {
            match best {
                None => best = Some((rel_err, period)),
                Some((best_err, _)) if rel_err < best_err => best = Some((rel_err, period)),
                _ => {}
            }
        }
    }

    let mut period = if let Some((_, period)) = best {
        period
    } else {
        // Fallback: 10% of series length (integer), never 0.
        (n / 10).max(1)
    };

    // Keep it meaningful for downstream seasonality logic.
    if period >= n {
        period = n - 1;
    }

    Ok(period)
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
