use crate::common::Sample;

#[inline]
pub(crate) fn min_with_nan(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left < right {
        left
    } else {
        right
    }
}

#[inline]
pub(crate) fn max_with_nan(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left > right {
        left
    } else {
        right
    }
}

/// NaN-ignoring minimum, used by the `min` aggregator and `min_of`: NaN is only
/// returned if both values are NaN, and a tie keeps `left`, so `-0` and `+0`
/// resolve the way Prometheus does.
///
/// NaN is the identity element (`min_ignore_nan(NaN, v) == v`), which is what
/// lets the push-down partial fold start from NaN and still match the
/// single-node `reduce(min_ignore_nan)`.
#[inline]
pub(crate) fn min_ignore_nan(left: f64, right: f64) -> f64 {
    if left > right || left.is_nan() {
        right
    } else {
        left
    }
}

/// NaN-ignoring maximum, used by the `max` aggregator and `max_of`; see
/// [`min_ignore_nan`].
#[inline]
pub(crate) fn max_ignore_nan(left: f64, right: f64) -> f64 {
    if left < right || left.is_nan() {
        right
    } else {
        left
    }
}

pub(in crate::promql) fn sample_regression(samples: &[Sample]) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }

    let first = samples.first()?;

    let mut all_equal = true;
    let mut v_prev = first.value;

    for sample in samples.iter().skip(1) {
        let v = sample.value;
        if v.is_nan() {
            return Some((f64::NAN, v));
        }
        if v != v_prev {
            all_equal = false;
        }
        v_prev = v;
    }

    if all_equal {
        // A flat line has zero slope and an intercept equal to its value. The
        // return order is `(slope, intercept)` — the same as the general path
        // below and what both callers destructure — so getting it backwards
        // here made `deriv` of a constant return the constant, and
        // `predict_linear` return `value * elapsed` instead of `value`.
        return Some((0.0, first.value));
    }

    let first_ts = first.timestamp;

    let mut count = 0.0;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;

    for &Sample { timestamp, value } in samples {
        let x = (timestamp - first_ts) as f64 / 1_000f64;
        count += 1.0;
        sum_x += x;
        sum_y += value;
        sum_xy += x * value;
        sum_x2 += x * x;
    }

    let denominator = count * sum_x2 - sum_x * sum_x;
    if denominator == 0.0 {
        return None;
    }

    let slope = (count * sum_xy - sum_x * sum_y) / denominator;
    let intercept = (sum_y - slope * sum_x) / count;
    Some((slope, intercept))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples on a 15s grid starting at an arbitrary non-zero instant, so a
    /// regression that leaked absolute timestamps would show up.
    fn grid(values: &[f64]) -> Vec<Sample> {
        values
            .iter()
            .enumerate()
            .map(|(i, v)| Sample {
                timestamp: 1_767_218_400_000 + i as i64 * 15_000,
                value: *v,
            })
            .collect()
    }

    #[test]
    fn ignore_nan_helpers_return_nan_only_when_both_are_nan() {
        for helper in [min_ignore_nan, max_ignore_nan] {
            assert_eq!(helper(f64::NAN, 3.0), 3.0);
            assert_eq!(helper(3.0, f64::NAN), 3.0);
            assert_eq!(helper(f64::NAN, f64::INFINITY), f64::INFINITY);
            assert_eq!(helper(f64::NEG_INFINITY, f64::NAN), f64::NEG_INFINITY);
            assert!(helper(f64::NAN, f64::NAN).is_nan());
        }
        assert_eq!(min_ignore_nan(3.0, 5.0), 3.0);
        assert_eq!(max_ignore_nan(3.0, 5.0), 5.0);
    }

    #[test]
    fn ignore_nan_helpers_keep_the_first_of_two_zeros() {
        for helper in [min_ignore_nan, max_ignore_nan] {
            assert!(helper(-0.0, 0.0).is_sign_negative());
            assert!(helper(0.0, -0.0).is_sign_positive());
        }
    }

    #[test]
    fn with_nan_helpers_still_propagate_nan() {
        // `clamp*` keeps math.Min/math.Max semantics; only the aggregators and
        // `min_of`/`max_of` ignore NaN.
        for helper in [min_with_nan, max_with_nan] {
            assert!(helper(f64::NAN, 3.0).is_nan());
            assert!(helper(3.0, f64::NAN).is_nan());
        }
    }

    #[test]
    fn a_constant_series_has_zero_slope_and_its_value_as_intercept() {
        // The regression this file exists for. Returning `(value, 0.0)` here
        // made `deriv(up[5m])` answer 1 for a series pinned at 1, and
        // `predict_linear` answer `value * elapsed`.
        let (slope, intercept) = sample_regression(&grid(&[1.0; 20])).unwrap();
        assert_eq!(slope, 0.0);
        assert_eq!(intercept, 1.0);

        let (slope, intercept) = sample_regression(&grid(&[7.5; 4])).unwrap();
        assert_eq!(slope, 0.0);
        assert_eq!(intercept, 7.5);

        // Including zero, where a swap would be invisible.
        let (slope, intercept) = sample_regression(&grid(&[0.0; 4])).unwrap();
        assert_eq!(slope, 0.0);
        assert_eq!(intercept, 0.0);
    }

    #[test]
    fn a_linear_ramp_recovers_its_slope_and_intercept() {
        // 0.5 per second on a 15s grid is 7.5 per sample.
        let values: Vec<f64> = (0..20).map(|i| i as f64 * 7.5).collect();
        let (slope, intercept) = sample_regression(&grid(&values)).unwrap();
        assert!((slope - 0.5).abs() < 1e-12, "slope was {slope}");
        assert!((intercept - 0.0).abs() < 1e-9, "intercept was {intercept}");

        // With an offset, the intercept is the value at the first sample.
        let values: Vec<f64> = (0..20).map(|i| 100.0 + i as f64 * 7.5).collect();
        let (slope, intercept) = sample_regression(&grid(&values)).unwrap();
        assert!((slope - 0.5).abs() < 1e-12);
        assert!(
            (intercept - 100.0).abs() < 1e-9,
            "intercept was {intercept}"
        );
    }

    #[test]
    fn a_descending_series_has_a_negative_slope() {
        let values: Vec<f64> = (0..10).map(|i| 100.0 - i as f64 * 15.0).collect();
        let (slope, _) = sample_regression(&grid(&values)).unwrap();
        assert!((slope + 1.0).abs() < 1e-12, "slope was {slope}");
    }

    #[test]
    fn the_constant_and_general_paths_agree_at_the_boundary() {
        // Two series that differ by one ulp take different code paths but must
        // not produce wildly different answers.
        let flat = sample_regression(&grid(&[5.0; 8])).unwrap();
        let nearly_flat = {
            let mut v = vec![5.0; 8];
            v[7] = 5.0 + f64::EPSILON;
            sample_regression(&grid(&v)).unwrap()
        };
        assert!((flat.0 - nearly_flat.0).abs() < 1e-9, "slopes diverge");
        assert!((flat.1 - nearly_flat.1).abs() < 1e-9, "intercepts diverge");
    }

    #[test]
    fn fewer_than_two_samples_has_no_regression() {
        assert!(sample_regression(&[]).is_none());
        assert!(sample_regression(&grid(&[1.0])).is_none());
    }

    #[test]
    fn a_nan_anywhere_poisons_the_slope() {
        // `rollup_deriv` returns this NaN as the series' value (matching
        // Prometheus) rather than dropping the series.
        let (slope, _) = sample_regression(&grid(&[1.0, 2.0, f64::NAN, 4.0])).unwrap();
        assert!(slope.is_nan());
    }
}
