#[cfg(test)]
mod test_data;

use crate::analysis::forecasting::imputation::interpolate_series;
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use crate::common::Sample;
use anofox_forecast::detection::{PeriodDetectionConfig, detect_periods};
use anofox_forecast::seasonality::{MSTL, STL};

#[derive(Clone, Debug)]
pub enum Seasonality {
    Auto,
    Periods(Vec<usize>),
}

impl Seasonality {
    /// The periods to decompose `values` with: the given ones, or those detected in `values`
    /// for `Auto`. Not validated; see [`MIN_SEASONAL_PERIOD`].
    pub fn resolve(&self, values: &[f64]) -> Vec<usize> {
        match self {
            Seasonality::Periods(periods) => periods.clone(),
            Seasonality::Auto => detect_periods(values, &PeriodDetectionConfig::default())
                .iter()
                .map(|p| p.period)
                .collect(),
        }
    }
}

/// How many periods `TS.PERIODS` reports.
pub const MAX_REPORTED_PERIODS: usize = 5;

/// The strongest period in `values`: the first of the list `TS.PERIODS` reports.
///
/// Every "dominant period" in the module comes from here, so `TS.PERIODS … DOMINANT` names the
/// same period `SEASONALITY AUTO` and `POLICY SEASONAL auto` use. anofox's own dominant-period
/// search passes a limit of one into candidate selection, which can pick a different, weaker
/// period than the head of the full list.
pub fn dominant_period(values: &[f64]) -> Option<usize> {
    let config = PeriodDetectionConfig {
        max_periods: MAX_REPORTED_PERIODS,
        ..Default::default()
    };
    detect_periods(values, &config).first().map(|p| p.period)
}

/// Smallest seasonal period STL/MSTL can decompose: a season needs at least two positions.
/// Period 0 panics inside the decomposition (a division by zero).
pub const MIN_SEASONAL_PERIOD: usize = 2;

/// `values` with non-finite entries linearly interpolated (edges take the nearest value).
fn fill_missing(values: &[f64]) -> Vec<f64> {
    let samples: Vec<Sample> = values
        .iter()
        .enumerate()
        .map(|(i, &v)| Sample::new(i as i64, v))
        .collect();
    interpolate_series(&samples, true)
        .into_iter()
        .map(|s| s.value)
        .collect()
}

/// `remainder` with NaN put back wherever `input` was missing.
fn restore_missing(input: &[f64], mut remainder: Vec<f64>) -> Vec<f64> {
    for (r, v) in remainder.iter_mut().zip(input) {
        if !v.is_finite() {
            *r = f64::NAN;
        }
    }
    remainder
}

/// Seasonal adjustment using (M)Stl decomposition
///
/// A single NaN would propagate through the whole decomposition and leave nothing to score, so
/// missing (non-finite) values are linearly filled for the decomposition and come back as NaN
/// in the remainder, exactly where they were in the input.
pub fn seasonally_adjust(
    input: &[f64],
    seasonality: &Seasonality,
) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let filled;
    let ts = if input.iter().all(|v| v.is_finite()) {
        input
    } else {
        filled = fill_missing(input);
        filled.as_slice()
    };
    let mut periods = seasonality.resolve(ts);

    if periods.is_empty() {
        return Ok(input.to_vec());
    }

    periods.sort_unstable();

    let n = ts.len();
    for &period in &periods {
        if period < MIN_SEASONAL_PERIOD {
            return Err(TimeSeriesAnalysisError::InvalidInput(format!(
                "seasonal period must be at least {MIN_SEASONAL_PERIOD}, got {period}"
            )));
        }
        // Saturating, so a huge period fails the check instead of wrapping past it.
        validate_insufficient_data::<()>(period.saturating_mul(2), n)?;
    }

    if periods.len() == 1 {
        STL::new(periods[0])
            .robust()
            .decompose(ts)
            .map(|res| restore_missing(input, res.remainder))
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("STL decomposition failed".to_string())
            })
    } else {
        // MSTL alternates between fitting the trend and each seasonal component; its
        // default of 2 outer iterations often hasn't converged by the time it stops,
        // which can leave point-anomalies partially absorbed into trend/seasonal
        // rather than showing up in the remainder. A few extra iterations noticeably
        // stabilizes convergence at negligible extra cost.
        MSTL::new(periods.to_vec())
            .robust()
            .with_iterations(5)
            .decompose(ts)
            .map(|res| restore_missing(input, res.remainder))
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("MSTL decomposition failed".to_string())
            })
    }
}

fn validate_insufficient_data<T: Default>(
    required: usize,
    actual: usize,
) -> TimeSeriesAnalysisResult<T> {
    if actual >= required {
        return Ok(T::default());
    }
    Err(TimeSeriesAnalysisError::InsufficientData {
        message: "TSDB: insufficient samples for anomaly detection".to_string(),
        required,
        actual,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::TimeSeriesAnalysisError;
    use crate::analysis::seasonality::test_data::SEASON_SEVEN;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn assert_insufficient_data(
        err: &TimeSeriesAnalysisError,
        expected_required: usize,
        expected_actual: usize,
    ) {
        match err {
            TimeSeriesAnalysisError::InsufficientData {
                required, actual, ..
            } => {
                assert_eq!(
                    *required, expected_required,
                    "required mismatch: got {required}, want {expected_required}"
                );
                assert_eq!(
                    *actual, expected_actual,
                    "actual mismatch: got {actual}, want {expected_actual}"
                );
            }
            other => panic!("expected InsufficientData, got {other:?}"),
        }
    }

    // ── Seasonality::Periods(vec![]) ─────────────────────────────────────────

    #[test]
    fn test_empty_periods_returns_input_unchanged() {
        let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = seasonally_adjust(&data, &Seasonality::Periods(vec![])).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_empty_periods_on_empty_slice_returns_empty() {
        let result = seasonally_adjust(&[], &Seasonality::Periods(vec![])).unwrap();
        assert!(result.is_empty());
    }

    // ── single period — insufficient data ────────────────────────────────────

    #[test]
    fn test_single_period_insufficient_data_returns_error() {
        let period = 7_usize;
        // n < 2 * period → error
        let data: Vec<f64> = vec![1.0; period * 2 - 1];
        let err = seasonally_adjust(&data, &Seasonality::Periods(vec![period])).unwrap_err();
        assert_insufficient_data(&err, 2 * period, data.len());
    }

    #[test]
    fn test_single_period_zero_samples_returns_error() {
        let period = 4_usize;
        let err = seasonally_adjust(&[], &Seasonality::Periods(vec![period])).unwrap_err();
        assert_insufficient_data(&err, 2 * period, 0);
    }

    // ── single period — boundary: exactly 2 * period ────────────────────────

    #[test]
    fn test_single_period_exactly_minimum_samples_succeeds() {
        let period = 4_usize;
        // exactly 2 * period — should succeed (>= check)
        let data: Vec<f64> = (0..(2 * period) as i64)
            .map(|i| (i % period as i64) as f64)
            .collect();
        let result = seasonally_adjust(&data, &Seasonality::Periods(vec![period]));
        assert!(
            result.is_ok(),
            "expected Ok with exactly 2*period samples, got {result:?}"
        );
        assert_eq!(result.unwrap().len(), data.len());
    }

    // ── single period — sufficient data ─────────────────────────────────────

    #[test]
    fn test_single_period_returns_remainder_same_length() {
        let data = SEASON_SEVEN;
        let result = seasonally_adjust(data, &Seasonality::Periods(vec![7])).unwrap();
        assert_eq!(
            result.len(),
            data.len(),
            "remainder length must equal input length"
        );
    }

    #[test]
    fn test_single_period_remainder_values_are_finite() {
        let data = SEASON_SEVEN;
        let result = seasonally_adjust(data, &Seasonality::Periods(vec![7])).unwrap();
        assert!(
            result.iter().all(|v| v.is_finite()),
            "all remainder values should be finite"
        );
    }

    // ── multiple periods — insufficient data ─────────────────────────────────

    #[test]
    fn test_multi_period_insufficient_data_returns_error() {
        let periods = vec![4_usize, 7];
        let max_period = *periods.last().unwrap();
        // n < 2 * max_period → error
        let data: Vec<f64> = vec![1.0; 2 * max_period - 1];
        let err = seasonally_adjust(&data, &Seasonality::Periods(periods)).unwrap_err();
        assert_insufficient_data(&err, 2 * max_period, data.len());
    }

    #[test]
    fn test_multi_period_period_ordering_determines_max() {
        // periods = [7, 8] — max is the last element (8) as used by the code
        // with exactly 2*8=16 samples it should succeed
        let periods = vec![7_usize, 8];
        let max_period = 8_usize;
        let data: Vec<f64> = (0..(2 * max_period) as i64)
            .map(|i| (i % 7) as f64)
            .collect();
        let result = seasonally_adjust(&data, &Seasonality::Periods(periods));
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert_eq!(result.unwrap().len(), data.len());
    }

    // ── Seasonality::Auto ────────────────────────────────────────────────────

    #[test]
    fn test_auto_empty_periods_returns_input_unchanged() {
        // With n=10: default_max_period = floor(10/3) = 3, which is less than min_period=4, so
        // the period filter (per >= 4 && per < 3) is always false.  The detector returns an empty
        // vec and seasonally_adjust returns the original slice unchanged.
        let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 4.0, 3.0, 2.0, 1.0, 2.0];
        let result = seasonally_adjust(&data, &Seasonality::Auto).unwrap();
        assert_eq!(
            result, data,
            "with no detectable period the input must be returned unchanged"
        );
    }

    #[test]
    fn test_auto_three_samples_returns_input_unchanged() {
        let data = vec![1.0, 2.0, 1.0];
        let result = seasonally_adjust(&data, &Seasonality::Auto).unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn seasonally_adjust_rejects_degenerate_periods() {
        let data: Vec<f64> = (0..100).map(|i| (i % 7) as f64).collect();
        // Period 0 would divide by zero inside STL.
        for period in [0, 1] {
            let err = seasonally_adjust(&data, &Seasonality::Periods(vec![period])).unwrap_err();
            assert!(
                matches!(err, TimeSeriesAnalysisError::InvalidInput(_)),
                "{err:?}"
            );
        }
        // 2 * usize::MAX wraps to a small number; it must still count as insufficient data.
        let err = seasonally_adjust(&data, &Seasonality::Periods(vec![usize::MAX])).unwrap_err();
        assert_insufficient_data(&err, usize::MAX, 100);
    }

    #[test]
    fn seasonally_adjust_keeps_missing_values_local() {
        let mut data: Vec<f64> = (0..240)
            .map(|i| 10.0 + (i as f64 * std::f64::consts::TAU / 24.0).sin())
            .collect();
        data[50] = f64::NAN;
        data[130] = f64::INFINITY;
        let remainder = seasonally_adjust(&data, &Seasonality::Periods(vec![24])).unwrap();
        for (i, r) in remainder.iter().enumerate() {
            if i == 50 || i == 130 {
                assert!(r.is_nan(), "position {i} should stay missing");
            } else {
                assert!(
                    r.is_finite(),
                    "position {i} should have a remainder, got {r}"
                );
            }
        }
    }
}
