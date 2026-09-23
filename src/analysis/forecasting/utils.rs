use crate::common::Sample;
use crate::common::hash::IntMap;
use crate::error::TsdbError;
use anofox_forecast::models::ensemble::CombinationMethod;
use anofox_forecast::seasonality::auto_trend::TrendCriterion;
use anofox_forecast::{ForecastError, core::TimeSeries as ForecastTimeSeries};
use chrono::{DateTime, Utc};
use std::time::Duration;
use valkey_module::{ValkeyError, ValkeyResult};

pub fn make_forecast_time_series(
    iter: impl Iterator<Item = Sample>,
) -> Result<ForecastTimeSeries, TsdbError> {
    let initial_capacity = iter.size_hint().0;
    let mut timestamps = Vec::with_capacity(initial_capacity);
    let mut values = Vec::with_capacity(initial_capacity);
    for sample in iter {
        let dt: DateTime<Utc> =
            DateTime::from_timestamp_millis(sample.timestamp).ok_or_else(|| {
                TsdbError::ForecastError(format!(
                    "Invalid timestamp {} in sample. ",
                    sample.timestamp
                ))
            })?;

        timestamps.push(dt);
        values.push(sample.value);
    }
    ForecastTimeSeries::univariate(timestamps, values).map_err(|e| e.into())
}

impl From<ForecastError> for TsdbError {
    fn from(err: ForecastError) -> Self {
        TsdbError::ForecastError(err.to_string())
    }
}

pub fn normalize_model_name(selected_model: &str) -> &str {
    match selected_model {
        "AutoARIMA (SARIMA)" => "SARIMA",
        "AutoARIMA" => "ARIMA",
        "AutoTheta" => "Theta",
        "AutoETS" => "ETS",
        other => other, // fallback to original name if it doesn't match known models
    }
}

pub fn try_parse_trend_criterion(s: &str) -> Result<TrendCriterion, TsdbError> {
    match s.to_lowercase().as_str() {
        "aicc" => Ok(TrendCriterion::AICc),
        "bic" => Ok(TrendCriterion::BIC),
        "holdout" => Ok(TrendCriterion::Holdout),
        other => Err(TsdbError::ForecastError(format!(
            "TSDB: Invalid trend criterion '{}'. Valid options are: AICc, BIC, and Holdout.",
            other
        ))),
    }
}

pub fn try_parse_combination_method(s: &str) -> Result<CombinationMethod, TsdbError> {
    match s.to_lowercase().as_str() {
        "mean" => Ok(CombinationMethod::Mean),
        "median" => Ok(CombinationMethod::Median),
        "weightedmse" => Ok(CombinationMethod::WeightedMSE),
        "custom" => Ok(CombinationMethod::Custom),
        "inverseaic" => Ok(CombinationMethod::InverseAIC),
        "horizonadaptive" => Ok(CombinationMethod::HorizonAdaptive),

        other => Err(TsdbError::ForecastError(format!(
            "TSDB: Invalid combination method '{}'. Valid options are: Mean, Median, WeightedMSE, Custom, InverseAIC, HorizonAdaptive.",
            other
        ))),
    }
}

/// Smallest share of the intervals that must equal their GCD for the GCD to replace the modal
/// interval as the inferred frequency.
const MIN_GCD_INTERVAL_SHARE: f64 = 0.1;

/// Infer the frequency (in milliseconds) from a set of samples by finding the most
/// common interval between consecutive timestamps.
///
/// Uses a two-step approach:
/// 1. Find the modal (most common) interval, the smaller one on a tie, and require it to
///    represent at least 50% of all intervals.
/// 2. Compute the GCD of all intervals. If the GCD is smaller than the modal, divides
///    the modal evenly, and is itself at least [`MIN_GCD_INTERVAL_SHARE`] of the intervals,
///    prefer the GCD — this recovers the original frequency when samples have been deleted
///    from a uniformly-spaced series (leaving intervals that are multiples of the true
///    frequency), without letting one stray off-grid sample halve it.
pub fn infer_frequency_from_samples(samples: &[Sample]) -> ValkeyResult<Duration> {
    if samples.len() < 2 {
        return Err(ValkeyError::String(
            "TSDB: insufficient data to infer frequency; at least 2 samples required".to_string(),
        ));
    }

    // Calculate all differences between consecutive samples
    let diffs: Vec<i64> = samples
        .windows(2)
        .map(|w| w[1].timestamp - w[0].timestamp)
        .filter(|&d| d > 0)
        .collect();

    if diffs.is_empty() {
        return Err(ValkeyError::String(
            "TSDB: cannot infer frequency; no valid intervals found".to_string(),
        ));
    }

    // Find modal (most common) difference
    let mut counts: IntMap<i64, usize> = IntMap::default();
    for &diff in &diffs {
        *counts.entry(diff).or_insert(0) += 1;
    }

    // Most frequent interval; on a tie the smaller one, so the result does not depend on
    // hash-map iteration order.
    let (modal_diff, modal_count) = counts
        .iter()
        .max_by_key(|&(&diff, &count)| (count, std::cmp::Reverse(diff)))
        .map(|(&diff, &count)| (diff, count))
        .unwrap(); // Safe because diffs is non-empty

    let total_count: usize = counts.values().sum();
    let modal_ratio = modal_count as f64 / total_count as f64;

    // Require at least 50% of intervals to match the modal
    if modal_ratio < 0.5 {
        return Err(ValkeyError::String(
            "TSDB: cannot infer frequency; no dominant interval found".to_string(),
        ));
    }

    // Compute GCD of all intervals to detect the underlying base frequency.
    // This handles cases where samples were deleted from a uniformly-spaced series,
    // leaving only intervals that are multiples of the true frequency.
    let gcd_all = diffs.iter().fold(0i64, |a, &b| gcd(a, b));

    // Use the GCD if it is a proper divisor of the modal interval AND makes up a real share of
    // the intervals. Without the share, one stray sample (an extra point 30 s into a 60 s
    // series) would halve the frequency and double the grid.
    let gcd_share = counts.get(&gcd_all).copied().unwrap_or(0) as f64 / total_count as f64;
    if gcd_all > 0
        && gcd_all < modal_diff
        && modal_diff % gcd_all == 0
        && gcd_share >= MIN_GCD_INTERVAL_SHARE
    {
        return Ok(Duration::from_millis(gcd_all as u64));
    }

    Ok(Duration::from_millis(modal_diff as u64))
}

/// Compute the greatest common divisor of two non-negative integers.
fn gcd(mut a: i64, mut b: i64) -> i64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples_at(timestamps: &[i64]) -> Vec<Sample> {
        timestamps.iter().map(|&ts| Sample::new(ts, 1.0)).collect()
    }

    fn inferred_ms(timestamps: &[i64]) -> u128 {
        infer_frequency_from_samples(&samples_at(timestamps))
            .unwrap()
            .as_millis()
    }

    #[test]
    fn one_stray_sample_does_not_halve_the_frequency() {
        let mut timestamps: Vec<i64> = (0..60).map(|i| i * 60_000).collect();
        timestamps.insert(11, 10 * 60_000 + 30_000);
        assert_eq!(inferred_ms(&timestamps), 60_000);
    }

    #[test]
    fn deleted_samples_still_yield_the_base_frequency() {
        // Most minutes were deleted in pairs, so 2 minutes is the modal interval, but a real
        // share of the intervals are the 1-minute base.
        let mut timestamps = Vec::new();
        let mut ts = 0;
        for i in 0..40 {
            timestamps.push(ts);
            ts += if i % 4 == 0 { 60_000 } else { 120_000 };
        }
        assert_eq!(inferred_ms(&timestamps), 60_000);
    }

    #[test]
    fn a_tie_prefers_the_smaller_interval() {
        assert_eq!(inferred_ms(&[0, 60_000, 180_000]), 60_000);
        assert_eq!(inferred_ms(&[0, 120_000, 180_000]), 60_000);
    }

    #[test]
    fn irregular_intervals_are_rejected() {
        assert!(infer_frequency_from_samples(&samples_at(&[0, 10, 30, 60, 100])).is_err());
    }
}
