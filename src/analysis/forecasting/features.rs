use crate::analysis::MAX_ANALYSIS_LAG;
use crate::analysis::forecasting::stats::moments;
use crate::common::threads::map_on_current_pool;
use crate::error::TsdbError;
use anofox_forecast::features::Feature;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeatureCategory {
    Basic,
    Distribution,
    Autocorrelation,
    Trend,
}

impl FeatureCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeatureCategory::Basic => "basic",
            FeatureCategory::Distribution => "distribution",
            FeatureCategory::Autocorrelation => "autocorrelation",
            FeatureCategory::Trend => "trend",
        }
    }

    pub fn features(&self) -> &'static [Feature] {
        use Feature::*;
        match self {
            FeatureCategory::Basic => &[
                Mean,
                Median,
                Variance,
                VarianceSample,
                Minimum,
                Maximum,
                Length,
            ],
            FeatureCategory::Distribution => &[
                Skewness,
                Kurtosis,
                Quantile { q: 0.25 },
                Quantile { q: 0.5 },
                Quantile { q: 0.75 },
                Quantile { q: 0.9 },
                Quantile { q: 0.95 },
                Quantile { q: 0.99 },
            ],
            FeatureCategory::Autocorrelation => &[
                Autocorrelation { lag: 1 },
                Autocorrelation { lag: 2 },
                Autocorrelation { lag: 3 },
            ],
            FeatureCategory::Trend => &[
                LinearTrendIntercept,
                LinearTrendSlope,
                LinearTrendPValue,
                LinearTrendRSquared,
            ],
        }
    }
}

impl TryFrom<&str> for FeatureCategory {
    type Error = TsdbError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "basic" => Ok(FeatureCategory::Basic),
            "distribution" => Ok(FeatureCategory::Distribution),
            "autocorrelation" => Ok(FeatureCategory::Autocorrelation),
            "trend" => Ok(FeatureCategory::Trend),
            _ => {
                let msg = format!("Unknown feature category: {}", value);
                let err = TsdbError::ForecastError(msg);
                Err(err)
            }
        }
    }
}

impl std::fmt::Display for FeatureCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Parse a single feature string into a `Feature` enum variant.
///
/// Simple features (no parameters):
///   `mean`, `median`, `variance`, `skewness`, `kurtosis`, etc.
///
/// Parameterized features (name:value):
///   `quantile:0.5`  — q must be in [0.0, 1.0]
///   `autocorrelation:3`  — lag must be > 0
///   `partial_autocorrelation:3` or `pacf:3`  — lag must be > 0
pub fn parse_feature(s: &str) -> Result<Feature, TsdbError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TsdbError::ForecastError("Empty feature name".into()));
    }

    // Parameterized features: name:value
    if let Some((name, param)) = s.split_once(':') {
        let name_lower = name.trim().to_lowercase();
        let param = param.trim();

        match name_lower.as_str() {
            "quantile" => {
                let q: f64 = param.parse().map_err(|_| {
                    TsdbError::ForecastError(format!(
                        "Invalid quantile parameter '{}': expected a float between 0.0 and 1.0",
                        param
                    ))
                })?;
                if !(0.0..=1.0).contains(&q) {
                    return Err(TsdbError::ForecastError(format!(
                        "Quantile parameter must be between 0.0 and 1.0, got {}",
                        q
                    )));
                }
                Ok(Feature::Quantile { q })
            }
            "autocorrelation" => {
                let lag: usize = param.parse().map_err(|_| {
                    TsdbError::ForecastError(format!(
                        "Invalid autocorrelation lag '{}': expected a positive integer",
                        param
                    ))
                })?;
                if lag == 0 {
                    return Err(TsdbError::ForecastError(
                        "Autocorrelation lag must be greater than 0".into(),
                    ));
                }
                Ok(Feature::Autocorrelation { lag })
            }
            "partial_autocorrelation" | "partialautocorrelation" | "pacf" => {
                let lag: usize = param.parse().map_err(|_| {
                    TsdbError::ForecastError(format!(
                        "Invalid partial autocorrelation lag '{}': expected a positive integer",
                        param
                    ))
                })?;
                if lag == 0 {
                    return Err(TsdbError::ForecastError(
                        "Partial autocorrelation lag must be greater than 0".into(),
                    ));
                }
                if lag > MAX_ANALYSIS_LAG {
                    return Err(TsdbError::ForecastError(format!(
                        "Partial autocorrelation lag must not exceed {MAX_ANALYSIS_LAG}"
                    )));
                }
                Ok(Feature::PartialAutocorrelation { lag })
            }
            other => Err(TsdbError::ForecastError(format!(
                "Unknown parameterized feature '{}'. Supported: quantile, autocorrelation, partial_autocorrelation (pacf)",
                other
            ))),
        }
    } else {
        // Non-parameterized features
        parse_simple_feature(&s.to_lowercase())
    }
}

/// Parse a non-parameterized feature name.
fn parse_simple_feature(name: &str) -> Result<Feature, TsdbError> {
    match name {
        // Basic
        "mean" => Ok(Feature::Mean),
        "median" => Ok(Feature::Median),
        "variance" => Ok(Feature::Variance),
        "variance_sample" => Ok(Feature::VarianceSample),
        "standard_deviation" => Ok(Feature::StandardDeviation),
        "minimum" => Ok(Feature::Minimum),
        "maximum" => Ok(Feature::Maximum),
        "abs_energy" => Ok(Feature::AbsEnergy),
        "absolute_maximum" => Ok(Feature::AbsoluteMaximum),
        "absolute_sum_of_changes" => Ok(Feature::AbsoluteSumOfChanges),
        "length" => Ok(Feature::Length),
        "mean_abs_change" => Ok(Feature::MeanAbsChange),
        "mean_change" => Ok(Feature::MeanChange),
        "mean_second_derivative_central" => Ok(Feature::MeanSecondDerivativeCentral),
        "root_mean_square" => Ok(Feature::RootMeanSquare),
        "sum_values" => Ok(Feature::SumValues),

        // Distribution
        "skewness" => Ok(Feature::Skewness),
        "kurtosis" => Ok(Feature::Kurtosis),
        "variance_larger_than_std" => Ok(Feature::VarianceLargerThanStd),
        "variation_coefficient" => Ok(Feature::VariationCoefficient),

        // Autocorrelation (non-parameterized)
        "time_reversal_asymmetry" => Ok(Feature::TimeReversalAsymmetry { lag: 1 }),

        // Counting
        "count_above_mean" => Ok(Feature::CountAboveMean),
        "count_below_mean" => Ok(Feature::CountBelowMean),
        "number_crossing_mean" => Ok(Feature::NumberCrossingMean),
        "longest_strike_above_mean" => Ok(Feature::LongestStrikeAboveMean),
        "longest_strike_below_mean" => Ok(Feature::LongestStrikeBelowMean),
        "first_location_of_maximum" => Ok(Feature::FirstLocationOfMaximum),
        "first_location_of_minimum" => Ok(Feature::FirstLocationOfMinimum),
        "last_location_of_maximum" => Ok(Feature::LastLocationOfMaximum),
        "last_location_of_minimum" => Ok(Feature::LastLocationOfMinimum),
        "has_duplicate" => Ok(Feature::HasDuplicate),
        "has_duplicate_max" => Ok(Feature::HasDuplicateMax),
        "has_duplicate_min" => Ok(Feature::HasDuplicateMin),

        // Entropy
        "fourier_entropy" => Ok(Feature::FourierEntropy),

        // Trend
        "linear_trend_slope" => Ok(Feature::LinearTrendSlope),
        "linear_trend_intercept" => Ok(Feature::LinearTrendIntercept),
        "linear_trend_r_squared" => Ok(Feature::LinearTrendRSquared),
        "linear_trend_p_value" => Ok(Feature::LinearTrendPValue),
        "augmented_dickey_fuller" => Ok(Feature::AugmentedDickeyFuller),

        // Change
        "percentage_reoccurring_datapoints" => Ok(Feature::PercentageReoccurringDatapoints),
        "percentage_reoccurring_values" => Ok(Feature::PercentageReoccurringValues),
        "ratio_value_number_to_length" => Ok(Feature::RatioValueNumberToLength),
        "sum_of_reoccurring_data_points" => Ok(Feature::SumOfReoccurringDataPoints),
        "sum_of_reoccurring_values" => Ok(Feature::SumOfReoccurringValues),

        _ => Err(TsdbError::ForecastError(format!(
            "Unknown feature '{}'",
            name
        ))),
    }
}

/// Computes one feature. The moment statistics come from [`moments`] rather than anofox, which
/// computes them in single precision and mis-scales skewness and kurtosis.
fn compute_feature(feature: &Feature, data: &[f64]) -> f64 {
    match feature {
        Feature::Mean => moments::mean(data),
        Feature::Variance => moments::variance(data),
        Feature::VarianceSample => moments::variance_sample(data),
        Feature::StandardDeviation => moments::standard_deviation(data),
        Feature::SumValues => moments::sum(data),
        Feature::AbsEnergy => moments::abs_energy(data),
        Feature::RootMeanSquare => moments::root_mean_square(data),
        Feature::Skewness => moments::skewness(data),
        Feature::Kurtosis => moments::kurtosis(data),
        other => other.compute(data),
    }
}

/// Most samples `fourier_entropy` is computed over. The crate's implementation is a naive DFT,
/// quadratic in the number of samples (~1.2 s at 20k, ~11 s at 60k, hours at a million) on a
/// worker that nothing can cancel, so a larger range is refused rather than started.
pub const FOURIER_ENTROPY_MAX_SAMPLES: usize = 20_000;

/// Fails if `features` includes `fourier_entropy` and `sample_count` is past
/// [`FOURIER_ENTROPY_MAX_SAMPLES`].
pub fn check_fourier_entropy_size(features: &[Feature], sample_count: usize) -> Result<(), String> {
    if sample_count > FOURIER_ENTROPY_MAX_SAMPLES
        && features
            .iter()
            .any(|f| matches!(f, Feature::FourierEntropy))
    {
        return Err(format!(
            "TSDB: fourier_entropy is quadratic in the number of samples and is limited to \
             {FOURIER_ENTROPY_MAX_SAMPLES}; the range has {sample_count}"
        ));
    }
    Ok(())
}

/// The cost of computing `features` over `sample_count` values, as the sum of what each visits:
/// the samples once, once per lag for `partial_autocorrelation`, and every pair of samples for
/// `fourier_entropy`. A saturating sum, so an absurd range cannot wrap into looking cheap.
pub fn features_work(sample_count: usize, features: &[Feature]) -> usize {
    features
        .iter()
        .map(|feature| match feature {
            Feature::FourierEntropy => sample_count.saturating_mul(sample_count),
            Feature::PartialAutocorrelation { lag } => sample_count.saturating_mul(*lag),
            _ => sample_count,
        })
        .fold(0, usize::saturating_add)
}

/// Compute features and return a map of feature name → value.
///
/// Features are deduplicated by their canonical name before computation.
pub fn compute_features_map(data: &[f64], features: &[Feature]) -> BTreeMap<String, f64> {
    // Deduplicate by canonical name (first occurrence wins)
    let mut seen = std::collections::HashSet::new();
    let unique: Vec<Feature> = features
        .iter()
        .filter(|f| seen.insert(f.name()))
        .cloned()
        .collect();

    map_on_current_pool(&unique, |feature| {
        (feature.name(), compute_feature(feature, data))
    })
    .into_iter()
    .collect()
}

#[cfg(test)]
mod work_tests {
    use super::*;

    #[test]
    fn linear_features_cost_one_visit_per_sample_each() {
        let features = [Feature::Mean, Feature::Median, Feature::Variance];
        assert_eq!(features_work(1_000, &features), 3_000);
        assert_eq!(features_work(1_000, &[]), 0);
    }

    #[test]
    fn partial_autocorrelation_costs_a_pass_per_lag() {
        let features = [Feature::PartialAutocorrelation { lag: 100 }];
        assert_eq!(features_work(2_000, &features), 200_000);
    }

    #[test]
    fn fourier_entropy_costs_every_pair_of_samples() {
        assert_eq!(features_work(3_000, &[Feature::FourierEntropy]), 9_000_000);
    }

    #[test]
    fn the_cost_of_an_absurd_range_saturates_instead_of_wrapping() {
        let features = [Feature::FourierEntropy, Feature::Mean];
        assert_eq!(features_work(usize::MAX, &features), usize::MAX);
    }

    #[test]
    fn fourier_entropy_is_limited_to_a_documented_size() {
        let with = [Feature::Mean, Feature::FourierEntropy];
        let without = [Feature::Mean];
        assert!(check_fourier_entropy_size(&with, FOURIER_ENTROPY_MAX_SAMPLES).is_ok());
        let err = check_fourier_entropy_size(&with, FOURIER_ENTROPY_MAX_SAMPLES + 1).unwrap_err();
        assert!(err.contains("limited to 20000"), "{err}");
        assert!(err.contains("the range has 20001"), "{err}");
        // Other features have no such limit.
        assert!(check_fourier_entropy_size(&without, 10_000_000).is_ok());
    }
}
