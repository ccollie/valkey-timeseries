//! Keyword values for model specs. Matching is case-insensitive, like model and keyword names.

use anofox_forecast::models::exponential::SeasonalType;
use anofox_forecast::models::theta::DecompositionType;
use anofox_forecast::models::{SeasonalForecastMethod, TrendForecastMethod};

pub fn parse_trend_forecast_method(method: &str) -> Option<TrendForecastMethod> {
    match method.to_ascii_lowercase().as_str() {
        "linear" => Some(TrendForecastMethod::Linear),
        "autoets" => Some(TrendForecastMethod::AutoETS),
        "ses" => Some(TrendForecastMethod::SES),
        "naive" => Some(TrendForecastMethod::Naive),
        _ => None,
    }
}

pub fn parse_decomposition_type(input: &str) -> Option<DecompositionType> {
    match input.to_ascii_lowercase().as_str() {
        "additive" => Some(DecompositionType::Additive),
        "multiplicative" => Some(DecompositionType::Multiplicative),
        _ => None,
    }
}

pub fn parse_seasonal_forecast_method(input: &str) -> Option<SeasonalForecastMethod> {
    match input.to_ascii_lowercase().as_str() {
        "naive" => Some(SeasonalForecastMethod::Naive),
        "average" => Some(SeasonalForecastMethod::Average),
        _ => None,
    }
}

pub fn parse_seasonal_type(input: &str) -> Option<SeasonalType> {
    match input.to_ascii_lowercase().as_str() {
        "additive" => Some(SeasonalType::Additive),
        "multiplicative" => Some(SeasonalType::Multiplicative),
        _ => None,
    }
}
