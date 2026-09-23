mod error;
pub mod forecasting;
pub mod math;
pub mod outliers;
pub mod quantile_estimators;
pub mod seasonality;

pub use error::*;

/// Largest lag accepted where the cost of the statistic grows with the lag: partial
/// autocorrelation (Durbin–Levinson keeps a `(lag + 1)²` matrix — 8 MB at this cap), aggregated
/// autocorrelation, the stationarity tests' `LAGS` and cross-correlation's `MAXLAG` (all
/// `O(samples × lag)`). Without it one command could ask for gigabytes or hours of CPU.
pub const MAX_ANALYSIS_LAG: usize = 1_000;
