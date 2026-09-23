//! In-sample fitted values on the scale of the series a model was fitted to.
//!
//! anofox's ARIMA-family models report `fitted_values()` on their internal, differenced scale
//! (ARIMA with `d ≥ 1` returns the fitted *differences*; SARIMA returns none at all), so scoring
//! them against the input series — `METRICS` — produced nonsense such as `r_squared` of −27, and
//! a `TRANSFORMS` pipeline inverted the wrong values. Their residuals, however, are the one-step
//! errors, which are the same on either scale (`y_t − ŷ_t = w_t − ŵ_t` for a differenced `w`),
//! so `input − residual` recovers the fit on the input scale.

use anofox_forecast::core::{Forecast, TimeSeries};
use anofox_forecast::models::{BoxedForecaster, FittedParams, Forecaster};
use std::collections::HashMap;

/// Whether a model reports fitted values on a differenced scale: anofox's ARIMA, SARIMA and
/// AutoARIMA (named `AutoARIMA` or `AutoARIMA (SARIMA)`).
pub fn is_arima_family(model_name: &str) -> bool {
    model_name.contains("ARIMA")
}

/// Rebuilds a model's fitted values on the scale of `input` from its residuals. The result is
/// aligned to the end of `input` and has `residuals.len()` values.
///
/// Warm-up positions — where the model has no lagged terms yet — stay NaN. They are the
/// positions where `fitted` is non-finite when the model reports fitted values of the same
/// length; otherwise (SARIMA) they are the leading run of zero residuals, which is how the
/// model leaves them. Returns `None` if the residuals are longer than the input.
pub fn input_scale_fitted(
    input: &[f64],
    fitted: Option<&[f64]>,
    residuals: &[f64],
) -> Option<Vec<f64>> {
    let offset = input.len().checked_sub(residuals.len())?;
    let fitted = fitted.filter(|f| f.len() == residuals.len());
    let leading_zeros = residuals.iter().take_while(|&&r| r == 0.0).count();
    Some(
        residuals
            .iter()
            .enumerate()
            .map(|(i, &residual)| {
                let warm_up = match fitted {
                    Some(fitted) => !fitted[i].is_finite(),
                    None => i < leading_zeros,
                };
                if warm_up || !residual.is_finite() {
                    f64::NAN
                } else {
                    input[offset + i] - residual
                }
            })
            .collect(),
    )
}

/// Wraps an ARIMA-family model so `fitted_values()` is on the scale of its input (see the
/// module docs). Everything else is forwarded unchanged.
pub struct InputScaleFitted {
    inner: BoxedForecaster,
    fitted: Option<Vec<f64>>,
}

impl InputScaleFitted {
    pub fn new(inner: BoxedForecaster) -> Self {
        Self {
            inner,
            fitted: None,
        }
    }
}

impl Forecaster for InputScaleFitted {
    fn fit(&mut self, series: &TimeSeries) -> anofox_forecast::Result<()> {
        self.fitted = None;
        self.inner.fit(series)?;
        self.fitted = self.inner.residuals().and_then(|residuals| {
            input_scale_fitted(
                series.primary_values(),
                self.inner.fitted_values(),
                residuals,
            )
        });
        Ok(())
    }

    fn predict(&self, horizon: usize) -> anofox_forecast::Result<Forecast> {
        self.inner.predict(horizon)
    }

    fn predict_with_intervals(
        &self,
        horizon: usize,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.inner.predict_with_intervals(horizon, level)
    }

    fn fitted_values(&self) -> Option<&[f64]> {
        self.fitted.as_deref()
    }

    /// The inner model's intervals are on its differenced scale, so none are offered.
    fn fitted_values_with_intervals(&self, _level: f64) -> Option<Forecast> {
        None
    }

    fn residuals(&self) -> Option<&[f64]> {
        self.inner.residuals()
    }

    fn trend_component(&self) -> anofox_forecast::Result<&[f64]> {
        self.inner.trend_component()
    }

    fn seasonal_component(&self) -> anofox_forecast::Result<&[f64]> {
        self.inner.seasonal_component()
    }

    fn explanation(
        &self,
    ) -> anofox_forecast::Result<anofox_forecast::models::inspect::Explanation> {
        self.inner.explanation()
    }

    fn residual_component(&self) -> anofox_forecast::Result<Vec<f64>> {
        self.inner.residual_component()
    }

    fn training_values(&self) -> anofox_forecast::Result<&[f64]> {
        self.inner.training_values()
    }

    fn training_regressors(&self) -> Option<&HashMap<String, Vec<f64>>> {
        self.inner.training_regressors()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn is_fitted(&self) -> bool {
        self.inner.is_fitted()
    }

    fn fitted_params(&self) -> Option<FittedParams> {
        self.inner.fitted_params()
    }

    fn supports_exog(&self) -> bool {
        self.inner.supports_exog()
    }

    fn has_exog(&self) -> bool {
        self.inner.has_exog()
    }

    fn exog_names(&self) -> Option<&[String]> {
        self.inner.exog_names()
    }

    fn exog_coefficients(&self) -> Option<&anofox_forecast::utils::OLSResult> {
        self.inner.exog_coefficients()
    }

    fn predict_with_exog(
        &self,
        horizon: usize,
        future_regressors: &HashMap<String, Vec<f64>>,
    ) -> anofox_forecast::Result<Forecast> {
        self.inner.predict_with_exog(horizon, future_regressors)
    }

    fn predict_with_exog_intervals(
        &self,
        horizon: usize,
        future_regressors: &HashMap<String, Vec<f64>>,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.inner
            .predict_with_exog_intervals(horizon, future_regressors, level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebuilds_the_fit_from_residuals_and_keeps_the_warm_up() {
        // A differenced model over 5 inputs: 4 residuals, the first one warm-up.
        let input = [10.0, 12.0, 15.0, 19.0, 24.0];
        let fitted_diff = [f64::NAN, 2.5, 3.5, 4.5];
        let residuals = [0.0, 0.5, 0.5, 0.5];
        let fitted = input_scale_fitted(&input, Some(&fitted_diff), &residuals).unwrap();
        assert!(fitted[0].is_nan());
        assert_eq!(&fitted[1..], &[14.5, 18.5, 23.5]);
    }

    #[test]
    fn without_fitted_values_leading_zero_residuals_are_warm_up() {
        let input = [1.0, 2.0, 3.0, 5.0];
        let residuals = [0.0, 0.0, 0.25, -0.5];
        let fitted = input_scale_fitted(&input, None, &residuals).unwrap();
        assert!(fitted[0].is_nan() && fitted[1].is_nan());
        assert_eq!(&fitted[2..], &[2.75, 5.5]);
    }

    #[test]
    fn residuals_longer_than_the_input_are_rejected() {
        assert!(input_scale_fitted(&[1.0], None, &[0.1, 0.2]).is_none());
    }

    #[test]
    fn recognises_the_arima_family() {
        for name in ["ARIMA", "SARIMA", "AutoARIMA", "AutoARIMA (SARIMA)"] {
            assert!(is_arima_family(name), "{name}");
        }
        assert!(!is_arima_family("ETS"));
    }
}
