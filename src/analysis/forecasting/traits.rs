use anofox_forecast::core::{Forecast, TimeSeries};
use anofox_forecast::models::{BoxedForecaster, FittedParams, Forecaster};
use anofox_forecast::transform::{InverseMode, Transform};
// for some reason, Forecaster is not implemented for Box<dyn Forecaster> in the anofox_forecast crate, so we need to re-export it here
use anofox_forecast::Result as ForecastResult;

pub struct DynForecaster(BoxedForecaster);

impl DynForecaster {
    pub fn new(forecaster: BoxedForecaster) -> Self {
        Self(forecaster)
    }

    pub fn into_inner(self) -> BoxedForecaster {
        self.0
    }
}

impl From<BoxedForecaster> for DynForecaster {
    fn from(forecaster: BoxedForecaster) -> Self {
        Self(forecaster)
    }
}

impl From<DynForecaster> for BoxedForecaster {
    fn from(forecaster: DynForecaster) -> Self {
        forecaster.0
    }
}

impl std::ops::Deref for DynForecaster {
    type Target = BoxedForecaster;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DynForecaster {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Forecaster for DynForecaster {
    fn fit(&mut self, series: &TimeSeries) -> anofox_forecast::Result<()> {
        self.0.fit(series)
    }

    fn predict(&self, horizon: usize) -> anofox_forecast::Result<Forecast> {
        self.0.predict(horizon)
    }

    fn predict_with_intervals(
        &self,
        horizon: usize,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.predict_with_intervals(horizon, level)
    }

    fn fit_predict(
        &mut self,
        series: &TimeSeries,
        horizon: usize,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.fit_predict(series, horizon)
    }

    fn fit_predict_with_intervals(
        &mut self,
        series: &TimeSeries,
        horizon: usize,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.fit_predict_with_intervals(series, horizon, level)
    }

    fn fitted_values(&self) -> Option<&[f64]> {
        self.0.fitted_values()
    }

    fn fitted_values_with_intervals(&self, level: f64) -> Option<Forecast> {
        self.0.fitted_values_with_intervals(level)
    }

    fn residuals(&self) -> Option<&[f64]> {
        self.0.residuals()
    }

    fn trend_component(&self) -> anofox_forecast::Result<&[f64]> {
        self.0.trend_component()
    }

    fn seasonal_component(&self) -> anofox_forecast::Result<&[f64]> {
        self.0.seasonal_component()
    }

    fn residual_component(&self) -> anofox_forecast::Result<Vec<f64>> {
        self.0.residual_component()
    }

    fn training_values(&self) -> anofox_forecast::Result<&[f64]> {
        self.0.training_values()
    }

    fn training_regressors(&self) -> Option<&std::collections::HashMap<String, Vec<f64>>> {
        self.0.training_regressors()
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn is_fitted(&self) -> bool {
        self.0.is_fitted()
    }

    fn fitted_params(&self) -> Option<FittedParams> {
        self.0.fitted_params()
    }

    fn supports_exog(&self) -> bool {
        self.0.supports_exog()
    }

    fn has_exog(&self) -> bool {
        self.0.has_exog()
    }

    fn exog_names(&self) -> Option<&[String]> {
        self.0.exog_names()
    }

    fn exog_coefficients(&self) -> Option<&anofox_forecast::utils::OLSResult> {
        self.0.exog_coefficients()
    }

    fn predict_with_exog(
        &self,
        horizon: usize,
        future_regressors: &std::collections::HashMap<String, Vec<f64>>,
    ) -> anofox_forecast::Result<Forecast> {
        self.0.predict_with_exog(horizon, future_regressors)
    }

    fn predict_with_exog_intervals(
        &self,
        horizon: usize,
        future_regressors: &std::collections::HashMap<String, Vec<f64>>,
        level: f64,
    ) -> anofox_forecast::Result<Forecast> {
        self.0
            .predict_with_exog_intervals(horizon, future_regressors, level)
    }
}

/// A `Transform` trait object that also fixes up NaN handling on the
/// fitted-value path.
///
/// `Pipeline` reconstructs in-sample fitted values by running the inner
/// model's fitted values back through each transform's `inverse` in
/// [`InverseMode::Fitted`]. For length-changing transforms (differencing)
/// that inverse is a cumulative sum from the initial anchor, so a single
/// non-finite warm-up value from the model (ARIMA, Naive, ...) poisons every
/// later position and `METRICS` fails with "missing values detected".
///
/// This wrapper remembers the transformed *actual* series from
/// `fit_transform`. On the fitted path it substitutes the actual value at
/// each non-finite position before calling the inner inverse — so the
/// running level stays correct through the warm-up — and then restores NaN
/// at those positions in the output, which `run_forecast` already trims.
#[derive(Debug)]
pub struct DynTransform {
    inner: Box<dyn Transform>,
    /// Output of the last `fit_transform`, i.e. the actual series in the
    /// input space of `inverse`. Empty until fitted.
    transformed: Vec<f64>,
}

impl DynTransform {
    pub fn new(transform: Box<dyn Transform>) -> Self {
        Self {
            inner: transform,
            transformed: Vec::new(),
        }
    }

    pub fn into_inner(self) -> Box<dyn Transform> {
        self.inner
    }

    /// Fitted-mode inverse that tolerates non-finite fitted values.
    fn inverse_fitted(&self, values: &[f64]) -> ForecastResult<Vec<f64>> {
        let nan_positions: Vec<usize> = values
            .iter()
            .enumerate()
            .filter(|(_, v)| !v.is_finite())
            .map(|(i, _)| i)
            .collect();

        // Nothing to patch, or the model returned fitted values of a length
        // we cannot line up with the actual series: defer to the inner inverse.
        if nan_positions.is_empty() || self.transformed.len() != values.len() {
            return self.inner.inverse(values, InverseMode::Fitted);
        }

        let mut patched = values.to_vec();
        for &i in &nan_positions {
            patched[i] = self.transformed[i];
        }
        let mut out = self.inner.inverse(&patched, InverseMode::Fitted)?;

        // Length-changing inverses prepend their anchors; shift the NaN
        // positions past them so they land on the same observations. The
        // anchors themselves are copies of actual values, not model output,
        // so they are blanked as well: that keeps the warm-up a contiguous
        // leading prefix, which is what the metrics path trims.
        let shift = out.len().saturating_sub(values.len());
        for slot in out.iter_mut().take(shift) {
            *slot = f64::NAN;
        }
        for &i in &nan_positions {
            if let Some(slot) = out.get_mut(i + shift) {
                *slot = f64::NAN;
            }
        }
        Ok(out)
    }
}

impl From<Box<dyn Transform>> for DynTransform {
    fn from(transform: Box<dyn Transform>) -> Self {
        Self::new(transform)
    }
}

impl From<DynTransform> for Box<dyn Transform> {
    fn from(transform: DynTransform) -> Self {
        transform.inner
    }
}

impl std::ops::Deref for DynTransform {
    type Target = Box<dyn Transform>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for DynTransform {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Transform for DynTransform {
    fn fit_transform(&mut self, values: &[f64]) -> ForecastResult<Vec<f64>> {
        let transformed = self.inner.fit_transform(values)?;
        self.transformed = transformed.clone();
        Ok(transformed)
    }

    fn inverse(&self, values: &[f64], mode: InverseMode) -> ForecastResult<Vec<f64>> {
        match mode {
            InverseMode::Fitted => self.inverse_fitted(values),
            InverseMode::Predict => self.inner.inverse(values, mode),
        }
    }

    fn offset(&self) -> usize {
        self.inner.offset()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn clone_box(&self) -> Box<dyn Transform> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            transformed: self.transformed.clone(),
        })
    }
}
