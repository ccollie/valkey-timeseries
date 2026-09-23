use anofox_forecast::Result as ForecastResult;
use anofox_forecast::transform::{InverseMode, Transform};

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
