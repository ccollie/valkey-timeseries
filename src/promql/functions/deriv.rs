use crate::common::Sample;
use crate::promql::common::math::sample_regression;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

#[derive(Copy, Clone)]
pub(in crate::promql) struct DerivFunction;

impl PromQLFunction for DerivFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let series = arg.into_range_vector()?;
        eval_range(ctx, series, rollup_deriv)
    }
}

/// `deriv` over one window: the slope of the simple linear regression through
/// its samples, or nothing when there are too few samples. A NaN or infinite
/// sample in the window poisons the slope to NaN, which is returned as the
/// series' value rather than dropping the series, matching Prometheus.
///
/// Named rather than inline so the pushed-down path reduces a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_deriv(values: &[Sample]) -> Option<f64> {
    let (slope, _intercept) = sample_regression(values)?;
    Some(slope)
}

#[cfg(test)]
mod tests {
    use super::rollup_deriv;
    use crate::common::Sample;

    fn s(t: i64, v: f64) -> Sample {
        Sample::new(t, v)
    }

    #[test]
    fn deriv_is_the_least_squares_slope_per_second() {
        // x = 0, 10, 20 s; y = 1, 3, 2: Sxy = 10, Sxx = 200, slope 0.05/s.
        let slope = rollup_deriv(&[s(0, 1.0), s(10_000, 3.0), s(20_000, 2.0)]).unwrap();
        assert!((slope - 0.05).abs() < 1e-12, "{slope}");
    }

    #[test]
    fn deriv_needs_two_samples_and_keeps_a_nan() {
        assert_eq!(rollup_deriv(&[s(0, 1.0)]), None);
        assert!(
            rollup_deriv(&[s(0, 1.0), s(1_000, f64::NAN)])
                .unwrap()
                .is_nan()
        );
    }
}
