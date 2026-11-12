use crate::promql::common::math::sample_regression;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::utils::{expect_scalar, min_arity_error};
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

/// `predict_linear(v range-vector, t scalar)`
///
/// predicts the value of time series t seconds from now, based on the range vector v, using simple linear regression.
#[derive(Copy, Clone)]
pub(in crate::promql) struct PredictLinearFunction;

impl PromQLFunction for PredictLinearFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        if args.len() != 2 {
            return Err(min_arity_error("predict_linear", 2, args.len()));
        }
        let mut arg_iter = args.into_iter();
        let range_arg = arg_iter.next().unwrap();
        let secs_arg = arg_iter.next().unwrap();

        let series = range_arg.into_range_vector()?;
        let seconds_ahead = expect_scalar(secs_arg, "predict_linear", "t")?;

        // A regression needs two points *within a series*; `sample_regression`
        // enforces that and returns `None` otherwise, which drops that series
        // from the output. Counting the series instead would suppress the whole
        // result whenever the selector matched a single series.
        let eval_time = ctx.evaluation_ts as f64 / 1000_f64;
        eval_range(ctx, series, |samples| {
            let (slope, intercept) = sample_regression(samples)?;

            let origin = samples
                .first()
                .map(|sample| sample.timestamp)
                .unwrap_or(ctx.evaluation_ts) as f64
                / 1_000f64;

            let x = eval_time + seconds_ahead - origin;
            let value = slope * x + intercept;

            Some(value)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::PredictLinearFunction;
    use crate::common::Sample;
    use crate::promql::functions::{PromQLArg, PromQLFunction};
    use crate::promql::{EvalContext, EvalLabels, EvalSamples, ExprResult};

    fn predict(samples: &[(i64, f64)], at_ms: i64, seconds_ahead: f64) -> Vec<f64> {
        let series = EvalSamples {
            values: samples
                .iter()
                .map(|&(t, v)| Sample::new(t, v))
                .collect::<Vec<_>>()
                .into(),
            labels: EvalLabels::from_pairs(&[("job", "a")]),
            drop_name: false,
            range_ms: at_ms,
            range_end_ms: at_ms,
        };
        let ctx = EvalContext {
            evaluation_ts: at_ms,
            ..Default::default()
        };
        let result = PredictLinearFunction
            .apply_args(
                vec![
                    PromQLArg::RangeVector(vec![series]),
                    PromQLArg::Scalar(seconds_ahead),
                ],
                &ctx,
            )
            .unwrap();
        let ExprResult::InstantVector(samples) = result else {
            panic!("expected an instant vector");
        };
        samples.into_iter().map(|s| s.value).collect()
    }

    /// The line through the window, evaluated `t` seconds past the evaluation
    /// time, not past the window's first sample.
    #[test]
    fn predict_linear_extends_the_fit_past_the_evaluation_time() {
        // One per second from 0 at t = 0 s.
        let ramp = [(0, 0.0), (10_000, 10.0), (20_000, 20.0)];
        assert_eq!(predict(&ramp, 20_000, 10.0), vec![30.0]);
        assert_eq!(predict(&ramp, 20_000, 0.0), vec![20.0]);
        assert_eq!(predict(&ramp, 30_000, 0.0), vec![30.0]);
        assert_eq!(predict(&ramp, 20_000, -20.0), vec![0.0]);
    }

    #[test]
    fn a_flat_window_predicts_its_value_and_one_sample_predicts_nothing() {
        assert_eq!(
            predict(&[(0, 5.0), (10_000, 5.0)], 10_000, 3600.0),
            vec![5.0]
        );
        assert!(predict(&[(0, 5.0)], 10_000, 60.0).is_empty());
    }
}
