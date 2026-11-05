use crate::common::math::kahan_inc;
use crate::promql::functions::rollups::eval_rollups;
use crate::promql::functions::types::{
    FunctionCallContext, PromQLArg, PromQLFunction, RollupWindow,
};
use crate::promql::functions::utils::{
    expect_range_vector, expect_scalar, max_arity_error, min_arity_error,
};
use crate::promql::{EvalResult, ExprResult};

/// The quadrature rule applied to each adjacent pair of samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::promql) enum IntegralStrategy {
    LeftPoint,
    RightPoint,
    Trapezoidal,
}

impl IntegralStrategy {
    /// The rule a strategy argument names: absent means the trapezoidal default,
    /// and `None` means the argument names no rule.
    pub(in crate::promql) fn from_param(param: Option<f64>) -> Option<IntegralStrategy> {
        match param {
            None => Some(IntegralStrategy::Trapezoidal),
            Some(0.0) => Some(IntegralStrategy::LeftPoint),
            Some(1.0) => Some(IntegralStrategy::RightPoint),
            Some(2.0) => Some(IntegralStrategy::Trapezoidal),
            Some(_) => None,
        }
    }
}

/// `integral(v range-vector, strategy=2 scalar)`
///
/// The time integral of each series over its window, in value-seconds.
#[derive(Copy, Clone)]
pub(in crate::promql) struct IntegralFunction;

impl PromQLFunction for IntegralFunction {
    fn apply_call(
        &self,
        args: Vec<PromQLArg>,
        ctx: &FunctionCallContext,
    ) -> EvalResult<ExprResult> {
        if args.is_empty() {
            return Err(min_arity_error("integral", 1, args.len()));
        }
        if args.len() > 2 {
            return Err(max_arity_error("integral", 2, args.len()));
        }
        let mut args = args.into_iter();
        let range = expect_range_vector(args.next().expect("checked arg count"), "integral")?;
        let strategy = args
            .next()
            .map(|arg| expect_scalar(arg, "integral", "strategy"))
            .transpose()?;

        Ok(ExprResult::InstantVector(eval_rollups(
            ctx,
            range,
            strategy,
            rollup_integral,
        )?))
    }
}

fn nan_as_zero(value: f64) -> f64 {
    if value.is_nan() { 0.0 } else { value }
}

/// `integral` over one window. Named rather than inline so the pushed-down path
/// reduces a window with the very same function the local path runs.
///
/// A strategy naming no rule answers as trapezoidal. Prometheus does the same
/// and adds a warning, which this engine has no way to report.
///
/// Each pair is weighted by its real interval, so integration runs straight
/// across gaps. A NaN sample counts as zero, which makes an all-NaN window
/// answer 0, not NaN. The trapezoid midpoint is not guarded against overflow:
/// near `f64::MAX` it answers `Inf`, as the reference does.
pub(in crate::promql) fn rollup_integral(window: &RollupWindow, strategy: Option<f64>) -> f64 {
    let strategy = IntegralStrategy::from_param(strategy).unwrap_or(IntegralStrategy::Trapezoidal);
    let (mut sum, mut c) = (0.0, 0.0);
    for i in 1..window.values.len() {
        let prev = nan_as_zero(window.values[i - 1]);
        let curr = nan_as_zero(window.values[i]);
        let (value, value_c) = match strategy {
            IntegralStrategy::LeftPoint => (prev, 0.0),
            IntegralStrategy::RightPoint => (curr, 0.0),
            IntegralStrategy::Trapezoidal => {
                let (value, value_c) = kahan_inc(curr, prev, 0.0);
                (value / 2.0, value_c / 2.0)
            }
        };
        if value != 0.0 || value_c != 0.0 {
            let dt = (window.timestamps[i] - window.timestamps[i - 1]) as f64 / 1000.0;
            (sum, c) = kahan_inc(value * dt, sum, c + value_c * dt);
        }
    }
    if sum.is_infinite() { sum } else { sum + c }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Timestamp;

    /// Every strategy argument the function accepts, including the default.
    const STRATEGIES: [Option<f64>; 4] = [None, Some(0.0), Some(1.0), Some(2.0)];

    fn integrate_at(values: &[f64], timestamps: &[Timestamp], strategy: Option<f64>) -> f64 {
        rollup_integral(&RollupWindow { values, timestamps }, strategy)
    }

    /// Samples 10 s apart, the spacing of the conformance fixtures.
    fn integrate(values: &[f64], strategy: Option<f64>) -> f64 {
        let timestamps: Vec<Timestamp> = (0..values.len() as i64).map(|i| i * 10_000).collect();
        integrate_at(values, &timestamps, strategy)
    }

    const IRREGULAR_VALUES: [f64; 3] = [0.0, 100.0, 100.0];
    const IRREGULAR_TIMESTAMPS: [Timestamp; 3] = [0, 1_000, 60_000];

    #[test]
    fn weights_each_pair_by_its_real_interval() {
        let at = |s| integrate_at(&IRREGULAR_VALUES, &IRREGULAR_TIMESTAMPS, s);
        assert_eq!(at(None), 5950.0);
        assert_eq!(at(Some(2.0)), 5950.0);
        assert_eq!(at(Some(0.0)), 5900.0);
        assert_eq!(at(Some(1.0)), 6000.0);
    }

    #[test]
    fn single_sample_window_is_zero() {
        for strategy in STRATEGIES {
            assert_eq!(integrate(&[7.0], strategy), 0.0, "{strategy:?}");
        }
    }

    /// Unlike `sum_over_time`, which answers NaN here.
    #[test]
    fn all_nan_window_is_zero() {
        for strategy in STRATEGIES {
            assert_eq!(
                integrate(&[f64::NAN, f64::NAN], strategy),
                0.0,
                "{strategy:?}"
            );
        }
    }

    #[test]
    fn nan_counts_as_zero() {
        for strategy in STRATEGIES {
            assert_eq!(
                integrate(&[f64::NAN, 1.0, f64::NAN], strategy),
                10.0,
                "{strategy:?}"
            );
        }
        assert_eq!(integrate(&[f64::NAN, 1.0], Some(2.0)), 5.0);
        assert_eq!(integrate(&[f64::NAN, 1.0], Some(0.0)), 0.0);
        assert_eq!(integrate(&[f64::NAN, 1.0], Some(1.0)), 10.0);
    }

    #[test]
    fn compensates_large_magnitudes() {
        let values = [0.0, 1.0, 1e100, 1.0, -1e100, 1.0, 0.0];
        for strategy in STRATEGIES {
            assert_eq!(integrate(&values, strategy), 30.0, "{strategy:?}");
        }
    }

    #[test]
    fn infinities_follow_ieee() {
        let inf = f64::INFINITY;
        assert_eq!(integrate(&[inf, 0.0, inf], None), inf);
        assert!(integrate(&[1.0, inf, 1.0, -inf, 1.0], None).is_nan());
    }

    /// The midpoint overflows rather than falling back to an incremental mean
    /// as `kahan_avg` does: the reference answers `Inf` here.
    #[test]
    fn midpoint_overflow_is_inf() {
        let near_max = 9.988465674311579e307;
        assert_eq!(integrate(&[near_max, near_max], None), f64::INFINITY);
    }

    #[test]
    fn invalid_strategy_answers_as_trapezoidal() {
        for strategy in [3.0, -1.0, 0.5, f64::NAN, f64::INFINITY] {
            assert_eq!(
                integrate_at(&IRREGULAR_VALUES, &IRREGULAR_TIMESTAMPS, Some(strategy)),
                5950.0,
                "{strategy}"
            );
        }
    }

    #[test]
    fn from_param_accepts_only_the_three_rules() {
        use IntegralStrategy::*;
        assert!(matches!(
            IntegralStrategy::from_param(None),
            Some(Trapezoidal)
        ));
        assert!(matches!(
            IntegralStrategy::from_param(Some(0.0)),
            Some(LeftPoint)
        ));
        assert!(matches!(
            IntegralStrategy::from_param(Some(1.0)),
            Some(RightPoint)
        ));
        assert!(matches!(
            IntegralStrategy::from_param(Some(2.0)),
            Some(Trapezoidal)
        ));
        for invalid in [3.0, -1.0, 0.5, f64::NAN, f64::INFINITY] {
            assert!(
                IntegralStrategy::from_param(Some(invalid)).is_none(),
                "{invalid}"
            );
        }
    }
}
