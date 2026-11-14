use super::go_compat::{cosh, sinh};
use crate::promql::common::math::{max_ignore_nan, max_with_nan, min_ignore_nan, min_with_nan};
use crate::promql::functions::PromqlFunctionKind;
use crate::promql::functions::types::{PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{
    exact_arity_error, map_scalar_or_vector, map_values, three_args, two_args,
};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};

#[inline]
fn exec_unary_fn(arg: PromQLArg, f: fn(f64) -> f64) -> EvalResult<ExprResult> {
    map_scalar_or_vector(arg, f)
}

fn promql_sgn(v: f64) -> f64 {
    if v.is_nan() {
        f64::NAN
    } else if v > 0.0 {
        1.0
    } else if v < 0.0 {
        -1.0
    } else {
        0.0
    }
}

macro_rules! make_unary_function {
    ( $name: ident, $rf: expr ) => {
        #[derive(Copy, Clone, Default)]
        pub(crate) struct $name;

        impl PromQLFunction for $name {
            fn apply(&self, arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
                exec_unary_fn(arg, $rf)
            }
        }
    };
}

/// Every unary math function: its kind, the struct that evaluates it, and the
/// kernel that struct applies — which [`unary_math_kernel`] hands to constant
/// folding, so a folded constant and an evaluated one cannot differ.
macro_rules! unary_math_functions {
    ( $( $kind: ident => $name: ident, $rf: expr; )* ) => {
        $( make_unary_function!($name, $rf); )*

        /// The kernel the unary math function `kind` applies to each value,
        /// or `None` when `kind` is not one.
        pub(in crate::promql) fn unary_math_kernel(kind: PromqlFunctionKind) -> Option<fn(f64) -> f64> {
            match kind {
                $( PromqlFunctionKind::$kind => Some($rf), )*
                _ => None,
            }
        }
    };
}

unary_math_functions! {
    Abs => AbsFunction, f64::abs;
    Acos => AcosFunction, f64::acos;
    Acosh => AcoshFunction, f64::acosh;
    Asin => AsinFunction, f64::asin;
    Asinh => AsinhFunction, f64::asinh;
    Atan => AtanFunction, f64::atan;
    Atanh => AtanhFunction, f64::atanh;
    Ceil => CeilFunction, f64::ceil;
    Cos => CosFunction, f64::cos;
    Cosh => CoshFunction, cosh;
    Deg => DegFunction, f64::to_degrees;
    Exp => ExpFunction, f64::exp;
    Floor => FloorFunction, f64::floor;
    Ln => LnFunction, f64::ln;
    Log10 => Log10Function, f64::log10;
    Log2 => Log2Function, f64::log2;
    Rad => RadFunction, f64::to_radians;
    Sgn => SgnFunction, promql_sgn;
    Sin => SinFunction, f64::sin;
    Sinh => SinhFunction, sinh;
    Sqrt => SqrtFunction, f64::sqrt;
    Tan => TanFunction, f64::tan;
    Tanh => TanhFunction, f64::tanh;
}

/// Pi function: returns PI encoded as a single-sample vector.
#[derive(Copy, Clone)]
pub(in crate::promql) struct PiFunction;

impl PromQLFunction for PiFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, ctx: &EvalContext) -> EvalResult<ExprResult> {
        if !args.is_empty() {
            return Err(exact_arity_error("pi", 0, args.len()));
        }

        Ok(ExprResult::InstantVector(vec![EvalSample::unlabelled(
            ctx.evaluation_ts,
            std::f64::consts::PI,
        )]))
    }
}

/// Round function with an optional scalar second argument (`to_nearest`).
#[derive(Copy, Clone)]
pub(in crate::promql) struct RoundFunction;

impl RoundFunction {
    fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
        if to_nearest == 0.0 {
            return value;
        }
        let inv = 1.0 / to_nearest;
        (value * inv + 0.5).floor() / inv
    }
}

impl PromQLFunction for RoundFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        self.apply_args(vec![arg], ctx)
    }

    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let mut args_iter = args.into_iter();
        let Some(first_arg) = args_iter.next() else {
            return Err(EvaluationError::InternalError(
                "round requires at least one argument".to_string(),
            ));
        };
        let samples = first_arg.into_instant_vector()?;
        let to_nearest = match args_iter.next() {
            None => 1.0,
            Some(second_arg) => {
                // Keep this defensive arity guard because function handlers can
                // still be called directly in tests/internal paths.
                if args_iter.next().is_some() {
                    return Err(EvaluationError::InternalError(
                        "round accepts at most two arguments".to_string(),
                    ));
                }
                second_arg.into_scalar()?.abs()
            }
        };

        Ok(map_values(samples, |value| {
            Self::round_to_nearest(value, to_nearest)
        }))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct ClampMaxFunction;

impl PromQLFunction for ClampMaxFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let (vector, max) = two_args("clamp_max", args)?;
        let samples = vector.into_instant_vector()?;
        let max = max.into_scalar()?;
        Ok(map_values(samples, |value| min_with_nan(value, max)))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct ClampMinFunction;

impl PromQLFunction for ClampMinFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let (vector, min) = two_args("clamp_min", args)?;
        let samples = vector.into_instant_vector()?;
        let min = min.into_scalar()?;
        Ok(map_values(samples, |value| max_with_nan(value, min)))
    }
}

#[derive(Copy, Clone)]
pub(in crate::promql) struct ClampFunction;

impl PromQLFunction for ClampFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let (vector, min, max) = three_args("clamp", args)?;
        let samples = vector.into_instant_vector()?;
        let min = min.into_scalar()?;
        let max = max.into_scalar()?;
        if min > max {
            return Ok(ExprResult::InstantVector(vec![]));
        }

        Ok(map_values(samples, |value| {
            max_with_nan(min_with_nan(value, max), min)
        }))
    }
}

/// max_of returns the maximum of two scalars as a scalar. Like the `max`
/// aggregator, it returns NaN only if both arguments are NaN.
#[derive(Copy, Clone)]
pub(in crate::promql) struct MaxOfFunction;

impl PromQLFunction for MaxOfFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let (a, b) = two_args("max_of", args)?;
        Ok(ExprResult::Scalar(max_ignore_nan(
            a.into_scalar()?,
            b.into_scalar()?,
        )))
    }
}

/// min_of returns the minimum of two scalars as a scalar. Like the `min`
/// aggregator, it returns NaN only if both arguments are NaN.
#[derive(Copy, Clone)]
pub(in crate::promql) struct MinOfFunction;

impl PromQLFunction for MinOfFunction {
    fn apply_args(&self, args: Vec<PromQLArg>, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        let (a, b) = two_args("min_of", args)?;
        Ok(ExprResult::Scalar(min_ignore_nan(
            a.into_scalar()?,
            b.into_scalar()?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::promql_sgn;

    #[test]
    fn should_match_promql_sgn_semantics() {
        assert_eq!(promql_sgn(f64::NEG_INFINITY), -1.0);
        assert_eq!(promql_sgn(-50.0), -1.0);
        assert_eq!(promql_sgn(-0.0), 0.0);
        assert_eq!(promql_sgn(0.0), 0.0);
        assert_eq!(promql_sgn(100.0), 1.0);
        assert_eq!(promql_sgn(f64::INFINITY), 1.0);
        assert!(promql_sgn(f64::NAN).is_nan());
    }
}
