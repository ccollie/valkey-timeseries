use crate::promql::functions::types::{FunctionCallContext, PromQLArg, PromQLFunction};
use crate::promql::functions::utils::{exact_arity_error, labels_for_absent};
use crate::promql::{EvalContext, EvalResult, EvalSample, EvaluationError, ExprResult};
use promql_parser::parser::Expr;

/// `absent(instant-vector)`
///
/// Returns 1.0 if the input is empty, an empty vector otherwise. The labels on
/// that sample are derived from the argument selector's matchers rather than
/// from data — see [`labels_for_absent`] — so this overrides `apply_call` to
/// reach the unevaluated argument.
#[derive(Copy, Clone)]
pub(in crate::promql) struct AbsentFunction;

impl AbsentFunction {
    fn evaluate(
        arg: PromQLArg,
        ctx: &EvalContext,
        raw_arg: Option<&Expr>,
    ) -> EvalResult<ExprResult> {
        let samples = arg.into_instant_vector()?;
        if samples.is_empty() {
            // Return a single sample with value 1.0 at the evaluation timestamp
            Ok(ExprResult::InstantVector(vec![EvalSample {
                timestamp_ms: ctx.evaluation_ts,
                value: 1.0,
                labels: labels_for_absent(raw_arg),
                drop_name: false,
            }]))
        } else {
            // Return empty vector when input has samples
            Ok(ExprResult::InstantVector(vec![]))
        }
    }
}

impl PromQLFunction for AbsentFunction {
    /// Reached only by callers that have no call site to speak of — the AST-less
    /// path cannot know the matchers, so it answers with no labels.
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Self::evaluate(arg, ctx, None)
    }

    fn apply_call(
        &self,
        args: Vec<PromQLArg>,
        ctx: &FunctionCallContext,
    ) -> EvalResult<ExprResult> {
        let mut args = args;
        if args.len() != 1 {
            return Err(exact_arity_error("absent", 1, args.len()));
        }
        Self::evaluate(args.swap_remove(0), ctx, ctx.raw_arg(0))
    }
}

/// Scalar function: converts a single-element vector to scalar (returns as-is or empty)
#[derive(Copy, Clone)]
pub(in crate::promql) struct ScalarFunction;

impl PromQLFunction for ScalarFunction {
    fn apply(&self, arg: PromQLArg, _ctx: &EvalContext) -> EvalResult<ExprResult> {
        match arg {
            PromQLArg::Scalar(f) => Ok(ExprResult::Scalar(f)),
            PromQLArg::InstantVector(samples) => {
                if samples.len() == 1 {
                    Ok(ExprResult::Scalar(samples[0].value))
                } else {
                    Ok(ExprResult::Scalar(f64::NAN))
                }
            }
            _ => Err(EvaluationError::InternalError(
                "scalar function expects a scalar or single-sample vector argument".to_string(),
            )),
        }
    }
}

/// Vector function: converts a scalar value to a single-element instant vector with no labels.
#[derive(Copy, Clone)]
pub(in crate::promql) struct VectorFunction;

impl PromQLFunction for VectorFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        Ok(ExprResult::InstantVector(vec![EvalSample::unlabelled(
            ctx.evaluation_ts,
            arg.into_scalar()?,
        )]))
    }
}

/// A function of the query alone, taking no arguments: its value is read off
/// the evaluation context.
macro_rules! query_function {
    ( $(#[$meta: meta])* $name: ident, |$ctx: ident| $value: expr ) => {
        $(#[$meta])*
        #[derive(Copy, Clone)]
        pub(in crate::promql) struct $name;

        impl PromQLFunction for $name {
            fn apply_args(&self, _args: Vec<PromQLArg>, $ctx: &EvalContext) -> EvalResult<ExprResult> {
                Ok(ExprResult::Scalar($value))
            }
        }
    };
}

query_function!(
    /// Start function: returns the start timestamp of the query as the number of seconds since
    /// January 1, 1970 UTC.
    StartFunction,
    |ctx| ctx.query_start as f64 / 1000.0
);

query_function!(
    /// End function: returns the end timestamp of the query as the number of seconds since
    /// January 1, 1970 UTC. It is fixed for the whole query, not the current step, so it equals the
    /// evaluation timestamp only for instant queries.
    EndFunction,
    |ctx| ctx.query_end as f64 / 1000.0
);

query_function!(
    /// Range function: returns the range duration in seconds for range selectors, or 0 for instant selectors
    RangeFunction,
    |ctx| ctx.query_end.saturating_sub(ctx.query_start).max(0) as f64 / 1000.0
);

query_function!(
    /// step function: returns the step duration in seconds for range selectors, or 0 for instant selectors
    StepFunction,
    |ctx| if ctx.step_ms > 0 {
        ctx.step_ms as f64 / 1000.0
    } else {
        0.0
    }
);
