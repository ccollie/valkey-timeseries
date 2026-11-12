use crate::common::Sample;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

#[derive(Clone, Copy, Debug)]
pub(in crate::promql) struct IDeltaFunction;
impl PromQLFunction for IDeltaFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        eval_range(ctx, samples, rollup_idelta)
    }
}

/// `idelta` over one window: the difference between its **last two** samples,
/// not across the whole window. `idelta` is the instant counterpart of `delta`
/// — everything before the final pair is only there to establish that a pair
/// exists.
///
/// Named rather than inline so the pushed-down path reduces a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_idelta(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let previous = &samples[samples.len() - 2];
    let last = &samples[samples.len() - 1];
    if previous.timestamp == last.timestamp {
        return None;
    }
    Some(last.value - previous.value)
}

#[cfg(test)]
mod tests {
    use super::rollup_idelta;
    use crate::common::Sample;

    fn s(t: i64, v: f64) -> Sample {
        Sample::new(t, v)
    }

    #[test]
    fn idelta_is_the_difference_of_the_last_two_samples() {
        assert_eq!(
            rollup_idelta(&[s(0, 100.0), s(10_000, 3.0), s(20_000, 7.5)]),
            Some(4.5)
        );
    }

    #[test]
    fn a_fall_is_negative_not_a_reset() {
        // Unlike irate, idelta is for gauges: no counter-reset correction.
        assert_eq!(rollup_idelta(&[s(0, 9.0), s(1_000, 4.0)]), Some(-5.0));
    }

    #[test]
    fn idelta_needs_two_samples_at_different_times() {
        assert_eq!(rollup_idelta(&[]), None);
        assert_eq!(rollup_idelta(&[s(0, 1.0)]), None);
        assert_eq!(rollup_idelta(&[s(1_000, 1.0), s(1_000, 2.0)]), None);
    }
}
