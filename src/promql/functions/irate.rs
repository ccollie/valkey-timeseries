use crate::common::Sample;
use crate::promql::functions::range_vector_functions::eval_range;
use crate::promql::functions::{PromQLArg, PromQLFunction};
use crate::promql::{EvalContext, EvalResult, ExprResult};

#[derive(Clone, Copy, Debug)]
pub(in crate::promql) struct IRateFunction;

impl PromQLFunction for IRateFunction {
    fn apply(&self, arg: PromQLArg, ctx: &EvalContext) -> EvalResult<ExprResult> {
        let samples = arg.into_range_vector()?;
        eval_range(ctx, samples, rollup_irate)
    }
}

/// `irate` over one window: the per-second rate between its last two samples.
///
/// Named rather than inline so the pushed-down path can reduce a window with the
/// very same function the local path runs.
pub(in crate::promql) fn rollup_irate(samples: &[Sample]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let a = &samples[samples.len() - 2];
    let b = &samples[samples.len() - 1];
    let dt = b.timestamp.saturating_sub(a.timestamp);
    if dt <= 0 {
        return None;
    }
    let mut dv = b.value - a.value;
    if dv < 0.0 {
        dv = b.value;
    }
    Some(dv / (dt as f64 / 1000f64))
}

#[cfg(test)]
mod tests {
    use super::rollup_irate;
    use crate::common::Sample;

    fn s(t: i64, v: f64) -> Sample {
        Sample::new(t, v)
    }

    #[test]
    fn irate_is_the_per_second_rate_of_the_last_two_samples() {
        // Only the final pair counts: 30 -> 50 over 10 s.
        assert_eq!(
            rollup_irate(&[s(0, 0.0), s(10_000, 30.0), s(20_000, 50.0)]),
            Some(2.0)
        );
    }

    #[test]
    fn a_counter_reset_counts_from_zero() {
        // 50 -> 5 is a reset: the increase is the 5 since it.
        assert_eq!(rollup_irate(&[s(0, 50.0), s(5_000, 5.0)]), Some(1.0));
    }

    #[test]
    fn irate_needs_two_samples_at_different_times() {
        assert_eq!(rollup_irate(&[]), None);
        assert_eq!(rollup_irate(&[s(0, 1.0)]), None);
        assert_eq!(rollup_irate(&[s(1_000, 1.0), s(1_000, 2.0)]), None);
    }
}
