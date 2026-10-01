//! The analysis lane: where analysis commands run work too large for the main thread.
//!
//! A [`BoundedExecutor`] rather than the shared pool: an analysis job runs for a long time,
//! and would hold a shared worker that GIL holders wait on (R2 in `common::threads`). Bounded,
//! so a burst of large requests is refused rather than queued without limit.

use crate::common::context::is_blocking_denied;
use crate::common::replies::{ThreadSafeReplyContext, block_client};
use crate::common::threads::{BoundedExecutor, Capacity, lane_workers};
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyValue};

/// Jobs waiting for an analysis worker before new ones are refused.
const ANALYSIS_QUEUE_CAPACITY: usize = 256;

static ANALYSIS_EXECUTOR: BoundedExecutor = BoundedExecutor::new(
    "ts-analysis",
    lane_workers,
    Capacity::Fixed(ANALYSIS_QUEUE_CAPACITY),
);

/// Set by `TS._DEBUG PANIC_NEXT_ANALYSIS_JOB` (debug mode only): the next analysis job panics.
/// Integration tests use it to check that a failed job still answers its client.
static PANIC_NEXT_ANALYSIS_JOB: AtomicBool = AtomicBool::new(false);

/// Makes the next job on the analysis lane panic. For tests; see [`PANIC_NEXT_ANALYSIS_JOB`].
pub(crate) fn panic_next_analysis_job() {
    PANIC_NEXT_ANALYSIS_JOB.store(true, Ordering::Relaxed);
}

/// Blocks the client of `ctx` and runs `job` on the analysis lane, which answers it.
///
/// The queue place is reserved before the client is blocked, so a busy lane is an ordinary
/// error reply (`TSDB: <what>: too many queued jobs (limit N)`). A job that panics, or ends
/// without replying, is answered with an error when its reply context drops; the cause goes
/// to the server log.
///
/// A client that cannot be blocked (inside `MULTI`, a script, or an `RM_Call` without the K
/// flag) is refused: the server asserts on blocking one. Callers run such requests inline
/// instead, and check [`is_blocking_denied`] first.
pub(crate) fn run_on_analysis_lane<F>(ctx: &Context, what: &str, job: F) -> ValkeyResult
where
    F: FnOnce(&ThreadSafeReplyContext) + Send + 'static,
{
    if is_blocking_denied(ctx) {
        return Err(ValkeyError::String(format!(
            "TSDB: {what} cannot run in the background inside MULTI, a script or a module call"
        )));
    }
    let slot = ANALYSIS_EXECUTOR
        .try_reserve()
        .map_err(|rejected| ValkeyError::String(format!("TSDB: {what}: {rejected}")))?;
    let blocked_client = block_client(ctx);
    slot.spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        if PANIC_NEXT_ANALYSIS_JOB.swap(false, Ordering::Relaxed) {
            panic!("TS._DEBUG PANIC_NEXT_ANALYSIS_JOB");
        }
        job(&thread_ctx);
    });

    // Answered later, from the analysis lane.
    Ok(ValkeyValue::NoReply)
}

/// [`run_analysis`] for work that is never cheap enough to run inline by choice, such as model
/// fitting: always on the analysis pool, except where the client cannot be blocked, where
/// `work` is held to `unblockable_max`.
pub(super) fn run_analysis_in_background<T, C, R>(
    ctx: &Context,
    work: usize,
    unblockable: WorkLimits,
    timeout: AnalysisTimeout,
    compute: C,
    reply: R,
) -> ValkeyResult
where
    T: Send + 'static,
    C: FnOnce() -> ValkeyResult<T> + Send + 'static,
    R: FnOnce(&AnalysisCtx<'_>, T) -> ValkeyResult + Send + 'static,
{
    debug_assert_eq!(unblockable.inline_max, 0, "use WorkLimits::background_only");
    // `max(1)`: an empty measure must still take the pool path, not the inline one.
    run_analysis(ctx, work.max(1), unblockable, timeout, compute, reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: WorkLimits = WorkLimits {
        inline_max: 10,
        unblockable_max: 1_000,
        unit: "samples",
    };

    #[test]
    fn work_up_to_the_limit_is_allowed_where_blocking_is_denied() {
        assert!(LIMITS.check_unblockable(0).is_ok());
        assert!(LIMITS.check_unblockable(LIMITS.inline_max + 1).is_ok());
        assert!(LIMITS.check_unblockable(LIMITS.unblockable_max).is_ok());
    }

    #[test]
    fn work_over_the_limit_is_refused_with_its_size_and_unit() {
        let err = LIMITS.check_unblockable(1_001).unwrap_err().to_string();
        assert!(err.contains("too large to run inside MULTI"), "{err}");
        assert!(
            err.contains("1001 samples exceeds the limit of 1000"),
            "{err}"
        );
        assert!(err.contains("outside"), "{err}");
    }

    #[test]
    fn a_background_only_command_has_no_inline_threshold() {
        let limits = WorkLimits::background_only(500, "sample-models");
        assert_eq!(limits.inline_max, 0);
        assert!(limits.check_unblockable(500).is_ok());
        assert!(limits.check_unblockable(501).is_err());
    }

    #[test]
    fn the_limit_does_not_overflow_on_saturated_work() {
        // Commands compute work with saturating products.
        assert!(LIMITS.check_unblockable(usize::MAX).is_err());
    }
}
