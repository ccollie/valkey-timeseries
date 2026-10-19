//! The analysis lane: outlier detection, and standalone label searches and `TS.QUERYLABELS`
//! reads too large for the main thread.
//!
//! A [`BoundedExecutor`] rather than the shared pool: an analysis job runs for a long time,
//! and would hold a shared worker that GIL holders wait on (R2 in `common::threads`). Bounded,
//! so a burst of large requests is refused rather than queued without limit.

use crate::common::acl::caller_user_name;
use crate::common::context::{get_current_db, is_blocking_denied};
use crate::common::replies::{ReplyContext, ThreadSafeReplyContext, block_client};
use crate::common::threads::{BoundedExecutor, Capacity, lane_workers};
use crate::fanout::FanoutContext;
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

/// [`run_on_analysis_lane`] for a read of the caller's database: `read` runs on the lane
/// through a [`FanoutContext`] carrying the caller's ACL user and selected database, so each
/// lock it takes re-selects the one and re-installs the other. `reply` answers the client with
/// what it read; an error from `read` is answered as an error reply.
///
/// The read gives the module lock back between batches, so it is not a point-in-time view:
/// it can observe writes made while it runs. Callers check [`is_blocking_denied`] first and
/// run inline when it is set.
pub(crate) fn run_read_on_analysis_lane<T, R, P>(
    ctx: &Context,
    what: &str,
    read: R,
    reply: P,
) -> ValkeyResult
where
    R: FnOnce(&FanoutContext) -> ValkeyResult<T> + Send + 'static,
    P: FnOnce(&ReplyContext, T) + Send + 'static,
{
    // Read here, on the main thread, where the GIL is held.
    let src = FanoutContext::new(caller_user_name(ctx), get_current_db(ctx));
    run_on_analysis_lane(ctx, what, move |thread_ctx| match read(&src) {
        Ok(value) => reply(&thread_ctx.get_reply_context(), value),
        Err(err) => {
            thread_ctx.reply(Err(err));
        }
    })
}
