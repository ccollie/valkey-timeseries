//! Inline-or-background execution for CPU-bound analysis commands.
//!
//! Analysis commands (`TS.TREND`, `TS.DECOMPOSE`, `TS.PERIODS`, `TS.STATIONARITY`,
//! `TS.AUTOCORRELATION`, `TS.XCORR`, `TS.FEATURES`, and the forecasting family) split their
//! work into a pure `compute` step and a `reply` step. Small inputs run both inline on the main
//! thread, which keeps the common case cheap; above a per-command work threshold the client
//! is blocked and both steps run on the analysis lane, so a large input can never stall
//! the server. Either way the reply code is written once against [`AnalysisCtx`].
//!
//! The lane is a [`BoundedExecutor`] rather than a pool: an analysis job runs for a long time,
//! and would hold a shared-pool worker that GIL holders wait on (R2 in `common::threads`), and
//! its `STORE` step takes the GIL, which no pool worker may (R1). Bounded, so a burst of large
//! requests is refused at once (`too many queued jobs`) rather than queued without limit. A
//! job fans out on the shared pool (`par_rayon`), never on a pool of its own: a worker waiting
//! on its own pool would pick up a whole queued analysis job and run it first.
//!
//! Where the client cannot be blocked — inside `MULTI`/`EXEC`, a Lua script or a module's
//! `RM_Call` — everything runs inline instead. Blocking such a client makes the server answer
//! with an error while the job still runs (and may still `STORE`), and some of those contexts
//! trip a server assert.
//!
//! Inline there means on the main thread, which nothing can cancel and no `TIMEOUT` reaches, so
//! the size of the work is capped instead: past [`WorkLimits::unblockable_max`] the command is
//! refused with a pointer to running it outside the transaction. The cap is far above the
//! threshold for going to the lane (that threshold is about latency; this one is about not
//! freezing the server), and is set per command from how long its work takes.

use crate::commands::CommandArgIterator;
use crate::common::context::is_blocking_denied;
use crate::common::replies::{
    BlockedClient, ReplyContext, ThreadSafeReplyContext, block_client, block_client_with_timeout,
};
use crate::common::threads::{BoundedExecutor, Capacity, lane_workers};
use crate::error_consts;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyValue};

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

/// Analysis jobs queued or running on the lane. A job abandoned by a client timeout keeps
/// running, so this is how tests (via `TS._DEBUG ANALYSIS_JOBS`) wait for the lane to drain
/// before checking that such a job left no trace.
pub(crate) fn analysis_jobs_in_flight() -> usize {
    let stats = ANALYSIS_EXECUTOR.stats();
    stats.queued + stats.running
}

/// Reserves a place on the analysis lane, blocks the client with `block`, and queues `job`,
/// which answers it. The place is reserved first, so a busy lane is an ordinary error reply
/// (`TSDB: <what>: too many queued jobs (limit N)`) and no client is blocked for a job that
/// will not run. A job that panics, or ends without replying, is answered with an error when
/// its reply context drops; the cause goes to the server log.
fn spawn_on_lane<J>(
    ctx: &Context,
    what: &str,
    block: impl FnOnce(&Context) -> BlockedClient,
    job: J,
) -> ValkeyResult<()>
where
    J: FnOnce(&ThreadSafeReplyContext) + Send + 'static,
{
    let slot = ANALYSIS_EXECUTOR
        .try_reserve()
        .map_err(|rejected| ValkeyError::String(format!("TSDB: {what}: {rejected}")))?;
    let blocked_client = block(ctx);
    slot.spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        if PANIC_NEXT_ANALYSIS_JOB.swap(false, Ordering::Relaxed) {
            panic!("TS._DEBUG PANIC_NEXT_ANALYSIS_JOB");
        }
        job(&thread_ctx);
    });
    Ok(())
}

/// Blocks the client of `ctx` and runs `job` on the analysis lane, which answers it. For a
/// command with its own inline/background split and no deadline (`TS.OUTLIERS`); analysis
/// commands with a `TIMEOUT` go through [`run_analysis`].
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
    spawn_on_lane(ctx, what, block_client, job)?;

    // Answered later, from the analysis lane.
    Ok(ValkeyValue::NoReply)
}

/// Deadline for an analysis command: the `TIMEOUT` argument if given, else
/// `ts-analysis-timeout`. Zero means no deadline.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct AnalysisTimeout(Option<u64>);

impl AnalysisTimeout {
    pub fn set(&mut self, ms: u64) {
        self.0 = Some(ms);
    }

    pub fn resolve(self) -> u64 {
        self.0.unwrap_or_else(crate::config::analysis_timeout_ms)
    }
}

/// Parse the value after a `TIMEOUT` keyword: a non-negative millisecond count.
pub(super) fn parse_timeout(args: &mut CommandArgIterator) -> ValkeyResult<u64> {
    let value = args
        .next_i64()
        .map_err(|_| ValkeyError::Str("TSDB: missing value for TIMEOUT"))?;
    if value < 0 {
        return Err(ValkeyError::Str("TSDB: TIMEOUT must be zero or positive"));
    }
    Ok(value as u64)
}

pub(super) const ANALYSIS_TIMEOUT_ERROR: &str =
    "TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)";

/// How much work a command may be given, in the command's own measure (`unit`): usually the
/// sample count, or samples × lags / models / folds where the cost grows with both.
#[derive(Clone, Copy, Debug)]
pub(super) struct WorkLimits {
    /// Up to this much work the command runs on the main thread by choice, since it is cheaper
    /// than handing it to the lane. Zero for a command that always uses the lane.
    pub inline_max: usize,
    /// Where the client cannot be blocked the command can only run on the main thread; above
    /// this much work it is refused instead of stalling the server.
    pub unblockable_max: usize,
    /// What `work` counts, for the error message.
    pub unit: &'static str,
}

impl WorkLimits {
    /// Limits for a command that always runs on the lane where it can.
    pub const fn background_only(unblockable_max: usize, unit: &'static str) -> Self {
        Self {
            inline_max: 0,
            unblockable_max,
            unit,
        }
    }

    /// Refuses `work` that is too large to run where the client cannot be blocked.
    pub fn check_unblockable(&self, work: usize) -> ValkeyResult<()> {
        if work <= self.unblockable_max {
            return Ok(());
        }
        Err(ValkeyError::String(format!(
            "TSDB: range too large to run inside MULTI, a script or a module call: \
             {work} {} exceeds the limit of {}; run the command outside of it",
            self.unit, self.unblockable_max
        )))
    }
}

/// Block the client and run `job` on the analysis lane.
///
/// The deadline is server-enforced and starts when the client is blocked, so time spent
/// queued counts against it: on expiry the client gets [`ANALYSIS_TIMEOUT_ERROR`] and the
/// job's own reply is discarded. Jobs check [`ThreadSafeReplyContext::is_timed_out`] before
/// side effects such as `STORE`.
///
/// A panicking job is answered with an internal error. The jobs run third-party model code on
/// user data, so input validation cannot be relied on to rule every panic out.
fn run_analysis_job<F>(ctx: &Context, timeout: AnalysisTimeout, job: F) -> ValkeyResult<()>
where
    F: FnOnce(&ThreadSafeReplyContext) + Send + 'static,
{
    spawn_on_lane(
        ctx,
        "analysis",
        |ctx| block_client_with_timeout(ctx, timeout.resolve(), ANALYSIS_TIMEOUT_ERROR),
        move |thread_ctx| {
            if catch_unwind(AssertUnwindSafe(|| job(thread_ctx))).is_err() {
                thread_ctx.log_warning("TSDB: analysis job panicked; replying with an error");
                thread_ctx.reply(Err(ValkeyError::Str(error_consts::INTERNAL_ERROR)));
            }
        },
    )
}

/// Runs `f` on the main thread, turning a panic into an error reply: a panic must not unwind
/// out of a command handler into the server.
fn catch_inline_panic(ctx: &Context, f: impl FnOnce() -> ValkeyResult) -> ValkeyResult {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|_| {
        ctx.log_warning("TSDB: analysis command panicked; replying with an error");
        Err(ValkeyError::Str(error_consts::INTERNAL_ERROR))
    })
}

/// Where a command's reply step is running. Reply helpers take a [`ReplyContext`],
/// which both variants provide; anything that needs the GIL (key writes for `STORE`)
/// goes through [`AnalysisCtx::with_locked_context`], which is a no-op wrapper inline
/// and takes the thread-safe lock in the background.
pub(super) enum AnalysisCtx<'a> {
    Inline(&'a Context),
    Background(&'a ThreadSafeReplyContext),
}

impl AnalysisCtx<'_> {
    pub fn reply_ctx(&self) -> ReplyContext {
        match self {
            Self::Inline(ctx) => ReplyContext::new(ctx.ctx),
            Self::Background(ctx) => ctx.get_reply_context(),
        }
    }

    pub fn with_locked_context<R>(&self, f: impl FnOnce(&Context) -> R) -> R {
        match self {
            Self::Inline(ctx) => f(ctx),
            Self::Background(ctx) => {
                let guard = ctx.lock();
                f(&guard)
            }
        }
    }

    /// True once the server has answered the client with a timeout error; a reply
    /// step should then skip side effects. Never true inline.
    pub fn is_timed_out(&self) -> bool {
        match self {
            Self::Inline(_) => false,
            Self::Background(ctx) => ctx.is_timed_out(),
        }
    }

    /// Deliver a value or error to the client. Inline, the command handler's own
    /// return value does this, so it is only meaningful in the background.
    fn send(&self, result: ValkeyResult) {
        if let Self::Background(ctx) = self {
            ctx.reply(result);
        }
    }
}

/// Run `compute` then `reply`, inline when `work <= limits.inline_max` (or when the client
/// cannot be blocked) and on the analysis lane, with the client blocked under `timeout`,
/// otherwise. `work` is the command's own cost measure (see [`WorkLimits`]).
///
/// Where the client cannot be blocked the command can only run inline, so `work` above
/// `limits.unblockable_max` fails with an error instead of running.
///
/// `reply` returns the same `ValkeyResult` a command handler would: `NoReply` after
/// writing raw replies, a value to be sent, or an error. In the background the value or
/// error is forwarded to the blocked client. Validate before writing raw replies — an
/// error after a partial reply corrupts the stream on either path.
pub(super) fn run_analysis<T, C, R>(
    ctx: &Context,
    work: usize,
    limits: WorkLimits,
    timeout: AnalysisTimeout,
    compute: C,
    reply: R,
) -> ValkeyResult
where
    T: Send + 'static,
    C: FnOnce() -> ValkeyResult<T> + Send + 'static,
    R: FnOnce(&AnalysisCtx<'_>, T) -> ValkeyResult + Send + 'static,
{
    let blocking_denied = is_blocking_denied(ctx);
    if blocking_denied {
        limits.check_unblockable(work)?;
    }
    if work <= limits.inline_max || blocking_denied {
        return catch_inline_panic(ctx, || {
            let output = compute()?;
            reply(&AnalysisCtx::Inline(ctx), output)
        });
    }

    run_analysis_job(ctx, timeout, move |thread_ctx| {
        let actx = AnalysisCtx::Background(thread_ctx);
        match compute().and_then(|output| reply(&actx, output)) {
            Ok(ValkeyValue::NoReply) => {}
            result => actx.send(result),
        }
    })?;

    // Reply will be sent from the analysis lane
    Ok(ValkeyValue::NoReply)
}

/// [`run_analysis`] for work that is never cheap enough to run inline by choice, such as model
/// fitting: always on the analysis lane, except where the client cannot be blocked, where
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
    // `max(1)`: an empty measure must still take the lane path, not the inline one.
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
