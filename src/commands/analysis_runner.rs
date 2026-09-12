//! Inline-or-background execution for CPU-bound analysis commands.
//!
//! Analysis commands (`TS.TREND`, `TS.DECOMPOSE`, `TS.PERIODS`, `TS.STATIONARITY`,
//! `TS.AUTOCORRELATION`, and the forecasting family) split their work into a pure
//! `compute` step and a `reply` step. Small inputs run both inline on the main thread,
//! which keeps the common case cheap; above a per-command sample threshold the client
//! is blocked and both steps run on the analysis pool, so a large input can never stall
//! the server. Either way the reply code is written once against [`AnalysisCtx`].

use crate::commands::CommandArgIterator;
use crate::common::replies::{ReplyContext, ThreadSafeReplyContext, block_client_with_timeout};
use crate::common::threads::spawn_analysis;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyValue};

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

/// Block the client and run `job` on the analysis pool.
///
/// The deadline is server-enforced: on expiry the client gets
/// [`ANALYSIS_TIMEOUT_ERROR`] and the job's own reply is discarded. Jobs check
/// [`ThreadSafeReplyContext::is_timed_out`] before side effects such as `STORE`.
pub(super) fn run_analysis_job<F>(ctx: &Context, timeout: AnalysisTimeout, job: F)
where
    F: FnOnce(ThreadSafeReplyContext) + Send + 'static,
{
    let blocked_client = block_client_with_timeout(ctx, timeout.resolve(), ANALYSIS_TIMEOUT_ERROR);
    spawn_analysis(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        job(thread_ctx);
    });
}

/// Where a command's reply step is running. Reply helpers take a [`ReplyContext`],
/// which both variants provide; anything that needs the GIL (key writes for `STORE`)
/// goes through [`AnalysisCtx::with_locked_context`], which is a no-op wrapper inline
/// and takes the thread-safe lock in the background.
pub(super) enum AnalysisCtx<'a> {
    Inline(&'a Context),
    Background(ThreadSafeReplyContext),
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

    pub fn log_warning(&self, message: &str) {
        match self {
            Self::Inline(ctx) => ctx.log_warning(message),
            Self::Background(ctx) => ctx.log_warning(message),
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

/// Run `compute` then `reply`, inline when `samples <= inline_max` and on the analysis
/// pool (with the client blocked under `timeout`) otherwise.
///
/// `reply` returns the same `ValkeyResult` a command handler would: `NoReply` after
/// writing raw replies, a value to be sent, or an error. In the background the value or
/// error is forwarded to the blocked client. Validate before writing raw replies — an
/// error after a partial reply corrupts the stream on either path.
pub(super) fn run_analysis<T, C, R>(
    ctx: &Context,
    samples: usize,
    inline_max: usize,
    timeout: AnalysisTimeout,
    compute: C,
    reply: R,
) -> ValkeyResult
where
    T: Send + 'static,
    C: FnOnce() -> ValkeyResult<T> + Send + 'static,
    R: FnOnce(&AnalysisCtx<'_>, T) -> ValkeyResult + Send + 'static,
{
    if samples <= inline_max {
        let output = compute()?;
        return reply(&AnalysisCtx::Inline(ctx), output);
    }

    run_analysis_job(ctx, timeout, move |thread_ctx| {
        let actx = AnalysisCtx::Background(thread_ctx);
        match compute().and_then(|output| reply(&actx, output)) {
            Ok(ValkeyValue::NoReply) => {}
            result => actx.send(result),
        }
    });

    // Reply will be sent from the analysis pool
    Ok(ValkeyValue::NoReply)
}
