//! The pinned pools PromQL runs on. Each is built on first use, so a node that never runs
//! PromQL never starts them. They take `ts-num-threads` as given, where the shared pool caps it
//! at the core count: an evaluation worker can park waiting on the selector executor, and
//! threads beyond the cores let such waits overlap.

use super::{ThreadRole, set_thread_role};
use crate::config::num_threads;
use rayon_core::{ThreadPool, ThreadPoolBuilder};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    /// The pool the evaluations this thread runs fan out on: one per PromQL query worker
    /// (`run_evaluation` in the query workers), built on its first evaluation.
    ///
    /// An evaluation's parallel jobs block: they ask the selector executor for data and wait,
    /// and the executor needs the GIL to answer. On the shared pool those parked jobs could
    /// occupy every worker while a GIL holder — `TS.RANGE` over enough chunks, `TS.MRANGE`, the
    /// trim cron, a shard's local fan-out handler — waits on that same pool for its own parallel
    /// decode: the holder waits on the pool, the pool waits on the executor, the executor waits
    /// on the holder, and the server freezes. Here the parked jobs can only exhaust this pool,
    /// which no GIL holder ever waits on (R2), so the shared pool always drains. Its workers are
    /// pinned, so the parallel work they start stays here (R4).
    ///
    /// One per query worker, not one for every query: an evaluation enters its pool as an
    /// injected job, and a rayon worker waiting in a `join` takes injected jobs too. With one
    /// pool, a worker waiting inside query A took query B's whole evaluation and ran it on its
    /// own stack, so A could not answer before B did — B perhaps waiting out its deadline on a
    /// fan-out — and evaluations stacked that way could overflow the worker's stack. A query
    /// worker runs one evaluation at a time, so its pool only ever runs that one's jobs.
    static EVAL_POOL: ThreadPool = build_pinned_pool(
        &format!("ts-promql-eval-{}", NEXT_EVAL_POOL.fetch_add(1, Ordering::Relaxed)),
        ThreadRole::BlockingPool,
    );
}

/// Numbers the evaluation pools, for their threads' names.
static NEXT_EVAL_POOL: AtomicUsize = AtomicUsize::new(0);

/// Runs `f` with the calling thread's evaluation pool; see [`EVAL_POOL`].
pub(crate) fn with_eval_pool<R>(f: impl FnOnce(&ThreadPool) -> R) -> R {
    EVAL_POOL.with(f)
}

/// The pool the PromQL selector executor materializes on. Private to the executor so that its
/// work never depends on a pool whose workers may all be parked waiting for exactly this work.
/// Its jobs never block (R1 without exception), so the processor may wait on it while holding
/// the GIL (R2).
pub(crate) static MATERIALIZE_POOL: LazyLock<ThreadPool> =
    LazyLock::new(|| build_pinned_pool("ts-promql-io", ThreadRole::IsolatedPool));

fn build_pinned_pool(name: &str, role: ThreadRole) -> ThreadPool {
    let prefix = name.to_string();
    ThreadPoolBuilder::new()
        .num_threads(num_threads())
        .thread_name(move |index| format!("{prefix}-{index}"))
        .start_handler(move |_| set_thread_role(role))
        .build()
        .unwrap_or_else(|err| panic!("failed to build the {name} pool: {err}"))
}
