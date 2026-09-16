mod batch_worker;

use crate::common::context::{get_current_db, set_current_db};
use crate::is_main_thread;
#[allow(unused_imports)]
pub(crate) use batch_worker::{
    BatchRequest, BatchWorker, global_valkey_task_worker, submit_task_no_wait,
    submit_task_with_payload,
};
use orx_parallel::{Par, Runner};
use rayon_core::{Scope, ThreadPool, ThreadPoolBuilder};
use std::os::raw::c_void;
use std::sync::LazyLock;
use valkey_module::logging::log_notice;
use valkey_module::{Context, MODULE_CONTEXT, raw};

/// Builds the module's global rayon thread pool, sized from `config::NUM_THREADS`
/// (`ts-num-threads`). Config registration runs before this in `initialize()`, so the config
/// value is already resolved (from `valkey.conf`/`MODULE LOAD` args, or its default) by the
/// time this reads it. `ts-num-threads` is registered `IMMUTABLE` because rayon's global pool
/// cannot be resized once built — there is no later point at which this needs to re-run.
pub fn init_thread_pool() {
    let threads = crate::config::num_threads();
    log_notice(format!("Setting number of threads to {threads}"));
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("valkey-timeseries-{index}"))
        .build_global()
        .unwrap();
    // Build the request pool now rather than on the first command, so its threads
    // exist before the server starts serving.
    LazyLock::force(&REQUEST_POOL);
}

/// Pool for the parallel sections of command handlers (`into_par` over matched
/// series, shard responses, MADD groups).
///
/// It is deliberately *not* the global rayon pool. Jobs on the global pool take the
/// module GIL (`MODULE_CONTEXT.lock()` in cluster RPC, index persistence and server
/// events), and a command handler already holds the GIL while it waits in a `scope`;
/// if every global worker were parked on that lock the wait could never end. Nothing
/// here may touch a `Context`: the sites (`.on_request_pool()`) hand their closures plain `&TimeSeries`
/// references or decoded shard data, and anything that needs the context (`LATEST`
/// lookups, compaction propagation) is done before or after the parallel section on
/// the calling thread.
///
/// Before this pool existed these sites used orx-parallel's default runner, which
/// spawns fresh OS threads on every call (`std::thread::scope`): about 75 µs on macOS
/// and 200 µs inside a Linux VM even for a one-element input, which was most of
/// TS.MRANGE's overhead over TS.RANGE for a single matched series.
static REQUEST_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    ThreadPoolBuilder::new()
        .num_threads(crate::config::num_threads())
        .thread_name(|index| format!("valkey-timeseries-req-{index}"))
        .build()
        .expect("request thread pool")
});

/// The pool per-request parallel sections run on. See [`REQUEST_POOL`].
pub fn request_pool() -> &'static ThreadPool {
    &REQUEST_POOL
}

/// `.on_request_pool()`: run a parallel pipeline on [`REQUEST_POOL`].
///
/// This is the one place that names the runner. It is the fixed-chunk runner —
/// what orx-parallel 3.4's `with_pool` used — because the adaptive one measured
/// no different on the request-pool shapes (interleaved A/B, MRANGE/GROUPBY over
/// a uniform 1000×1000 fixture, 2026-09-16). Sites pair it with
/// `.num_threads(request_par_threads(items, work))`.
///
/// An extension trait rather than a `request_runner()` function because
/// orx-parallel does not export `ParRunner`, so the runner's type cannot be
/// written down outside the crate; the return type of [`Par::runner`] can.
pub trait RequestPoolPar: Par {
    fn on_request_pool(self) -> impl Par<Item = Self::Item, Xap = Self::Xap, Input = Self::Input> {
        self.runner(Runner::fixed_with_pool(request_pool()))
    }
}

impl<P: Par> RequestPoolPar for P {}

/// Fewer items than this, or less work than [`PARALLEL_MIN_WORK`], and a request
/// runs its parallel section sequentially on the calling thread.
pub const PARALLEL_MIN_ITEMS: usize = 2;

/// Minimum amount of work — in samples, or sample-sized units of shard payload —
/// before dispatching to the pool pays for itself. Dispatch plus join costs on the
/// order of 10–20 µs; decoding a sample costs well under 0.1 µs, so below a few
/// thousand samples the sequential path wins. Measured, not derived: a one-series
/// TS.MRANGE over 100 samples went from 342 µs to 269 µs when it stopped
/// dispatching at all (paired A/B, three trials).
pub const PARALLEL_MIN_WORK: usize = 4096;

/// The `num_threads` argument for a per-request `into_par()`: `1` (orx-parallel's
/// sequential fast path, no dispatch) unless there are at least
/// [`PARALLEL_MIN_ITEMS`] items and [`PARALLEL_MIN_WORK`] units of work, else `0`
/// (auto, bounded by the pool size).
pub fn request_par_threads(items: usize, work: usize) -> usize {
    if items < PARALLEL_MIN_ITEMS || work < PARALLEL_MIN_WORK {
        1
    } else {
        0
    }
}

/// Spawn a job which runs asynchronously.
/// The job must be `'static` and thus cannot borrow local variables.
pub fn spawn<F: FnOnce() + Send + 'static>(job: F) {
    rayon_core::spawn(job)
}

/// Spawn a job in the context of a valkey GIL (Global Interpreter Lock).
pub fn spawn_with_context<F: FnOnce(&Context) + Send + 'static>(job: F) {
    spawn(move || {
        let ctx = MODULE_CONTEXT.lock();
        job(&ctx);
    });
}

/// Spawn scoped jobs which guarantee to be finished before this method returns and thus allows
/// borrowing local variables.
pub fn spawn_scoped<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&Scope<'scope>) -> R + Send,
    R: Send,
{
    rayon_core::scope(op)
}

pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce() -> RA,
    B: Send + FnOnce() -> RB,
    RA: Send,
    RB: Send,
{
    rayon_core::join(oper_a, oper_b)
}

pub fn join_scoped<'scope, A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: Send + FnOnce(&Scope<'scope>) -> RA + 'scope,
    B: Send + FnOnce(&Scope<'scope>) -> RB + 'scope,
    RA: Send + 'scope,
    RB: Send + 'scope,
{
    // does this make sense?
    spawn_scoped(|s| rayon_core::join(|| oper_a(s), || oper_b(s)))
}

extern "C" fn event_loop_callback_wrapper<F>(data: *mut c_void)
where
    F: FnOnce() + 'static,
{
    let callback: Box<F> = unsafe { Box::from_raw(data as *mut F) };
    callback();
}

/// Runs `callback` with a module [`Context`] while already on the main thread.
///
/// The caller must be on the main thread with the module GIL held (true for command handlers,
/// server-event callbacks, and event-loop one-shot callbacks). We therefore must NOT lock a
/// thread-safe/detached context — that re-acquires the GIL and self-deadlocks. Instead we obtain a
/// throwaway thread-safe context (which does no locking) and use it directly.
fn with_main_thread_context<F>(callback: F)
where
    F: FnOnce(&Context),
{
    let raw_ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(std::ptr::null_mut()) };
    let ctx = Context::new(raw_ctx);
    let saved_db = get_current_db(&ctx);
    callback(&ctx);
    set_current_db(&ctx, saved_db);
    unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(raw_ctx) };
}

extern "C" fn event_loop_callback_wrapper_with_context<F>(data: *mut c_void)
where
    F: FnOnce(&Context) + 'static,
{
    let callback: Box<F> = unsafe { Box::from_raw(data as *mut F) };
    with_main_thread_context(|ctx| callback(ctx));
}

/// Executes a given closure on the Valkey main thread. The provided closure will be executed as a one-shot operation.
///
/// # Parameters
/// - `force_async`: If true, the closure will be executed asynchronously even if it's already on the main thread.
/// - `callback`: The closure to be executed on the main thread.
///
/// # Example
/// ```rust,no_run
/// use valkey_timeseries::common::threads::run_on_main_thread;
///
/// // A simple closure to be executed on the main thread
/// run_on_main_thread(false, || {
///     println!("This is running on the main thread!");
/// });
/// ```
pub fn run_on_main_thread<F>(force_async: bool, callback: F)
where
    F: FnOnce() + Send + 'static,
{
    if is_main_thread() && !force_async {
        callback();
        return;
    }

    // Move the closure to the heap so it has a stable memory address
    let boxed_callback = Box::new(callback);
    let raw_data = Box::into_raw(boxed_callback) as *mut c_void;

    let event_loop_callback = event_loop_callback_wrapper::<F>;

    unsafe {
        raw::ValkeyModule_EventLoopAddOneShot.unwrap()(Some(event_loop_callback), raw_data);
    }
}

/// Executes a given closure on the Valkey main thread, providing a reference to the module [`Context`].
/// The provided closure will be executed as a one-shot operation.
///
/// # Parameters
/// - `force_async`: If true, the closure will be executed asynchronously even if it's already on the main thread.
/// - `callback`: The closure to be executed on the main thread. It receives a reference to the module [`Context`].
///
/// # Example
/// ```rust,no_run
/// use valkey_module::Context;
/// use valkey_timeseries::common::threads::run_on_main_thread_with_context;
///
/// // A simple closure to be executed on the main thread with access to Context
/// run_on_main_thread_with_context(false, |ctx: &Context| {
///     ctx.log_notice("This is running on the main thread with context!");
/// });
/// ```
pub fn run_on_main_thread_with_context<F>(force_async: bool, callback: F)
where
    F: FnOnce(&Context) + Send + 'static,
{
    if is_main_thread() && !force_async {
        with_main_thread_context(callback);
        return;
    }

    // Move the closure to the heap so it has a stable memory address
    let boxed_callback = Box::new(callback);
    let raw_data = Box::into_raw(boxed_callback) as *mut c_void;

    let event_loop_callback = event_loop_callback_wrapper_with_context::<F>;

    unsafe {
        raw::ValkeyModule_EventLoopAddOneShot.unwrap()(Some(event_loop_callback), raw_data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_request_pool_dispatches() {
        use orx_parallel::IntoParIter;
        let sum: usize = (0..1000usize)
            .into_par()
            .on_request_pool()
            .num_threads(0)
            .sum();
        assert_eq!(sum, 499_500);
    }

    #[test]
    fn small_requests_stay_sequential() {
        assert_eq!(request_par_threads(1, usize::MAX), 1);
        assert_eq!(request_par_threads(1000, PARALLEL_MIN_WORK - 1), 1);
        assert_eq!(
            request_par_threads(PARALLEL_MIN_ITEMS, PARALLEL_MIN_WORK),
            0
        );
        assert_eq!(request_par_threads(1000, 1_000_000), 0);
    }
}
