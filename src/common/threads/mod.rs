use crate::is_main_thread;
use rayon_core::{Scope, ThreadPoolBuilder};
use std::os::raw::c_void;
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;
use valkey_module::{Context, MODULE_CONTEXT, raw};

pub const DEFAULT_NUM_CPUS: usize = 4;

pub static NUM_CPUS: LazyLock<usize> =
    LazyLock::new(|| match std::thread::available_parallelism() {
        Err(e) => {
            let msg = format!("Failed to get available parallelism {e:?}");
            crate::common::logging::log_warning(msg);
            DEFAULT_NUM_CPUS
        }
        Ok(v) => v.get(),
    });

pub static NUM_THREADS: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(*NUM_CPUS));

pub fn init_thread_pool() {
    let threads = NUM_THREADS.load(std::sync::atomic::Ordering::Relaxed);
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("valkey-timeseries-{index}"))
        .build_global()
        .unwrap();
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

/// Executes a given closure on the Valkey main thread. The provided closure will be executed as a one-shot operation.
///
/// # Parameters
/// - `force_async`: If true, the closure will be executed asynchronously even if it's already on the main thread.
/// - `callback`: The closure to be executed on the main thread.
///
/// # Example
/// ```rust
/// use your_crate::run_on_main_thread;
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
