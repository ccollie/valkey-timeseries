use rayon_core::{Scope, ThreadPoolBuilder};
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;
pub const DEFAULT_NUM_CPUS: usize = 4;

pub static NUM_CPUS: LazyLock<usize> =
    LazyLock::new(|| match std::thread::available_parallelism() {
        Err(e) => {
            log::warn!("Failed to get available parallelism: {e:?}");
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

pub fn spawn<F: FnOnce() + Send + 'static>(job: F) {
    rayon_core::spawn(job)
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
