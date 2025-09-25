use super::NUM_THREADS;
use std::sync::LazyLock;
use yastl::{Pool, Scope, ThreadConfig};

pub static THREAD_POOL: LazyLock<Pool> = LazyLock::new(|| construct_pool(None));
fn construct_pool(num_threads: Option<usize>) -> Pool {
    let threads =
        num_threads.unwrap_or_else(|| NUM_THREADS.load(std::sync::atomic::Ordering::Relaxed));
    let config = ThreadConfig::new().prefix("valkey-timeseries");
    Pool::with_config(threads, config)
}

pub fn spawn<F: FnOnce() + Send + 'static>(job: F) {
    THREAD_POOL.spawn(job)
}

/// Spawn scoped jobs which guarantee to be finished before this method returns and thus allows
/// borrowing local variables.
pub fn spawn_scoped<'scope, F, R>(job: F) -> R
where
    F: FnOnce(&Scope<'scope>) -> R,
{
    THREAD_POOL.scoped(job)
}
