mod join;
pub mod thread_pool;

pub use join::*;
pub use thread_pool::*;
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
