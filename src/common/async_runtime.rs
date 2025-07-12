use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(feature = "tokio")] {
        use tokio::runtime::Runtime;
        use std::sync::LazyLock;
        use std::future::Future;

        static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .thread_name("valkey-timeseries")
                .build()
                .expect("Failed to create Tokio runtime")
        });

        pub fn with_runtime_context<F, R>(f: F) -> R
        where
            F: FnOnce() -> R,
        {
            let _guard = TOKIO_RUNTIME.enter();

            // Within this scope, Handle::current() will return our runtime's handle
            f()
        }

        pub fn block_on<F: Future>(future: F) -> F::Output {
            let _guard = TOKIO_RUNTIME.enter();
            TOKIO_RUNTIME.block_on(future)
        }
    }
    else if #[cfg(feature = "smol")] {
        pub use metricsql_common::prelude::block_on;
    } else {
        unimplemented!("No async runtime feature enabled");
    }
}
