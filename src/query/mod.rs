use metricsql_runtime::prelude::Context as QueryContext;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

pub mod types;
mod handlers;
mod tracing;
mod ts_metric_storage;
pub(crate) mod config;
mod matchers;
mod cluster;
mod query_utils;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod bench_test;
        mod test_metric_storage;
        mod test_utils;
        mod query_tests;

        pub(super) use test_metric_storage::*;
    } else {
        pub use ts_metric_storage::VMMetricStorage;
    }
}

pub(crate) static QUERY_CONTEXT: LazyLock<QueryContext> = LazyLock::new(create_query_context);

pub fn get_query_context() -> &'static QueryContext {
    &QUERY_CONTEXT
}

pub(super) fn create_query_context() -> QueryContext {
    #[cfg(test)]
    let provider = Arc::new(TestMetricStorage::new());
    #[cfg(not(test))]
    let provider = Arc::new(VMMetricStorage { db: 0 });
    let default_config = 
        QUERY_CONTEXT_CONFIG.lock().expect("Default Config mutex poisoned");

    let mut context = QueryContext::new()
        .with_config(default_config.clone())
        .with_metric_storage(provider);
    
    context.config.latency_offset = Duration::ZERO;
    context
}

pub(crate) use crate::query::cluster::register_cluster_message_handlers;
use crate::query::config::QUERY_CONTEXT_CONFIG;
pub(crate) use crate::query::query_utils::*;
pub use types::*;
pub(crate) use handlers::*;
