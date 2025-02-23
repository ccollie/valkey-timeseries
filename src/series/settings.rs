use crate::common::rounding::RoundingStrategy;
use crate::config::{
    get_series_settings, DEFAULT_CHUNK_SIZE_BYTES, DEFAULT_SERIES_WORKER_INTERVAL,
};
use crate::series::chunks::ChunkCompression;
use crate::series::index::optimize_all_timeseries_indexes;
use crate::series::DuplicatePolicy;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{Context, RedisModuleTimerID};

#[derive(Clone, Copy)]
pub struct SeriesSettings {
    pub retention_period: Option<Duration>,
    pub chunk_compression: Option<ChunkCompression>,
    pub chunk_size_bytes: usize,
    pub duplicate_policy: DuplicatePolicy,
    pub dedupe_interval: Option<Duration>,
    pub rounding: Option<RoundingStrategy>,
    pub worker_interval: Duration,
}

impl Default for SeriesSettings {
    fn default() -> Self {
        Self {
            retention_period: None,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunk_compression: Some(ChunkCompression::Gorilla),
            duplicate_policy: DuplicatePolicy::KeepLast,
            worker_interval: DEFAULT_SERIES_WORKER_INTERVAL,
            dedupe_interval: None,
            rounding: None,
        }
    }
}

pub static SERIES_SETTINGS: LazyLock<SeriesSettings> = LazyLock::new(get_series_settings);
static SERIES_WORKER_TIMER_ID: LazyLock<Mutex<RedisModuleTimerID>> =
    LazyLock::new(|| Mutex::new(0));

pub(crate) fn start_series_background_worker() {
    let mut timer_id = SERIES_WORKER_TIMER_ID.lock().unwrap();
    if *timer_id == 0 {
        let ctx = valkey_module::MODULE_CONTEXT.lock();
        let interval = SERIES_SETTINGS.worker_interval;
        *timer_id = ctx.create_timer(interval, series_worker_callback, 0usize);
    }
}

pub(crate) fn stop_series_background_worker() {
    let mut timer_id = SERIES_WORKER_TIMER_ID.lock().unwrap();
    if *timer_id != 0 {
        let ctx = valkey_module::MODULE_CONTEXT.lock();
        if ctx.stop_timer::<usize>(*timer_id).is_err() {
            let msg = format!(
                "Failed to stop series timer {}. Timer may not exist",
                *timer_id
            );
            ctx.log_debug(&msg);
        }
        *timer_id = 0;
    }
}

fn series_worker_callback(ctx: &Context, _ignore: usize) {
    ctx.log_debug("[series worker callback]: optimizing series indexes");
    // use rayon threadpool to run off the main thread
    rayon::spawn(optimize_all_timeseries_indexes);
}
