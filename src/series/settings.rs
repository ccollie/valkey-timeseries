use crate::common::rounding::RoundingStrategy;
use crate::config::{
    get_series_config_settings, DEFAULT_CHUNK_SIZE_BYTES, DEFAULT_SERIES_WORKER_INTERVAL,
};
use crate::series::chunks::ChunkEncoding;
use crate::series::index::optimize_all_timeseries_indexes;
use crate::series::SampleDuplicatePolicy;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{Context, RedisModuleTimerID};

#[derive(Clone, Copy)]
pub struct ConfigSettings {
    pub retention_period: Option<Duration>,
    pub chunk_encoding: Option<ChunkEncoding>,
    pub chunk_size_bytes: usize,
    pub rounding: Option<RoundingStrategy>,
    pub worker_interval: Duration,
    pub duplicate_policy: SampleDuplicatePolicy,
}

impl Default for ConfigSettings {
    fn default() -> Self {
        Self {
            retention_period: None,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            chunk_encoding: Some(ChunkEncoding::Gorilla),
            worker_interval: DEFAULT_SERIES_WORKER_INTERVAL,
            rounding: None,
            duplicate_policy: SampleDuplicatePolicy::default(),
        }
    }
}

pub static SERIES_SETTINGS: LazyLock<ConfigSettings> = LazyLock::new(get_series_config_settings);
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
