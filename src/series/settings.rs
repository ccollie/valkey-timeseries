use crate::series::index::optimize_all_timeseries_indexes;
use crate::series::tasks::{process_expires_task, process_remove_stale_series};
use lazy_static::lazy_static;
use std::time::Duration;
use valkey_module::{Context, RedisModuleTimerID, ValkeyGILGuard};

struct StaticData {
    timer_id: RedisModuleTimerID,
    interval: Duration,
}

impl Default for StaticData {
    fn default() -> Self {
        Self {
            timer_id: 0,
            interval: Duration::from_millis(5000),
        }
    }
}

lazy_static! {
    static ref STATIC_DATA: ValkeyGILGuard<StaticData> = ValkeyGILGuard::new(StaticData::default());
}

pub(crate) fn start_series_background_worker(ctx: &Context) {
    let mut static_data = STATIC_DATA.lock(ctx);
    let timer_id = static_data.timer_id;
    if timer_id == 0 {
        static_data.timer_id =
            ctx.create_timer(static_data.interval, series_worker_callback, 0usize);
    }
}

pub(crate) fn stop_series_background_worker(ctx: &Context) {
    let mut static_data = STATIC_DATA.lock(ctx);
    let timer_id = static_data.timer_id;
    if timer_id != 0 {
        if ctx.stop_timer::<usize>(timer_id).is_err() {
            let msg = format!("Failed to stop series timer {timer_id}. Timer may not exist",);
            ctx.log_debug(&msg);
        }
        static_data.timer_id = 0;
    }
}

fn series_worker_callback(ctx: &Context, _ignore: usize) {
    ctx.log_debug("[series worker callback]: optimizing series indexes");
    // use rayon threadpool to run off the main thread
    // todo: run these on a dedicated schedule
    rayon::spawn(optimize_all_timeseries_indexes);
    rayon::spawn(process_expires_task);
    rayon::spawn(process_remove_stale_series);
}
