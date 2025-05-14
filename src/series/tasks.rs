use crate::common::db::{get_current_db, set_current_db};
use crate::common::hash::IntMap;
use crate::common::parallel::join;
use crate::series::index::{
    optimize_all_timeseries_indexes, with_timeseries_index, TIMESERIES_INDEX,
};
use crate::series::{get_timeseries_mut, SeriesRef};
use ahash::HashMapExt;
use blart::AsBytes;
use lazy_static::lazy_static;
use num_traits::Zero;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicI32;
use std::sync::{LazyLock, Mutex, RwLock};
use std::time::Duration;
use valkey_module::{
    Context, DetachedContext, RedisModuleTimerID, Status, ValkeyGILGuard, ValkeyString,
};

const BATCH_SIZE: usize = 500;

/// Per-db task metadata
type IdleSeriesMap = IntMap<i32, Vec<SeriesRef>>;
type SeriesCursorMap = IntMap<i32, SeriesRef>;
static SERIES_TRIM_CURSORS: LazyLock<RwLock<SeriesCursorMap>> =
    LazyLock::new(|| RwLock::new(SeriesCursorMap::new()));
static STALE_SERIES: LazyLock<Mutex<IdleSeriesMap>> =
    LazyLock::new(|| Mutex::new(IdleSeriesMap::new()));

// During maintenance tasks, we only process one db during a cycle. We use this atomic to keep track of the current db
static CURRENT_DB: AtomicI32 = AtomicI32::new(0);

fn get_used_dbs() -> Vec<i32> {
    let index = TIMESERIES_INDEX.pin();
    let mut keys: Vec<i32> = index.keys().copied().collect();
    keys.sort();
    keys
}

fn next_db() -> i32 {
    let used_dbs = get_used_dbs();
    let current = CURRENT_DB.load(std::sync::atomic::Ordering::Relaxed);
    let next = if let Some(dn) = used_dbs.iter().find(|&&d| d > current) {
        *dn
    } else {
        used_dbs.first().copied().unwrap_or(0)
    };
    CURRENT_DB.store(next, std::sync::atomic::Ordering::Relaxed);
    next
}

fn trim_series(ctx: &Context, db: i32) -> usize {
    if set_current_db(ctx, db) == Status::Err {
        ctx.log_warning(&format!("Failed to select db {}", db));
        return 0;
    }

    let cursor = {
        let mut map = SERIES_TRIM_CURSORS.write().unwrap();
        match map.entry(db) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let cursor = 0;
                entry.insert(cursor);
                cursor
            }
        }
    };

    let mut processed = 0;
    let mut last_processed = cursor;
    let mut keys = IntMap::with_capacity(BATCH_SIZE);
    let mut to_delete = Vec::with_capacity(BATCH_SIZE);
    let mut total_deletes: usize = 0;
    let mut trimmed_count: usize = 0;

    if fetch_keys_batch(ctx, last_processed + 1, BATCH_SIZE, &mut keys) {
        // todo: can we use rayon here ?
        for (id, key) in keys.iter() {
            if let Ok(Some(mut series)) = get_timeseries_mut(ctx, key, false, None) {
                match series.trim() {
                    Ok(deleted) => {
                        total_deletes += deleted;
                        if deleted > 0 {
                            trimmed_count += 1;
                        }
                    }
                    Err(e) => {
                        ctx.log_warning(&format!("Failed to trim series {key}: {:?}", e));
                    }
                }
                processed += 1;
            } else {
                to_delete.push(*id);
            }
            last_processed = last_processed.max(*id);
        }
    } else {
        ctx.log_debug("No more series to trim");
        last_processed = 0;
    }

    let mut map = SERIES_TRIM_CURSORS.write().unwrap();
    map.insert(db, last_processed);
    if !to_delete.is_empty() {
        remove_stale_series(ctx, &to_delete);
    }

    if processed.is_zero() {
        ctx.log_debug("No series to trim");
    } else {
        ctx.log_notice(&format!("Processed: {processed} Trimmed {trimmed_count}, Deleted Samples: {total_deletes} samples"));
    }

    processed
}

fn fetch_keys_batch(
    ctx: &Context,
    start_id: SeriesRef,
    batch_size: usize,
    ids: &mut IntMap<SeriesRef, ValkeyString>,
) -> bool {
    with_timeseries_index(ctx, |index| {
        let mut state = ();
        index.with_postings(&mut state, |postings, _| {
            let mut id = start_id;
            let max_id = postings.max_id();
            let mut added: usize = 0;
            for _ in 0..batch_size {
                if id > max_id {
                    return !added.is_zero();
                }
                if let Some(k) = postings.get_key_by_id(id) {
                    added += 1;
                    let key = ctx.create_string(k.as_bytes());
                    ids.insert(id, key);
                }
                id += 1;
            }
            true
        })
    })
}

fn remove_stale_series_internal(db: i32) {
    let series = {
        let mut map = STALE_SERIES.lock().unwrap();
        map.remove(&db).unwrap_or_default()
    };

    if series.is_empty() {
        return;
    }

    if let Some(index) = TIMESERIES_INDEX.pin().get(&db) {
        index.remove_series_by_ids(&series);
    }
}

pub(crate) fn remove_stale_series(ctx: &Context, ids: &[SeriesRef]) {
    if !ids.is_empty() {
        let id = get_current_db(ctx);
        let mut map = STALE_SERIES.lock().unwrap();
        match map.entry(id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().extend(ids.iter().copied());
            }
            Entry::Vacant(entry) => {
                entry.insert(ids.to_vec());
            }
        }
    }
}

pub fn process_expires_task() {
    let detached_ctx = DetachedContext::new();
    let mut db = next_db();
    let first_db = db;
    let mut iterations = 0;
    loop {
        let guard = detached_ctx.lock();
        let processed = trim_series(&guard, db);
        if processed > 10 {
            // todo: make it a configurable constant
            break;
        }
        db = next_db();
        if db == first_db {
            break;
        }
        iterations += 1;
        if iterations > 10 {
            break;
        }
    }
}

pub fn process_remove_stale_series() {
    let db_ids = {
        let map = STALE_SERIES.lock().unwrap();
        map.iter()
            .filter_map(|(db, series)| if !series.is_empty() { Some(*db) } else { None })
            .collect::<Vec<_>>()
    };

    if db_ids.is_empty() {
        return;
    }

    fn run_internal(db_ids: &[i32]) {
        match db_ids {
            [] => {}
            [db] => remove_stale_series_internal(*db),
            [first, second] => {
                let _ = join(
                    || remove_stale_series_internal(*first),
                    || remove_stale_series_internal(*second),
                );
            }
            _ => {
                let (db, rest) = db_ids.split_first().unwrap();
                remove_stale_series_internal(*db);
                run_internal(rest);
            }
        }
    }

    run_internal(&db_ids);
}

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
