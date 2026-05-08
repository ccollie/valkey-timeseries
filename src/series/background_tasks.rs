use crate::common::context::{get_current_db, set_current_db};
use crate::common::hash::{BuildNoHashHasher, IntMap};
use crate::common::logging::{log_debug, log_warning};
use crate::common::threads::{spawn, spawn_with_context};
use crate::series::index::{
    IndexKey, TIMESERIES_INDEX, get_db_index, get_timeseries_index, with_timeseries_postings,
};
use crate::series::{SeriesGuardMut, SeriesRef, TimeSeries, get_timeseries_mut};
use blart::AsBytes;
use orx_parallel::{ParIter, ParallelizableCollectionMut};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{Context, Status};
use valkey_module_macros::{cron_event_handler, shutdown_event_handler};

const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const SERIES_TRIM_BATCH_SIZE: usize = 25;
const STALE_ID_CLEANUP_INTERVAL: Duration = Duration::from_secs(15);
const STALE_ID_BATCH_SIZE: usize = 25;
const INDEX_OPTIMIZE_BATCH_SIZE: usize = 50;
const INDEX_OPTIMIZE_INTERVAL: Duration = Duration::from_secs(60);
const DB_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

const INDEX_LOCK_POISON_MSG: &str = "Failed to lock INDEX_CURSORS";
const MAX_TRIM_TURNS: usize = 5;

type IndexCursorMap = std::collections::HashMap<i32, IndexMeta, BuildNoHashHasher<i32>>;
type SeriesCursorMap = IntMap<i32, SeriesRef>;

type DispatchMap = papaya::HashMap<u64, Vec<TaskType>, BuildNoHashHasher<u64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    TrimSeries,
    RemoveStaleSeries,
    OptimizeIndices,
    TrimUnusedDbs,
}

#[derive(Debug, Default)]
struct IndexMeta {
    stale_id_cursor: Option<IndexKey>,
    optimize_cursor: Option<IndexKey>,
}

static CRON_TICKS: AtomicU64 = AtomicU64::new(0);
static CRON_INTERVAL_MS: AtomicU64 = AtomicU64::new(100);

static SERIES_TRIM_CURSORS: LazyLock<Mutex<SeriesCursorMap>> =
    LazyLock::new(|| Mutex::new(SeriesCursorMap::default()));
static INDEX_CURSORS: LazyLock<Mutex<IndexCursorMap>> =
    LazyLock::new(|| Mutex::new(IndexCursorMap::default()));

static CURRENT_TRIM_DB: AtomicI32 = AtomicI32::new(0);
static CURRENT_OPTIMIZE_DB: AtomicI32 = AtomicI32::new(0);
static CURRENT_STALE_IDS_DB: AtomicI32 = AtomicI32::new(0);

static DISPATCH_MAP: LazyLock<DispatchMap> = LazyLock::new(DispatchMap::default);
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

pub(crate) fn init_background_tasks(ctx: &Context) {
    let interval_ms = get_ticks_interval(ctx);
    CRON_INTERVAL_MS.store(interval_ms, Ordering::Relaxed);

    let cron_interval = Duration::from_millis(interval_ms);

    register_task(
        ctx,
        RETENTION_CLEANUP_INTERVAL,
        cron_interval,
        TaskType::TrimSeries,
    );
    register_task(
        ctx,
        STALE_ID_CLEANUP_INTERVAL,
        cron_interval,
        TaskType::RemoveStaleSeries,
    );
    register_task(
        ctx,
        INDEX_OPTIMIZE_INTERVAL,
        cron_interval,
        TaskType::OptimizeIndices,
    );
    register_task(
        ctx,
        DB_CLEANUP_INTERVAL,
        cron_interval,
        TaskType::TrimUnusedDbs,
    );
}

fn register_task(ctx: &Context, task_interval: Duration, cron_interval: Duration, task: TaskType) {
    if cron_interval.is_zero() {
        panic!("register_task: cron_interval is zero");
    }

    let floored = floor_duration(task_interval, cron_interval);
    if floored.is_zero() {
        ctx.log_warning(&format!(
            "register_task: interval is zero (task_interval={task_interval:?}, cron_interval={cron_interval:?})",
        ));
        return;
    }

    let interval_ticks = ticks_for_interval(floored, cron_interval);

    let map = DISPATCH_MAP.pin();
    map.update_or_insert_with(
        interval_ticks,
        |handlers| {
            if handlers.contains(&task) {
                handlers.clone()
            } else {
                let mut v = handlers.clone();
                v.push(task);
                v
            }
        },
        || vec![task],
    );
}

#[inline]
fn ticks_for_interval(task_interval: Duration, cron_interval: Duration) -> u64 {
    let task_ms = task_interval.as_millis() as u64;
    let cron_ms = cron_interval.as_millis() as u64;
    std::cmp::max(1, task_ms / cron_ms)
}

fn floor_duration(d: Duration, interval: Duration) -> Duration {
    if interval.is_zero() {
        return Duration::ZERO;
    }
    let nanos = d.as_nanos();
    let int_nanos = interval.as_nanos();
    let floored = (nanos / int_nanos) * int_nanos;

    if floored == 0 {
        return Duration::ZERO;
    }

    // Saturate instead of truncating if it does not fit u64 nanos.
    if floored > u64::MAX as u128 {
        Duration::from_nanos(u64::MAX)
    } else {
        Duration::from_nanos(floored as u64)
    }
}

fn get_used_dbs() -> Vec<i32> {
    let index = TIMESERIES_INDEX.pin();
    let mut keys: Vec<i32> = index.keys().copied().collect();
    keys.sort_unstable();
    keys
}

#[inline]
fn with_series_trim_cursors<T>(f: impl FnOnce(&mut SeriesCursorMap) -> T) -> T {
    let mut map = SERIES_TRIM_CURSORS.lock().unwrap();
    f(&mut map)
}

#[inline]
fn with_index_cursors<T>(f: impl FnOnce(&mut IndexCursorMap) -> T) -> T {
    let mut map = INDEX_CURSORS.lock().expect(INDEX_LOCK_POISON_MSG);
    f(&mut map)
}

#[inline]
fn with_index_meta<T>(db: i32, f: impl FnOnce(&mut IndexMeta) -> T) -> T {
    with_index_cursors(|map| {
        let meta = map.entry(db).or_default();
        f(meta)
    })
}

#[inline]
fn get_stale_id_cursor(db: i32) -> Option<IndexKey> {
    with_index_cursors(|map| map.get(&db).and_then(|meta| meta.stale_id_cursor.clone()))
}

#[inline]
fn set_stale_id_cursor(db: i32, cursor: Option<IndexKey>) {
    with_index_meta(db, |meta| {
        meta.stale_id_cursor = cursor;
    })
}

#[inline]
fn get_trim_cursor(db: i32) -> SeriesRef {
    with_series_trim_cursors(|map| *map.entry(db).or_default())
}

#[inline]
fn set_trim_cursor(db: i32, cursor: SeriesRef) {
    with_series_trim_cursors(|map| {
        *map.entry(db).or_default() = cursor;
    })
}

#[inline]
fn get_optimize_cursor(db: i32) -> Option<IndexKey> {
    with_index_cursors(|map| map.get(&db).and_then(|meta| meta.optimize_cursor.clone()))
}

#[inline]
fn set_optimize_cursor(db: i32, cursor: Option<IndexKey>) {
    with_index_meta(db, |meta| {
        meta.optimize_cursor = cursor;
    })
}

fn advance_db(cursor: &AtomicI32) -> i32 {
    let used_dbs = get_used_dbs();
    let current = cursor.load(Ordering::Relaxed);

    let next = used_dbs
        .iter()
        .find(|&&d| d > current)
        .copied()
        .or_else(|| used_dbs.first().copied())
        .unwrap_or(0);

    cursor.store(next, Ordering::Relaxed);
    next
}

fn next_trim_db() -> i32 {
    advance_db(&CURRENT_TRIM_DB)
}

fn next_optimize_db() -> i32 {
    advance_db(&CURRENT_OPTIMIZE_DB)
}

fn next_stale_ids_db() -> i32 {
    advance_db(&CURRENT_STALE_IDS_DB)
}

fn process_trim(ctx: &Context) {
    let mut processed = 0;
    let start_db = next_trim_db();
    let mut db = start_db;

    for _ in 0..MAX_TRIM_TURNS {
        if is_shutting_down() {
            break;
        }

        processed += trim_series(ctx, db);

        if processed >= SERIES_TRIM_BATCH_SIZE {
            break;
        }

        db = next_trim_db();
        if db == start_db {
            break;
        }
    }
}

fn trim_series(ctx: &Context, db: i32) -> usize {
    let cursor = get_trim_cursor(db);
    let save_db = get_current_db(ctx);

    if set_current_db(ctx, db) == Status::Err {
        log_warning(format!("Failed to select db {db}"));
        return 0;
    }

    let mut batch = fetch_series_batch(ctx, cursor + 1, |series| {
        !series.retention.is_zero() && !series.is_empty()
    });

    set_current_db(ctx, save_db);

    if batch.is_empty() {
        set_trim_cursor(db, 0);
        return 0;
    }

    let last_processed = batch.last().map(|s| s.id).unwrap_or(0);
    let processed = batch.len();

    let total_deletes = batch
        .par_mut()
        .map(|series| match series.trim() {
            Ok(deletes) => deletes,
            Err(_) => {
                log_warning(format!(
                    "Failed to trim series {}",
                    series.prometheus_metric_name()
                ));
                0
            }
        })
        .sum();

    set_trim_cursor(db, last_processed);

    if processed > 0 {
        log_debug(format!(
            "Processed: {processed} Deleted Samples: {total_deletes} samples"
        ));
    }

    processed
}

/// Process optimization for a specific database, called by the dispatcher.
fn optimize_indices_for_db() {
    let db = next_optimize_db();
    let index = get_db_index(db);
    let cursor = get_optimize_cursor(db);
    let new_cursor = index.optimize_incremental(cursor, INDEX_OPTIMIZE_BATCH_SIZE);
    set_optimize_cursor(db, new_cursor);
}

fn fetch_series_batch(
    ctx: &'_ Context,
    start_id: SeriesRef,
    pred: fn(&TimeSeries) -> bool,
) -> Vec<SeriesGuardMut<'_>> {
    let (result, stale_ids) = with_timeseries_postings(ctx, |postings| {
        let all_postings = &postings.all_postings;
        let mut cursor = all_postings.cursor();
        cursor.reset_at_or_after(start_id);
        let mut stale_ids = Vec::new();
        let mut result = Vec::new();

        // loop through a max of 3 times to fill the batch, to allow skipping stale ids without returning a smaller batch
        for _ in 0..3 {
            let mut buf = [0_u64; SERIES_TRIM_BATCH_SIZE];
            let n = cursor.read_many(&mut buf);
            if n == 0 {
                break;
            }
            result.reserve(n);
            for &id in &buf[..n] {
                let Some(k) = postings.get_key_by_id(id) else {
                    continue;
                };

                let key = ctx.create_string(k.as_bytes());
                let Ok(Some(series)) = get_timeseries_mut(ctx, &key, false, None) else {
                    stale_ids.push(id);
                    continue;
                };

                if pred(&series) {
                    result.push(series);
                    if result.len() >= SERIES_TRIM_BATCH_SIZE {
                        break;
                    }
                }
            }
        }

        (result, stale_ids)
    });

    if !stale_ids.is_empty() {
        let index = get_timeseries_index(ctx);
        for id in stale_ids {
            index.mark_id_as_stale(id)
        }
    }

    result
}

fn remove_stale_series_internal() {
    if is_shutting_down() {
        return;
    }
    let db = next_stale_ids_db();
    if let Some(index) = TIMESERIES_INDEX.pin().get(&db) {
        let mut state = 0;
        let cursor = get_stale_id_cursor(db);
        let was_none = cursor.is_none();

        index.with_postings_mut(&mut state, move |postings, _| {
            let new_cursor = postings.remove_stale_ids(cursor, STALE_ID_BATCH_SIZE);
            if new_cursor.is_some() {
                // if we have a new cursor, we need to update it
                set_stale_id_cursor(db, new_cursor);
            } else if !was_none {
                // if we were not given a cursor, we need to set it to None, but only if it was not
                // already None
                set_stale_id_cursor(db, None);
            }
        });
    }
}

fn trim_unused_dbs() {
    let index = TIMESERIES_INDEX.pin();
    index.retain(|db, ts_index| {
        if ts_index.is_empty() {
            log_debug(format!("Removing unused db {db} from index"));
            false
        } else {
            true
        }
    });
}

fn dispatch_background_task(task: TaskType) {
    match task {
        TaskType::TrimSeries => spawn_with_context(process_trim),
        TaskType::RemoveStaleSeries => {
            spawn(remove_stale_series_internal);
        }
        TaskType::OptimizeIndices => {
            spawn(optimize_indices_for_db);
        }
        TaskType::TrimUnusedDbs => {
            spawn(trim_unused_dbs);
        }
    }
}

#[cron_event_handler]
fn cron_event_handler(_ctx: &Context, _hz: u64) {
    if is_shutting_down() {
        return;
    }

    let ticks = CRON_TICKS.fetch_add(1, Ordering::Relaxed);
    let map = DISPATCH_MAP.pin();

    for (&interval, handlers) in map.iter() {
        if ticks.is_multiple_of(interval) {
            for task in handlers {
                dispatch_background_task(*task);
            }
        }
    }
}

fn get_hz(ctx: &Context) -> u64 {
    let server_info = ctx.server_info("");
    server_info
        .field("hz")
        .and_then(|v| v.parse_integer().ok())
        .unwrap_or(10) as u64
}

fn get_ticks_interval(ctx: &Context) -> u64 {
    get_ticks_interval_from_hz(get_hz(ctx))
}

#[inline]
fn get_ticks_interval_from_hz(hz: u64) -> u64 {
    let hz = if hz == 0 { 10 } else { hz };
    1000 / hz
}

#[inline]
fn is_shutting_down() -> bool {
    SHUTTING_DOWN.load(Ordering::Relaxed)
}

#[shutdown_event_handler]
fn shutdown_event_handler(ctx: &Context, _event: u64) {
    ctx.log_notice("Server shutdown callback event ...");
    SHUTTING_DOWN.store(true, Ordering::Relaxed);
}
