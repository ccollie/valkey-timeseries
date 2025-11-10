use crate::common::db::{get_current_db, set_current_db};
use crate::common::hash::{BuildNoHashHasher, IntMap};
use crate::common::threads::spawn;
use crate::series::index::{
    IndexKey, TIMESERIES_INDEX, with_db_index, with_timeseries_index, with_timeseries_postings,
};
use crate::series::{SeriesGuardMut, SeriesRef, TimeSeries, get_timeseries_mut};
use ahash::HashMapExt;
use blart::AsBytes;
use orx_parallel::{IntoParIter, ParIter, ParallelizableCollection, ParallelizableCollectionMut};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use valkey_module::{BlockedClient, Context, Status, ThreadSafeContext};

use valkey_module_macros::{cron_event_handler, shutdown_event_handler};

const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(10); // minimum interval between retention cleanups
const SERIES_TRIM_BATCH_SIZE: usize = 50; // number of series to trim in one batch
const STALE_ID_CLEANUP_INTERVAL: Duration = Duration::from_secs(15); // minimum interval between stale id cleanups
const STALE_ID_BATCH_SIZE: usize = 25;
const INDEX_OPTIMIZE_BATCH_SIZE: usize = 50;
const INDEX_OPTIMIZE_INTERVAL: Duration = Duration::from_secs(60); // minimum interval between index optimizations
const DB_CLEANUP_INTERVAL: Duration = Duration::from_secs(300); // minimum interval between db cleanups

#[derive(Debug, Default)]
struct IndexMeta {
    /// Cursor for the last stale eseries id processed in the index
    stale_id_cursor: Option<IndexKey>,
    /// Cursor for the last optimized key in the index
    optimize_cursor: Option<IndexKey>,
    last_updated: u64, // last time the index was updated
}

type SeriesCursorMap = IntMap<i32, SeriesRef>;
type IndexCursorMap = HashMap<i32, IndexMeta, BuildNoHashHasher<i32>>;

type DispatchMap =
    papaya::HashMap<u64, Vec<fn(&ThreadSafeContext<BlockedClient>)>, BuildNoHashHasher<u64>>;

static CRON_TICKS: AtomicU64 = AtomicU64::new(0);
static CRON_INTERVAL_MS: AtomicU64 = AtomicU64::new(100); // default to 100ms interval

static SERIES_TRIM_CURSORS: LazyLock<Mutex<SeriesCursorMap>> =
    LazyLock::new(|| Mutex::new(SeriesCursorMap::new()));

static INDEX_CURSORS: LazyLock<Mutex<IndexCursorMap>> =
    LazyLock::new(|| Mutex::new(IndexCursorMap::default()));

// During maintenance tasks, we only process one db during a cycle. We use this atomic to keep track of the current db
static CURRENT_DB: AtomicI32 = AtomicI32::new(0);

static DISPATCH_MAP: LazyLock<DispatchMap> = LazyLock::new(|| DispatchMap::default());
static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

pub(crate) fn init_background_tasks(ctx: &Context) {
    // transform hz to milliseconds
    let interval_ms = get_ticks_interval(ctx);
    CRON_INTERVAL_MS.store(interval_ms, Ordering::Relaxed);

    let cron_interval = Duration::from_millis(interval_ms);

    register_task(ctx, RETENTION_CLEANUP_INTERVAL, cron_interval, process_trim);

    register_task(
        ctx,
        STALE_ID_CLEANUP_INTERVAL,
        cron_interval,
        process_remove_stale_series,
    );

    register_task(
        ctx,
        INDEX_OPTIMIZE_INTERVAL,
        cron_interval,
        optimize_indices,
    );

    register_task(ctx, DB_CLEANUP_INTERVAL, cron_interval, trim_unused_dbs);
}

fn register_task(
    ctx: &Context,
    task_interval: Duration,
    cron_interval: Duration,
    f: fn(&ThreadSafeContext<BlockedClient>),
) {
    let floored_interval = floor_duration(task_interval, cron_interval);
    if floored_interval.is_zero() {
        ctx.log_warning(&format!(
            "register_task: interval is zero (task_interval={task_interval:?}, cron_interval={cron_interval:?})",
        ));
        return;
    }
    if cron_interval.is_zero() {
        panic!("register_task: cron_interval is zero");
    }
    let interval_ticks = std::cmp::max(
        1,
        floored_interval.as_millis() as u64 / cron_interval.as_millis() as u64,
    );

    let map = DISPATCH_MAP.pin();
    map.update_or_insert_with(
        interval_ticks,
        |handlers| {
            let mut v = handlers.clone();
            v.push(f);
            v
        },
        || vec![f],
    );
}

// Floor 'd' down to nearest interval 'interval'
fn floor_duration(d: Duration, interval: Duration) -> Duration {
    let nanos = d.as_nanos();
    let int_nanos = interval.as_nanos();
    let floored = (nanos / int_nanos) * int_nanos;
    Duration::from_nanos(floored as u64)
}

fn get_used_dbs() -> Vec<i32> {
    let index = TIMESERIES_INDEX.pin();
    let mut keys: Vec<i32> = index.keys().copied().collect();
    keys.sort();
    keys
}

fn next_db() -> i32 {
    let used_dbs = get_used_dbs();
    let current = CURRENT_DB.load(Ordering::Relaxed);
    let next = if let Some(dn) = used_dbs.iter().find(|&&d| d > current) {
        *dn
    } else {
        used_dbs.first().copied().unwrap_or(0)
    };
    CURRENT_DB.store(next, Ordering::Relaxed);
    next
}

fn get_trim_cursor(db: i32) -> SeriesRef {
    let mut map = SERIES_TRIM_CURSORS.lock().unwrap();
    *map.entry(db).or_default()
}

fn set_trim_cursor(db: i32, cursor: SeriesRef) {
    let mut map = SERIES_TRIM_CURSORS.lock().unwrap();
    match map.get_mut(&db) {
        Some(existing_cursor) => {
            *existing_cursor = cursor;
        }
        None => {
            map.insert(db, cursor);
        }
    }
}

const INDEX_LOCK_POISON_MSG: &str = "Failed to lock INDEX_CURSORS";

fn get_stale_id_cursor(db: i32) -> Option<IndexKey> {
    let map = INDEX_CURSORS.lock().expect(INDEX_LOCK_POISON_MSG);
    match map.get(&db) {
        Some(meta) => meta.stale_id_cursor.clone(),
        None => None,
    }
}

fn set_stale_id_cursor(db: i32, cursor: Option<IndexKey>) {
    let mut map = INDEX_CURSORS.lock().expect(INDEX_LOCK_POISON_MSG);
    let meta = map.entry(db).or_default();
    meta.stale_id_cursor = cursor;
}

fn process_trim(ctx: &ThreadSafeContext<BlockedClient>) {
    let mut processed = 0;

    let mut db = next_db();
    let save_db = db;
    let mut turns = 0;
    while processed < SERIES_TRIM_BATCH_SIZE && turns < 5 {
        processed += trim_series(ctx, db);
        if processed >= SERIES_TRIM_BATCH_SIZE {
            break;
        }
        db = next_db();
        if db == save_db {
            // if we are back to the first db, we are done
            break;
        }
        turns += 1;
    }
}

/// Perform active expiration of time series data for series which have a retention set.
fn trim_series(ctx: &ThreadSafeContext<BlockedClient>, db: i32) -> usize {
    let cursor = get_trim_cursor(db);

    let ctx_ = ctx.lock();
    if set_current_db(&ctx_, db) == Status::Err {
        log::warn!("Failed to select db {db}");
        return 0;
    }

    let mut batch = fetch_series_batch(&ctx_, cursor + 1, |series| {
        !series.retention.is_zero() && !series.is_empty()
    });

    if batch.is_empty() {
        log::debug!("No series to trim");
        set_trim_cursor(db, 0);
        return 0;
    }
    log::debug!("cron_event_handler: fetched {} series", batch.len());

    let last_processed = batch.last().map(|s| s.id).unwrap_or(0);
    let processed = batch.len();

    let total_deletes = batch
        .par_mut()
        .map(|series| {
            let Ok(deletes) = series.trim() else {
                log::warn!("Failed to trim series {}", series.prometheus_metric_name());
                return 0;
            };
            deletes
        })
        .sum();

    drop(ctx_);

    set_trim_cursor(db, last_processed);

    if processed == 0 {
        log::debug!("No series to trim");
    } else {
        log::info!("Processed: {processed} Deleted Samples: {total_deletes} samples");
    }

    processed
}

fn fetch_series_batch(
    ctx: &'_ Context,
    start_id: SeriesRef,
    pred: fn(&TimeSeries) -> bool,
) -> Vec<SeriesGuardMut<'_>> {
    with_timeseries_postings(ctx, |postings| {
        let all_postings = postings.all_postings();
        let mut cursor = all_postings.cursor();
        cursor.reset_at_or_after(start_id);
        let mut buf = [0_u64; SERIES_TRIM_BATCH_SIZE];
        let n = cursor.read_many(&mut buf);

        let mut stale_ids = Vec::new();
        if n == 0 {
            return vec![]; // no more keys to fetch
        }

        let mut result = Vec::with_capacity(n);
        for &id in &buf[..n] {
            if let Some(k) = postings.get_key_by_id(id) {
                let key = ctx.create_string(k.as_bytes());
                let Ok(Some(series)) = get_timeseries_mut(ctx, &key, false, None) else {
                    stale_ids.push(id);
                    continue;
                };

                if pred(&series) {
                    result.push(series);
                }
            }
        }

        if !stale_ids.is_empty() {
            // mark non-existing series for removal from the index
            with_timeseries_index(ctx, |index| {
                for id in stale_ids {
                    index.mark_id_as_stale(id)
                }
            });
        }
        result
    })
}

fn remove_stale_series_internal(db: i32) {
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

fn process_remove_stale_series(_ctx: &ThreadSafeContext<BlockedClient>) {
    let db_ids = get_used_dbs();
    if db_ids.is_empty() {
        return;
    }
    // process the databases in threads
    db_ids
        .par()
        .for_each(|&db| remove_stale_series_internal(db));
}

fn optimize_indices(_ctx: &ThreadSafeContext<BlockedClient>) {
    let db_ids = get_used_dbs();
    if db_ids.is_empty() {
        return;
    }

    let mut cursors: Vec<_> = Vec::with_capacity(db_ids.len());
    {
        let mut map = INDEX_CURSORS.lock().expect(INDEX_LOCK_POISON_MSG);
        for &db in db_ids.iter() {
            let Some(cursor) = map.get(&db) else {
                map.remove(&db);
                continue;
            };
            cursors.push((db, cursor.optimize_cursor.clone()));
        }
    }

    if cursors.is_empty() {
        return; // nothing to optimize
    }

    let results = cursors
        .into_par()
        .map(|(db, cursor)| {
            let new_cursor = with_db_index(db, |index| {
                index.optimize_incremental(cursor, INDEX_OPTIMIZE_BATCH_SIZE)
            });
            (db, new_cursor)
        })
        .collect::<Vec<_>>();

    // Store the updated cursors back in INDEX_CURSORS
    {
        let mut map = INDEX_CURSORS.lock().expect(INDEX_LOCK_POISON_MSG);
        for (db, new_cursor) in results {
            let meta = map.entry(db).or_default();
            meta.optimize_cursor = new_cursor;
        }
    }
}

fn trim_unused_dbs(_ctx: &ThreadSafeContext<BlockedClient>) {
    let mut index = TIMESERIES_INDEX.pin();
    index.retain(|db, ts_index| {
        if ts_index.is_empty() {
            log::info!("Removing unused db {db} from index");
            false
        } else {
            true
        }
    });
}

#[cron_event_handler]
fn cron_event_handler(ctx: &Context, _hz: u64) {
    // relaxed ordering is fine here since this code is not run threaded
    let ticks = CRON_TICKS.fetch_add(1, Ordering::Relaxed);
    let save_db = get_current_db(ctx);
    dispatch_tasks(ctx, ticks);
    // i'm not sure if this is necessary
    set_current_db(ctx, save_db);
}

fn dispatch_tasks(ctx: &Context, ticks: u64) {
    let map = DISPATCH_MAP.pin();
    let tasks = map
        .iter()
        .flat_map(|(&x, fns)| if ticks % x == 0 { Some(fns) } else { None })
        .flatten()
        .cloned()
        .collect::<Vec<_>>();

    drop(map);

    if tasks.is_empty() {
        return;
    }

    log::debug!("cron_event_handler: tasks={tasks:?}");
    // Create a thread-safe context for use in spawned threads
    for task in tasks.into_iter() {
        let thread_ctx = ThreadSafeContext::with_blocked_client(ctx.block_client());
        spawn(move || task(&thread_ctx));
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
    let hz = get_hz(ctx);
    get_ticks_interval_from_hz(hz)
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
