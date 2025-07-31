use crate::common::db::set_current_db;
use crate::common::hash::{BuildNoHashHasher, IntMap};
use crate::common::parallel::maybe_par_iter;
use crate::series::index::{
    optimize_all_timeseries_indexes, with_timeseries_index, IndexKey, TIMESERIES_INDEX,
};
use crate::series::{get_timeseries_mut, SeriesRef};
use ahash::HashMapExt;
use blart::AsBytes;
use papaya::HashMap;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};
use valkey_module::{Context, Status, ValkeyString};

use valkey_module_macros::cron_event_handler;

const RETENTION_CLEANUP_TICKS: u64 = 10; // run retention cleanup every RETENTION_CLEANUP_TICKS cron ticks
const SERIES_TRIM_BATCH_SIZE: usize = 50; // number of series to trim in one batch
const STALE_ID_CLEANUP_TICKS: u64 = 25; // run stale id clean up every STALE_ID_CLEANUP_TICKS cron ticks
const STALE_ID_BATCH_SIZE: usize = 25;
const INDEX_OPTIMIZE_TICKS: u64 = 25000; // run index optimization every INDEX_OPTIMIZE_TICKS cron ticks

type SeriesCursorMap = IntMap<i32, SeriesRef>;
type StaleIdCursorMap = HashMap<i32, Option<IndexKey>, BuildNoHashHasher<i32>>;

static CRON_TICKS: AtomicU64 = AtomicU64::new(0);
static SERIES_TRIM_CURSORS: LazyLock<Mutex<SeriesCursorMap>> =
    LazyLock::new(|| Mutex::new(SeriesCursorMap::new()));
static STALE_ID_CURSORS: LazyLock<StaleIdCursorMap> = LazyLock::new(StaleIdCursorMap::default);

// During maintenance tasks, we only process one db during a cycle. We use this atomic to keep track of the current db
static CURRENT_DB: AtomicI32 = AtomicI32::new(0);

fn get_used_dbs() -> SmallVec<i32, 16> {
    let index = TIMESERIES_INDEX.pin();
    let mut keys: SmallVec<i32, 16> = index.keys().copied().collect();
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

fn get_stale_id_cursor(db: i32) -> Option<IndexKey> {
    let map = STALE_ID_CURSORS.pin();
    map.get(&db).and_then(|meta| meta.clone())
}

fn set_stale_id_cursor(db: i32, cursor: Option<IndexKey>) {
    let map = STALE_ID_CURSORS.pin();
    map.insert(db, cursor);
}

/// Perform active expiration of time series data for series which have a retention set.
fn trim_series(ctx: &Context, db: i32) -> usize {
    if set_current_db(ctx, db) == Status::Err {
        ctx.log_warning(&format!("Failed to select db {db}"));
        return 0;
    }
    let cursor = get_trim_cursor(db);
    let mut processed = 0;
    let mut last_processed = cursor;
    let mut keys = IntMap::with_capacity(SERIES_TRIM_BATCH_SIZE);
    let mut to_delete = Vec::with_capacity(SERIES_TRIM_BATCH_SIZE);
    let mut total_deletes: usize = 0;

    if fetch_keys_batch(ctx, last_processed + 1, &mut keys) {
        let mut series_to_trim = Vec::with_capacity(SERIES_TRIM_BATCH_SIZE);
        for (&id, key) in keys.iter() {
            let Ok(Some(series)) = get_timeseries_mut(ctx, key, false, None) else {
                to_delete.push(id);
                last_processed = id;
                continue;
            };

            if series.retention.is_zero() || series.is_empty() {
                // no need to trim series with no data or retention
                continue;
            }

            series_to_trim.push(series);
            processed += 1;
            last_processed = id;
        }
        let processed_count: AtomicUsize = AtomicUsize::default();
        // trimming is lightweight, so we set the threshold higher to minimize the number of threads spawned
        maybe_par_iter(5, series_to_trim, |mut series| {
            let Ok(deletes) = series.trim() else {
                // todo: log this
                return;
            };
            processed_count.fetch_add(deletes, Ordering::SeqCst); // this is ugly
        });

        total_deletes = processed_count.load(Ordering::Relaxed);
    } else {
        ctx.log_debug("No more series to trim");
        last_processed = 0;
    }

    set_trim_cursor(db, last_processed);

    if !to_delete.is_empty() {
        // mark non-existing series for removal from the index
        with_timeseries_index(ctx, |index| {
            for id in to_delete {
                index.mark_id_as_stale(id)
            }
        });
    }

    if processed == 0 {
        ctx.log_debug("No series to trim");
    } else {
        ctx.log_notice(&format!(
            "Processed: {processed} Deleted Samples: {total_deletes} samples"
        ));
    }

    processed
}

fn fetch_keys_batch(
    ctx: &Context,
    start_id: SeriesRef,
    ids: &mut IntMap<SeriesRef, ValkeyString>,
) -> bool {
    with_timeseries_index(ctx, |index| {
        let mut state = ();
        index.with_postings(&mut state, |postings, _| {
            let all_postings = postings.all_postings();
            let mut cursor = all_postings.cursor();
            cursor.reset_at_or_after(start_id);
            let mut buf = [0_u64; SERIES_TRIM_BATCH_SIZE];
            let n = cursor.read_many(&mut buf);
            if n == 0 {
                return false; // no more keys to fetch
            }
            let mut added = 0;
            for &id in &buf[..n] {
                if let Some(k) = postings.get_key_by_id(id) {
                    let key = ctx.create_string(k.as_bytes());
                    ids.insert(id, key);
                    added += 1;
                }
            }
            added > 0
        })
    })
}

pub fn process_trim(ctx: &Context) {
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

pub fn process_remove_stale_series() {
    let db_ids = get_used_dbs();
    if db_ids.is_empty() {
        return;
    }
    // process the databases in parallel
    maybe_par_iter(2, &db_ids[..], |&db| {
        remove_stale_series_internal(db);
    });
}

#[cron_event_handler]
fn cron_event_handler(ctx: &Context, _hz: u64) {
    // relaxed ordering is fine here since this code is not run threaded
    let ticks = CRON_TICKS.fetch_add(1, Ordering::Relaxed);
    schedule_periodic_tasks(ctx, ticks);
}

fn schedule_periodic_tasks(ctx: &Context, ticks: u64) {
    if should_run_task(ticks, STALE_ID_CLEANUP_TICKS) {
        rayon::spawn(process_remove_stale_series);
    }

    if should_run_task(ticks, INDEX_OPTIMIZE_TICKS) {
        rayon::spawn(optimize_all_timeseries_indexes);
    }

    if should_run_task(ticks, RETENTION_CLEANUP_TICKS) {
        process_trim(ctx);
    }
}

#[inline]
fn should_run_task(current_ticks: u64, interval_ticks: u64) -> bool {
    current_ticks % interval_ticks == 0
}
