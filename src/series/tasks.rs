use crate::common::db::get_current_db;
use crate::common::hash::IntMap;
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::index::{with_timeseries_index, TIMESERIES_INDEX};
use crate::series::{SeriesRef, TimeSeries};
use ahash::HashMapExt;
use blart::AsBytes;
use num_traits::Zero;
use papaya::{Guard, HashMap};
use std::sync::atomic::{AtomicI32, AtomicU64};
use std::sync::{LazyLock, Mutex};
use valkey_module::{Context, ValkeyString};

const BATCH_SIZE: usize = 1000;

/// Per-db task metadata
pub struct TaskMeta {
    pub(crate) last_processed_id: AtomicU64,
    pub(crate) stale_ids: Mutex<Vec<SeriesRef>>,
}

impl TaskMeta {
    pub fn new() -> Self {
        Self {
            last_processed_id: AtomicU64::new(0),
            stale_ids: Mutex::new(Vec::new()),
        }
    }

    pub fn reset(&self) {
        self.last_processed_id
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.stale_ids.lock().unwrap().clear();
    }

    pub fn add_stale_id(&self, id: SeriesRef) {
        self.add_stale_ids(&[id])
    }

    pub fn add_stale_ids(&self, ids: &[SeriesRef]) {
        self.stale_ids.lock().unwrap().extend(ids);
    }

    pub fn get_stale_ids(&self) -> Vec<SeriesRef> {
        self.stale_ids.lock().unwrap().clone()
    }

    pub fn take_stale_ids(&self) -> Vec<SeriesRef> {
        let mut ids = self.stale_ids.lock().unwrap();
        std::mem::take(&mut ids)
    }

    pub fn mark_processed(&self, id: SeriesRef) {
        self.last_processed_id
            .fetch_max(id, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn last_processed_id(&self) -> u64 {
        self.last_processed_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Map from db to TaskMeta
type TaskMetaMap = HashMap<i32, TaskMeta>;

pub(crate) static DB_TASK_METAS: LazyLock<TaskMetaMap> = LazyLock::new(TaskMetaMap::new);
// During maintenance tasks, we only process one db during a cycle. We use this atomic to keep track of the current db
static CURRENT_DB: AtomicI32 = AtomicI32::new(0);

fn is_db_used(db: i32) -> bool {
    TIMESERIES_INDEX.pin().contains_key(&db)
}

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

pub fn trim_series_task(ctx: &Context) {
    let last_id = with_db_task_meta(ctx, |meta| meta.last_processed_id());
    let mut processed = 0;
    let mut last_processed = last_id;
    let mut keys = IntMap::with_capacity(BATCH_SIZE);
    let mut to_delete = Vec::with_capacity(BATCH_SIZE);
    let mut total_deletes: usize = 0;
    let mut trimmed_count: usize = 0;

    if fetch_keys_batch(ctx, last_processed + 1, BATCH_SIZE, &mut keys) {
        // todo: can we use rayon here ?
        for (id, key) in keys.iter() {
            let redis_key = ctx.open_key_writable(key);
            if let Ok(Some(series)) = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
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

    with_db_task_meta(ctx, |meta| {
        meta.mark_processed(last_processed);
        if !to_delete.is_empty() {
            meta.add_stale_ids(&to_delete);
        }
    });

    if processed.is_zero() {
        ctx.log_debug("No series to trim");
    } else {
        ctx.log_notice(&format!("Processed: {processed} Trimmed {trimmed_count}, Deleted Samples: {total_deletes} samples"));
    }
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

pub fn remove_stale_series_task(ctx: &Context) {
    let stale_ids = with_db_task_meta(ctx, |meta| meta.take_stale_ids());
    remove_stale_series(ctx, &stale_ids);
}

pub(crate) fn remove_stale_series(ctx: &Context, ids: &[SeriesRef]) {
    if !ids.is_empty() {
        with_timeseries_index(ctx, |index| {
            let removed = index.slow_remove_series_by_ids(&ids);
            ctx.log_notice(&format!("Removed {} stale series", removed));
        });
    }
}

fn with_db_task_meta<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&TaskMeta) -> R,
{
    let db = get_current_db(ctx);
    let guard = DB_TASK_METAS.guard();
    let index = get_task_meta_for_db(db, &guard);
    let res = f(index);
    drop(guard);
    res
}

#[inline]
fn get_task_meta_for_db(db: i32, guard: &impl Guard) -> &TaskMeta {
    DB_TASK_METAS.get_or_insert_with(db, TaskMeta::new, guard)
}
