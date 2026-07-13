//! This module subscribes to Valkey events to ensure secondary index consistency.
//!
use crate::common::context::{get_current_db, register_server_event_handler, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::common::logging::{log_debug, log_notice};
use crate::fanout::cluster_migrations::{
    AtomicSlotMigrationEvent, register_atomic_slot_migration_event_handler,
    supports_atomic_slot_migration,
};
use crate::series::index::{
    TIMESERIES_INDEX, clear_timeseries_index, get_db_index, get_timeseries_index,
    get_timeseries_index_for_db, index_series_by_key,
};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::tasks::remove_all_stale_series_internal;
use crate::series::{SeriesRef, TimeSeries, get_timeseries, get_timeseries_mut};
use range_set_blaze::RangeSetBlaze;
use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, RwLock};
use valkey_module::server_events::PersistenceSubevent;
use valkey_module::{Context, MODULE_CONTEXT, NotifyEvent, ValkeyResult, logging, raw};
use valkey_module_macros::persistence_event_handler;

const BATCH_SIZE: usize = 256;

/// Collects indexed keys that are pending indexing for each database. This is used during slot migrations
/// to track keys that have been imported but not yet indexed, so we can ensure they are indexed after the migration completes.
/// Doing this at the end of the migration process
/// - to ensure that we have a complete view of all
/// - avoids the complication of filtering out "phantom keys" in the read path during migration
/// - indexes them efficiently in batches after the migration completes.
type DelayedKeysMap = papaya::HashMap<i32, RwLock<Vec<Box<[u8]>>>, BuildNoHashHasher<i32>>;

static DELAYED_KEYS_MAP: LazyLock<DelayedKeysMap> = LazyLock::new(DelayedKeysMap::default);
static DELAYED_KEYS_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) fn clear_delayed_keys_map() {
    DELAYED_KEYS_MAP.pin().clear();
}

fn clear_delayed_keys_in_db(db: i32) {
    let pending_keys = DELAYED_KEYS_MAP.pin();
    pending_keys.remove(&db);
}

pub fn add_delayed_indexing_key(db: i32, key: &[u8]) {
    let pending_keys = DELAYED_KEYS_MAP.pin();

    let converted_key = key.to_vec().into_boxed_slice();
    pending_keys
        .get_or_insert_with(db, || RwLock::new(Vec::with_capacity(64)))
        .write()
        .unwrap()
        .push(converted_key);

    let count = DELAYED_KEYS_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    // log every 100 queued keys to avoid excessive noise
    if count.is_multiple_of(100) {
        log_debug(format!(
            "ASM queued delayed indexing keys: total_queued={count}, db={db}"
        ));
    }
}

/// Indexes a batch of imported keys. Returns the number of keys that had no timeseries value at
/// index time (already deleted, wrong type, or import aborted).
fn index_timeseries_in_batch(db: i32, batch: &[Box<[u8]>]) -> usize {
    let mut skipped = 0usize;

    log_debug(format!(
        "ASM indexing batch: db={}, batch_size={}",
        db,
        batch.len()
    ));

    let ctx = MODULE_CONTEXT.lock();
    let save_db = get_current_db(&ctx);
    set_current_db(&ctx, db);

    let index = get_db_index(db);
    let mut postings = index.inner.write().unwrap();

    for key_name in batch.iter() {
        let valkey_key = ctx.create_string(key_name.as_ref());
        let writeable_key = ctx.open_key_writable(&valkey_key);
        let Ok(Some(series)) = writeable_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) else {
            skipped += 1;
            continue;
        };
        series._db = Some(db);
        postings.index_timeseries(series, valkey_key.as_slice());
    }

    set_current_db(&ctx, save_db);
    skipped
}

fn process_delayed_keys_for_db(db: i32) {
    let pending_keys = DELAYED_KEYS_MAP.pin();
    let Some(lock) = pending_keys.get(&db) else {
        return;
    };

    // Take ownership of the queued keys so we can index without holding the write lock. Keys that
    // are appended concurrently (e.g. by an overlapping import) stay in the map and are handled by
    // a later drain; we intentionally leave the (now-empty) entry in place to avoid a race where a
    // concurrent append is dropped. Empty entries are reused on the next import and cleared on
    // flush/abort.
    let keys_vec = {
        let mut guard = lock.write().unwrap();
        std::mem::take(&mut *guard)
    };

    if keys_vec.is_empty() {
        return;
    }

    let total = keys_vec.len();
    let mut indexed = 0usize;
    let mut skipped = 0usize;
    for batch in keys_vec.chunks(BATCH_SIZE) {
        // Once dequeued above, a key is only indexed here — bailing out mid-drain leaves the
        // remaining keys in this db un-indexed. That's acceptable on shutdown (the process is
        // about to exit; a restart re-derives the index from the RDB/AOF), but not otherwise, so
        // this only checks the shutdown flag, not e.g. a generic cancellation.
        if crate::is_shutting_down() {
            log_debug(format!(
                "ASM delayed indexing for db={db} aborted by shutdown; {} of {total} key(s) left un-indexed",
                total - indexed
            ));
            return;
        }
        indexed += batch.len();
        skipped += index_timeseries_in_batch(db, batch);
    }

    if skipped > 0 {
        // A skipped key had no timeseries value at drain time; this is terminal (retrying would
        // never succeed), so we drop it rather than re-queueing it forever.
        log_debug(format!(
            "ASM delayed indexing dropped {skipped} key(s) with no series value in db={db}"
        ));
    }
}

static PROCESSING_DELAYED_INDEXING: AtomicBool = AtomicBool::new(false);

pub(super) fn process_delayed_indexing() {
    let result = PROCESSING_DELAYED_INDEXING.compare_exchange(
        false,
        true,
        Ordering::AcqRel,
        Ordering::Relaxed,
    );

    if let Err(true) = result {
        // Another cleanup is already in progress, we can skip this run
        log_debug("ASM delayed indexing drain skipped: already running");
        return;
    }

    let pending_keys = DELAYED_KEYS_MAP.pin();
    let mut dbs: Vec<i32> = pending_keys.keys().copied().collect();
    dbs.sort_unstable();
    let total_keys: usize = dbs
        .iter()
        .filter_map(|db| pending_keys.get(db))
        .map(|keys| keys.read().unwrap().len())
        .sum();

    log_debug(format!(
        "ASM delayed indexing drain scheduled: dbs={}, keys={}",
        dbs.len(),
        total_keys
    ));

    // Runs on the module's rayon pool (not a detached `std::thread`) so it participates in the
    // same thread lifecycle as every other background job, and checks `is_shutting_down()`
    // between dbs — an aborted drain leaves the remaining dbs' keys un-indexed, which is fine on
    // shutdown (see `process_delayed_keys_for_db`) but must not happen otherwise.
    crate::common::threads::spawn(move || {
        for db in dbs {
            if crate::is_shutting_down() {
                log_debug("ASM delayed indexing drain aborted by shutdown");
                PROCESSING_DELAYED_INDEXING.store(false, Ordering::SeqCst);
                return;
            }
            process_delayed_keys_for_db(db);
        }
        log_debug("ASM delayed indexing drain finished");
        PROCESSING_DELAYED_INDEXING.store(false, Ordering::SeqCst);
    });
}

/// Removes keys from the index that are not owned by the given shard. This is used during shard migrations on
/// the source to clean up keys that have moved to a different shard.
fn remove_non_owned_keys(db: i32, source_slots: &RangeSetBlaze<u16>) -> usize {
    let mut batch: Vec<SeriesRef> = Vec::with_capacity(BATCH_SIZE);

    fn flush(db: i32, batch: &mut Vec<SeriesRef>) -> bool {
        if batch.is_empty() {
            return false;
        }
        let index = get_db_index(db);
        let mut postings = index.inner.write().unwrap();
        postings.mark_ids_as_stale(batch);
        batch.clear();
        true
    }

    let mut deleted_count = 0usize;
    let mut cursor: u64 = 0;

    log_notice(format!(
        "ASM remove_non_owned_keys starting db={}, source_slots_count={}",
        db,
        source_slots.iter().count()
    ));

    loop {
        let index = get_db_index(db);
        // Acquire a read lock to iterate a window of keys starting at `cursor`.
        // We must drop this read lock before acquiring the write lock inside `flush` to avoid deadlocks.
        let postings_read = index.inner.read().unwrap();

        let mut processed = 0usize;
        for (id, key) in postings_read.id_to_key.range(cursor..).take(BATCH_SIZE) {
            let slot = crate::fanout::calculate_hash_slot(key.as_ref());

            // Advance cursor for every visited entry so the iterator makes progress
            // even when entries are skipped.
            cursor = *id + 1;

            if !source_slots.contains(slot) {
                // This key is outside the migration range; skip it.
                processed += 1;
                continue;
            }

            batch.push(*id);
            deleted_count += 1;
            processed += 1;

            // If we've reached the batch threshold, break so we can drop the read guard
            // and flush the batch without moving the guard.
            if batch.len() >= BATCH_SIZE {
                break;
            }
        }

        // Drop the read lock before any potential write lock acquisition
        drop(postings_read);

        if batch.len() >= BATCH_SIZE {
            log_debug(format!(
                "ASM flush (batch full) db={} batch_len={} cursor={} deleted_so_far={}",
                db,
                batch.len(),
                cursor,
                deleted_count
            ));
            flush(db, &mut batch);
            // Continue outer loop to reacquire fresh read lock and resume
            continue;
        }

        if !batch.is_empty() {
            log_debug(format!(
                "ASM flush (final) db={} batch_len={} cursor={} deleted_so_far={}",
                db,
                batch.len(),
                cursor,
                deleted_count
            ));
            flush(db, &mut batch);
        }

        if processed == 0 {
            break; // No more entries to process
        }
    }

    deleted_count
}

/// Handles post-migration cleanup for the source shard by removing keys that are no longer owned after a successful migration.
/// Once a migration succeeds and the cluster topology updates, the slots are no longer owned by the source shard.
///
/// ## Primary Source Shard Handling
/// Valkey primaries automatically clean up unowned keys in the background, which calls standard engine deletion routines.
/// We need to ensure that the timeseries index is also cleaned up accordingly by marking the relevant series as stale in the index,
/// which will prevent them from being returned in queries and allow them to be cleaned up lazily over time as they are accessed,
/// or when the index performs maintenance.
/// The event triggering this is only received on the source primary shard.
///
/// ## Source Replica Handling
/// Because the source replicas are completely blind to the export event state machine, we cannot rely on module events there.
///
/// These deletions are natively propagated down the replication stream as standard DEL or UNLINK commands to the source replicas. We handle these events already
/// by subscribing to the Keyspace Notification hooks in `server_events` to handle the standard data eviction/deletion hooks.
///
/// In other words, we only need special handling for the source primary node.
/// ## Possible Future Optimization
/// Check if the source_slots covers all slots the current node is responsible for (or a full reshard) and if so, we can just clear
/// the entire index instead.
fn handle_post_migration_cleanup(source_slots: RangeSetBlaze<u16>) {
    let slots_count = source_slots.iter().count();
    log_notice(format!(
        "ASM post-migration cleanup scheduled: source_slots_count={slots_count}"
    ));

    // Spawn a background task so we don't block the main thread; this can take a while if there are
    // a lot of keys to clean up.
    crate::common::threads::spawn(move || {
        let index = TIMESERIES_INDEX.pin();
        let mut dbs: Vec<i32> = index.keys().copied().collect();
        dbs.sort_unstable();
        let db_count = dbs.len();
        drop(index);

        log_notice(format!(
            "ASM post-migration cleanup starting: db_count={db_count} slots_count={slots_count}"
        ));

        let mut deleted_count = 0usize;
        for db in dbs {
            deleted_count += remove_non_owned_keys(db, &source_slots);
        }

        if deleted_count > 0 {
            remove_all_stale_series_internal();
        }

        log_notice(format!(
            "ASM post-migration cleanup finished: deleted_count={deleted_count} db_count={db_count} slots_count={slots_count}"
        ));
    });
}

static IN_SLOT_IMPORT: AtomicBool = AtomicBool::new(false);
static IS_PERSISTING: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn is_in_asm_slot_import() -> bool {
    IN_SLOT_IMPORT.load(Ordering::Relaxed)
}

pub(crate) fn is_persisting() -> bool {
    let value = IS_PERSISTING.load(Ordering::Relaxed);
    value > 0
}

pub(crate) fn slot_migration_event_handler(
    event: AtomicSlotMigrationEvent,
    slots: RangeSetBlaze<u16>,
) {
    // Relaxed ordering is enough for IN_SLOT_IMPORT, as Valkey itself will ensure that
    // this callback is called serially with respect to the migration events.
    match event {
        AtomicSlotMigrationEvent::ExportCompleted => {
            handle_post_migration_cleanup(slots);
        }
        AtomicSlotMigrationEvent::ImportStarted => {
            IN_SLOT_IMPORT.store(true, Ordering::Relaxed);
        }
        AtomicSlotMigrationEvent::ImportCompleted => {
            IN_SLOT_IMPORT.store(false, Ordering::Relaxed);
            let persistence_depth = IS_PERSISTING.load(Ordering::Relaxed);
            log_debug(format!(
                "ASM ImportCompleted received; triggering delayed indexing drain (persistence_depth={persistence_depth})"
            ));
            process_delayed_indexing();
        }
        AtomicSlotMigrationEvent::ImportAborted => {
            IN_SLOT_IMPORT.store(false, Ordering::Relaxed);
            clear_delayed_keys_map();
        }
        _ => {
            // no action needed for other events
        }
    }
}

#[persistence_event_handler]
fn persistence_event_handler(ctx: &Context, persistence_event: PersistenceSubevent) {
    fn increment() {
        IS_PERSISTING.fetch_add(1, Ordering::SeqCst);
    }

    fn decrement() -> bool {
        let prev = IS_PERSISTING.fetch_sub(1, Ordering::SeqCst);
        prev == 1
    }

    match persistence_event {
        PersistenceSubevent::RdbStart => {
            increment();
            ctx.log_notice("RDB persistence started");
        }
        PersistenceSubevent::AofStart => {
            increment();
            ctx.log_notice("AOF persistence started");
        }
        PersistenceSubevent::SyncRdbStart => {
            increment();
            ctx.log_notice("Sync RDB persistence started");
        }
        PersistenceSubevent::SyncAofStart => {
            increment();
            ctx.log_notice("Sync AOF persistence started");
        }
        PersistenceSubevent::Ended => {
            ctx.log_notice("Persistence operation ended");
            decrement();
        }
        PersistenceSubevent::Failed => {
            ctx.log_warning("Persistence operation failed");
            if decrement() {
                clear_delayed_keys_map();
            }
        }
    }
}

fn handle_key_move(ctx: &Context, key: &[u8], old_db: i32) {
    let new_db = get_current_db(ctx);
    // fetch the series from the new
    let valkey_key = ctx.create_string(key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &valkey_key, false, None) else {
        logging::log_warning("Failed to load series for key move");
        return;
    };

    // remove the series from the old db index
    let old_index = get_db_index(old_db);
    old_index.remove_timeseries(&series);

    // add the series to the new db index
    series._db = Some(new_db);
    let new_index = get_db_index(new_db);
    new_index.index_timeseries(&series, key);
}

fn handle_key_rename(ctx: &Context, _old_key: &[u8], new_key: &[u8]) {
    let index = get_timeseries_index(ctx);
    let key = ctx.create_string(new_key);
    let Ok(Some(series)) = get_timeseries(ctx, &key, None, false) else {
        logging::log_warning("Failed to load series for key rename");
        return;
    };
    index.reindex_timeseries(&series, new_key);
}

/// Handle the "restore" event, which is triggered for each key restored from disk during server startup
/// or slot migration. It collects the keys for later indexing if we're in the middle of an ASM slot import,
/// otherwise it indexes them immediately.
fn handle_key_restore(ctx: &Context, key: &[u8]) {
    let db = get_current_db(ctx);
    if is_in_asm_slot_import() {
        add_delayed_indexing_key(db, key);
        return;
    }
    index_series_by_key(ctx, key);
}

/// Indexes the destination of a `COPY` command. The type `copy` callback cannot maintain the index
/// itself, so this runs from the `copy_to` keyspace notification. `COPY` fires this event for every
/// key type, so non-timeseries keys are ignored quietly.
fn handle_key_copy(ctx: &Context, key: &[u8]) {
    let db = get_current_db(ctx);
    let valkey_key = ctx.create_string(key);
    let Ok(Some(mut series)) = get_timeseries_mut(ctx, &valkey_key, false, None) else {
        return;
    };
    series._db = Some(db);
    let index = get_db_index(db);
    if !index.has_id(series.id) {
        index.index_timeseries(&series, key);
    }
}

static RENAME_FROM_KEY: Mutex<Vec<u8>> = Mutex::new(vec![]);
static MOVE_FROM_DB: Mutex<i32> = Mutex::new(-1);

pub(crate) fn generic_key_events_handler(
    ctx: &Context,
    _event_type: NotifyEvent,
    event: &str,
    key: &[u8],
) {
    hashify::fnc_map!(event.as_bytes(),
        "loaded" => {
            // Handle the "loaded" event, which is triggered for each key loaded from disk during server startup.
            handle_key_restore(ctx, key);
        },
        "move_from" => {
            *MOVE_FROM_DB.lock().unwrap() = get_current_db(ctx);
        },
        "move_to" => {
            let mut lock = MOVE_FROM_DB.lock().unwrap();
            let old_db = *lock;
            *lock = -1;
            if old_db != -1 {
                 handle_key_move(ctx, key, old_db);
            }
        },
        "rename_from" => {
            *RENAME_FROM_KEY.lock().unwrap() = key.to_vec();
        },
        "rename_to" => {
            let mut old_key = RENAME_FROM_KEY.lock().unwrap();
            if !old_key.is_empty() {
                handle_key_rename(ctx, &old_key, key);
                old_key.clear();
            }
        },
        "restore" => {
            handle_key_restore(ctx, key);
        },
        "copy_to" => {
            // The type `copy` callback cannot touch the index (it runs on the main thread with the
            // GIL held). Index the freshly-copied destination key here, where we have a valid
            // context with the destination db selected.
            handle_key_copy(ctx, key);
        },
        _ => {}
    );
}

unsafe extern "C" fn on_flush_event(
    ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    if sub_event == raw::REDISMODULE_SUBEVENT_FLUSHDB_END {
        let fi: &raw::RedisModuleFlushInfo = unsafe { &*(data as *mut raw::RedisModuleFlushInfo) };

        if fi.dbnum == -1 {
            clear_delayed_keys_map();
        } else {
            let ctx = Context::new(ctx);
            set_current_db(&ctx, fi.dbnum);
            clear_timeseries_index(&ctx);
            clear_delayed_keys_in_db(fi.dbnum);
        }
    };
}

fn swap_timeseries_index_dbs(from_db: i32, to_db: i32) {
    let guard = TIMESERIES_INDEX.guard();

    let first = get_timeseries_index_for_db(from_db, &guard);
    let second = get_timeseries_index_for_db(to_db, &guard);
    first.swap(second)
}

unsafe extern "C" fn on_swap_db_event(
    _ctx: *mut raw::RedisModuleCtx,
    eid: raw::RedisModuleEvent,
    _sub_event: u64,
    data: *mut c_void,
) {
    if eid.id == raw::REDISMODULE_EVENT_SWAPDB {
        let ei: &raw::RedisModuleSwapDbInfo =
            unsafe { &*(data as *mut raw::RedisModuleSwapDbInfo) };

        let from_db = ei.dbnum_first;
        let to_db = ei.dbnum_second;

        swap_timeseries_index_dbs(from_db, to_db);
    }
}

pub(crate) fn register_server_event_handlers(ctx: &Context) -> ValkeyResult<()> {
    if supports_atomic_slot_migration(ctx) {
        ctx.log_notice("Registering atomic slot migration event handler");
        register_atomic_slot_migration_event_handler(ctx, Some(slot_migration_event_handler));
    } else {
        ctx.log_notice(
            "Atomic slot migration not supported, skipping registration of related event handler",
        );
    }
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_FLUSHDB, Some(on_flush_event))?;
    register_server_event_handler(ctx, raw::REDISMODULE_EVENT_SWAPDB, Some(on_swap_db_event))?;
    Ok(())
}
