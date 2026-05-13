use crate::common::context::{get_current_db, set_current_db};
use crate::common::hash::BuildNoHashHasher;
use crate::fanout::cluster_migrations::AtomicSlotMigrationEvent;
use crate::series::index::{TIMESERIES_INDEX, get_db_index};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::tasks::remove_stale_series_internal;
use crate::series::{SeriesRef, TimeSeries};
use range_set_blaze::RangeSetBlaze;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{LazyLock, RwLock};
use valkey_module::server_events::PersistenceSubevent;
use valkey_module::{Context, MODULE_CONTEXT};
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

pub(super) fn clear_delayed_keys_map() {
    DELAYED_KEYS_MAP.pin().clear();
}

pub fn add_delayed_indexing_key(db: i32, keys: &[u8]) {
    let pending_keys = DELAYED_KEYS_MAP.pin();

    let converted_key = keys.to_vec().into_boxed_slice();
    pending_keys
        .get_or_insert_with(db, || RwLock::new(Vec::with_capacity(64)))
        .write()
        .unwrap()
        .push(converted_key);
}

fn index_timeseries_in_batch(db: i32, batch: &[Box<[u8]>]) {
    let ctx = MODULE_CONTEXT.lock();
    let save_db = get_current_db(&ctx);
    set_current_db(&ctx, db);

    let index = get_db_index(db);
    let mut postings = index.inner.write().unwrap();

    for key_name in batch.iter() {
        let valkey_key = ctx.create_string(key_name.as_ref());
        let writeable_key = ctx.open_key_writable(&valkey_key);
        let Ok(Some(series)) = writeable_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) else {
            continue;
        };
        series._db = Some(db);
        postings.index_timeseries(series, valkey_key.as_slice());
    }

    set_current_db(&ctx, save_db);
}

// todo: guard against panic mid-process. Maybe store status in aux
fn process_delayed_keys_for_db(db: i32) {
    let pending_keys = DELAYED_KEYS_MAP.pin();
    let keys = pending_keys.get(&db).unwrap().read().unwrap();

    for batch in keys.chunks(BATCH_SIZE) {
        index_timeseries_in_batch(db, batch);
    }

    pending_keys.remove(&db);
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
        return;
    }

    let pending_keys = DELAYED_KEYS_MAP.pin();
    let mut dbs: Vec<i32> = pending_keys.keys().copied().collect();
    dbs.sort_unstable();

    std::thread::spawn(move || {
        for db in dbs {
            process_delayed_keys_for_db(db);
        }
    });

    PROCESSING_DELAYED_INDEXING.store(false, Ordering::SeqCst);
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
    loop {
        let index = get_db_index(db);
        let postings = index.inner.read().unwrap();

        let mut count = 0;
        postings
            .id_to_key
            .range(cursor..)
            .take(BATCH_SIZE)
            .for_each(|(id, key)| {
                let slot = crate::fanout::calculate_hash_slot(key.as_ref());
                if !source_slots.contains(slot) {
                    // This key is outside the migration range, skip it
                    return;
                }
                batch.push(*id);
                deleted_count += 1;
                count += 1;
                cursor = *id + 1; // Move cursor forward
            });

        if batch.len() >= BATCH_SIZE {
            flush(db, &mut batch);
        }

        if count == 0 {
            break; // No more entries to process
        }
    }

    flush(db, &mut batch);

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
fn handle_post_migration_cleanup(source_slots: RangeSetBlaze<u16>) {
    // spawn a background task to clean up the index so we don't block the main thread, this can take a while if there are a lot of keys to clean up
    std::thread::spawn(move || {
        let index = TIMESERIES_INDEX.pin();
        let mut dbs: Vec<i32> = index.keys().copied().collect();
        dbs.sort_unstable();
        drop(index);

        let mut deleted_count = 0;
        for db in dbs {
            deleted_count += remove_non_owned_keys(db, &source_slots);
        }

        if deleted_count > 0 {
            remove_stale_series_internal();
        }
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
            if decrement() {
                process_delayed_indexing();
            }
        }
        PersistenceSubevent::Failed => {
            ctx.log_warning("Persistence operation failed");
            if decrement() {
                clear_delayed_keys_map();
            }
        }
    }
}
