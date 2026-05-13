use super::utils::find_next_db;
use crate::is_shutting_down;
use crate::series::index::{IndexKey, TIMESERIES_INDEX};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};

const STALE_ID_BATCH_SIZE: usize = 50;

#[derive(Default)]
struct StaleIdContext {
    db: i32,
    cursor: Option<IndexKey>,
}

impl StaleIdContext {
    fn next_db(&mut self) -> i32 {
        let current = self.db;
        self.db = find_next_db(self.db).unwrap_or_default();

        current
    }
}

static STALE_ID_CLEANUP_CONTEXT: LazyLock<Mutex<StaleIdContext>> =
    LazyLock::new(|| Mutex::new(StaleIdContext::default()));
static IN_STALE_ID_CLEANUP: AtomicBool = AtomicBool::new(false);

pub(in crate::series) fn remove_stale_series_internal() {
    if is_shutting_down() {
        return;
    }

    let result =
        IN_STALE_ID_CLEANUP.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed);

    if let Err(true) = result {
        // Another cleanup is already in progress, we can skip this run
        return;
    }

    let mut context = STALE_ID_CLEANUP_CONTEXT.lock().unwrap();

    let db = context.next_db();
    if let Some(index) = TIMESERIES_INDEX.pin().get(&db) {
        let mut state = 0;
        let cursor = context.cursor.take();
        let was_none = cursor.is_none();

        index.with_postings_mut(&mut state, move |postings, _| {
            let new_cursor = postings.remove_stale_ids(cursor, STALE_ID_BATCH_SIZE);
            if new_cursor.is_some() {
                // if we have a new cursor, we need to update it
                context.cursor = new_cursor;
            } else if !was_none {
                // if we were not given a cursor, we need to set it to None, but only if it was not
                // already None
                context.cursor = None;
            }
        });
    }

    IN_STALE_ID_CLEANUP.store(false, Ordering::SeqCst);
}

pub(crate) fn remove_stale_series_ids() {
    std::thread::spawn(remove_stale_series_internal);
}
