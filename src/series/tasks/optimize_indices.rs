use crate::common::threads::spawn;
use crate::series::index::{IndexKey, get_db_index};
use crate::series::tasks::utils::find_next_db;
use std::sync::{LazyLock, Mutex};

const INDEX_OPTIMIZE_BATCH_SIZE: usize = 50;

#[derive(Default)]
struct OptimizeContext {
    db: i32,
    cursor: Option<IndexKey>,
}

static LAZY_OPTIMIZE_CURSOR: LazyLock<Mutex<OptimizeContext>> =
    LazyLock::new(|| Mutex::new(OptimizeContext::default()));

#[inline]
fn set_optimize_cursor(db: i32, cursor: Option<IndexKey>) {
    let mut context = LAZY_OPTIMIZE_CURSOR.lock().unwrap();
    if cursor.is_none() {
        context.db = find_next_db(db).unwrap_or(0);
    } else {
        context.db = db;
    }
    context.cursor = cursor;
}

pub fn optimize_indices_for_db() {
    spawn(optimize_indices_internal);
}

/// Process optimization for a specific database, called by the dispatcher.
fn optimize_indices_internal() {
    let (db, cursor) = {
        let mut context = LAZY_OPTIMIZE_CURSOR.lock().unwrap();
        let db = context.db;
        if let Some(next_db) = find_next_db(db) {
            context.db = next_db;
        } else {
            context.cursor = None;
            context.db = 0;
        }
        (db, context.cursor.take())
    };

    let index = get_db_index(db);
    let new_cursor = index.optimize_incremental(cursor, INDEX_OPTIMIZE_BATCH_SIZE);

    set_optimize_cursor(db, new_cursor);
}
