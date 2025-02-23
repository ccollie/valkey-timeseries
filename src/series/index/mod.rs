use rayon::iter::ParallelIterator;
use std::sync::atomic::AtomicU64;
mod index_key;
mod memory_postings;
mod posting_stats;
mod querier;
#[cfg(test)]
mod querier_tests;
pub mod serialization;
mod timeseries_index;

use crate::common::db::get_current_db;
use papaya::{Guard, HashMap};
use rayon::iter::ParallelBridge;
use std::sync::LazyLock;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

use crate::labels::matchers::Matchers;
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::TimeSeries;
pub use timeseries_index::*;
pub use posting_stats::*;
pub use querier::*;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<i32, TimeSeriesIndex>;

pub(crate) static TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> =
    LazyLock::new(TimeSeriesIndexMap::new);

pub(crate) static TIMESERIES_ID: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(0));

pub fn next_timeseries_id() -> u64 {
    TIMESERIES_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub fn reset_timeseries_id(id: u64) {
    TIMESERIES_ID.store(id, std::sync::atomic::Ordering::SeqCst);
}

#[inline]
pub fn get_timeseries_index_for_db(db: i32, guard: &impl Guard) -> &TimeSeriesIndex {
    TIMESERIES_INDEX.get_or_insert_with(db, TimeSeriesIndex::new, guard)
}

pub fn with_timeseries_index<F, R>(ctx: &Context, f: F) -> R
where
    F: FnOnce(&TimeSeriesIndex) -> R,
{
    let db = get_current_db(ctx);
    let guard = TIMESERIES_INDEX.guard();
    let index = get_timeseries_index_for_db(db, &guard);
    let res = f(index);
    drop(guard);
    res
}

pub(crate) fn with_matched_series<F, STATE>(
    ctx: &Context,
    acc: &mut STATE,
    matchers: &[Matchers],
    mut f: F,
) -> ValkeyResult<()>
where
    F: FnMut(&mut STATE, &TimeSeries, ValkeyString) -> ValkeyResult<()>,
{
    with_timeseries_index(ctx, move |index| {
        let keys = series_keys_by_matchers(ctx, index, matchers)?;
        if keys.is_empty() {
            return Err(ValkeyError::Str("ERR no series found"));
        }
        for key in keys {
            let db_key = ctx.open_key(&key);
            if let Some(series) = db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)? {
                f(acc, series, key)?
            }
        }
        Ok(())
    })
}

pub fn series_keys_by_matchers(
    ctx: &Context,
    ts_index: &TimeSeriesIndex,
    matchers: &[Matchers],
) -> ValkeyResult<Vec<ValkeyString>> {
    get_keys_by_matchers(ctx, ts_index, matchers)
}

pub fn clear_timeseries_index(ctx: &Context) {
    let db = get_current_db(ctx);
    TIMESERIES_INDEX.pin().remove(&db);
}

pub fn clear_all_timeseries_indexes() {
    TIMESERIES_INDEX.pin().clear();
}

pub fn swap_timeseries_index_dbs(from_db: i32, to_db: i32) {
    let map = TIMESERIES_INDEX.pin();
    let from = map.remove(&from_db);
    let to = map.remove(&from_db);

    // change this if https://github.com/ibraheemdev/papaya/issues/29 is resolved
    if let Some(to) = to {
        map.insert(from_db, to.clone());
    }
    if let Some(from) = from {
        map.insert(to_db, from.clone());
    }
}

pub fn optimize_all_timeseries_indexes() {
    let guard = TIMESERIES_INDEX.guard();
    let values: Vec<_> = TIMESERIES_INDEX.values(&guard).collect();
    values.into_iter().par_bridge().for_each(|index| {
        index.optimize();
    });
    guard.flush();
}
