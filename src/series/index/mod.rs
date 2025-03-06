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
use crate::arg_types::MatchFilterOptions;
use crate::common::time::current_time_millis;
use crate::error_consts;

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

pub fn with_matched_series<F, STATE>(
    ctx: &Context,
    acc: &mut STATE,
    filter: &MatchFilterOptions,
    mut f: F,
) -> ValkeyResult<()>
where
    F: FnMut(&mut STATE, &TimeSeries, ValkeyString),
{
    with_timeseries_index(ctx, move |index| {
        let keys = series_keys_by_matchers(ctx, index, &filter.matchers)?;
        if keys.is_empty() {
            return Err(ValkeyError::Str(error_consts::NO_SERIES_FOUND));
        }

        let now = Some(current_time_millis());

        for key in keys {
            let redis_key = ctx.open_key(&key);
            // get series from redis
            match redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                Ok(Some(series)) => {
                    if let Some(range) = filter.date_range {
                        let (start, end) = range.get_series_range(series, now, true);
                        if series.overlaps(start, end) {
                            f(acc, series, key);
                        }
                    } else {
                        f(acc, series, key);
                    }
                }
                Err(e) => {
                    return Err(e);
                }
                _ => {}
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
    get_keys_by_matchers(ctx, ts_index, matchers, None)
}

pub fn clear_timeseries_index(ctx: &Context) {
    let db = get_current_db(ctx);
    TIMESERIES_INDEX.pin().remove(&db);
}

pub fn clear_all_timeseries_indexes() {
    reset_timeseries_id(0);
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
