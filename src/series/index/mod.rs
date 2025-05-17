use rayon::iter::ParallelIterator;
use std::sync::atomic::AtomicU64;
mod index_key;
mod memory_postings;
mod posting_stats;
mod querier;
mod timeseries_index;

use crate::common::db::get_current_db;
use papaya::{Guard, HashMap};
use rayon::iter::ParallelBridge;
use std::sync::LazyLock;
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString};

use crate::common::hash::BuildNoHashHasher;
use crate::common::time::current_time_millis;
use crate::series::acl::check_key_permissions;
use crate::series::request_types::MatchFilterOptions;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::TimeSeries;
pub use posting_stats::*;
pub use querier::*;
pub use timeseries_index::*;

#[cfg(test)]
mod querier_tests;

/// Map from db to TimeseriesIndex
pub type TimeSeriesIndexMap = HashMap<i32, TimeSeriesIndex, BuildNoHashHasher<i32>>;

pub(crate) static TIMESERIES_INDEX: LazyLock<TimeSeriesIndexMap> =
    LazyLock::new(TimeSeriesIndexMap::default);

pub(crate) static TIMESERIES_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_timeseries_id() -> u64 {
    TIMESERIES_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
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
    acls: Option<AclPermissions>,
    mut f: F,
) -> ValkeyResult<()>
where
    F: FnMut(&mut STATE, &TimeSeries, ValkeyString),
{
    let keys = series_keys_by_matchers(ctx, &filter.matchers, None)?;
    if keys.is_empty() {
        return Ok(());
    }
    if let Some(acls) = acls {
        let perm = &acls;
        for key in &keys {
            check_key_permissions(ctx, key, perm)?;
        }
    }

    let now = Some(current_time_millis());

    for key in keys {
        let redis_key = ctx.open_key(&key);
        // get series from redis
        match redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
            Ok(Some(series)) => {
                if let Some(range) = filter.date_range {
                    let (start, end) = range.get_series_range(series, now, true);
                    if series.has_samples_in_range(start, end) {
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
}

pub fn clear_timeseries_index(ctx: &Context) {
    let db = get_current_db(ctx);
    let map = TIMESERIES_INDEX.pin();
    if map.remove(&db).is_some() && map.is_empty() {
        // if we removed indices for all dbs, we need to reset the id
        // to 0 so that we can start from 1 again
        reset_timeseries_id(0);
    }
}

pub fn clear_all_timeseries_indexes() {
    reset_timeseries_id(0);
    TIMESERIES_INDEX.pin().clear();
}

pub fn swap_timeseries_index_dbs(from_db: i32, to_db: i32) {
    let guard = TIMESERIES_INDEX.guard();

    let first = get_timeseries_index_for_db(from_db, &guard);
    let second = get_timeseries_index_for_db(to_db, &guard);
    first.swap(second)
}

pub fn optimize_all_timeseries_indexes() {
    let guard = TIMESERIES_INDEX.guard();
    let values: Vec<_> = TIMESERIES_INDEX
        .values(&guard)
        .filter(|x| x.is_empty())
        .collect();
    values.into_iter().par_bridge().for_each(|index| {
        index.optimize();
    });
    guard.flush();
}

pub(crate) fn init_croaring_allocator() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| unsafe { croaring::configure_rust_alloc() });
}
