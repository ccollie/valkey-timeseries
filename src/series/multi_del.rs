use crate::common::Timestamp;
use crate::common::context::is_real_user_client;
use crate::common::threads::NUM_THREADS;
use crate::labels::filters::SeriesSelector;
use crate::series::index::{PostingsBitmap, with_timeseries_index, with_timeseries_postings};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{
    CompactionOp, SeriesGuardMut, SeriesRef, TimeSeries, TimestampRange, apply_compaction,
};
use blart::AsBytes;
use croaring::bitmap64::Bitmap64Iterator;
use logger_rust::log_debug;
use orx_parallel::ParIter;
use orx_parallel::ParallelizableCollectionMut;
use smallvec::SmallVec;
use std::sync::atomic::Ordering;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
};

pub fn delete_series_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    date_range: Option<TimestampRange>,
) -> ValkeyResult<usize> {
    match date_range {
        Some(range) => {
            let (start_ts, end_ts) = range.get_timestamps(None);
            handle_delete_range(ctx, selectors, start_ts, end_ts)
        }
        None => handle_delete_keys(ctx, selectors),
    }
}

fn delete_key(ctx: &Context, key: &ValkeyString) -> ValkeyResult<usize> {
    match ctx.open_key_writable(key).delete() {
        Ok(_) => Ok(1),
        Err(e) => {
            let msg = format!(
                "multi-del: error deleting key {}: {:?}",
                key.to_string_lossy(),
                e
            );
            ctx.log_warning(&msg);
            Ok(0)
        }
    }
}

fn handle_delete_keys(ctx: &Context, filters: &[SeriesSelector]) -> ValkeyResult<usize> {
    // get keys from ids
    with_timeseries_index(ctx, |index| {
        let keys = index.keys_for_selectors(ctx, filters, Some(AclPermissions::DELETE))?;
        let mut total_deleted = 0;
        for key in keys {
            total_deleted += delete_key(ctx, &ctx.create_string(key.as_ref()))?;
        }
        Ok(total_deleted)
    })
}

fn handle_delete_range(
    ctx: &Context,
    filters: &[SeriesSelector],
    start: Timestamp,
    end: Timestamp,
) -> ValkeyResult<usize> {
    // we iterate over ids instead of keys to be able to do parallel deletions
    let ids = with_timeseries_postings(ctx, |index| {
        let ids = index.postings_for_selectors(filters)?.into_owned();
        Ok::<PostingsBitmap, ValkeyError>(ids)
    })?;

    let num_threads = usize::min(NUM_THREADS.load(Ordering::Relaxed), 2);
    let mut total_deleted = 0;
    log_debug!(
        "Starting deletion of range [{start}, {end}] for {} series. Num threads: {num_threads}",
        ids.cardinality()
    );

    let mut iter = ids.iter();
    loop {
        let (series_batch, keys_batch) = fetch_series_batch(ctx, &mut iter, num_threads);
        if series_batch.is_empty() {
            log_debug!(
                "No more series to process, total deleted: {}",
                total_deleted
            );
            break;
        }

        let deleted = delete_range_batch(ctx, series_batch, &keys_batch, start, end)?;
        total_deleted += deleted;
    }
    Ok(total_deleted)
}

fn delete_range_batch(
    ctx: &Context,
    series: Vec<SeriesGuardMut>,
    keys: &[ValkeyString],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> ValkeyResult<usize> {
    let mut total_deleted = 0;
    let mut series = series;
    let res = series
        .par_mut()
        .map(|guard| guard.remove_range(start_ts, end_ts))
        .collect::<Vec<_>>();

    // Run compaction after deletions
    for (i, (deleted, ts)) in res.iter().zip(series.iter_mut()).enumerate() {
        if let Err(err) = deleted {
            log_debug!(
                "Got error removing range from series {}: {err:?}",
                keys[i].to_string_lossy()
            );
        }
        if let Ok(deleted) = deleted {
            log_debug!(
                "Deleted {deleted} points from series {}",
                keys[i].to_string_lossy()
            );
            if *deleted == 0 {
                continue;
            }

            total_deleted += *deleted;
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.del", &keys[i]);
            // run compaction if needed
            apply_compaction(
                ctx,
                ts,
                CompactionOp::RemoveRange {
                    start: start_ts,
                    end: end_ts,
                },
            )?;
        }
    }

    Ok(total_deleted)
}

fn fetch_series_batch<'a>(
    ctx: &'a Context,
    cursor: &mut Bitmap64Iterator<'_>,
    buf_size: usize,
) -> (Vec<SeriesGuardMut<'a>>, Vec<ValkeyString>) {
    let user = ctx.get_current_user();
    let is_user_client = is_real_user_client(ctx);
    let has_all_keys_permission = if !is_user_client {
        true
    } else {
        ctx.acl_check_key_permission(&user, &ctx.create_string("*"), &AclPermissions::DELETE)
            .is_ok()
    };

    with_timeseries_postings(ctx, |postings| {
        let mut stale_ids: SmallVec<SeriesRef, 8> = SmallVec::new();
        let mut result: Vec<SeriesGuardMut<'a>> = Vec::with_capacity(buf_size);
        let mut keys: Vec<ValkeyString> = Vec::with_capacity(buf_size);

        // Read ids in chunks until we gather `buf_size` valid series or cursor is exhausted.
        for id in cursor.by_ref() {
            log_debug!("fetch_series_batch: reading {id} from iter");
            let Some(k) = postings.get_key_by_id(id) else {
                stale_ids.push(id);
                continue;
            };

            let key = ctx.create_string(k.as_bytes());

            if is_user_client
                && !has_all_keys_permission
                && ctx
                    .acl_check_key_permission(&user, &key, &AclPermissions::DELETE)
                    .is_err()
            {
                continue;
            }

            match get_timeseries(ctx, &key) {
                Err(_) => {
                    stale_ids.push(id);
                    continue;
                }
                Ok(None) => {
                    stale_ids.push(id);
                    continue;
                }
                Ok(Some(series)) => {
                    result.push(series);
                    keys.push(key);
                }
            }

            if result.len() >= buf_size {
                break;
            }
        }

        if !stale_ids.is_empty() {
            with_timeseries_index(ctx, |index| {
                for id in stale_ids {
                    index.mark_id_as_stale(id);
                }
            });
        }

        (result, keys)
    })
}

fn get_timeseries<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
) -> ValkeyResult<Option<SeriesGuardMut<'a>>> {
    let value_key = ctx.open_key_writable(key);
    match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Ok(Some(series)) => Ok(Some(SeriesGuardMut { series })),
        Ok(None) => Ok(None),
        Err(_e) => Err(ValkeyError::WrongType),
    }
}
