// Based on code from the Prometheus project
// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::postings::{EMPTY_BITMAP, KeyType, Postings};
use super::{PostingsBitmap, get_db_index, get_timeseries_index};
use crate::common::Timestamp;
use crate::common::context::get_current_db;
use crate::common::hash::IntMap;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::series::acl::{check_key_read_permission, has_all_keys_permissions};
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries, get_timeseries};
use blart::AsBytes;
use orx_parallel::{IterIntoParIter, ParIter};
use std::borrow::Cow;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn series_by_selectors<'a>(
    ctx: &'a Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);
    let postings = index.get_postings();

    let series_refs = postings.postings_for_selectors(selectors)?;
    collect_series_from_postings(ctx, &postings, series_refs.iter(), range)
}

#[allow(dead_code)]
pub(super) fn series_posting_ids_by_selectors<'a>(
    ctx: &Context,
    selectors: &[SeriesSelector],
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if selectors.is_empty() {
        return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
    }
    let db = get_current_db(ctx);
    let index = get_db_index(db);
    let postings = index.get_postings();

    let series_ids = postings.postings_for_selectors(selectors)?;
    if series_ids.is_empty() {
        return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
    }
    if date_range.is_none() {
        return Ok(Cow::Owned(series_ids.into_owned()));
    }
    let series = collect_series_from_postings(ctx, &postings, series_ids.iter(), date_range)?;
    let id_iter = series.into_iter().map(|(guard, _)| guard.id);
    let bitmap = PostingsBitmap::from_iter(id_iter);
    Ok(Cow::Owned(bitmap))
}

pub fn series_keys_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    let db = get_current_db(ctx);
    let index = get_db_index(db);
    let postings = index.get_postings();

    let series_refs = postings.postings_for_selectors(selectors)?;
    collect_series_keys(ctx, &postings, series_refs.iter(), range)
}

fn collect_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if let Some(date_range) = date_range {
        let series = collect_series_from_postings(ctx, postings, ids, Some(date_range))?;
        let keys = series.into_iter().map(|g| g.1).collect();
        return Ok(keys);
    }

    let keys = ids
        .filter_map(|id| {
            let key = postings.get_key_by_id(id)?;
            let real_key = ctx.create_string(key.as_bytes());
            if check_key_read_permission(ctx, &real_key) {
                Some(real_key)
            } else {
                None
            }
        })
        .collect();

    Ok(keys)
}

pub(super) fn collect_series_from_postings<'a>(
    ctx: &'a Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let result = get_multi_series_by_id(ctx, postings, ids)?;

    if result.is_empty() {
        return Ok(result);
    }

    // If no date range filter, return early
    let Some(date_range) = date_range else {
        return Ok(result);
    };

    filter_series_by_date_range(result, &date_range)
}

fn get_multi_series_by_id<'a>(
    ctx: &'a Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let capacity_estimate = ids.size_hint().1.unwrap_or(8);
    let mut result = Vec::with_capacity(capacity_estimate);
    for id in ids {
        let Some(key) = postings.get_key_by_id(id) else {
            continue;
        };
        let k = ctx.create_string(key.as_bytes());
        let perms = Some(AclPermissions::ACCESS);
        if let Some(guard) = get_timeseries(ctx, &k, perms, false)? {
            result.push((guard, k));
        }
    }
    Ok(result)
}

fn filter_series_by_date_range<'a>(
    mut series: Vec<(SeriesGuard<'a>, ValkeyString)>,
    date_range: &MetaDateRangeFilter,
) -> ValkeyResult<Vec<(SeriesGuard<'a>, ValkeyString)>> {
    let (start, end) = date_range.range();
    let exclude = date_range.is_exclude();

    #[inline(always)]
    fn matches_date_range(
        series: &TimeSeries,
        start: Timestamp,
        end: Timestamp,
        exclude: bool,
    ) -> bool {
        let in_range = series.has_samples_in_range(start, end);
        in_range != exclude
    }

    if series.len() == 1 {
        // SAFETY: we have already checked above that we have at least one element.
        let ts = unsafe { series.get_unchecked(0).0.as_ref() };
        return if matches_date_range(ts, start, end, exclude) {
            Ok(series)
        } else {
            Ok(Vec::new())
        };
    }

    // Parallel filter for multiple series. Note that we don't collect the guards directly
    // since they hold a reference to the Context, which is not `Send`/`Sync` - hence the
    // need to collect IDs first and then reconstruct the guards from the original vector.
    // NOTE: we should evaluate the possible implications for a large number of selected series
    // (e.g. thousands) - in that case, we might want to consider batching access to the
    // GIL while checking below.
    let matching_ids: Vec<u64> = series
        .iter()
        .map(|guard| guard.0.as_ref())
        .iter_into_par()
        .filter_map(|ts| {
            if matches_date_range(ts, start, end, exclude) {
                Some(ts.id)
            } else {
                None
            }
        })
        .collect();

    match matching_ids.len() {
        0 => Ok(Vec::new()),                  // none match
        n if n == series.len() => Ok(series), // all match
        n if n < 32 => {
            series.retain(|(guard, _)| matching_ids.contains(&guard.id));
            Ok(series)
        }
        _ => {
            let mut guard_map: IntMap<u64, (SeriesGuard, ValkeyString)> = series
                .into_iter()
                .map(|(guard, key)| (guard.id, (guard, key)))
                .collect();

            Ok(matching_ids
                .into_iter()
                .filter_map(|id| guard_map.remove(&id))
                .collect())
        }
    }
}

pub(super) fn get_guard_from_key<'a>(
    ctx: &'a Context,
    key: &KeyType,
) -> ValkeyResult<Option<SeriesGuard<'a>>> {
    let real_key = ctx.create_string(key.as_bytes());
    let perms = Some(AclPermissions::ACCESS);
    get_timeseries(ctx, &real_key, perms, false)
}

pub fn count_matched_series(
    ctx: &Context,
    date_range: Option<MetaDateRangeFilter>,
    matchers: &[SeriesSelector],
) -> ValkeyResult<usize> {
    let count = match (date_range, matchers.is_empty()) {
        (None, true) => {
            // check to see if the user can read all keys, otherwise error
            // a bare TS.CARD is a request for the cardinality of the entire index
            let current_user = ctx.get_current_user();
            let can_access_all_keys =
                has_all_keys_permissions(ctx, &current_user, Some(AclPermissions::ACCESS));
            if !can_access_all_keys {
                return Err(ValkeyError::Str(
                    error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                ));
            }
            let index = get_timeseries_index(ctx);
            index.count()
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            let index = get_timeseries_index(ctx);
            index.get_cardinality_by_selectors(matchers)?
        }
        (Some(range), false) => {
            let matched_series = series_by_selectors(ctx, matchers, Some(range))?;
            matched_series.len()
        }
        _ => {
            // if we don't have a date range, we need at least one matcher, otherwise we
            // end up scanning the entire index
            return Err(ValkeyError::Str(
                "TSDB: TS.CARD requires at least one matcher or a date range",
            ));
        }
    };
    Ok(count)
}
