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

use super::postings::{KeyType, Postings};
use super::{with_timeseries_index, with_timeseries_postings};
use crate::common::Timestamp;
use crate::common::hash::IntMap;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::series::acl::check_key_read_permission;
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries};
use blart::AsBytes;
use orx_parallel::{IterIntoParIter, ParIter};
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn series_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<SeriesGuard>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    with_timeseries_postings(ctx, |postings| {
        let first = postings.postings_for_selector(&selectors[0])?;
        // done early if we have only one selector. Do not collapse with the loop below, since
        // this condition possibly spares us an allocation (by forcing us to own the Cow).
        if selectors.len() == 1 {
            return collect_series(ctx, postings, first.iter(), range);
        }

        let mut result = first.into_owned();
        for selector in &selectors[1..] {
            let bitmap = postings.postings_for_selector(selector)?;
            result.and_inplace(&bitmap);
        }
        collect_series(ctx, postings, result.iter(), range)
    })
}

pub fn series_keys_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    with_timeseries_postings(ctx, |postings| {
        let first = postings.postings_for_selector(&selectors[0])?;
        if selectors.len() == 1 {
            return collect_series_keys(ctx, postings, first.iter(), range);
        }

        let mut result = first.into_owned();
        for selector in &selectors[1..] {
            let bitmap = postings.postings_for_selector(selector)?;
            result.and_inplace(&bitmap);
        }
        collect_series_keys(ctx, postings, result.iter(), range)
    })
}

fn collect_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if let Some(date_range) = date_range {
        let series = collect_series(ctx, postings, ids, Some(date_range))?;
        let keys = series.into_iter().map(|g| g.key_inner).collect();
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

fn collect_series(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<MetaDateRangeFilter>,
) -> ValkeyResult<Vec<SeriesGuard>> {
    let capacity_estimate = ids.size_hint().1.unwrap_or(8);
    let iter = ids.filter_map(|id| postings.get_key_by_id(id));

    let mut result: Vec<SeriesGuard> = Vec::with_capacity(capacity_estimate);
    for key in iter {
        if let Some(guard) = get_guard_from_key(ctx, key)? {
            result.push(guard);
        }
    }

    if result.is_empty() {
        return Ok(result);
    }

    // If no date range filter or empty results, return early
    let Some(date_range) = date_range else {
        return Ok(result);
    };

    // Filter series by date range
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

    if result.len() == 1 {
        // SAFETY: we have already checked above that we have at least one element.
        return if unsafe { matches_date_range(result.get_unchecked(0), start, end, exclude) } {
            Ok(result)
        } else {
            Ok(Vec::new())
        };
    }

    // Parallel filter for multiple series. Note that we don't collect the guards directly
    // since they hold a reference to the Context, which is not `Send`/`Sync` - hence the
    // need to collect IDs first and then reconstruct the guards from the original vector.
    let matching_ids: Vec<u64> = result
        .iter()
        .map(|guard| (guard.get_series(), guard.id))
        .iter_into_par()
        .filter_map(|(series, id)| {
            if matches_date_range(series, start, end, exclude) {
                Some(id)
            } else {
                None
            }
        })
        .collect();

    match matching_ids.len() {
        0 => Ok(Vec::new()),                  // none match
        n if n == result.len() => Ok(result), // all match
        n if n < 32 => {
            result.retain(|guard| matching_ids.contains(&guard.id));
            Ok(result)
        }
        _ => {
            let mut guard_map: IntMap<u64, SeriesGuard> =
                result.into_iter().map(|guard| (guard.id, guard)).collect();

            Ok(matching_ids
                .into_iter()
                .filter_map(|id| guard_map.remove(&id))
                .collect())
        }
    }
}

fn get_guard_from_key(ctx: &Context, key: &KeyType) -> ValkeyResult<Option<SeriesGuard>> {
    let real_key = ctx.create_string(key.as_bytes());
    let perms = Some(AclPermissions::ACCESS);
    match SeriesGuard::new(ctx, real_key, perms) {
        Ok(g) => Ok(Some(g)),
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("permission") {
                return Err(ValkeyError::Str(
                    error_consts::ALL_KEYS_READ_PERMISSION_ERROR,
                ));
            }
            if msg.as_str() == error_consts::KEY_NOT_FOUND {
                let msg = format!(
                    "Failed to find series key in index: {}",
                    str::from_utf8(key.as_bytes()).unwrap_or("<invalid utf8>")
                );
                ctx.log_warning(&msg);
                return Ok(None);
            }
            Err(err)
        }
    }
}

pub fn count_matched_series(
    ctx: &Context,
    date_range: Option<MetaDateRangeFilter>,
    matchers: &[SeriesSelector],
) -> ValkeyResult<usize> {
    let count = match (date_range, matchers.is_empty()) {
        (None, true) => {
            // todo: check to see if user can read all keys, otherwise error
            // a bare TS.CARD is a request for the cardinality of the entire index
            with_timeseries_index(ctx, |index| index.count())
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            with_timeseries_index(ctx, |index| index.get_cardinality_by_selectors(matchers))?
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
