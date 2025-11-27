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
use crate::common::hash::IntMap;
use crate::error_consts;
use crate::labels::filters::SeriesSelector;
use crate::series::acl::check_key_read_permission;
use crate::series::{SeriesGuard, SeriesRef, TimestampRange};
use ahash::HashMapExt;
use blart::AsBytes;
use orx_parallel::IterIntoParIter;
use orx_parallel::ParIter;
use std::str;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn series_by_selectors(
    ctx: &Context,
    selectors: &[SeriesSelector],
    range: Option<TimestampRange>,
) -> ValkeyResult<Vec<SeriesGuard>> {
    if selectors.is_empty() {
        return Ok(Vec::new());
    }

    with_timeseries_postings(ctx, |postings| {
        let first = postings.postings_for_selector(&selectors[0])?;
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
    range: Option<TimestampRange>,
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
    date_range: Option<TimestampRange>,
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
    date_range: Option<TimestampRange>,
) -> ValkeyResult<Vec<SeriesGuard>> {
    let capacity_estimate = ids.size_hint().1.unwrap_or(8);

    let iter = ids.filter_map(|id| postings.get_key_by_id(id));

    let mut result: Vec<SeriesGuard> = Vec::with_capacity(capacity_estimate);
    for key in iter {
        let guard = match get_guard_from_key(ctx, key) {
            Ok(Some(g)) => g,
            Ok(None) => continue,
            Err(err) => return Err(err),
        };
        result.push(guard);
    }

    if result.is_empty() {
        return Ok(result);
    }

    let Some(date_range) = date_range else {
        return Ok(result);
    };

    if result.len() == 1 {
        // SAFETY: we have already checked above that we have at least one element.
        let series = unsafe { result.get_unchecked(0) };
        let (start, end) = date_range.get_timestamps(None);
        return if series.has_samples_in_range(start, end) {
            Ok(result)
        } else {
            Ok(Vec::new())
        };
    }

    let (start, end) = date_range.get_timestamps(None);
    let ids = result
        .iter()
        .map(|guard| {
            let series = guard.get_series();
            (series, series.id)
        })
        .iter_into_par()
        .filter_map(|(series, id)| {
            if series.has_samples_in_range(start, end) {
                Some(id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if ids.len() == result.len() {
        // all series are in range, return early
        return Ok(result);
    }

    if ids.is_empty() {
        // no series are in range, return early
        return Ok(Vec::new());
    }

    // if we have only a few series in range, do a brute-force search in the result set
    if ids.len() < 32 {
        result.retain(|guard| {
            let id = guard.id;
            ids.contains(&id)
        });
    } else {
        // many series are in range, do a hashmap lookup
        let mut guard_map: IntMap<u64, SeriesGuard> = IntMap::with_capacity(capacity_estimate);
        for guard in result {
            let series = guard.get_series();
            guard_map.insert(series.id, guard);
        }
        result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(guard) = guard_map.remove(&id) {
                result.push(guard);
            }
        }
    }

    Ok(result)
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
    date_range: Option<TimestampRange>,
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
        (Some(_), false) => {
            let matched_series = series_by_selectors(ctx, matchers, date_range)?;
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
