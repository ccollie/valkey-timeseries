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

use super::postings::{EMPTY_BITMAP, Postings, PostingsBitmap, handle_equal_match};
use super::{TimeSeriesIndex, with_timeseries_postings};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::time::current_time_millis;
use crate::error_consts::MISSING_FILTER;
use crate::labels::matchers::{
    MatchOp, Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
};
use crate::series::acl::{check_key_permissions, check_key_read_permission};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries, TimestampRange};
use ahash::AHashSet;
use blart::AsBytes;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::str;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn series_by_matchers(
    ctx: &Context,
    matchers: &[Matchers],
    range: Option<TimestampRange>,
    require_permissions: bool,
    raise_permission_error: bool,
) -> ValkeyResult<Vec<SeriesGuard>> {
    if matchers.is_empty() {
        return Ok(Vec::new());
    }

    with_timeseries_postings(ctx, |postings| {
        let first = postings_for_matchers_internal(postings, &matchers[0])?;
        if matchers.len() == 1 {
            return collect_series(
                ctx,
                postings,
                first.iter(),
                range,
                require_permissions,
                raise_permission_error,
            );
        }
        // todo: use chili here ?
        let mut result = first.into_owned();
        for matcher in &matchers[1..] {
            let bitmap = postings_for_matchers_internal(postings, matcher)?;
            result.and_inplace(&bitmap);
        }
        collect_series(
            ctx,
            postings,
            result.iter(),
            range,
            require_permissions,
            raise_permission_error,
        )
    })
}

pub fn series_keys_by_matchers(
    ctx: &Context,
    matchers: &[Matchers],
    range: Option<TimestampRange>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if matchers.is_empty() {
        return Ok(Vec::new());
    }

    with_timeseries_postings(ctx, |postings| {
        let first = postings_for_matchers_internal(postings, &matchers[0])?;
        if matchers.len() == 1 {
            let keys = collect_series_keys(ctx, postings, first.iter(), range);
            return Ok(keys);
        }

        let mut result = first.into_owned();
        for matcher in &matchers[1..] {
            let bitmap = postings_for_matchers_internal(postings, matcher)?;
            result.and_inplace(&bitmap);
        }
        let keys = collect_series_keys(ctx, postings, result.iter(), range);
        Ok(keys)
    })
}

pub fn get_cardinality_by_matchers_list(
    ix: &TimeSeriesIndex,
    matchers: &[Matchers],
) -> ValkeyResult<u64> {
    if matchers.is_empty() {
        return Ok(0);
    }

    let mut state: u64 = 0;

    ix.with_postings(&mut state, move |inner, _state| {
        let first = postings_for_matchers_internal(inner, &matchers[0])?;
        if matchers.len() == 1 {
            return Ok(first.cardinality());
        }
        // todo: use chili here ?
        let mut result = first.into_owned();
        for matcher in &matchers[1..] {
            let postings = postings_for_matchers_internal(inner, matcher)?;
            result.and_inplace(&postings);
        }

        Ok(result.cardinality())
    })
}

fn collect_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<TimestampRange>,
) -> Vec<ValkeyString> {
    let mut keys = Vec::new();

    for key in ids.filter_map(|id| postings.get_key_by_id(id)) {
        let real_key = ctx.create_string(key.as_bytes());
        if check_key_read_permission(ctx, &real_key) {
            keys.push(real_key);
        }
    }
    if let Some(date_range) = date_range {
        let now = Some(current_time_millis());
        keys.retain(|key| {
            let redis_key = ctx.open_key(key);
            if let Ok(Some(series)) = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                let (start, end) = date_range.get_series_range(series, now, true);
                series.has_samples_in_range(start, end)
            } else {
                false
            }
        });
    }
    keys
}

fn collect_series(
    ctx: &Context,
    postings: &Postings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<TimestampRange>,
    require_permissions: bool,
    raise_permission_error: bool,
) -> ValkeyResult<Vec<SeriesGuard>> {
    let mut keys = Vec::with_capacity(8);

    for key in ids.filter_map(|id| postings.get_key_by_id(id)) {
        let real_key = ctx.create_string(key.as_bytes());
        // check permissions if provided
        if require_permissions {
            if let Err(err) = check_key_permissions(ctx, &real_key, &AclPermissions::ACCESS) {
                if raise_permission_error {
                    return Err(err);
                } else {
                    // skip series that we can't access
                    continue;
                }
            }
        }

        keys.push(real_key);
    }

    if keys.is_empty() {
        // If we didn't find any series, return an empty vector.
        return Ok(Vec::new());
    }

    if let Some(date_range) = date_range {
        // If we have a date range, filter the series by it.
        let (start, end) = date_range.get_timestamps(None);
        // keep these values alive

        let mut series = Vec::with_capacity(keys.len());

        // at some point we should parallelize this.
        for key in keys.into_iter() {
            // SAFETY: we have already checked permissions above.
            let valkey_key = ctx.open_key(&key);
            // get series from valkey
            match valkey_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                Ok(Some(ts)) => {
                    if ts.has_samples_in_range(start, end) {
                        // SAFETY: we have already checked permissions above.
                        let guard = SeriesGuard::open(ctx, key);
                        series.push(guard);
                    } else {
                        continue;
                    }
                }
                Ok(None) => {
                    ctx.log_warning("Failed to find series key in index");
                    // todo: mark the series as stale?
                }
                Err(e) => return Err(e),
            }
        }

        return Ok(series);
    }

    // If we don't have a date range, we can return all the series.
    let res = keys
        .into_iter()
        .map(|key| {
            // SAFETY: we have already checked permissions above.
            SeriesGuard::new(ctx, key, &None)
        })
        .collect::<ValkeyResult<Vec<SeriesGuard>>>()?;

    Ok(res)
}

/// `postings_for_matchers` assembles a single postings iterator against the series index
/// based on the given matchers.
#[allow(dead_code)]
pub fn postings_for_matchers(
    ix: &TimeSeriesIndex,
    matchers: &Matchers,
) -> ValkeyResult<PostingsBitmap> {
    let mut state = ();
    ix.with_postings(&mut state, move |inner, _| {
        let postings = postings_for_matchers_internal(inner, matchers)?;
        let res = postings.into_owned();
        Ok(res)
    })
}

pub(crate) fn postings_for_matchers_internal<'a>(
    ix: &'a Postings,
    matchers: &Matchers,
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if matchers.is_empty() {
        return Ok(Cow::Borrowed(ix.all_postings()));
    }

    let mut name_postings: Option<Cow<'a, PostingsBitmap>> = None;
    let mut other_postings: Option<Cow<'a, PostingsBitmap>> = None;

    if let Some(name) = &matchers.name {
        let postings_for_name = ix.postings_for_label_value(METRIC_NAME_LABEL, name.as_str());
        name_postings = Some(postings_for_name);
    }

    match &matchers.matchers {
        MatcherSetEnum::And(matchers) => {
            if !matchers.is_empty() {
                let postings = process_and_matchers(ix, matchers)?;
                other_postings = Some(postings);
            }
        }
        MatcherSetEnum::Or(matchers) => {
            let postings = process_or_matchers(ix, matchers)?;
            other_postings = Some(postings);
        }
    }

    match (name_postings, other_postings) {
        (Some(name), Some(other)) => {
            if name.cardinality() < other.cardinality() {
                let mut result = name.into_owned();
                result.and_inplace(&other);
                Ok(Cow::Owned(result))
            } else {
                let mut result = other.into_owned();
                result.and_inplace(&name);
                Ok(Cow::Owned(result))
            }
        }
        (Some(name), None) => Ok(name),
        (None, Some(other)) => Ok(other),
        _ => Ok(Cow::Borrowed(ix.all_postings())),
    }
}

fn process_or_matchers<'a>(
    ix: &'a Postings,
    matchers: &[Vec<Matcher>],
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if matchers.len() == 1 {
        let m = matchers
            .first()
            .expect("Out of bounds error running matchers");
        process_and_matchers(ix, m)
    } else {
        let mut result = PostingsBitmap::new();
        // maybe chili here to run in parallel
        for matcher in matchers {
            let postings = process_and_matchers(ix, matcher)?;
            result.or_inplace(&postings);
        }
        Ok(Cow::Owned(result))
    }
}

fn process_and_matchers<'a>(
    ix: &'a Postings,
    matchers: &[Matcher],
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    postings_for_matcher_slice(ix, matchers)
}

/// `postings_for_matchers` assembles a single postings iterator against the index
/// based on the given matchers.
pub(super) fn postings_for_matcher_slice<'a>(
    ix: &'a Postings,
    ms: &[Matcher],
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if ms.is_empty() {
        return Ok(Cow::Borrowed(ix.all_postings()));
    }
    if ms.len() == 1 {
        let m = &ms[0];
        if m.label.is_empty() && m.label.is_empty() {
            return Ok(Cow::Borrowed(ix.all_postings()));
        }
    }

    let mut its: SmallVec<_, 4> = SmallVec::new();
    let mut not_its: SmallVec<Cow<PostingsBitmap>, 4> = SmallVec::new();

    let mut sorted_matchers: SmallVec<(&Matcher, bool, bool), 4> = SmallVec::new();
    // See which label must be non-empty.
    // Optimization for a case like {l=~".", l!="1"}.
    let mut label_must_be_set: AHashSet<&str> = AHashSet::with_capacity(ms.len());

    let mut has_subtracting_matchers = false;
    let mut has_intersecting_matchers = false;
    for m in ms {
        let matches_empty = m.matches("");
        if !matches_empty {
            label_must_be_set.insert(&m.label);
        }

        let is_subtracting = is_subtracting_matcher(m, &label_must_be_set);
        if is_subtracting {
            has_subtracting_matchers = true;
        } else {
            has_intersecting_matchers = true;
        }

        sorted_matchers.push((m, matches_empty, is_subtracting))
    }

    if has_subtracting_matchers && !has_intersecting_matchers {
        // If there's nothing to subtract from, add in everything and remove the not_its later.
        // We prefer to get all_postings so that the base of subtraction (i.e., all_postings)
        // doesn't include series that may be added to the index reader during this function call.
        its.push(Cow::Borrowed(ix.all_postings()));
    };

    // Sort matchers to have the intersecting matchers first.
    // This way the base for subtraction is smaller, and there is no chance that the set we subtract
    // from contains postings of series that didn't exist when we constructed the set we subtract by.
    sorted_matchers.sort_by(|i, j| -> Ordering {
        let is_i_subtracting = i.2;
        let is_j_subtracting = j.2;
        if !is_i_subtracting && is_j_subtracting {
            return Ordering::Less;
        }
        // sort by match cost
        let cost_i = i.0.cost();
        let cost_j = j.0.cost();
        cost_i.cmp(&cost_j)
    });

    for (m, matches_empty, _is_subtracting) in sorted_matchers {
        //let value = &m.value;
        let name = &m.label;

        if name.is_empty() && matches_empty {
            // We already handled the case at the top of the function,
            // and it is unexpected to get all postings again here.
            return Err(ValkeyError::Str(MISSING_FILTER));
        }

        let typ = m.op();
        let regex_value = m.regex_text().unwrap_or("");

        match (typ, regex_value) {
            // .* regexp matches any string: do nothing
            (MatchOp::RegexEqual, ".*") => continue,

            // .* regexp does not match any string: return empty
            (MatchOp::RegexNotEqual, ".*") => {
                return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
            }

            // .+ regexp matches any non-empty string
            (MatchOp::RegexEqual, ".+") => {
                // .+ regexp matches any non-empty string: get postings for all label values.
                let it = ix.postings_for_all_label_values(&m.label);
                its.push(Cow::Owned(it));
            }

            // .+ regexp does not match any non-empty string
            (MatchOp::RegexNotEqual, ".+") => {
                let it = ix.postings_for_all_label_values(&m.label);
                not_its.push(Cow::Owned(it));
            }
            _ if label_must_be_set.contains(name.as_str()) => {
                // If this matcher must be non-empty, we can be smarter.
                let is_not = matches!(typ, MatchOp::NotEqual | MatchOp::RegexNotEqual);
                match (is_not, matches_empty) {
                    // l!="foo"
                    (true, true) => {
                        // If the label can't be empty and is a Not and the inner matcher
                        // doesn't match empty, then subtract it out at the end.
                        let inverse = m.clone().inverse();
                        let it = ix.postings_for_matcher(&inverse);
                        not_its.push(it);
                    }
                    // l!=""
                    (true, false) => {
                        // If the label can't be empty and is a Not, but the inner matcher can
                        // be empty, we need to use inverse_postings_for_matcher.
                        let inverse = m.clone().inverse();
                        let it = inverse_postings_for_matcher(ix, &inverse);
                        if it.is_empty() {
                            return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                        }
                        its.push(it);
                    }
                    // l="a", l=~"a|b", etc.
                    _ => {
                        // Non-Not matcher, use normal postings_for_matcher.
                        let it = ix.postings_for_matcher(m);
                        if it.is_empty() {
                            return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
                        }
                        its.push(it);
                    }
                }
            }
            _ => {
                // l=""
                // If the matchers for a label name selects an empty value, it selects all
                // the series which don't have the label name set too. See:
                // https://github.com/prometheus/prometheus/issues/3575 and
                // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
                let it = inverse_postings_for_matcher(ix, m);

                not_its.push(it)
            }
        }
    }

    // sort by cardinality first to reduce the amount of work
    its.sort_by_key(|a| a.cardinality());

    let mut result = if its.is_empty() {
        ix.all_postings().clone()
    } else {
        intersection(its)
    };

    for not in not_its {
        result.andnot_inplace(&not)
    }

    Ok(Cow::Owned(result))
}

#[inline]
fn is_subtracting_matcher(m: &Matcher, label_must_be_set: &AHashSet<&str>) -> bool {
    if !label_must_be_set.contains(&m.label.as_str()) {
        return true;
    }
    matches!(m.op(), MatchOp::NotEqual | MatchOp::RegexNotEqual) && m.matches("")
}

fn inverse_postings_for_matcher<'a>(ix: &'a Postings, m: &Matcher) -> Cow<'a, PostingsBitmap> {
    match &m.matcher {
        PredicateMatch::NotEqual(pv) => handle_equal_match(ix, &m.label, pv),
        // If the matcher being inverted is ="", we just want all the values.
        PredicateMatch::Equal(PredicateValue::String(s)) if s.is_empty() => {
            Cow::Owned(ix.postings_for_all_label_values(&m.label))
        }
        // If the matcher being inverted is =~"", we just want all the values.
        PredicateMatch::RegexEqual(re) if matches!(re.regex.as_str(), "" | ".*") => {
            Cow::Owned(ix.postings_for_all_label_values(&m.label))
        }
        _ => {
            let mut state = m;
            let postings = ix.postings_for_label_matching(&m.label, &mut state, |s, state| {
                let valid = state.matches(s);
                !valid
            });
            Cow::Owned(postings)
        }
    }
}

fn intersection<'a, I>(its: I) -> PostingsBitmap
where
    I: IntoIterator<Item = Cow<'a, PostingsBitmap>>,
{
    let mut its = its.into_iter();
    if let Some(it) = its.next() {
        let mut result = it.into_owned();

        for it in its {
            if it.is_empty() {
                result.clear();
                return result;
            }

            result.and_inplace(&it);
        }

        result
    } else {
        PostingsBitmap::new()
    }
}
