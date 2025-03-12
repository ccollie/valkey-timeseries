use super::memory_postings::{handle_equal_match, MemoryPostings, PostingsBitmap, EMPTY_BITMAP};
use super::TimeSeriesIndex;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::time::current_time_millis;
use crate::error_consts::MISSING_FILTER;
use crate::labels::matchers::{
    MatchOp, Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
};
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesRef, TimeSeries, TimestampRange};
use ahash::AHashSet;
use blart::AsBytes;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::str;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub fn query_index(
    ctx: &Context,
    ix: &TimeSeriesIndex,
    matchers: &Matchers,
    range: Option<TimestampRange>,
) -> ValkeyResult<Vec<ValkeyString>> {
    let mut state = ();

    ix.with_postings(&mut state, move |inner, _| {
        match postings_for_matchers(ix, matchers) {
            Ok(postings) => {
                let keys = collect_series_keys(ctx, inner, postings.iter(), range);
                Ok(keys)
            }
            Err(e) => Err(e),
        }
    })
}

pub fn get_keys_by_matchers(
    ctx: &Context,
    ix: &TimeSeriesIndex,
    matchers: &[Matchers],
    range: Option<TimestampRange>,
) -> ValkeyResult<Vec<ValkeyString>> {
    if matchers.is_empty() {
        return Ok(Vec::new());
    }

    let mut state = ();

    ix.with_postings(&mut state, move |inner, _| {
        let first = postings_for_matchers_internal(inner, &matchers[0])?;
        if matchers.len() == 1 {
            let keys = collect_series_keys(ctx, inner, first.iter(), range);
            return Ok(keys);
        }
        // todo: use chili here ?
        let mut result = first.into_owned();
        for matcher in &matchers[1..] {
            let postings = postings_for_matchers_internal(inner, matcher)?;
            result.and_inplace(&postings);
        }
        let keys = collect_series_keys(ctx, inner, result.iter(), range);
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
    postings: &MemoryPostings,
    ids: impl Iterator<Item = SeriesRef>,
    date_range: Option<TimestampRange>,
) -> Vec<ValkeyString> {
    let mut keys = Vec::new();
    for key in ids.filter_map(|id| postings.get_key_by_id(id)) {
        if check_key_is_allowed_to_read(ctx, key.as_bytes()) {
            let real_key = ctx.create_string(key.as_bytes());
            keys.push(real_key);
        }
    }
    if let Some(date_range) = date_range {
        let now = Some(current_time_millis());
        keys.retain(|key| {
            let redis_key = ctx.open_key_writable(key);
            if let Ok(Some(series)) = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                let (start, end) = date_range.get_series_range(series, now, true);
                series.overlaps(start, end)
            } else {
                false
            }
        });
    }
    keys
}

fn check_key_is_allowed_to_read(_ctx: &Context, _key: &[u8]) -> bool {
    // todo: implement permission checking
    true
}

/// `postings_for_matchers` assembles a single postings iterator against the series index
/// based on the given matchers.
pub fn postings_for_matchers(
    ix: &TimeSeriesIndex,
    matchers: &Matchers,
) -> ValkeyResult<PostingsBitmap> {
    let mut state = ();
    ix.with_postings(&mut state, move |inner, _| {
        let postings = postings_for_matchers_internal(inner, matchers)?;
        #[cfg(test)]
        debug_bitmap("first", &postings);
        let res = postings.into_owned();
        Ok(res)
    })
}

pub(crate) fn postings_for_matchers_internal<'a>(
    ix: &'a MemoryPostings,
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
    ix: &'a MemoryPostings,
    matchers: &[Vec<Matcher>],
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if matchers.len() == 1 {
        let m = matchers
            .first()
            .expect("Out of bounds error running matchers");
        process_and_matchers(ix, m)
    } else {
        // todo: use chili here
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
    ix: &'a MemoryPostings,
    matchers: &[Matcher],
) -> ValkeyResult<Cow<'a, PostingsBitmap>> {
    if matchers.len() == 1 {
        let m = matchers
            .first()
            .expect("Out of bounds error running matchers");
        Ok(ix.postings_for_matcher(m))
    } else {
        postings_for_matcher_slice(ix, matchers)
    }
}

/// `postings_for_matchers` assembles a single postings iterator against the index
/// based on the given matchers.
fn postings_for_matcher_slice<'a>(
    ix: &'a MemoryPostings,
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
    let mut not_its: SmallVec<_, 4> = SmallVec::new();

    let mut has_subtracting_matchers = false;
    let mut has_intersecting_matchers = false;

    let mut sorted_matchers: SmallVec<(&Matcher, bool, bool), 4> = SmallVec::new();
    // See which label must be non-empty.
    // Optimization for case like {l=~".", l!="1"}.
    let mut label_must_be_set: AHashSet<&str> = AHashSet::with_capacity(ms.len());
    for m in ms {
        let matches_empty = m.matches("");
        if !matches_empty {
            label_must_be_set.insert(&m.label);
        }
        let is_subtracting = is_subtracting_matcher(m, &label_must_be_set);

        has_subtracting_matchers |= is_subtracting;
        has_intersecting_matchers |= !is_subtracting;

        sorted_matchers.push((m, matches_empty, is_subtracting))
    }

    if has_subtracting_matchers && !has_intersecting_matchers {
        // If there's nothing to subtract from, add in everything and remove the not_its later.
        // We prefer to get all_postings so that the base of subtraction (i.e. all_postings)
        // doesn't include series that may be added to the index reader during this function call.
        its.push(Cow::Borrowed(ix.all_postings()));
    };

    // Sort matchers to have the intersecting matchers first.
    // This way the base for subtraction is smaller and there is no chance that the set we subtract
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
        let typ = m.op();
        let regex_value = m.regex_text().unwrap_or("");

        if name.is_empty() && matches_empty {
            // We already handled the case at the top of the function,
            // and it is unexpected to get all postings again here.
            return Err(ValkeyError::Str(MISSING_FILTER));
        }

        if typ == MatchOp::RegexEqual && regex_value == ".*" {
            // .* regexp matches any string: do nothing.
            continue;
        }

        if typ == MatchOp::RegexNotEqual && regex_value == ".*" {
            return Ok(Cow::Borrowed(&*EMPTY_BITMAP));
        }

        if typ == MatchOp::RegexEqual && regex_value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values.
            let it = ix.postings_for_all_label_values(&m.label);
            its.push(Cow::Owned(it));
        } else if typ == MatchOp::RegexNotEqual && regex_value == ".+" {
            // .+ regexp matches any non-empty string: get postings for all label values and remove them.
            let it = ix.postings_for_all_label_values(name);
            not_its.push(Cow::Owned(it));
        } else if label_must_be_set.contains(name.as_str()) {
            // If this matcher must be non-empty, we can be smarter.
            let is_not = matches!(typ, MatchOp::NotEqual | MatchOp::RegexNotEqual);

            if is_not {
                let inverse = m.clone().inverse();

                // If the label can't be empty and is a Not, then subtract it out at the end.
                if matches_empty {
                    // l!="foo"
                    // If the label can't be empty and is a Not and the inner matcher
                    // doesn't match empty, then subtract it out at the end.
                    // NOTE: we resolve immediately here to avoid borrowing issue with inverse
                    let it = ix.postings_for_matcher(&inverse);
                    not_its.push(it);
                } else {
                    // l!=""
                    // If the label can't be empty and is a Not, but the inner matcher can
                    // be empty we need to use inverse_postings_for_matcher.
                    let it = inverse_postings_for_matcher(ix, &inverse);
                    its.push(it);
                }
            } else {
                // l="a", l=~"a|b", l=~"a.b", etc.
                // Non-Not matcher, use normal `postings_for_matcher`.
                let it = ix.postings_for_matcher(m);
                #[cfg(test)]
                debug_bitmap(&format!("ids for matcher: {}", &m), &it);
                its.push(it);
            }
        } else {
            // l=""
            // If the matchers for a label name selects an empty value, it selects all
            // the series which don't have the label name set too. See:
            // https://github.com/prometheus/prometheus/issues/3575 and
            // https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
            let it = inverse_postings_for_matcher(ix, m);

            #[cfg(test)]
            debug_bitmap(&format!("ids for matcher: {}", &m), &it);

            not_its.push(it)
        }
    }

    // sort by cardinality first to reduce the amount of work
    its.sort_by_key(|a| a.cardinality());

    let mut it = intersection(its);

    for not in not_its {
        #[cfg(test)]
        debug_bitmap("not-its", &not);
        it.andnot_inplace(&not)
    }

    Ok(Cow::Owned(it))
}

#[inline]
fn is_subtracting_matcher(m: &Matcher, label_must_be_set: &AHashSet<&str>) -> bool {
    if !label_must_be_set.contains(&m.label.as_str()) {
        return true;
    }
    matches!(m.op(), MatchOp::NotEqual | MatchOp::RegexNotEqual) && m.matches("")
}

fn inverse_postings_for_matcher<'a>(
    ix: &'a MemoryPostings,
    m: &Matcher,
) -> Cow<'a, PostingsBitmap> {
    match &m.matcher {
        PredicateMatch::NotEqual(pv) => handle_equal_match(ix, &m.label, pv),
        PredicateMatch::Equal(PredicateValue::String(s)) if s.is_empty() => {
            Cow::Owned(ix.postings_for_all_label_values(&m.label))
        }
        _ => {
            let op = m.op();
            if matches!(op, MatchOp::RegexEqual)
                && matches!(m.regex_text().unwrap_or(""), "" | ".*")
            {
                return Cow::Owned(ix.postings_for_all_label_values(&m.label));
            }
            let mut state = m;
            let postings =
                ix.postings_for_label_matching(&m.label, &mut state, |s, state| !state.matches(s));
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
        #[cfg(test)]
        debug_bitmap("first", &it);

        let mut result = it.into_owned();

        for it in its {
            #[cfg(test)]
            debug_bitmap("item", &it);

            result.and_inplace(&it);

            #[cfg(test)]
            debug_bitmap("and_inplace", &result);
        }

        #[cfg(test)]
        debug_bitmap("intersect", &result);

        result
    } else {
        PostingsBitmap::new()
    }
}

#[cfg(test)]
fn debug_bitmap(context: &str, bitmap: &PostingsBitmap) {
    let s = bitmap_to_string(bitmap);
    println!("{context}: {s}");
}

// for debugging purposes
#[cfg(test)]
fn bitmap_to_string(bitmap: &PostingsBitmap) -> String {
    let mut s = String::with_capacity((bitmap.cardinality() * 4) as usize);
    for (i, id) in bitmap.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{}", id));
    }
    s
}
