//! High-level label query APIs layered on top of index postings and selector queriers.
//!
//! This module provides label-oriented discovery features (value listing, fuzzy matching,
//! similarity ranking, cardinality-aware sorting, and bounded result shaping) that are
//! consumed by label commands.
//!
//! Compared with `querier.rs`, which focuses on selector-to-series resolution with ACL and
//! series materialization, this module is value-centric: it evaluates and ranks label values
//! using index primitives while reusing selector hints when available.
//!
//! A scoped query (selectors or a date range) reads the matching series through the batched
//! reader ([`for_each_series`]): only the label names or values are copied out under the
//! module lock, a batch at a time, and fuzzy scoring and cardinality lookups run once the
//! lock is released. On a fan-out share the lock is given back between batches; a caller on
//! the main thread already holds it and reads in one batch.

use super::{
    DEFAULT_SERIES_BATCH_SIZE, FuzzyFilter, GilSource, IndexKey, PostingsBitmap, SimilarityFilter,
    TimeSeriesIndex, for_each_series, get_db_index,
};
use crate::common::acl::check_metadata_permissions;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::error::{TsdbError, TsdbResult};
use crate::labels::filters::SeriesSelector;
use crate::series::index::key_buffer::KeyBuffer;
use crate::series::index::postings::Postings;
use crate::series::request_types::MetaDateRangeFilter;
use ahash::{AHashMap, AHashSet};
use itertools::Itertools;
use min_max_heap::MinMaxHeap;
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Display;
use std::ops::ControlFlow;
use valkey_module::ValkeyError;

const DEFAULT_LIMIT: usize = 100;

// todo: get from config
pub const SEARCH_RESULT_LIMIT_MAX: usize = 1000;

/// SortBy is a closed set of label search result sort keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SortBy {
    #[default]
    Value,
    Score,
    Cardinality,
}

impl Display for SortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortBy::Value => write!(f, "value"),
            SortBy::Score => write!(f, "score"),
            SortBy::Cardinality => write!(f, "cardinality"),
        }
    }
}

impl TryFrom<&str> for SortBy {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "value" => Ok(SortBy::Value),
            "score" => Ok(SortBy::Score),
            "cardinality" => Ok(SortBy::Cardinality),
            _ => Err(ValkeyError::Str(
                "TSDB: SORTBY must be value, score, or cardinality",
            )),
        }
    }
}

/// Ordering is a closed set of label search result orderings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchResultOrdering {
    /// Orders results ascending by Value.
    #[default]
    ValueAsc,
    /// Orders results descending by Value.
    ValueDesc,
    /// Orders results descending by Score, breaking ties ascending by Value.
    ScoreDesc,
    /// Orders results ascending by Cardinality, breaking ties ascending by Value.
    CardinalityAsc,
    /// Orders results descending by Cardinality, breaking ties ascending by Value.
    CardinalityDesc,
}

impl SearchResultOrdering {
    pub fn is_ascending(&self) -> bool {
        matches!(
            self,
            SearchResultOrdering::ValueAsc | SearchResultOrdering::CardinalityAsc
        )
    }

    pub fn is_descending(&self) -> bool {
        matches!(
            self,
            SearchResultOrdering::ValueDesc
                | SearchResultOrdering::ScoreDesc
                | SearchResultOrdering::CardinalityDesc
        )
    }

    pub fn sort_field(&self) -> SortBy {
        match self {
            SearchResultOrdering::ValueAsc | SearchResultOrdering::ValueDesc => SortBy::Value,
            SearchResultOrdering::ScoreDesc => SortBy::Score,
            SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc => {
                SortBy::Cardinality
            }
        }
    }
}

/// SelectHints specifies hints passed for data selections.
#[derive(Debug, Clone, Default)]
pub struct SelectHints {
    /// Series selectors to apply to series selection.
    pub selectors: Vec<SeriesSelector>,
    /// Optional date range filter to apply to series selection.
    pub date_range: Option<MetaDateRangeFilter>,
}

impl SelectHints {
    pub fn new(selectors: Vec<SeriesSelector>, date_range: Option<MetaDateRangeFilter>) -> Self {
        Self {
            selectors,
            date_range,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty() && self.date_range.is_none()
    }
}

pub const SEARCH_RESULT_DEFAULT_LIMIT: usize = 100;

/// SearchHints configures search operations with filtering and scoring.
pub struct SearchHints<'a> {
    /// Filter determines which values to include and their relevance scores.
    pub filter: Option<Box<dyn FuzzyFilter + 'a>>,

    /// Maximum number of results to return.
    pub limit: usize,

    /// Selects the ordering of results.
    pub order_by: SearchResultOrdering,

    pub include_meta: bool,
}

impl<'a> Default for SearchHints<'a> {
    fn default() -> Self {
        Self {
            filter: None,
            limit: SEARCH_RESULT_DEFAULT_LIMIT,
            order_by: SearchResultOrdering::default(),
            include_meta: false,
        }
    }
}

/// SearchResult represents a single search result with its relevance score.
#[derive(Debug, Clone, Default)]
pub struct LabelSearchResult {
    /// The label name or label value.
    pub value: String,

    /// Relevance score, with 1.0 being a perfect match.
    pub score: f64,

    /// Optional cardinality of the label name or label value, if known. This can be used for relevance ranking or filtering.
    pub cardinality: usize,
}

impl LabelSearchResult {
    pub fn new(value: String, score: f64) -> Self {
        Self {
            value,
            score,
            cardinality: 1,
        }
    }
}

impl PartialEq for LabelSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
            && self.score == other.score
            && self.cardinality == other.cardinality
    }
}

impl Eq for LabelSearchResult {}

/// `LabelQueryResult` is the paginated result of a label query. It pairs the matched
/// [`LabelSearchResult`] entries with a `has_more` flag that signals whether the underlying
/// data set contained more entries than the requested `limit`.
#[derive(Debug, Default)]
pub struct LabelQueryResult {
    /// The matched label search results, sorted and limited according to the query hints.
    pub results: Vec<LabelSearchResult>,
    /// `has_more` is `true` when the result set was truncated at `limit`, indicating
    /// that additional entries exist beyond what was returned.
    pub has_more: bool,
}

impl LabelQueryResult {
    pub fn new(results: Vec<LabelSearchResult>, has_more: bool) -> Self {
        Self { results, has_more }
    }

    /// Merges another query result into this one.
    ///
    /// Duplicate values are coalesced by summing cardinality and keeping the best score.
    /// The merged set is then sorted and limited according to `order_by` and `limit`.
    pub fn merge_from(
        &mut self,
        other: LabelQueryResult,
        order_by: SearchResultOrdering,
        limit: usize,
    ) {
        let mut merged: AHashMap<String, LabelSearchResult> =
            AHashMap::with_capacity(self.results.len() + other.results.len());

        for item in self.results.drain(..).chain(other.results) {
            match merged.get_mut(&item.value) {
                Some(existing) => {
                    existing.cardinality += item.cardinality;
                    if item.score > existing.score {
                        existing.score = item.score;
                    }
                }
                None => {
                    let key = item.value.clone();
                    merged.insert(key, item);
                }
            }
        }

        let merged_has_more = merged.len() > limit;

        let results = if merged_has_more {
            let comparator = compare_search_results(order_by);
            let mut items: Vec<_> = merged.into_values().collect();
            items.sort_by(comparator);
            items.truncate(limit);
            items
        } else {
            collect_limited_with_heap(merged.into_values(), order_by, limit).results
        };

        self.has_more = self.has_more || other.has_more || merged_has_more;
        self.results = results;
    }
}

impl std::ops::Deref for LabelQueryResult {
    type Target = Vec<LabelSearchResult>;

    fn deref(&self) -> &Self::Target {
        &self.results
    }
}

impl IntoIterator for LabelQueryResult {
    type Item = LabelSearchResult;
    type IntoIter = std::vec::IntoIter<LabelSearchResult>;

    fn into_iter(self) -> Self::IntoIter {
        self.results.into_iter()
    }
}

/// LabelQuerier defines the interface for querying label names and values with flexible filtering and ordering.
///
/// Each method takes the module lock from `src` as it needs it, so it must not be called
/// while holding the lock through a [`GilSource`] that releases it. Pass the
/// [`Context`](valkey_module::Context) on the main thread, and the request's `FanoutContext` on a fan-out share.
pub trait LabelQuerier {
    /// Returns a list of label names matching the given hints.
    fn get_label_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;

    /// Returns a list of label values for the given label name, matching the given hints.
    fn get_label_values<S: GilSource + ?Sized>(
        &self,
        src: &S,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;

    /// Returns a list of metric names (`__name__` values) matching the given hints.
    fn get_metric_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;
}

type ComparisonFn = fn(&LabelSearchResult, &LabelSearchResult) -> Ordering;

/// `compare_search_results` returns the total-order comparison function for the
/// given Ordering. For `OrderByValueAsc` and `OrderByValueDesc` the order is on
/// value alone. For `OrderByScoreDesc` the order is (score desc, value asc),
/// which is a total order and defines the position at which a duplicate value
/// is first emitted by the streaming merge.
fn compare_search_results(order: SearchResultOrdering) -> ComparisonFn {
    fn value_asc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        a.value.cmp(&b.value)
    }

    fn value_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        b.value.cmp(&a.value)
    }

    fn score_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        match b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    fn cardinality_asc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        match a.cardinality.cmp(&b.cardinality) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    fn cardinality_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> Ordering {
        match b.cardinality.cmp(&a.cardinality) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    match order {
        SearchResultOrdering::ValueDesc => value_desc,
        SearchResultOrdering::ScoreDesc => score_desc,
        SearchResultOrdering::CardinalityAsc => cardinality_asc,
        SearchResultOrdering::CardinalityDesc => cardinality_desc,
        SearchResultOrdering::ValueAsc => value_asc,
    }
}

fn normalize_limit(limit: usize) -> usize {
    if limit == 0 {
        SEARCH_RESULT_DEFAULT_LIMIT
    } else {
        limit
    }
    .min(SEARCH_RESULT_LIMIT_MAX)
}

/// `apply_search_hints` sorts and limits a slice of values according to hints,
/// returning a [`LabelQueryResult`] that includes a `has_more` flag indicating
/// whether any entries were truncated at the limit.
pub(crate) fn apply_search_hints(
    mut values: Vec<LabelSearchResult>,
    hints: &SearchHints,
) -> LabelQueryResult {
    let limit = normalize_limit(hints.limit);

    let comparator = compare_search_results(hints.order_by);
    values.sort_by(comparator);

    let has_more = limit > 0 && values.len() > limit;
    if has_more {
        values.truncate(limit);
    }
    LabelQueryResult::new(values, has_more)
}

fn with_search_filter<R>(hints: &SearchHints, f: impl FnOnce(&dyn FuzzyFilter) -> R) -> R {
    if let Some(filter) = &hints.filter {
        return f(filter.as_ref());
    }
    let default_filter: Box<dyn FuzzyFilter> = Box::new(SimilarityFilter::default());
    f(default_filter.as_ref())
}

fn can_use_unscoped_scan(select_hints: Option<&SelectHints>) -> bool {
    select_hints.map(|h| h.is_empty()).unwrap_or(true)
}

/// `collect_limited` collects up to `limit + 1` items from `iter`, then truncates to `limit`
/// and sets `has_more` if the extra item was present. When `limit` is 0, all items are
/// collected and `has_more` is always `false`.
fn collect_limited(
    iter: impl Iterator<Item = LabelSearchResult>,
    limit: usize,
) -> LabelQueryResult {
    let limit = if limit == 0 { DEFAULT_LIMIT } else { limit };

    let mut items: Vec<LabelSearchResult> = iter.take(limit + 1).collect();
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    LabelQueryResult::new(items, has_more)
}

/// `collect_limited_with_heap` keeps only the best `limit` results in memory for non-value
/// orderings where we cannot stream directly from the index order.
fn collect_limited_with_heap(
    iter: impl Iterator<Item = LabelSearchResult>,
    order: SearchResultOrdering,
    limit: usize,
) -> LabelQueryResult {
    let cmp = compare_search_results(order);

    let mut items = if order.is_ascending() {
        iter.k_smallest_relaxed_by(limit + 1, &cmp)
    } else {
        iter.k_largest_relaxed_by(limit + 1, &cmp)
    }
    .collect::<Vec<_>>();

    items.sort_by(cmp);

    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }

    LabelQueryResult::new(items, has_more)
}

/// Label-index entries a name scan reads under one hold of the postings read lock.
///
/// Each entry is a key comparison and a bitmap cardinality, so a hold this size is a few
/// microseconds: short enough that an index writer waiting on the lock is not starved by an
/// unscoped scan over a large index, long enough that re-taking the lock is noise.
const LABEL_SCAN_ENTRIES_PER_HOLD: usize = 1024;

/// Get label names in ascending order, applying a fuzzy filter if provided.
fn iterate_label_names_asc<F>(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
    f: F,
) -> bool
where
    F: FnMut(&str, f64, usize),
{
    iterate_label_names_asc_in_holds(index, filter, limit, LABEL_SCAN_ENTRIES_PER_HOLD, f)
}

/// [`iterate_label_names_asc`] reading at most `entries_per_hold` entries per lock hold.
fn iterate_label_names_asc_in_holds<F>(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
    entries_per_hold: usize,
    mut f: F,
) -> bool
where
    F: FnMut(&str, f64, usize),
{
    let entries_per_hold = entries_per_hold.max(1);
    let limit = limit.unwrap_or(SEARCH_RESULT_LIMIT_MAX);
    // The scan resumes after this key: each restart, whether to skip a rejected name or to
    // re-take the lock, is a fresh range from here. "!" sorts before every valid label name.
    let mut resume_after: Vec<u8> = b"!".to_vec();
    // The name of the entries being read, and, if it was accepted, its score and the
    // cardinality summed so far. Owned, so a name whose entries straddle a release keeps
    // accumulating after the lock is taken again.
    let mut curr_name = String::new();
    let mut pending: Option<(f64, usize)> = None;
    let mut has_more = false;
    let mut count: usize = 0;

    'hold: loop {
        // Note: releasing the lock costs the scan its point-in-time view: an entry added before the resume
        // point after it was passed is not seen, and a label name whose entries straddle a release
        // is counted from both holds. `f` gets each name as it is completed; the `&str` is valid only
        // for that call.
        let postings = index.get_postings();
        let mut read_this_hold = 0usize;

        'range: loop {
            let start_bound: Bound<&[u8]> = Bound::Excluded(resume_after.as_slice());

            for (key, map) in postings
                .label_index
                .range::<[u8], _>((start_bound, Bound::<&[u8]>::Unbounded))
            {
                read_this_hold += 1;
                let Some((key_name, _)) = key.split() else {
                    // Defensive: a malformed entry is passed over, never re-visited.
                    if read_this_hold >= entries_per_hold {
                        // Malformed entries still count toward the lock budget.
                        resume_after.clear();
                        resume_after.extend_from_slice(key.as_ref());
                        continue 'hold;
                    }
                    continue;
                };

                if curr_name != key_name {
                    if let Some((score, cardinality)) = pending.take() {
                        if count == limit {
                            has_more = true;
                            break 'hold;
                        }
                        f(&curr_name, score, cardinality);
                        count += 1;
                    }
                    curr_name.clear();
                    curr_name.push_str(key_name);

                    let (accepted, score) = filter.accept(key_name);
                    if !accepted {
                        // Jump past every value of this name: "name?" sorts after every
                        // "name=value" key, since `?` > `=` in byte order.
                        resume_after.clear();
                        resume_after.extend_from_slice(key_name.as_bytes());
                        resume_after.push(b'?');
                        if read_this_hold >= entries_per_hold {
                            continue 'hold;
                        }
                        continue 'range;
                    }
                    pending = Some((score, map.cardinality() as usize));
                } else if let Some((_, cardinality)) = pending.as_mut() {
                    *cardinality += map.cardinality() as usize;
                }

                if read_this_hold >= entries_per_hold {
                    // Give the lock back and resume after this entry.
                    resume_after.clear();
                    resume_after.extend_from_slice(key.as_ref());
                    continue 'hold;
                }
            }
            // The range ran out: the scan is done.
            break 'hold;
        }
    }

    if let Some((score, cardinality)) = pending
        && count < limit
    {
        f(&curr_name, score, cardinality);
    }

    has_more
}

/// Get label names in ascending order, applying a fuzzy filter if provided. Takes the postings
/// read lock itself and gives it back periodically; see [`iterate_label_names_asc`].
fn get_label_names_asc(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
) -> LabelQueryResult {
    let mut results: Vec<LabelSearchResult> = Vec::with_capacity(DEFAULT_LIMIT);
    let has_more = iterate_label_names_asc(index, filter, limit, |key_name, score, cardinality| {
        results.push(LabelSearchResult {
            value: key_name.to_owned(),
            score,
            cardinality,
        });
    });

    LabelQueryResult::new(results, has_more)
}

/// Get label names in descending order, applying a fuzzy filter if provided. Efficiently skips label names
/// that don't match the filter by using the index ordering to jump past them.
///
/// Like [`iterate_label_names_asc`], it takes the postings read lock itself and gives it back
/// every [`LABEL_SCAN_ENTRIES_PER_HOLD`] entries, so it must not be called with the postings
/// lock held, and it is not a point-in-time view.
fn get_label_names_desc(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
) -> LabelQueryResult {
    get_label_names_desc_in_holds(index, filter, limit, LABEL_SCAN_ENTRIES_PER_HOLD)
}

/// [`get_label_names_desc`] reading at most `entries_per_hold` entries per lock hold.
fn get_label_names_desc_in_holds(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    limit: Option<usize>,
    entries_per_hold: usize,
) -> LabelQueryResult {
    let entries_per_hold = entries_per_hold.max(1);
    let mut results: Vec<LabelSearchResult> = Vec::with_capacity(DEFAULT_LIMIT);
    let mut current_result: Option<LabelSearchResult> = None;
    let mut has_more = false;
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    // The scan resumes below this key: each restart, whether to skip a rejected name or to
    // re-take the lock, is a fresh range under it. `None` means from the top of the index.
    let mut resume_below: Option<Vec<u8>> = None;
    // The name of the entries being read. Owned, so a name whose entries straddle a release
    // keeps accumulating into `current_result` after the lock is taken again.
    let mut curr_name = String::new();

    'hold: loop {
        let postings = index.get_postings();
        let mut read_this_hold = 0usize;

        'range: loop {
            let end_bound: Bound<&[u8]> = match &resume_below {
                Some(buf) => Bound::Excluded(buf.as_slice()),
                None => Bound::Unbounded,
            };

            for (key, map) in postings
                .label_index
                .range::<[u8], _>((Bound::<&[u8]>::Unbounded, end_bound))
                .rev()
            {
                read_this_hold += 1;
                let Some((key_name, _)) = key.split() else {
                    if read_this_hold >= entries_per_hold {
                        // Malformed entries still count toward the lock budget.
                        let next = resume_below.get_or_insert_with(Vec::new);
                        next.clear();
                        next.extend_from_slice(key.as_ref());
                        continue 'hold;
                    }
                    continue;
                };

                if curr_name != key_name {
                    if let Some(stored) = current_result.take() {
                        if results.len() == limit {
                            has_more = true;
                            break 'hold;
                        }
                        results.push(stored);
                    }
                    curr_name.clear();
                    curr_name.push_str(key_name);

                    let (accepted, score) = filter.accept(key_name);
                    if !accepted {
                        // Jump past every value of this name: each is "name=value", so all of
                        // them are at or above "name=". The bound must be "name=", not "name":
                        // a longer name such as "name2" sorts between the two ("2" < "=") and
                        // would be skipped with it.
                        let next = resume_below.get_or_insert_with(Vec::new);
                        next.clear();
                        next.extend_from_slice(key_name.as_bytes());
                        next.push(b'=');
                        if read_this_hold >= entries_per_hold {
                            continue 'hold;
                        }
                        continue 'range;
                    }

                    current_result = Some(LabelSearchResult {
                        value: key_name.to_owned(),
                        score,
                        cardinality: map.cardinality() as usize,
                    });
                } else if let Some(active) = current_result.as_mut() {
                    active.cardinality += map.cardinality() as usize;
                }

                if read_this_hold >= entries_per_hold {
                    // Give the lock back and resume below this entry.
                    let next = resume_below.get_or_insert_with(Vec::new);
                    next.clear();
                    next.extend_from_slice(key.as_ref());
                    continue 'hold;
                }
            }
            // The range ran out: the scan is done.
            break 'hold;
        }
    }

    if let Some(stored) = current_result
        && results.len() < limit
    {
        results.push(stored);
    }

    LabelQueryResult::new(results, has_more)
}

/// `HeapResultHolder` is a helper struct for use in the binary heap below.
///
/// It stores the full [`SearchResultOrdering`] (not just [`SortBy`]) because the
/// correct value tie-break direction depends on which drain direction will be used:
///
/// * `drain_asc()` (ascending orderings) yields min → max, so items that should appear
///   first in the output must compare **less** — value-asc tie-break is correct.
/// * `drain_desc()` (descending orderings) yields max → min, so items that should appear
///   first must compare **greater** — value-desc tie-break in `Ord` produces value-asc
///   output after the drain.
///
/// This means the `Ord` implementation can encode the complete desired output order
/// directly, and no post-drain sort is required.
///
/// The value is owned: the name scan feeding the heap gives the postings lock back as it goes,
/// so nothing borrowed from the postings outlives a hold. Only names that enter the heap are
/// copied.
#[derive(Debug, Clone)]
struct HeapResultHolder {
    value: String,
    score: f64,
    cardinality: usize,
    order: SearchResultOrdering,
}

impl HeapResultHolder {
    fn new(value: &str, score: f64, cardinality: usize, order: SearchResultOrdering) -> Self {
        Self {
            value: value.to_owned(),
            score,
            cardinality,
            order,
        }
    }

    fn compare_parts(
        order: SearchResultOrdering,
        a_value: &str,
        a_score: f64,
        a_cardinality: usize,
        b_value: &str,
        b_score: f64,
        b_cardinality: usize,
    ) -> Ordering {
        match order {
            SearchResultOrdering::ValueAsc => a_value.cmp(b_value),
            SearchResultOrdering::ValueDesc => b_value.cmp(a_value),
            SearchResultOrdering::ScoreDesc => {
                match b_score.partial_cmp(&a_score).unwrap_or(Ordering::Equal) {
                    Ordering::Equal => a_value.cmp(b_value),
                    other => other,
                }
            }
            SearchResultOrdering::CardinalityAsc => match a_cardinality.cmp(&b_cardinality) {
                Ordering::Equal => a_value.cmp(b_value),
                other => other,
            },
            SearchResultOrdering::CardinalityDesc => match b_cardinality.cmp(&a_cardinality) {
                Ordering::Equal => a_value.cmp(b_value),
                other => other,
            },
        }
    }

    fn compare_candidate(
        &self,
        candidate_value: &str,
        candidate_score: f64,
        candidate_cardinality: usize,
    ) -> Ordering {
        Self::compare_parts(
            self.order,
            candidate_value,
            candidate_score,
            candidate_cardinality,
            &self.value,
            self.score,
            self.cardinality,
        )
    }
}

impl PartialOrd<Self> for HeapResultHolder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapResultHolder {
    fn cmp(&self, other: &Self) -> Ordering {
        Self::compare_parts(
            self.order,
            &self.value,
            self.score,
            self.cardinality,
            &other.value,
            other.score,
            other.cardinality,
        )
    }
}

impl PartialEq for HeapResultHolder {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
            && self.score == other.score
            && self.cardinality == other.cardinality
            && self.order == other.order
    }
}

impl Eq for HeapResultHolder {}

fn get_label_names_top_k(
    index: &TimeSeriesIndex,
    filter: &dyn FuzzyFilter,
    order: SearchResultOrdering,
    limit: usize,
) -> LabelQueryResult {
    let limit = if limit == 0 { DEFAULT_LIMIT } else { limit };
    let mut heap: MinMaxHeap<HeapResultHolder> = MinMaxHeap::with_capacity(limit + 2);
    let mut has_more = false;
    let _ = iterate_label_names_asc(index, filter, None, |key_name, score, cardinality| {
        if heap.len() == limit {
            let Some(worst) = heap.peek_max() else {
                return;
            };

            has_more = true;

            let candidate_cmp = worst.compare_candidate(key_name, score, cardinality);
            if candidate_cmp != Ordering::Less {
                return;
            }

            heap.pop_max();
        }

        heap.push(HeapResultHolder::new(key_name, score, cardinality, order));
    });

    let results: Vec<LabelSearchResult> = heap
        .drain_asc()
        .map(|h| LabelSearchResult {
            value: h.value,
            score: h.score,
            cardinality: h.cardinality,
        })
        .collect();

    LabelQueryResult::new(results, has_more)
}

/// `collect_unscoped_label_names` attempts to satisfy a label name search by scanning the label index directly
///
/// Takes the index rather than locked postings so that each scan owns its postings read lock
/// and gives it back periodically, so writers are not starved (see
/// [`iterate_label_names_asc`]). No lock is held here: the scans take it themselves, and a read
/// lock taken again behind a waiting writer can deadlock.
fn collect_unscoped_label_names(index: &TimeSeriesIndex, hints: &SearchHints) -> LabelQueryResult {
    let limit = normalize_limit(hints.limit);

    with_search_filter(hints, |filter| match hints.order_by {
        SearchResultOrdering::ValueAsc => get_label_names_asc(index, filter, Some(hints.limit)),
        SearchResultOrdering::ValueDesc => get_label_names_desc(index, filter, Some(hints.limit)),
        SearchResultOrdering::ScoreDesc
        | SearchResultOrdering::CardinalityAsc
        | SearchResultOrdering::CardinalityDesc => {
            get_label_names_top_k(index, filter, hints.order_by, limit)
        }
    })
}

fn collect_unscoped_label_values(
    postings: &Postings,
    label_name: &str,
    hints: &SearchHints,
) -> LabelQueryResult {
    let limit = normalize_limit(hints.limit);
    let prefix = KeyBuffer::for_prefix(label_name);

    with_search_filter(hints, |filter| {
        let check = |(entry, map): (&IndexKey, &PostingsBitmap)| {
            if map.is_empty() {
                return None;
            }
            let (_, value) = entry.split()?;
            let (accepted, score) = filter.accept(value);
            let cardinality = map.cardinality() as usize;
            accepted.then(|| LabelSearchResult {
                value: value.to_owned(),
                score,
                cardinality,
            })
        };

        match hints.order_by {
            SearchResultOrdering::ValueAsc => collect_limited(
                postings
                    .label_index
                    .prefix(prefix.as_bytes())
                    .filter_map(check),
                limit,
            ),
            SearchResultOrdering::ValueDesc => collect_limited(
                postings
                    .label_index
                    .prefix(prefix.as_bytes())
                    .rev()
                    .filter_map(check),
                limit,
            ),
            SearchResultOrdering::ScoreDesc
            | SearchResultOrdering::CardinalityAsc
            | SearchResultOrdering::CardinalityDesc => collect_limited_with_heap(
                postings
                    .label_index
                    .prefix(prefix.as_bytes())
                    .filter_map(check),
                hints.order_by,
                limit,
            ),
        }
    })
}

/// `get_label_cardinality` returns the cardinality of a label name by looking it up in the label index.
fn get_label_cardinality(postings: &Postings, label_name: &str) -> usize {
    let prefix = KeyBuffer::for_prefix(label_name);
    postings
        .label_index
        .prefix(prefix.as_bytes())
        .map(|(_entry, map)| map.cardinality())
        .sum::<u64>() as usize
}

fn collect_label_names<S: GilSource + ?Sized>(
    src: &S,
    select_hints: Option<&SelectHints>,
    hints: &SearchHints,
) -> TsdbResult<LabelQueryResult> {
    if can_use_unscoped_scan(select_hints) {
        // The index alone answers this: no series is opened, so no module lock is taken.
        let index = get_db_index(src.db());
        return Ok(collect_unscoped_label_names(&index, hints));
    }

    // At this point we know we have non-empty selectors or a date range.
    let default_hints = SelectHints::default();
    let opts = select_hints.unwrap_or(&default_hints);

    // Under the lock, only the distinct names are copied out: a hash lookup per label. The
    // fuzzy filter and the cardinality lookups wait until the read is over.
    //
    // Still unbounded: every matched series is scanned, `names` holds every distinct name in
    // the match, and on the main thread (`&Context`) the whole scan is one lock hold. The
    // batched reader bounds only a fan-out share's lock holds, not the scan or the set.
    let mut names: AHashSet<String> = AHashSet::with_capacity(DEFAULT_LIMIT);
    for_each_series(
        src,
        &opts.selectors,
        opts.date_range,
        DEFAULT_SERIES_BATCH_SIZE,
        |_ctx, ts, _key| {
            for label in ts.labels.iter() {
                // Not `names.insert(label.name.to_string())`: that allocates for every
                // label, seen or not.
                if !names.contains(label.name) {
                    names.insert(label.name.to_string());
                }
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;

    let need_cardinality = matches!(
        hints.order_by,
        SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc
    ) || hints.include_meta;

    // Taken only now: the batched read takes the postings lock itself, for writing when it
    // has a stale id to record, and `RwLock` is not reentrant. A name's cardinality is its
    // database-wide one, not the selection's.
    let index = need_cardinality.then(|| get_db_index(src.db()));
    let postings = index.as_ref().map(|index| index.get_postings());

    Ok(with_search_filter(hints, |filter| {
        let mut result = Vec::with_capacity(names.len());
        for name in names {
            let (accepted, score) = filter.accept(&name);
            if !accepted {
                continue;
            }
            let cardinality = match postings.as_ref() {
                Some(postings) => match get_label_cardinality(postings, &name) {
                    0 => continue,
                    cardinality => cardinality,
                },
                None => 0,
            };
            result.push(LabelSearchResult {
                value: name,
                score,
                cardinality,
            });
        }
        apply_search_hints(result, hints)
    }))
}

fn collect_label_values<S: GilSource + ?Sized>(
    src: &S,
    label_name: &str,
    select_hints: Option<&SelectHints>,
    search_hints: &SearchHints,
) -> TsdbResult<LabelQueryResult> {
    if label_name.is_empty() {
        return Ok(LabelQueryResult::default());
    }

    if can_use_unscoped_scan(select_hints) {
        // The index alone answers this: no series is opened, so no module lock is taken.
        let index = get_db_index(src.db());
        let postings = index.get_postings();
        return Ok(collect_unscoped_label_values(
            &postings,
            label_name,
            search_hints,
        ));
    }

    // At this point we know we have non-empty selectors or a date range.
    let default_hints = SelectHints::default();
    let opts = select_hints.unwrap_or(&default_hints);

    // Under the lock, only each distinct value and how many matched series carry it are
    // copied out. The filter then runs once per distinct value, after the read.
    //
    // Still unbounded, as in `collect_label_names`: `counts` holds every distinct value in
    // the match, and a main-thread scan is one lock hold.
    let mut counts: AHashMap<String, usize> = AHashMap::with_capacity(DEFAULT_LIMIT);
    for_each_series(
        src,
        &opts.selectors,
        opts.date_range,
        DEFAULT_SERIES_BATCH_SIZE,
        |_ctx, ts, _key| {
            if let Some(label) = ts.get_label(label_name) {
                match counts.get_mut(label.value) {
                    Some(count) => *count += 1,
                    None => {
                        counts.insert(label.value.to_string(), 1);
                    }
                }
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;

    let need_cardinality = matches!(
        search_hints.order_by,
        SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc
    ) || search_hints.include_meta;

    Ok(with_search_filter(search_hints, |filter| {
        let mut result = Vec::with_capacity(counts.len());
        for (value, count) in counts {
            let (accepted, score) = filter.accept(&value);
            if accepted {
                result.push(LabelSearchResult {
                    value,
                    score,
                    cardinality: if need_cardinality { count } else { 0 },
                });
            }
        }
        apply_search_hints(result, search_hints)
    }))
}

/// The coarse gate on label discovery, checked under one short lock hold: on a fan-out
/// share the caller's ACL identity is only installed while the lock is held.
fn validate_permissions<S: GilSource + ?Sized>(src: &S) -> TsdbResult<()> {
    let ctx = src.lock()?;
    check_metadata_permissions(&ctx).map_err(|_| {
        TsdbError::PermissionsError("insufficient permissions to query labels".to_string())
    })
}

#[derive(Default)]
pub struct DefaultLabelQuerier;

impl LabelQuerier for DefaultLabelQuerier {
    fn get_label_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        validate_permissions(src)?;
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_names(src, select_hints.as_ref(), hints)
    }

    fn get_label_values<S: GilSource + ?Sized>(
        &self,
        src: &S,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        validate_permissions(src)?;
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(src, name, select_hints.as_ref(), hints)
    }

    fn get_metric_names<S: GilSource + ?Sized>(
        &self,
        src: &S,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        validate_permissions(src)?;
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(src, METRIC_NAME_LABEL, select_hints.as_ref(), hints)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FuzzyFilter, LabelQueryResult, LabelSearchResult, Postings, SearchHints,
        SearchResultOrdering, SimilarityFilter, TimeSeriesIndex, collect_limited_with_heap,
        collect_unscoped_label_names, collect_unscoped_label_values, get_label_names_desc_in_holds,
        iterate_label_names_asc_in_holds,
    };
    use crate::series::index::FuzzyAlgorithm;
    use std::sync::RwLock;

    /// An index holding `postings`, for the collectors that take the index and lock it
    /// themselves.
    fn index_of(postings: Postings) -> TimeSeriesIndex {
        TimeSeriesIndex {
            inner: RwLock::new(postings),
        }
    }

    struct StaticScoreFilter;

    impl FuzzyFilter for StaticScoreFilter {
        fn accept(&self, value: &str) -> (bool, f64) {
            match value {
                "us-east" => (true, 0.9),
                "us-west" => (true, 0.9),
                "eu-west" => (true, 0.4),
                _ => (false, 0.0),
            }
        }
    }

    struct StaticNameScoreFilter;

    impl FuzzyFilter for StaticNameScoreFilter {
        fn accept(&self, value: &str) -> (bool, f64) {
            match value {
                "alpha" => (true, 0.9),
                "beta" => (true, 0.9),
                "gamma" => (true, 0.4),
                _ => (false, 0.0),
            }
        }
    }

    fn build_label_values_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "region", "us-east");
        postings.add_posting_for_label_value(2, "region", "us-west");
        postings.add_posting_for_label_value(3, "region", "eu-west");
        // Distractor label to verify prefix scoping.
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    fn values(result: LabelQueryResult) -> Vec<String> {
        result.into_iter().map(|r| r.value).collect()
    }

    #[test]
    fn similarity_filter_defaults_to_case_sensitive() {
        let filter = SimilarityFilter::new("node", FuzzyAlgorithm::JaroWinkler, 1.0);
        let (accepted, score) = filter.accept("node");
        assert!(accepted);
        assert_eq!(1.0, score);
    }

    #[test]
    fn similarity_filter_case_sensitive_rejects_case_mismatch() {
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "NoDe",
            FuzzyAlgorithm::JaroWinkler,
            1.0,
            true,
        );

        let (accepted, score) = filter.accept("node");
        assert!(!accepted);
        assert!(score < 1.0);
    }

    #[test]
    fn similarity_filter_case_insensitive_accepts_case_mismatch() {
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "NoDe",
            FuzzyAlgorithm::JaroWinkler,
            1.0,
            false,
        );

        let (accepted, score) = filter.accept("node");
        assert!(accepted);
        assert_eq!(1.0, score);
    }

    #[test]
    fn collect_unscoped_label_values_orders_ascending() {
        let postings = build_label_values_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&postings, "region", &hints);

        assert!(!result.has_more);
        assert_eq!(values(result), vec!["eu-west", "us-east", "us-west"]);
    }

    #[test]
    fn collect_unscoped_label_values_orders_descending() {
        let postings = build_label_values_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&postings, "region", &hints);

        assert!(!result.has_more);
        assert_eq!(values(result), vec!["us-west", "us-east", "eu-west"]);
    }

    fn build_label_names_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "alpha", "alpha");
        postings.add_posting_for_label_value(2, "beta", "beta");
        postings.add_posting_for_label_value(3, "gamma", "gamma");
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    fn build_label_names_metric_index_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "alpha", "a");
        postings.add_posting_for_label_value(2, "beta", "b");
        postings.add_posting_for_label_value(3, "gamma", "g");
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    #[test]
    fn collect_unscoped_label_names_orders_ascending() {
        let postings = build_label_names_metric_index_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["alpha", "beta", "gamma", "service"]);
    }

    #[test]
    fn collect_unscoped_label_names_orders_descending() {
        let postings = build_label_names_metric_index_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["service", "gamma", "beta", "alpha"]);
    }

    #[test]
    fn collect_unscoped_label_values_sets_has_more_when_truncated() {
        let postings = build_label_values_postings();
        // limit of 2 when there are 3 region values → has_more must be true
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::ValueAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&postings, "region", &hints);

        assert!(result.has_more);
        assert_eq!(result.results.len(), 2);
    }

    fn build_label_names_cardinality_postings() -> Postings {
        let mut postings = Postings::default();
        // label 'b' -> cardinality 1
        postings.add_posting_for_label_value(1, "beta", "b");
        // label 'a' -> cardinality 2
        postings.add_posting_for_label_value(2, "alpha", "a");
        postings.add_posting_for_label_value(3, "alpha", "a");
        // label 'c' -> cardinality 3
        postings.add_posting_for_label_value(4, "gamma", "c");
        postings.add_posting_for_label_value(5, "gamma", "c");
        postings.add_posting_for_label_value(6, "gamma", "c");
        postings
    }

    fn build_label_values_cardinality_postings() -> Postings {
        let mut postings = Postings::default();
        // us-east -> cardinality 1
        postings.add_posting_for_label_value(1, "region", "us-east");
        // us-west -> cardinality 2
        postings.add_posting_for_label_value(2, "region", "us-west");
        postings.add_posting_for_label_value(3, "region", "us-west");
        // eu-west -> cardinality 3
        postings.add_posting_for_label_value(4, "region", "eu-west");
        postings.add_posting_for_label_value(5, "region", "eu-west");
        postings.add_posting_for_label_value(6, "region", "eu-west");
        postings
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_orders_ascending() {
        let postings = build_label_names_cardinality_postings();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::CardinalityAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        // cardinalities: beta=1, alpha=2, gamma=3 -> ascending by cardinality
        assert_eq!(values, vec!["beta", "alpha", "gamma"]);
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_sets_has_more_when_truncated() {
        let postings = build_label_names_cardinality_postings();
        // limit of 2 when there are 3 label names -> has_more must be true
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::CardinalityAsc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(result.has_more);
        assert_eq!(result.results.len(), 2);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["beta", "alpha"]);
    }

    #[test]
    fn collect_unscoped_label_names_score_orders_descending_with_ties() {
        let postings = build_label_names_postings();
        let hints = SearchHints {
            filter: Some(Box::new(StaticNameScoreFilter)),
            limit: 2,
            order_by: SearchResultOrdering::ScoreDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(result.has_more);
        assert_eq!(values(result), vec!["alpha", "beta"]);
    }

    #[test]
    fn collect_unscoped_label_names_score_truncation_keeps_best_tie() {
        let postings = build_label_names_postings();
        let hints = SearchHints {
            filter: Some(Box::new(StaticNameScoreFilter)),
            limit: 1,
            order_by: SearchResultOrdering::ScoreDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(result.has_more);
        assert_eq!(values(result), vec!["alpha"]);
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_desc_replaces_worst_candidate() {
        let postings = build_label_names_cardinality_postings();
        let hints = SearchHints {
            filter: None,
            limit: 1,
            order_by: SearchResultOrdering::CardinalityDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_names(&index_of(postings), &hints);

        assert!(result.has_more);
        assert_eq!(values(result), vec!["gamma"]);
    }

    #[test]
    fn collect_unscoped_label_values_score_orders_descending_with_ties() {
        let postings = build_label_values_postings();
        let hints = SearchHints {
            filter: Some(Box::new(StaticScoreFilter)),
            limit: 2,
            order_by: SearchResultOrdering::ScoreDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&postings, "region", &hints);

        assert!(result.has_more);
        assert_eq!(values(result), vec!["us-east", "us-west"]);
    }

    #[test]
    fn collect_unscoped_label_values_cardinality_orders_descending_with_limit() {
        let postings = build_label_values_cardinality_postings();
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::CardinalityDesc,
            include_meta: false,
        };

        let result = collect_unscoped_label_values(&postings, "region", &hints);

        assert!(result.has_more);
        assert_eq!(values(result), vec!["eu-west", "us-west"]);
    }

    #[test]
    fn jaro_winkler_transposition_matches() {
        // 'metirc' is a transposition of 'metric'
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "metirc",
            FuzzyAlgorithm::JaroWinkler,
            0.5,
            false,
        );
        let (accepted, score) = filter.accept("metric");
        assert!((0.0..=1.0).contains(&score));
        assert!(
            accepted,
            "Expected 'metric' to match 'metirc' with JaroWinkler (score={})",
            score
        );
    }

    #[test]
    fn subsequence_basic_match() {
        let filter = SimilarityFilter::new_with_case_sensitivity(
            "nde",
            FuzzyAlgorithm::Subsequence,
            0.3,
            false,
        );
        let (accepted, score) = filter.accept("node");
        assert!((0.0..=1.0).contains(&score));
        assert!(
            accepted,
            "Expected 'node' to match subsequence 'nde' (score={})",
            score
        );
    }

    #[test]
    fn label_query_result_merge_from_coalesces_duplicates() {
        let mut left = LabelQueryResult::new(
            vec![
                LabelSearchResult {
                    value: "a".into(),
                    score: 0.5,
                    cardinality: 2,
                },
                LabelSearchResult {
                    value: "c".into(),
                    score: 0.1,
                    cardinality: 1,
                },
            ],
            false,
        );

        let right = LabelQueryResult::new(
            vec![
                LabelSearchResult {
                    value: "a".into(),
                    score: 0.9,
                    cardinality: 3,
                },
                LabelSearchResult {
                    value: "b".into(),
                    score: 0.7,
                    cardinality: 4,
                },
            ],
            true,
        );

        left.merge_from(right, SearchResultOrdering::ValueAsc, 10);

        assert!(left.has_more);
        assert_eq!(left.results.len(), 3);
        assert_eq!(left.results[0].value, "a");
        assert_eq!(left.results[0].score, 0.9);
        assert_eq!(left.results[0].cardinality, 5);
    }

    #[test]
    fn label_query_result_merge_from_sets_has_more_when_truncated() {
        let mut left = LabelQueryResult::new(
            vec![LabelSearchResult {
                value: "a".into(),
                score: 0.1,
                cardinality: 1,
            }],
            false,
        );

        let right = LabelQueryResult::new(
            vec![
                LabelSearchResult {
                    value: "b".into(),
                    score: 0.2,
                    cardinality: 1,
                },
                LabelSearchResult {
                    value: "c".into(),
                    score: 0.3,
                    cardinality: 1,
                },
            ],
            false,
        );

        left.merge_from(right, SearchResultOrdering::ValueAsc, 2);

        assert!(left.has_more);
        assert_eq!(values(left), vec!["a", "b"]);
    }

    #[test]
    fn collect_limited_with_heap_returns_sorted_results() {
        let items = vec![
            LabelSearchResult {
                value: "c".to_string(),
                score: 0.5,
                cardinality: 3,
            },
            LabelSearchResult {
                value: "a".to_string(),
                score: 0.9,
                cardinality: 1,
            },
            LabelSearchResult {
                value: "b".to_string(),
                score: 0.7,
                cardinality: 2,
            },
        ];

        // Test ValueAsc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::ValueAsc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test ValueDesc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::ValueDesc,
            10,
        );
        assert_eq!(values(result), vec!["c", "b", "a"]);

        // Test ScoreDesc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::ScoreDesc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test CardinalityAsc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::CardinalityAsc,
            10,
        );
        assert_eq!(values(result), vec!["a", "b", "c"]);

        // Test CardinalityDesc
        let result = collect_limited_with_heap(
            items.clone().into_iter(),
            SearchResultOrdering::CardinalityDesc,
            10,
        );
        assert_eq!(values(result), vec!["c", "b", "a"]);
    }

    /// Accepts every name but `beta`, scoring each by its length.
    struct RejectBeta;

    impl FuzzyFilter for RejectBeta {
        fn accept(&self, value: &str) -> (bool, f64) {
            (value != "beta", value.len() as f64)
        }
    }

    /// Several values per name, so a lock release can fall between two entries of one name.
    fn build_multi_value_label_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "alpha", "a1");
        postings.add_posting_for_label_value(2, "alpha", "a1");
        postings.add_posting_for_label_value(3, "alpha", "a2");
        postings.add_posting_for_label_value(4, "beta", "b1");
        postings.add_posting_for_label_value(5, "beta", "b2");
        postings.add_posting_for_label_value(6, "beta", "b3");
        postings.add_posting_for_label_value(7, "gamma", "g1");
        postings.add_posting_for_label_value(8, "gamma", "g1");
        postings.add_posting_for_label_value(9, "service", "api");
        postings
    }

    fn scan_names(
        index: &TimeSeriesIndex,
        limit: Option<usize>,
        entries_per_hold: usize,
    ) -> (Vec<(String, f64, usize)>, bool) {
        let mut names = Vec::new();
        let has_more = iterate_label_names_asc_in_holds(
            index,
            &RejectBeta,
            limit,
            entries_per_hold,
            |name, score, cardinality| names.push((name.to_owned(), score, cardinality)),
        );
        (names, has_more)
    }

    #[test]
    fn name_scan_sums_cardinality_and_skips_rejected_names() {
        let index = index_of(build_multi_value_label_postings());

        let (names, has_more) = scan_names(&index, None, usize::MAX);

        assert!(!has_more);
        assert_eq!(
            names,
            vec![
                ("alpha".to_owned(), 5.0, 3),
                ("gamma".to_owned(), 5.0, 2),
                ("service".to_owned(), 7.0, 1),
            ]
        );
    }

    /// Giving the lock back after every entry, or every few, resumes where the scan stopped:
    /// a name split across holds keeps its summed cardinality, and limits cut at the same name.
    #[test]
    fn name_scan_is_unchanged_by_where_it_gives_the_lock_back() {
        let index = index_of(build_multi_value_label_postings());

        for limit in [None, Some(1), Some(2), Some(3)] {
            let one_hold = scan_names(&index, limit, usize::MAX);
            for entries_per_hold in [1, 2, 3, 4] {
                assert_eq!(
                    scan_names(&index, limit, entries_per_hold),
                    one_hold,
                    "limit {limit:?}, {entries_per_hold} entries per hold"
                );
            }
        }
    }

    /// `beta` is rejected; `beta2` shares its prefix and sorts between `beta` and `beta=…`.
    fn build_prefix_sharing_label_postings() -> Postings {
        let mut postings = build_multi_value_label_postings();
        postings.add_posting_for_label_value(10, "beta2", "x");
        postings.add_posting_for_label_value(11, "beta2", "y");
        postings
    }

    fn scan_names_desc(
        index: &TimeSeriesIndex,
        limit: Option<usize>,
        entries_per_hold: usize,
    ) -> (Vec<(String, f64, usize)>, bool) {
        let result = get_label_names_desc_in_holds(index, &RejectBeta, limit, entries_per_hold);
        let has_more = result.has_more;
        let names = result
            .into_iter()
            .map(|r| (r.value, r.score, r.cardinality))
            .collect();
        (names, has_more)
    }

    #[test]
    fn descending_name_scan_sums_cardinality_and_skips_rejected_names() {
        let index = index_of(build_multi_value_label_postings());

        let (names, has_more) = scan_names_desc(&index, None, usize::MAX);

        assert!(!has_more);
        assert_eq!(
            names,
            vec![
                ("service".to_owned(), 7.0, 1),
                ("gamma".to_owned(), 5.0, 2),
                ("alpha".to_owned(), 5.0, 3),
            ]
        );
    }

    #[test]
    fn descending_name_scan_is_unchanged_by_where_it_gives_the_lock_back() {
        for postings in [
            build_multi_value_label_postings(),
            build_prefix_sharing_label_postings(),
        ] {
            let index = index_of(postings);
            for limit in [None, Some(1), Some(2), Some(3)] {
                let one_hold = scan_names_desc(&index, limit, usize::MAX);
                for entries_per_hold in [1, 2, 3, 4] {
                    assert_eq!(
                        scan_names_desc(&index, limit, entries_per_hold),
                        one_hold,
                        "limit {limit:?}, {entries_per_hold} entries per hold"
                    );
                }
            }
        }
    }

    /// Skipping a rejected name must not skip a longer name that starts with it.
    #[test]
    fn name_scans_keep_a_name_that_extends_a_rejected_one() {
        let index = index_of(build_prefix_sharing_label_postings());
        let expected_asc = vec![
            ("alpha".to_owned(), 5.0, 3),
            ("beta2".to_owned(), 5.0, 2),
            ("gamma".to_owned(), 5.0, 2),
            ("service".to_owned(), 7.0, 1),
        ];
        let mut expected_desc = expected_asc.clone();
        expected_desc.reverse();

        for entries_per_hold in [1, 2, usize::MAX] {
            assert_eq!(
                scan_names(&index, None, entries_per_hold),
                (expected_asc.clone(), false)
            );
            assert_eq!(
                scan_names_desc(&index, None, entries_per_hold),
                (expected_desc.clone(), false)
            );
        }
    }
}
