// Copyright The Prometheus Authors
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

use super::{get_timeseries_index, FuzzyFilter, IndexKey, PostingsBitmap, SimilarityFilter};
use crate::labels::filters::SeriesSelector;
use crate::series::index::key_buffer::KeyBuffer;
use crate::series::index::postings::Postings;
use crate::series::index::querier::series_posting_ids_by_selectors;
use crate::series::request_types::MetaDateRangeFilter;
use std::borrow::Cow;
use std::fmt::Display;
use valkey_module::{Context, ValkeyError, ValkeyResult};
use crate::error::TsdbResult;

const DEFAULT_LIMIT: usize = 100;
const DEFAULT_MAX_LIMIT: usize = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SortBy {
    Alpha,
    Score,
}

impl Default for SortBy {
    fn default() -> Self {
        SortBy::Alpha
    }
}

impl Display for SortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortBy::Alpha => write!(f, "alpha"),
            SortBy::Score => write!(f, "score"),
        }
    }
}

impl TryFrom<&str> for SortBy {
    type Error = ValkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "alpha" => Ok(SortBy::Alpha),
            "score" => Ok(SortBy::Score),
            _ => Err(ValkeyError::Str("TSDB: SORT_BY must be alpha or score")),
        }
    }
}

/// Ordering is a closed set of result orderings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchResultOrdering {
    /// Orders results ascending by Value.
    #[default]
    OrderByValueAsc,
    /// Orders results descending by Value.
    OrderByValueDesc,
    /// Orders results descending by Score, breaking ties ascending by Value.
    OrderByScoreDesc,
}

pub struct LabelHints {
    pub start: i64,
    pub end: i64,
    pub limit: usize,
}

/// SelectHints specifies hints passed for data selections.
#[derive(Debug, Clone, Default)]
pub struct SelectHints {
    /// Series selectors to apply to series selection.
    selectors: Vec<SeriesSelector>,
    /// Optional date range filter to apply to series selection.
    date_range: Option<MetaDateRangeFilter>,
}

impl SelectHints {
    pub fn new(selectors: Vec<SeriesSelector>, date_range: Option<MetaDateRangeFilter>) -> Self {
        Self {
            selectors,
            date_range,
        }
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
}

impl<'a> Default for SearchHints<'a> {
    fn default() -> Self {
        Self {
            filter: None,
            limit: SEARCH_RESULT_DEFAULT_LIMIT,
            order_by: SearchResultOrdering::default(),
        }
    }
}

/// SearchResult represents a single search result with its relevance score.
#[derive(Debug, Clone, Default)]
pub struct SearchResult {
    /// The label name or label value.
    pub value: String,

    /// Relevance score, with 1.0 being a perfect match.
    pub score: f64,
}

impl SearchResult {
    pub fn new(value: String, score: f64) -> Self {
        Self { value, score }
    }
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.score == other.score
    }
}

impl Eq for SearchResult {}

/// Searcher provides search capabilities with relevance scoring.
pub trait LabelSearcher {
    /// Returns an iterator over label names matching the search criteria.
    fn search_label_names(
        &self,
        ctx: &Context,
        hints: &SearchHints,
        select_hints: Option<SelectHints>,
    ) -> TsdbResult<Vec<SearchResult>>;

    /// Returns an iterator over label values for the given label name.
    fn search_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: &SearchHints,
        select_hints: Option<SelectHints>,
    ) -> TsdbResult<Vec<SearchResult>>;
}

/// `compare_search_results` returns the total-order comparison function for the
/// given Ordering. For `OrderByValueAsc` and `OrderByValueDesc` the order is on
/// value alone. For `OrderByScoreDesc` the order is (score desc, value asc),
/// which is a total order and defines the position at which a duplicate value
/// is first emitted by the streaming merge.
fn compare_search_results(
    order: SearchResultOrdering,
) -> impl Fn(&SearchResult, &SearchResult) -> std::cmp::Ordering {
    fn value_asc(a: &SearchResult, b: &SearchResult) -> std::cmp::Ordering {
        a.value.cmp(&b.value)
    }

    fn value_desc(a: &SearchResult, b: &SearchResult) -> std::cmp::Ordering {
        b.value.cmp(&a.value)
    }

    fn score_desc(a: &SearchResult, b: &SearchResult) -> std::cmp::Ordering {
        match b
            .score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
        {
            std::cmp::Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    fn score_desc_value_desc(a: &SearchResult, b: &SearchResult) -> std::cmp::Ordering {
        match b
            .score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
        {
            std::cmp::Ordering::Equal => value_desc(a, b),
            other => other,
        }
    }

    match order {
        SearchResultOrdering::OrderByValueDesc => value_desc,
        SearchResultOrdering::OrderByScoreDesc => score_desc,
        SearchResultOrdering::OrderByValueAsc => value_asc,
    }
}

/// `merge_search_results` returns the total-order comparison function for merging two SearchResult streams ordered by
/// the same
pub(crate) fn merge_search_results(
    a: impl Iterator<Item=SearchResult>,
    b: impl Iterator<Item=SearchResult>,
    ordering: SearchResultOrdering,
) -> impl Iterator<Item=SearchResult> {
    use itertools::EitherOrBoth::{Both, Left, Right};
    use itertools::Itertools;

    let comparator = compare_search_results(ordering);

    a.merge_join_by(b, comparator).map(|either| match either {
        Both(a, _) => a,
        Left(a) => a,
        Right(b) => b,
    })
}

fn get_series_postings_for_selectors<'a>(
    ctx: &Context,
    select_hints: Option<&SelectHints>,
) -> ValkeyResult<Option<Cow<'a, PostingsBitmap>>> {
    let Some(select_hints) = select_hints else {
        return Ok(None);
    };

    if select_hints.selectors.is_empty() {
        return Ok(None);
    }

    let bitmap = series_posting_ids_by_selectors(
        ctx,
        &select_hints.selectors,
        select_hints.date_range,
    )?;
    Ok(Some(bitmap))
}

// apply_search_hints filters, sorts, and limits a slice of values according to hints,
// returning scored SearchResult entries. A None hints value is treated as the zero value.
// The input values slice is assumed to be ordered ascending by value; the function only
// performs extra work for orderings that differ from this.
fn apply_search_hints(mut values: Vec<SearchResult>, hints: &SearchHints) -> Vec<SearchResult> {
    let limit = hints.limit;

    if hints.order_by != SearchResultOrdering::OrderByValueAsc {
        let comparator = compare_search_results(hints.order_by);
        values.sort_by(comparator);
    }

    if limit > 0 && values.len() > limit {
        values.truncate(limit);
    }
    values
}

#[derive(Clone, Copy)]
enum LabelFilterType {
    Name,
    Value,
}

#[derive(Default)]
pub struct LabelQuerier;

impl LabelQuerier {
    fn handle_filter_internal(
        &self,
        filter: &dyn FuzzyFilter,
        filter_type: LabelFilterType,
        index_key: &IndexKey,
        map: &PostingsBitmap,
        series_ids: Option<&Cow<PostingsBitmap>>,
    ) -> Option<SearchResult> {
        if map.is_empty() {
            // so series have this label
            return None;
        }
        if let Some(series_ids) = series_ids
            && !map.intersect(&series_ids)
        {
            return None;
        }
        let Some((key, value)) = index_key.split() else {
            return None;
        };
        let target = match filter_type {
            LabelFilterType::Name => key,
            LabelFilterType::Value => value,
        };
        let (accepted, score) = filter.accept(target);
        if accepted {
            Some(SearchResult {
                value: target.to_string(),
                score,
            })
        } else {
            None
        }
    }

    fn get_filtered_label_values(
        &self,
        postings: &Postings,
        label_name: &str,
        filter: &dyn FuzzyFilter,
        series_ids: Option<&Cow<PostingsBitmap>>,
    ) -> Vec<SearchResult> {
        let prefix = KeyBuffer::for_prefix(label_name);
        postings
            .label_index
            .prefix(prefix.as_bytes())
            .filter_map(|(k, map)| {
                self.handle_filter_internal(filter, LabelFilterType::Value, k, map, series_ids)
            })
            .collect()
    }

    pub(super) fn get_filtered_label_names(
        &self,
        postings: &Postings,
        filter: &dyn FuzzyFilter,
        series_ids: Option<&Cow<PostingsBitmap>>,
    ) -> Vec<SearchResult> {
        postings
            .label_index
            .iter()
            .filter_map(|(k, map)| {
                self.handle_filter_internal(filter, LabelFilterType::Value, k, map, series_ids)
            })
            .collect()
    }

    fn collect_label_names(
        &self,
        ctx: &Context,
        select_hints: Option<&SelectHints>,
        hints: &SearchHints,
    ) -> ValkeyResult<Vec<SearchResult>> {
        let index = get_timeseries_index(ctx);
        let postings = index.get_postings();
        let default_filter: Box<dyn FuzzyFilter> = Box::new(SimilarityFilter::default());
        let filter: &dyn FuzzyFilter = hints.filter.as_ref().map(|f| f.as_ref()).unwrap_or(default_filter.as_ref());

        let has_selectors = select_hints
            .map(|h| !h.selectors.is_empty())
            .unwrap_or(false);
        let has_range = select_hints.and_then(|h| h.date_range).is_some();

        // optimize for the case of iterating over all label names when no selectors are specified,
        if !has_selectors && !has_range {
            let limit = hints.limit.min(DEFAULT_MAX_LIMIT);

            let check = |(entry, _): (&IndexKey, &PostingsBitmap)| {
                let (key, _) = entry.split()?;
                let (accepted, score) = filter.accept(key);
                if accepted {
                    return Some(SearchResult::new(key.to_owned(), score));
                }
                None
            };

            // If we sort strictly by value, we can take advantage of the fact that the label index is already
            // ordered by value to avoid collecting and sorting all results. For other orderings, we still
            // need to collect and sort all results, which is handled below.
            match hints.order_by {
                SearchResultOrdering::OrderByValueAsc => {
                    // truncate the results to the limit as early as possible since we know the order is correct,
                    // which is more efficient than collecting all results and sorting them.
                    return Ok(postings
                        .label_index
                        .iter()
                        .filter_map(check)
                        .take(limit)
                        .collect());
                }
                SearchResultOrdering::OrderByValueDesc => {
                    return Ok(postings      
                        .label_index
                        .iter()
                        .rev()
                        .filter_map(check)
                        .take(limit)
                        .collect());
                }
                _ => {
                    /* for other orderings we need to collect all results and sort them, which is handled below */
                }
            }
        }

        let series_postings = get_series_postings_for_selectors(ctx, select_hints)?;
        let names = self.get_filtered_label_names(&postings, filter, series_postings.as_ref());
        Ok(names)
    }

    fn collect_label_values(
        &self,
        ctx: &Context,
        name: &str,
        select_hints: Option<&SelectHints>,
        hints: &SearchHints,
    ) -> TsdbResult<Vec<SearchResult>> {
        if name.is_empty() {
            return Ok(Vec::new());
        }
        let default_filter: Box<dyn FuzzyFilter> = Box::new(SimilarityFilter::default());
        let filter: &dyn FuzzyFilter = hints.filter.as_ref().map(|f| f.as_ref()).unwrap_or(default_filter.as_ref());
        let series_postings = get_series_postings_for_selectors(ctx, select_hints)?;
        let index = get_timeseries_index(ctx);
        let postings = index.get_postings();
        Ok(self.get_filtered_label_values(
            &postings,
            name,
            filter,
            series_postings.as_ref(),
        ))
    }
}

impl LabelSearcher for LabelQuerier {
    fn search_label_names(
        &self,
        ctx: &Context,
        hints: &SearchHints,
        select_hints: Option<SelectHints>,
    ) -> TsdbResult<Vec<SearchResult>> {
        let values = self.collect_label_names(ctx, select_hints.as_ref(), hints)?;
        Ok(apply_search_hints(values, hints))
    }

    fn search_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: &SearchHints,
        select_hints: Option<SelectHints>,
    ) -> TsdbResult<Vec<SearchResult>> {
        let values = self.collect_label_values(ctx, name, select_hints.as_ref(), hints)?;
        Ok(apply_search_hints(values, hints))
    }
}

#[cfg(test)]
mod tests {
    use super::FuzzyFilter;
    use crate::series::index::{FuzzyAlgorithm, SimilarityFilter};

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
}
