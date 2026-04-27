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

use super::{get_timeseries_index, series_by_selectors};
use crate::common::logging::log_warning;
use crate::common::strings::JaroWinklerMatcher;
use crate::common::strings::SubsequenceMatcher;
use crate::labels::filters::SeriesSelector;
use std::collections::BTreeSet;
use std::fmt::Display;
use thiserror::Error;
use valkey_module::{Context, ValkeyError};

// The errors exposed.
#[derive(Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("not found")]
    NotFound,

    #[error("out of order sample")]
    OutOfOrderSample,

    #[error("out of bounds")]
    OutOfBounds,

    #[error("too old sample")]
    TooOldSample,

    #[error("start timestamp out of order, ignoring")]
    OutOfOrderST,
}

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

pub struct LabelHints {
    pub start: i64,
    pub end: i64,
    pub limit: usize,
}

/// SelectHints specifies hints passed for data selections.
#[derive(Debug, Clone, Default)]
pub struct SelectHints {
    /// Start time in milliseconds for this select.
    pub start: i64,

    /// End time in milliseconds for this select.
    pub end: i64,

    /// Maximum number of results returned. Use a value of 0 to disable.
    pub limit: usize,

    pub negative: bool,

    pub case_insensitive: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FuzzyAlgorithm {
    #[default]
    JaroWinkler,
    Subsequence,
}

impl TryFrom<&str> for FuzzyAlgorithm {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "jarowinkler" => Ok(FuzzyAlgorithm::JaroWinkler),
            "subsequence" => Ok(FuzzyAlgorithm::Subsequence),
            _ => Err(format!("Unknown fuzzy algorithm: {}", value)),
        }
    }
}

impl Display for FuzzyAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzyAlgorithm::JaroWinkler => write!(f, "jarowinkler"),
            FuzzyAlgorithm::Subsequence => write!(f, "subsequence"),
        }
    }
}

/// Filter determines whether a value should be included in results.
pub trait Filter {
    /// Returns (accepted, score) where score is used for relevance ranking.
    /// Score should be in range [0.0, 1.0] where 1.0 is perfect match.
    fn accept(&self, value: &str) -> (bool, f64);
}

pub struct NoOpSearchFilter;

impl Filter for NoOpSearchFilter {
    fn accept(&self, _value: &str) -> (bool, f64) {
        (true, 1.0)
    }
}

pub enum SimilarityMatcher {
    JaroWinkler(JaroWinklerMatcher),
    Subsequence(SubsequenceMatcher),
}

impl SimilarityMatcher {
    pub fn new(pattern: &str, algorithm: FuzzyAlgorithm) -> Self {
        match algorithm {
            FuzzyAlgorithm::JaroWinkler => {
                SimilarityMatcher::JaroWinkler(JaroWinklerMatcher::new(pattern.to_string()))
            }
            FuzzyAlgorithm::Subsequence => {
                SimilarityMatcher::Subsequence(SubsequenceMatcher::new(pattern))
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            SimilarityMatcher::JaroWinkler(_) => "jarowinkler",
            SimilarityMatcher::Subsequence(_) => "subsequence",
        }
    }

    pub fn algorithm(&self) -> FuzzyAlgorithm {
        match self {
            SimilarityMatcher::JaroWinkler(_) => FuzzyAlgorithm::JaroWinkler,
            SimilarityMatcher::Subsequence(_) => FuzzyAlgorithm::Subsequence,
        }
    }

    pub fn score(&self, value: &str) -> f64 {
        match self {
            SimilarityMatcher::JaroWinkler(m) => m.score(value),
            SimilarityMatcher::Subsequence(m) => m.score(value),
        }
    }
}

pub struct SimilarityFilter {
    matcher: SimilarityMatcher,
    threshold: f64,
    case_sensitive: bool,
}

impl SimilarityFilter {
    pub fn new(pattern: &str, algorithm: FuzzyAlgorithm, threshold: f64) -> Self {
        Self::new_with_case_sensitivity(pattern, algorithm, threshold, true)
    }

    pub fn new_with_case_sensitivity(
        pattern: &str,
        algorithm: FuzzyAlgorithm,
        threshold: f64,
        case_sensitive: bool,
    ) -> Self {
        let normalized_pattern = if case_sensitive {
            pattern.to_string()
        } else {
            pattern.to_lowercase()
        };

        Self {
            matcher: SimilarityMatcher::new(&normalized_pattern, algorithm),
            threshold,
            case_sensitive,
        }
    }
}

impl Filter for SimilarityFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let normalized_value;
        let candidate = if self.case_sensitive {
            value
        } else {
            normalized_value = value.to_lowercase();
            &normalized_value
        };

        let score = self.matcher.score(candidate);
        (score >= self.threshold, score)
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

pub const SEARCH_RESULT_DEFAULT_LIMIT: usize = 100;

/// SearchHints configures search operations with filtering and scoring.
pub struct SearchHints<'a> {
    /// Filter determines which values to include and their relevance scores.
    pub filter: Option<Box<dyn Filter + 'a>>,

    /// Maximum number of results to return.
    pub limit: usize,

    /// Selects the ordering of results.
    pub order_by: SearchResultOrdering,

    /// Whether to perform case-insensitive matching.
    pub case_sensitive: bool,
    // todo: start, end time hints for time-based relevance scoring, e.g. prefer label values that have been more recently active.
}

impl<'a> Default for SearchHints<'a> {
    fn default() -> Self {
        Self {
            filter: None,
            limit: SEARCH_RESULT_DEFAULT_LIMIT,
            case_sensitive: true,
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

/// Searcher provides search capabilities with relevance scoring.
pub trait LabelSearcher {
    /// Returns an iterator over label names matching the search criteria.
    fn search_label_names(
        &self,
        ctx: &Context,
        hints: &SearchHints,
        selectors: &[SeriesSelector],
    ) -> impl Iterator<Item=SearchResult>;

    /// Returns an iterator over label values for the given label name.
    fn search_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: &SearchHints,
        selectors: &[SeriesSelector],
    ) -> impl Iterator<Item=SearchResult>;
}

// apply_search_hints filters, sorts, and limits a slice of string values according to hints,
// returning scored SearchResult entries. A None hints value is treated as the zero value.
// The input values slice is assumed to be ordered ascending by value; the function only
// performs extra work for orderings that differ from this.
pub(super) fn apply_search_hints(
    values: Vec<String>,
    hints: Option<&SearchHints>,
) -> Vec<SearchResult> {
    let default_hints = SearchHints::default();
    let hints = hints.unwrap_or(&default_hints);
    let no_op = NoOpSearchFilter;
    let filter: &dyn Filter = hints.filter.as_deref().unwrap_or(&no_op);

    let mut results = Vec::with_capacity(values.len());
    for v in values {
        let (accepted, score) = filter.accept(&v);
        if accepted {
            results.push(SearchResult { value: v, score })
        }
    }
    match hints.order_by {
        SearchResultOrdering::OrderByValueAsc => {
            /* No additional work needed; input is already in the correct order. */
        }
        SearchResultOrdering::OrderByValueDesc => results.reverse(),
        SearchResultOrdering::OrderByScoreDesc => results.sort_by(compare_search_results(
            SearchResultOrdering::OrderByScoreDesc,
        )),
    }
    if hints.limit > 0 && results.len() > hints.limit {
        results.truncate(hints.limit);
    }
    results
}

/// `compare_search_results` returns the total-order comparison function for the
/// given Ordering. For `OrderByValueAsc` and `OrderByValueDesc` the order is on
/// value alone. For `OrderByScoreDesc` the order is (score desc, value asc),
/// which is a total order and defines the position at which a duplicate value
/// is first emitted by the streaming merge.
fn compare_search_results(
    o: SearchResultOrdering,
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

    match o {
        SearchResultOrdering::OrderByValueDesc => value_desc,
        SearchResultOrdering::OrderByScoreDesc => score_desc,
        SearchResultOrdering::OrderByValueAsc => value_asc,
    }
}

#[derive(Default)]
pub struct BaseLabelQuerier {
    threshold: f64,
    algorithm: FuzzyAlgorithm,
}

impl BaseLabelQuerier {
    pub fn new() -> Self {
        Self {
            threshold: 0.0,
            algorithm: FuzzyAlgorithm::JaroWinkler,
        }
    }

    fn collect_label_names(&self, ctx: &Context, selectors: &[SeriesSelector]) -> Vec<String> {
        if selectors.is_empty() {
            let index = get_timeseries_index(ctx);
            let mut state = ();
            return index.with_postings(&mut state, |postings, _| {
                postings.get_label_names().into_iter().collect::<Vec<_>>()
            });
        }

        match series_by_selectors(ctx, selectors, None) {
            Ok(series) => {
                let mut out = BTreeSet::new();
                for (guard, _) in series {
                    for label in guard.as_ref().labels.iter() {
                        out.insert(label.name.to_string());
                    }
                }
                out.into_iter().collect()
            }
            Err(err) => {
                log_warning(format!("failed to collect label names for search: {err}"));
                Vec::new()
            }
        }
    }

    fn collect_label_values(
        &self,
        ctx: &Context,
        name: &str,
        selectors: &[SeriesSelector],
    ) -> Vec<String> {
        if name.is_empty() {
            return Vec::new();
        }

        if selectors.is_empty() {
            let index = get_timeseries_index(ctx);
            let mut state = ();
            return index.with_postings(&mut state, |postings, _| postings.get_label_values(name));
        }

        match series_by_selectors(ctx, selectors, None) {
            Ok(series) => {
                let mut out = BTreeSet::new();
                for (guard, _) in series {
                    if let Some(label) = guard.as_ref().get_label(name) {
                        out.insert(label.value.into());
                    }
                }
                out.into_iter().collect()
            }
            Err(err) => {
                log_warning(format!(
                    "failed to collect label values for '{name}': {err}"
                ));
                Vec::new()
            }
        }
    }
}

impl LabelSearcher for BaseLabelQuerier {
    fn search_label_names(
        &self,
        ctx: &Context,
        hints: &SearchHints,
        selectors: &[SeriesSelector],
    ) -> impl Iterator<Item=SearchResult> {
        let values = self.collect_label_names(ctx, selectors);
        apply_search_hints(values, Some(hints)).into_iter()
    }

    fn search_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: &SearchHints,
        selectors: &[SeriesSelector],
    ) -> impl Iterator<Item=SearchResult> {
        let values = self.collect_label_values(ctx, name, selectors);
        apply_search_hints(values, Some(hints)).into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{Filter, FuzzyAlgorithm, SimilarityFilter};

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
