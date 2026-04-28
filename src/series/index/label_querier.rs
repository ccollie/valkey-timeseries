use super::{
    FuzzyFilter, IndexKey, PostingsBitmap, SimilarityFilter, get_timeseries_index,
    series_by_selectors,
};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::error::TsdbResult;
use crate::labels::filters::SeriesSelector;
use crate::series::index::key_buffer::KeyBuffer;
use crate::series::index::postings::Postings;
use crate::series::request_types::MetaDateRangeFilter;
use std::collections::BTreeSet;
use std::fmt::Display;
use valkey_module::{Context, ValkeyError};

const DEFAULT_LIMIT: usize = 100;
const DEFAULT_MAX_LIMIT: usize = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum SortBy {
    #[default]
    Alpha,
    Score,
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
pub struct LabelSearchResult {
    /// The label name or label value.
    pub value: String,

    /// Relevance score, with 1.0 being a perfect match.
    pub score: f64,
}

impl LabelSearchResult {
    pub fn new(value: String, score: f64) -> Self {
        Self { value, score }
    }
}

impl PartialEq for LabelSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.score == other.score
    }
}

impl Eq for LabelSearchResult {}

#[derive(Debug, Clone, Default)]
pub struct MetricNameSearchResult {
    pub value: String,
    pub score: f64,
    pub cardinality: usize,
}

/// LabelQuerier defines the interface for querying label names and values with flexible filtering and ordering.
pub trait LabelQuerier {
    /// Returns a list of label names matching the given hints.
    fn get_label_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<Vec<LabelSearchResult>>;

    /// Returns a list of label values for the given label name, matching the given hints.
    fn get_label_values(
        &self,
        ctx: &Context,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<Vec<LabelSearchResult>>;

    /// Returns a list of metric names (`__name__` values) matching the given hints.
    fn get_metric_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<Vec<LabelSearchResult>>;
}

/// `compare_search_results` returns the total-order comparison function for the
/// given Ordering. For `OrderByValueAsc` and `OrderByValueDesc` the order is on
/// value alone. For `OrderByScoreDesc` the order is (score desc, value asc),
/// which is a total order and defines the position at which a duplicate value
/// is first emitted by the streaming merge.
fn compare_search_results(
    order: SearchResultOrdering,
) -> impl Fn(&LabelSearchResult, &LabelSearchResult) -> std::cmp::Ordering {
    fn value_asc(a: &LabelSearchResult, b: &LabelSearchResult) -> std::cmp::Ordering {
        a.value.cmp(&b.value)
    }

    fn value_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> std::cmp::Ordering {
        b.value.cmp(&a.value)
    }

    fn score_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> std::cmp::Ordering {
        match b
            .score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
        {
            std::cmp::Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    match order {
        SearchResultOrdering::ValueDesc => value_desc,
        SearchResultOrdering::ScoreDesc => score_desc,
        SearchResultOrdering::ValueAsc => value_asc,
    }
}

/// `merge_search_results` returns the total-order comparison function for merging two SearchResult streams ordered by
/// the same
pub(crate) fn merge_search_results(
    a: impl Iterator<Item=LabelSearchResult>,
    b: impl Iterator<Item=LabelSearchResult>,
    ordering: SearchResultOrdering,
) -> impl Iterator<Item=LabelSearchResult> {
    use itertools::EitherOrBoth::{Both, Left, Right};
    use itertools::Itertools;

    let comparator = compare_search_results(ordering);

    a.merge_join_by(b, comparator).map(|either| match either {
        Both(a, _) => a,
        Left(a) => a,
        Right(b) => b,
    })
}

/// `apply_search_hints` sorts and limits a slice of values according to hints,
/// returning scored SearchResult entries.
fn apply_search_hints(
    mut values: Vec<LabelSearchResult>,
    hints: &SearchHints,
) -> Vec<LabelSearchResult> {
    let limit = hints.limit;

    let comparator = compare_search_results(hints.order_by);
    values.sort_by(comparator);

    if limit > 0 && values.len() > limit {
        values.truncate(limit);
    }
    values
}

fn push_unique_scored_value(
    seen: &mut BTreeSet<String>,
    results: &mut Vec<LabelSearchResult>,
    filter: &dyn FuzzyFilter,
    value: &str,
) {
    if !seen.insert(value.to_string()) {
        return;
    }

    let (accepted, score) = filter.accept(value);
    if accepted {
        results.push(LabelSearchResult::new(value.to_string(), score));
    }
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

/// `collect_unscoped_label_names` attempts to satisfy a label name search by scanning the label index directly,
/// without intersecting with series postings. This is only possible for orderings that are consistent with the
/// label index order (i.e., value ascending or descending), and when no selectors or date range are involved,
/// that would require intersecting with series postings.
fn collect_unscoped_label_names(
    postings: &Postings,
    filter: &dyn FuzzyFilter,
    hints: &SearchHints,
) -> Option<Vec<LabelSearchResult>> {
    let limit = hints.limit.min(DEFAULT_MAX_LIMIT);

    let check = |(entry, _): (&IndexKey, &PostingsBitmap)| {
        let (key, _) = entry.split()?;
        let (accepted, score) = filter.accept(key);
        accepted.then(|| LabelSearchResult::new(key.to_owned(), score))
    };

    match hints.order_by {
        SearchResultOrdering::ValueAsc => Some(
            postings
                .label_index
                .iter()
                .filter_map(check)
                .take(limit)
                .collect(),
        ),
        SearchResultOrdering::ValueDesc => Some(
            postings
                .label_index
                .iter()
                .rev()
                .filter_map(check)
                .take(limit)
                .collect(),
        ),
        _ => None,
    }
}

fn collect_unscoped_label_values(
    postings: &Postings,
    label_name: &str,
    filter: &dyn FuzzyFilter,
    hints: &SearchHints,
) -> Option<Vec<LabelSearchResult>> {
    let limit = hints.limit.min(DEFAULT_MAX_LIMIT);
    let prefix = KeyBuffer::for_prefix(label_name);

    let check = |(entry, _): (&IndexKey, &PostingsBitmap)| {
        let (_, value) = entry.split()?;
        let (accepted, score) = filter.accept(value);
        accepted.then(|| LabelSearchResult::new(value.to_owned(), score))
    };

    match hints.order_by {
        SearchResultOrdering::ValueAsc => Some(
            postings
                .label_index
                .prefix(prefix.as_bytes())
                .filter_map(check)
                .take(limit)
                .collect(),
        ),
        SearchResultOrdering::ValueDesc => Some(
            postings
                .label_index
                .prefix(prefix.as_bytes())
                .rev()
                .filter_map(check)
                .take(limit)
                .collect(),
        ),
        _ => None,
    }
}

fn collect_label_names(
    ctx: &Context,
    select_hints: Option<&SelectHints>,
    hints: &SearchHints,
) -> TsdbResult<Vec<LabelSearchResult>> {
    let index = get_timeseries_index(ctx);
    let postings = index.get_postings();
    with_search_filter(hints, |filter| {
        // fast path for when no selectors or date range are involved:
        // we can scan the label index directly without intersecting with series postings.
        // This is efficient for small result sets but also avoids doing potentially expensive intersections
        // when the filter is expected to be highly selective.
        if can_use_unscoped_scan(select_hints)
            && let Some(results) = collect_unscoped_label_names(&postings, filter, hints)
        {
            return Ok(apply_search_hints(results, hints));
        }

        let mut names: BTreeSet<String> = BTreeSet::new();

        // at this point we know we have non-empty selectors or a date range, but do the jig to
        // keep the compiler happy
        let default_hints = SelectHints::default();
        let opts = select_hints.unwrap_or(&default_hints);

        let mut result = Vec::with_capacity(DEFAULT_LIMIT);
        // todo! set a max limit on the number of series we scan here, to avoid OOM or stalls for very
        // broad selectors with many matches. We can still apply the filter and return results,
        // but we should avoid scanning an unbounded number of series.
        let matched_series = series_by_selectors(ctx, &opts.selectors, opts.date_range)?;
        for (ts, _) in matched_series {
            for label in ts.labels.iter() {
                push_unique_scored_value(&mut names, &mut result, filter, label.value);
            }
        }

        Ok(apply_search_hints(result, hints))
    })
}

fn collect_label_values(
    ctx: &Context,
    label_name: &str,
    select_hints: Option<&SelectHints>,
    search_hints: &SearchHints,
) -> TsdbResult<Vec<LabelSearchResult>> {
    if label_name.is_empty() {
        return Ok(Vec::new());
    }

    let index = get_timeseries_index(ctx);
    let postings = index.get_postings();

    with_search_filter(search_hints, |filter| {
        if can_use_unscoped_scan(select_hints)
            && let Some(results) =
            collect_unscoped_label_values(&postings, label_name, filter, search_hints)
        {
            return Ok(apply_search_hints(results, search_hints));
        }

        let mut name_set: BTreeSet<String> = BTreeSet::new();
        // at this point we know we have non-empty selectors or a date range, but do the jig to
        // keep the compiler happy
        let default_hints = SelectHints::default();
        let opts = select_hints.unwrap_or(&default_hints);

        let mut result = Vec::with_capacity(DEFAULT_LIMIT);
        // todo! set a max limit on the number of series we scan here, to avoid OOM or long processing times
        // for very broad selectors with many matches. We can still apply the filter and return results,
        // but we should avoid scanning an unbounded number of series.
        let matched_series = series_by_selectors(ctx, &opts.selectors, opts.date_range)?;
        for (ts, _) in matched_series {
            if let Some(label) = ts.get_label(label_name) {
                push_unique_scored_value(&mut name_set, &mut result, filter, label.value);
            }
        }

        Ok(apply_search_hints(result, search_hints))
    })
}

#[derive(Default)]
pub struct DefaultLabelQuerier;

impl LabelQuerier for DefaultLabelQuerier {
    fn get_label_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<Vec<LabelSearchResult>> {
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_names(ctx, select_hints.as_ref(), hints)
    }

    fn get_label_values(
        &self,
        ctx: &Context,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<Vec<LabelSearchResult>> {
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(ctx, name, select_hints.as_ref(), hints)
    }

    fn get_metric_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<Vec<LabelSearchResult>> {
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(ctx, METRIC_NAME_LABEL, select_hints.as_ref(), hints)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FuzzyFilter, Postings, SearchHints, SearchResultOrdering, SimilarityFilter,
        collect_unscoped_label_values,
    };
    use crate::series::index::FuzzyAlgorithm;

    fn build_label_values_postings() -> Postings {
        let mut postings = Postings::default();
        postings.add_posting_for_label_value(1, "region", "us-east");
        postings.add_posting_for_label_value(2, "region", "us-west");
        postings.add_posting_for_label_value(3, "region", "eu-west");
        // Distractor label to verify prefix scoping.
        postings.add_posting_for_label_value(4, "service", "api");
        postings
    }

    fn values(results: Vec<super::LabelSearchResult>) -> Vec<String> {
        results.into_iter().map(|r| r.value).collect()
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
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
        };

        let results = collect_unscoped_label_values(&postings, "region", &filter, &hints)
            .expect("expected unscoped fast path for value-asc ordering");

        assert_eq!(values(results), vec!["eu-west", "us-east", "us-west"]);
    }

    #[test]
    fn collect_unscoped_label_values_orders_descending() {
        let postings = build_label_values_postings();
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueDesc,
        };

        let results = collect_unscoped_label_values(&postings, "region", &filter, &hints)
            .expect("expected unscoped fast path for value-desc ordering");

        assert_eq!(values(results), vec!["us-west", "us-east", "eu-west"]);
    }

    #[test]
    fn collect_unscoped_label_values_returns_none_for_score_ordering() {
        let postings = build_label_values_postings();
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ScoreDesc,
        };

        let results = collect_unscoped_label_values(&postings, "region", &filter, &hints);

        assert!(results.is_none());
    }
}
