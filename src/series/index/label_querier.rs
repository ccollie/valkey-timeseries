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
use ahash::AHashSet;
use std::fmt::Display;
use valkey_module::{Context, ValkeyError};

const DEFAULT_LIMIT: usize = 100;

// todo: get from config
pub const SEARCH_RESULT_LIMIT_MAX: usize = 1000;

/// SortBy is a closed set of label search result sort keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum SortBy {
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
            "alpha" | "value" => Ok(SortBy::Value),
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
pub trait LabelQuerier {
    /// Returns a list of label names matching the given hints.
    fn get_label_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;

    /// Returns a list of label values for the given label name, matching the given hints.
    fn get_label_values(
        &self,
        ctx: &Context,
        name: &str,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;

    /// Returns a list of metric names (`__name__` values) matching the given hints.
    fn get_metric_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult>;
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

    fn cardinality_asc(a: &LabelSearchResult, b: &LabelSearchResult) -> std::cmp::Ordering {
        match a.cardinality.cmp(&b.cardinality) {
            std::cmp::Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    fn cardinality_desc(a: &LabelSearchResult, b: &LabelSearchResult) -> std::cmp::Ordering {
        match b.cardinality.cmp(&a.cardinality) {
            std::cmp::Ordering::Equal => value_asc(a, b),
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

/// `apply_search_hints` sorts and limits a slice of values according to hints,
/// returning a [`LabelQueryResult`] that includes a `has_more` flag indicating
/// whether any entries were truncated at the limit.
pub(crate) fn apply_search_hints(
    mut values: Vec<LabelSearchResult>,
    hints: &SearchHints,
) -> LabelQueryResult {
    let limit = hints.limit;

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
    iter: impl Iterator<Item=LabelSearchResult>,
    limit: usize,
) -> LabelQueryResult {
    if limit == 0 {
        return LabelQueryResult::new(iter.collect(), false);
    }
    let mut items: Vec<LabelSearchResult> = iter.take(limit + 1).collect();
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    LabelQueryResult::new(items, has_more)
}

/// `collect_unscoped_label_names` attempts to satisfy a label name search by scanning the label index directly,
/// without intersecting with series postings. This is only possible for orderings that are consistent with the
/// label index order (i.e., value ascending or descending), and when no selectors or date range are involved,
/// that would require intersecting with series postings.
fn collect_unscoped_label_names(
    postings: &Postings,
    filter: &dyn FuzzyFilter,
    hints: &SearchHints,
) -> Option<LabelQueryResult> {
    let limit = hints.limit.min(SEARCH_RESULT_LIMIT_MAX);

    let check = |(entry, map): (&IndexKey, &PostingsBitmap)| {
        if map.is_empty() {
            return None;
        }
        let (key, _) = entry.split()?;
        let (accepted, score) = filter.accept(key);
        let cardinality = map.cardinality() as usize;

        accepted.then(|| LabelSearchResult {
            value: key.to_owned(),
            score,
            cardinality,
        })
    };

    match hints.order_by {
        SearchResultOrdering::ValueAsc => Some(collect_limited(
            postings.label_index.iter().filter_map(check),
            limit,
        )),
        SearchResultOrdering::ValueDesc => Some(collect_limited(
            postings.label_index.iter().rev().filter_map(check),
            limit,
        )),
        SearchResultOrdering::ScoreDesc => None,
        SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc => {
            let values = postings.label_index.iter().filter_map(check).collect();
            Some(apply_search_hints(values, hints))
        }
    }
}

fn collect_unscoped_label_values(
    postings: &Postings,
    label_name: &str,
    filter: &dyn FuzzyFilter,
    hints: &SearchHints,
) -> Option<LabelQueryResult> {
    let limit = hints.limit.min(SEARCH_RESULT_LIMIT_MAX);
    let prefix = KeyBuffer::for_prefix(label_name);

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
        SearchResultOrdering::ValueAsc => Some(collect_limited(
            postings
                .label_index
                .prefix(prefix.as_bytes())
                .filter_map(check),
            limit,
        )),
        SearchResultOrdering::ValueDesc => Some(collect_limited(
            postings
                .label_index
                .prefix(prefix.as_bytes())
                .rev()
                .filter_map(check),
            limit,
        )),
        SearchResultOrdering::ScoreDesc => None,
        SearchResultOrdering::CardinalityAsc | SearchResultOrdering::CardinalityDesc => {
            let values = postings
                .label_index
                .prefix(prefix.as_bytes())
                .filter_map(check)
                .collect();
            Some(apply_search_hints(values, hints))
        }
    }
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

fn collect_label_names(
    ctx: &Context,
    select_hints: Option<&SelectHints>,
    hints: &SearchHints,
) -> TsdbResult<LabelQueryResult> {
    let index = get_timeseries_index(ctx);
    let postings = index.get_postings();
    with_search_filter(hints, |filter| {
        // fast path for when no selectors or date range are involved:
        // we can scan the label index directly without intersecting with series postings.
        // This is efficient for small result sets but also avoids doing potentially expensive intersections
        // when the filter is expected to be highly selective.
        if can_use_unscoped_scan(select_hints) {
            if let Some(result) = collect_unscoped_label_names(&postings, filter, hints) {
                return Ok(result);
            }

            // Score-based ordering is not emitted by the unscoped index-order fast path.
            // For no-selector requests, materialize candidate names from the postings index,
            // then apply global ordering/limit via search hints.
            let mut result = Vec::with_capacity(DEFAULT_LIMIT);
            for name in postings.get_label_names() {
                let (accepted, score) = filter.accept(&name);
                if accepted {
                    let cardinality = get_label_cardinality(&postings, &name);
                    if cardinality == 0 {
                        continue;
                    }
                    result.push(LabelSearchResult {
                        value: name,
                        score,
                        cardinality,
                    });
                }
            }

            return Ok(apply_search_hints(result, hints));
        }

        let mut names: AHashSet<String> = AHashSet::with_capacity(DEFAULT_LIMIT);

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
                // DO NOT use `names.insert()`, as it would require an allocation even if the label name is already see
                if names.contains(label.name) {
                    continue;
                }
                let name = label.name.to_string();
                let (accepted, score) = filter.accept(&name);
                if accepted {
                    // fetch the cardinality
                    let cardinality = get_label_cardinality(&postings, label.name);
                    if cardinality == 0 {
                        continue;
                    }
                    result.push(LabelSearchResult {
                        value: name.to_string(),
                        score,
                        cardinality,
                    });
                }
                names.insert(name);
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
) -> TsdbResult<LabelQueryResult> {
    if label_name.is_empty() {
        return Ok(LabelQueryResult::default());
    }

    let index = get_timeseries_index(ctx);
    let postings = index.get_postings();

    with_search_filter(search_hints, |filter| {
        if can_use_unscoped_scan(select_hints) {
            if let Some(result) =
                collect_unscoped_label_values(&postings, label_name, filter, search_hints)
            {
                return Ok(result);
            }

            // Score-based ordering is not emitted by the unscoped prefix-order fast path.
            // For no-selector requests, materialize candidate values from the postings index,
            // then apply global ordering/limit via search hints.
            let mut result = Vec::with_capacity(DEFAULT_LIMIT);
            for value in postings.get_label_values(label_name) {
                let (accepted, score) = filter.accept(&value);
                if accepted {
                    let label_postings = postings.postings_for_label_value(label_name, &value);
                    if label_postings.is_empty() {
                        continue;
                    }
                    let cardinality = label_postings.cardinality() as usize;
                    if cardinality == 0 {
                        continue;
                    }
                    result.push(LabelSearchResult {
                        value,
                        score,
                        cardinality,
                    });
                }
            }

            return Ok(apply_search_hints(result, search_hints));
        }

        let mut seen: AHashSet<String> = AHashSet::with_capacity(DEFAULT_LIMIT);
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
                // DO NOT use seen.insert(), as it would require an allocation even if the label value is already seen
                if seen.contains(label.value) {
                    continue;
                }
                let value = label.value.to_string();
                let (accepted, score) = filter.accept(&value);
                if accepted {
                    // fetch the cardinality
                    let label_postings = postings.postings_for_label_value(label_name, &value);
                    if label_postings.is_empty() {
                        continue;
                    }
                    let cardinality = label_postings.cardinality() as usize;
                    if cardinality == 0 {
                        continue;
                    }
                    result.push(LabelSearchResult {
                        value: value.clone(),
                        score,
                        cardinality,
                    });
                }
                seen.insert(value);
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
    ) -> TsdbResult<LabelQueryResult> {
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
    ) -> TsdbResult<LabelQueryResult> {
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(ctx, name, select_hints.as_ref(), hints)
    }

    fn get_metric_names(
        &self,
        ctx: &Context,
        select_hints: Option<SelectHints>,
        search_hints: Option<&SearchHints>,
    ) -> TsdbResult<LabelQueryResult> {
        let default_hints = SearchHints::default();
        let hints = search_hints.unwrap_or(&default_hints);
        collect_label_values(ctx, METRIC_NAME_LABEL, select_hints.as_ref(), hints)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FuzzyFilter, LabelQueryResult, Postings, SearchHints, SearchResultOrdering,
        SimilarityFilter, collect_unscoped_label_names, collect_unscoped_label_values,
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
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
        };

        let result = collect_unscoped_label_values(&postings, "region", &filter, &hints)
            .expect("expected unscoped fast path for value-asc ordering");

        assert!(!result.has_more);
        assert_eq!(values(result), vec!["eu-west", "us-east", "us-west"]);
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

        let result = collect_unscoped_label_values(&postings, "region", &filter, &hints)
            .expect("expected unscoped fast path for value-desc ordering");

        assert!(!result.has_more);
        assert_eq!(values(result), vec!["us-west", "us-east", "eu-west"]);
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

        let result = collect_unscoped_label_values(&postings, "region", &filter, &hints);

        assert!(result.is_none());
    }

    fn build_label_names_postings() -> Postings {
        let mut postings = Postings::default();
        // create one posting per label name; values are irrelevant for name-only scans
        postings.add_posting_for_label_value(1, "alpha", "v");
        postings.add_posting_for_label_value(2, "beta", "v");
        postings.add_posting_for_label_value(3, "gamma", "v");
        // add a distractor under a different label to ensure prefixing works
        postings
    }

    #[test]
    fn collect_unscoped_label_names_orders_ascending() {
        let postings = build_label_names_postings();
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueAsc,
        };

        let result = collect_unscoped_label_names(&postings, &filter, &hints)
            .expect("expected unscoped fast path for name-asc ordering");

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn collect_unscoped_label_names_orders_descending() {
        let postings = build_label_names_postings();
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ValueDesc,
        };

        let result = collect_unscoped_label_names(&postings, &filter, &hints)
            .expect("expected unscoped fast path for name-desc ordering");

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["gamma", "beta", "alpha"]);
    }

    #[test]
    fn collect_unscoped_label_names_returns_none_for_score_ordering() {
        let postings = build_label_names_postings();
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::ScoreDesc,
        };

        let result = collect_unscoped_label_names(&postings, &filter, &hints);

        assert!(result.is_none());
    }

    #[test]
    fn collect_unscoped_label_values_sets_has_more_when_truncated() {
        let postings = build_label_values_postings();
        let filter = SimilarityFilter::default();
        // limit of 2 when there are 3 region values → has_more must be true
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::ValueAsc,
        };

        let result = collect_unscoped_label_values(&postings, "region", &filter, &hints)
            .expect("expected unscoped fast path for value-asc ordering");

        assert!(result.has_more);
        assert_eq!(result.results.len(), 2);
    }

    fn build_label_names_cardinality_postings() -> Postings {
        let mut postings = Postings::default();
        // label 'b' -> cardinality 1
        postings.add_posting_for_label_value(1, "b", "v");
        // label 'a' -> cardinality 2
        postings.add_posting_for_label_value(2, "a", "v");
        postings.add_posting_for_label_value(3, "a", "v");
        // label 'c' -> cardinality 3
        postings.add_posting_for_label_value(4, "c", "v");
        postings.add_posting_for_label_value(5, "c", "v");
        postings.add_posting_for_label_value(6, "c", "v");
        postings
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_orders_ascending() {
        let postings = build_label_names_cardinality_postings();
        let filter = SimilarityFilter::default();
        let hints = SearchHints {
            filter: None,
            limit: 10,
            order_by: SearchResultOrdering::CardinalityAsc,
        };

        let result = collect_unscoped_label_names(&postings, &filter, &hints)
            .expect("expected unscoped fast path for name-cardinality-asc ordering");

        assert!(!result.has_more);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        // cardinalities: b=1, a=2, c=3 -> ascending by cardinality
        assert_eq!(values, vec!["b", "a", "c"]);
    }

    #[test]
    fn collect_unscoped_label_names_cardinality_sets_has_more_when_truncated() {
        let postings = build_label_names_cardinality_postings();
        let filter = SimilarityFilter::default();
        // limit of 2 when there are 3 label names -> has_more must be true
        let hints = SearchHints {
            filter: None,
            limit: 2,
            order_by: SearchResultOrdering::CardinalityAsc,
        };

        let result = collect_unscoped_label_names(&postings, &filter, &hints)
            .expect("expected unscoped fast path for name-cardinality-asc ordering");

        assert!(result.has_more);
        assert_eq!(result.results.len(), 2);
        let values: Vec<String> = result.into_iter().map(|r| r.value).collect();
        assert_eq!(values, vec!["b", "a"]);
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
}
