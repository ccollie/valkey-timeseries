use super::ts_labelnamesearch_fanout_command::LabelNameSearchFanoutCommand;
use crate::commands::command_parser::parse_metadata_command_args;
use crate::common::SortDir;
use crate::error_consts;
use crate::fanout::{is_clustered, FanoutClientCommand};
use crate::series::index::{
    BaseLabelQuerier, Filter, FuzzyAlgorithm, LabelSearcher, NoOpSearchFilter, SearchHints,
    SearchResult, SearchResultOrdering, SimilarityFilter, SortBy, SEARCH_RESULT_DEFAULT_LIMIT,
};
use crate::series::request_types::MatchFilterOptions;
use std::cmp::Ordering;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

#[derive(Debug, Clone)]
pub(crate) struct LabelNameSearchArgs {
    pub(crate) search_terms: Vec<String>,
    pub(crate) fuzz_threshold: f64,
    pub(crate) fuzz_algorithm: FuzzyAlgorithm,
    pub(crate) ignore_case: bool,
    pub(crate) include_score: bool,
    pub(crate) sort_by: Option<SortBy>,
    pub(crate) sort_dir: SortDir,
    pub(crate) metadata: MatchFilterOptions,
}

impl Default for LabelNameSearchArgs {
    fn default() -> Self {
        Self {
            search_terms: Vec::new(),
            fuzz_threshold: 0.0,
            fuzz_algorithm: FuzzyAlgorithm::JaroWinkler,
            ignore_case: false,
            include_score: false,
            sort_by: None,
            sort_dir: SortDir::Asc,
            metadata: Default::default(),
        }
    }
}

struct LabelNameSearchFilter {
    /// Pre-normalized terms (lowercased when case-insensitive) for substring matching.
    terms: Vec<String>,
    /// One `SimilarityFilter` per term; empty when `fuzz_threshold == 0`.
    similarity_filters: Vec<SimilarityFilter>,
    /// Retained for candidate normalization in the substring check.
    case_sensitive: bool,
}

impl LabelNameSearchFilter {
    fn new(
        terms: Vec<String>,
        algorithm: FuzzyAlgorithm,
        fuzz_threshold: f64,
        case_sensitive: bool,
    ) -> Self {
        let normalized_terms: Vec<String> = if case_sensitive {
            terms
        } else {
            terms.into_iter().map(|t| t.to_lowercase()).collect()
        };

        let similarity_filters = if fuzz_threshold == 0.0 {
            Vec::new()
        } else {
            normalized_terms
                .iter()
                .map(|term| {
                    SimilarityFilter::new_with_case_sensitivity(
                        term,
                        algorithm,
                        fuzz_threshold,
                        case_sensitive,
                    )
                })
                .collect()
        };

        Self {
            terms: normalized_terms,
            similarity_filters,
            case_sensitive,
        }
    }
}

impl Filter for LabelNameSearchFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.terms.is_empty() {
            return (true, 1.0);
        }

        // Normalize candidate once for the substring check.  SimilarityFilter handles
        // its own normalization internally, so we pass the original `value` to it.
        let normalized_value;
        let candidate = if self.case_sensitive {
            value
        } else {
            normalized_value = value.to_lowercase();
            &normalized_value
        };

        let mut accepted = false;
        let mut best_score: f64 = 0.0;

        for (idx, term) in self.terms.iter().enumerate() {
            if candidate.contains(term.as_str()) {
                // Exact substring match — perfect score.
                accepted = true;
                best_score = best_score.max(1.0);
                continue;
            }

            if !self.similarity_filters.is_empty() {
                // Fuzzy fallback — delegate to SimilarityFilter.
                let (sim_accepted, score) = self.similarity_filters[idx].accept(value);
                best_score = best_score.max(score);
                if sim_accepted {
                    accepted = true;
                }
            }
        }

        (accepted, best_score)
    }
}

fn compare_search_results(
    o: SearchResultOrdering,
) -> impl Fn(&SearchResult, &SearchResult) -> Ordering {
    fn value_asc(a: &SearchResult, b: &SearchResult) -> Ordering {
        a.value.cmp(&b.value)
    }

    fn value_desc(a: &SearchResult, b: &SearchResult) -> Ordering {
        b.value.cmp(&a.value)
    }

    fn score_desc(a: &SearchResult, b: &SearchResult) -> Ordering {
        match b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal) {
            Ordering::Equal => value_asc(a, b),
            other => other,
        }
    }

    match o {
        SearchResultOrdering::OrderByValueDesc => value_desc,
        SearchResultOrdering::OrderByScoreDesc => score_desc,
        SearchResultOrdering::OrderByValueAsc => value_asc,
    }
}

pub(crate) fn apply_label_name_search_ranking(
    values: Vec<String>,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<Vec<SearchResult>> {
    let hints = build_search_hints(parsed)?;
    let no_op = NoOpSearchFilter;
    let filter: &dyn Filter = hints.filter.as_deref().unwrap_or(&no_op);

    let mut results = Vec::with_capacity(values.len());
    for value in values {
        let (accepted, score) = filter.accept(&value);
        if accepted {
            results.push(SearchResult { value, score });
        }
    }

    match hints.order_by {
        SearchResultOrdering::OrderByValueAsc => {}
        SearchResultOrdering::OrderByValueDesc => results.reverse(),
        SearchResultOrdering::OrderByScoreDesc => {
            results.sort_by(compare_search_results(
                SearchResultOrdering::OrderByScoreDesc,
            ));
        }
    }

    if hints.limit > 0 && results.len() > hints.limit {
        results.truncate(hints.limit);
    }

    Ok(results)
}

pub(crate) fn process_label_name_search_request(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<Vec<SearchResult>> {
    let querier = BaseLabelQuerier::new();
    let raw_hints = SearchHints {
        filter: None,
        limit: 0,
        order_by: SearchResultOrdering::OrderByValueAsc,
        case_sensitive: !parsed.ignore_case,
    };

    let values = querier
        .search_label_names(ctx, &raw_hints, &parsed.metadata.matchers)
        .map(|r| r.value)
        .collect::<Vec<_>>();

    apply_label_name_search_ranking(values, parsed)
}

/// TS.LABELNAMESEARCH
/// [SEARCH term [term...]]
/// [FUZZ_THRESHOLD 0.0..1.0]
/// [FUZZ_ALG jarowinkler|subsequence]
/// [IGNORE_CASE true|false]
/// [INCLUDE_SCORE true|false]
/// [SORT_BY alpha|score]
/// [SORT_DIR asc|dsc]
/// [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
/// [LIMIT limit]
/// [FILTER seriesMatcher...]
pub fn ts_labelnamesearch_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }

    let mut parsed = parse_label_name_search_args(args)?;

    // Search APIs default to a bounded response to keep metadata discovery interactive.
    if parsed.metadata.limit.is_none() {
        parsed.metadata.limit = Some(SEARCH_RESULT_DEFAULT_LIMIT);
    }

    if is_clustered(ctx) {
        if parsed.metadata.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.LABELNAMESEARCH in cluster mode requires at least one matcher",
            ));
        }
        return LabelNameSearchFanoutCommand::new(parsed).exec(ctx);
    }

    let results = process_label_name_search_request(ctx, &parsed)?;
    let reply = if parsed.include_score {
        results
            .into_iter()
            .map(|r| {
                ValkeyValue::Array(vec![
                    ValkeyValue::BulkString(r.value),
                    ValkeyValue::BulkString(r.score.to_string()),
                ])
            })
            .collect::<Vec<_>>()
    } else {
        results
            .into_iter()
            .map(|r| ValkeyValue::BulkString(r.value))
            .collect::<Vec<_>>()
    };

    Ok(ValkeyValue::Array(reply))
}

fn parse_label_name_search_args(args: Vec<ValkeyString>) -> ValkeyResult<LabelNameSearchArgs> {
    let mut args = args.into_iter().skip(1).peekable();
    let mut parsed = LabelNameSearchArgs::default();
    let mut metadata_args: Vec<ValkeyString> = Vec::new();

    while let Some(arg) = args.next() {
        let token = arg.to_string_lossy();
        match parse_label_name_search_token(token.as_bytes()) {
            Some(LabelNameSearchToken::Search) => {
                let mut saw_term = false;
                while let Some(next) = args.peek() {
                    if is_search_token(next.as_slice()) {
                        break;
                    }
                    let next_arg = args.next_arg()?;
                    parsed.search_terms.push(next_arg.to_string_lossy());
                    saw_term = true;
                }
                if !saw_term {
                    return Err(ValkeyError::Str("TSDB: missing SEARCH value"));
                }
            }
            Some(LabelNameSearchToken::FuzzThreshold) => {
                let value = args.next_str()?;
                let threshold = value.parse::<f64>().map_err(|_| {
                    ValkeyError::Str("TSDB: FUZZ_THRESHOLD must be a number in [0.0..1.0]")
                })?;
                if !(0.0..=1.0).contains(&threshold) {
                    return Err(ValkeyError::Str(
                        "TSDB: FUZZ_THRESHOLD must be in [0.0..1.0]",
                    ));
                }
                parsed.fuzz_threshold = threshold;
            }
            Some(LabelNameSearchToken::FuzzAlg) => {
                let value = args.next_str()?;
                parsed.fuzz_algorithm =
                    FuzzyAlgorithm::try_from(value).map_err(ValkeyError::String)?;
            }
            Some(LabelNameSearchToken::IgnoreCase) => {
                let value = args.next_str()?;
                parsed.ignore_case = parse_bool(value)?;
            }
            Some(LabelNameSearchToken::IncludeScore) => {
                let value = args.next_str()?;
                parsed.include_score = parse_bool(value)?;
            }
            Some(LabelNameSearchToken::SortBy) => {
                let value = args.next_str()?;
                parsed.sort_by = Some(
                    SortBy::try_from(value)
                        .map_err(|_| ValkeyError::Str("TSDB: SORT_BY must be alpha or score"))?,
                );
            }
            Some(LabelNameSearchToken::SortDir) => {
                let value = args.next_str()?;
                parsed.sort_dir = SortDir::try_from(value)
                    .map_err(|_e| ValkeyError::Str("TSDB: SORT_DIR must be asc or desc"))?;
            }
            None => metadata_args.push(arg),
        }
    }

    let mut metadata_it = metadata_args.into_iter().skip(0).peekable();
    parsed.metadata = parse_metadata_command_args(&mut metadata_it, false)?;

    Ok(parsed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LabelNameSearchToken {
    Search,
    FuzzThreshold,
    FuzzAlg,
    IgnoreCase,
    IncludeScore,
    SortBy,
    SortDir,
}

fn parse_label_name_search_token(value: &[u8]) -> Option<LabelNameSearchToken> {
    hashify::tiny_map_ignore_case! {
        value,
        "search" => LabelNameSearchToken::Search,
        "fuzz_threshold" => LabelNameSearchToken::FuzzThreshold,
        "fuzz_alg" => LabelNameSearchToken::FuzzAlg,
        "ignore_case" => LabelNameSearchToken::IgnoreCase,
        "include_score" => LabelNameSearchToken::IncludeScore,
        "sort_by" => LabelNameSearchToken::SortBy,
        "sort_dir" => LabelNameSearchToken::SortDir,
    }
}

fn build_search_hints(
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<SearchHints<'static>> {
    let order_by = resolve_ordering(parsed.sort_by, parsed.sort_dir)?;

    let filter = if parsed.search_terms.is_empty() {
        None
    } else {
        Some(Box::new(LabelNameSearchFilter::new(
            parsed.search_terms.clone(),
            parsed.fuzz_algorithm,
            parsed.fuzz_threshold,
            !parsed.ignore_case,
        )) as Box<dyn Filter>)
    };

    Ok(SearchHints {
        filter,
        limit: parsed.metadata.limit.unwrap_or(SEARCH_RESULT_DEFAULT_LIMIT),
        order_by,
        case_sensitive: !parsed.ignore_case,
    })
}

pub(crate) fn resolve_ordering(
    sort_by: Option<SortBy>,
    sort_dir: SortDir,
) -> ValkeyResult<SearchResultOrdering> {
    match sort_by {
        None => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::OrderByValueAsc,
            SortDir::Desc => SearchResultOrdering::OrderByValueDesc,
        }),
        Some(SortBy::Alpha) => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::OrderByValueAsc,
            SortDir::Desc => SearchResultOrdering::OrderByValueDesc,
        }),
        Some(SortBy::Score) => match sort_dir {
            SortDir::Desc => Ok(SearchResultOrdering::OrderByScoreDesc),
            SortDir::Asc => Err(ValkeyError::Str(
                "TSDB: SORT_DIR asc is not supported for SORT_BY score",
            )),
        },
    }
}

pub(crate) fn parse_bool(value: &str) -> ValkeyResult<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(ValkeyError::Str(error_consts::INVALID_BOOLEAN)),
    }
}

fn is_search_token(value: &[u8]) -> bool {
    parse_label_name_search_token(value).is_some()
        || hashify::tiny_set_ignore_case!(value, "filter", "filter_by_range", "limit")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_name_search_filter_contains_match_scores_as_one() {
        let filter = LabelNameSearchFilter::new(
            vec!["node".to_string()],
            FuzzyAlgorithm::JaroWinkler,
            0.0,
            true,
        );

        let (accepted, score) = filter.accept("node_name");
        assert!(accepted);
        assert_eq!(1.0, score);
    }

    #[test]
    fn label_name_search_filter_case_insensitive_match() {
        let filter = LabelNameSearchFilter::new(
            vec!["NoDe".to_string()],
            FuzzyAlgorithm::JaroWinkler,
            0.0,
            false,
        );

        let (accepted, score) = filter.accept("node_name");
        assert!(accepted);
        assert_eq!(1.0, score);
    }

    #[test]
    fn resolve_ordering_for_alpha_desc() {
        let ordering = resolve_ordering(Some(SortBy::Alpha), SortDir::Desc).unwrap();
        assert_eq!(SearchResultOrdering::OrderByValueDesc, ordering);
    }

    #[test]
    fn resolve_ordering_rejects_score_asc() {
        let result = resolve_ordering(Some(SortBy::Score), SortDir::Asc);
        assert!(result.is_err());
    }

    #[test]
    fn apply_ranking_provides_scores_on_substring_match() {
        let args = LabelNameSearchArgs {
            search_terms: vec!["node".to_string()],
            ..Default::default()
        };
        let results =
            apply_label_name_search_ranking(vec!["node_name".to_string()], &args).unwrap();
        assert_eq!(1, results.len());
        assert_eq!("node_name", results[0].value);
        assert_eq!(1.0, results[0].score);
    }

    #[test]
    fn apply_ranking_include_score_flag_does_not_affect_result_values() {
        // include_score is a presentation concern; ranking output is identical.
        let args_with = LabelNameSearchArgs {
            search_terms: vec!["node".to_string()],
            include_score: true,
            ..Default::default()
        };
        let args_without = LabelNameSearchArgs {
            search_terms: vec!["node".to_string()],
            include_score: false,
            ..Default::default()
        };
        let values = vec!["node_name".to_string(), "node_count".to_string()];
        let with_results = apply_label_name_search_ranking(values.clone(), &args_with).unwrap();
        let without_results = apply_label_name_search_ranking(values, &args_without).unwrap();

        assert_eq!(with_results.len(), without_results.len());
        for (a, b) in with_results.iter().zip(without_results.iter()) {
            assert_eq!(a.value, b.value);
            assert_eq!(a.score, b.score);
        }
    }
}
