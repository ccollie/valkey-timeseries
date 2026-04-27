use super::ts_label_search_fanout_command::LabelSearchFanoutCommand;
use crate::commands::command_parser::parse_metadata_command_args;
use crate::common::SortDir;
use crate::error_consts;
use crate::fanout::{is_clustered, FanoutClientCommand};
use crate::series::index::{LabelQuerier, FuzzyAlgorithm, FuzzyFilter, LabelNameSearchFilter, LabelSearcher, SearchHints, SearchResult, SearchResultOrdering, SortBy, SEARCH_RESULT_DEFAULT_LIMIT};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::commands::fanout::LabelSearchType;

#[derive(Debug, Clone)]
pub(crate) struct LabelNameSearchArgs {
    pub(crate) label: Option<String>,
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
            label: None,
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

impl LabelNameSearchArgs {
    pub fn build_search_hints(&self) -> ValkeyResult<SearchHints<'static>> {
        let order_by = self.get_order_by();
        let case_sensitive = !self.ignore_case;
        let limit = self.get_limit();

        let filter = if self.search_terms.is_empty() {
            None
        } else {
            Some(Box::new(LabelNameSearchFilter::new(
                self.search_terms.clone(),
                self.fuzz_algorithm,
                self.fuzz_threshold,
                !self.ignore_case,
            )) as Box<dyn FuzzyFilter>)
        };

        Ok(SearchHints {
            filter,
            limit,
            order_by,
            case_sensitive,
        })
    }

    pub fn get_order_by(&self) -> SearchResultOrdering {
        resolve_ordering(self.sort_by, self.sort_dir).unwrap_or(SearchResultOrdering::OrderByValueAsc)
    }

    pub fn get_limit(&self) -> usize {
        self.metadata.limit.unwrap_or(SEARCH_RESULT_DEFAULT_LIMIT)
    }
}

pub(crate) fn process_label_name_search_request(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<Vec<SearchResult>> {
    let querier = LabelQuerier::default();
    let raw_hints = parsed.build_search_hints()?;
    Ok(querier
        .search_label_names(ctx, &raw_hints, &parsed.metadata.matchers)
        .collect::<Vec<_>>())
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

    let mut parsed = parse_label_name_search_args(args, false)?;

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
        let operation = LabelSearchFanoutCommand::new(parsed, LabelSearchType::Name)?;
        return operation.exec(ctx);
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

fn parse_label_name_search_args(args: Vec<ValkeyString>, allow_label_name: bool) -> ValkeyResult<LabelNameSearchArgs> {
    let mut args = args.into_iter().skip(1).peekable();
    let mut parsed = LabelNameSearchArgs::default();
    let mut metadata_args: Vec<ValkeyString> = Vec::new();

    while let Some(arg) = args.next() {
        match parse_label_name_search_token(arg.as_slice()) {
            Some(LabelNameSearchToken::Label) => {
                if !allow_label_name {
                    return Err(ValkeyError::Str("TSDB: LABEL is not allowed in this context"));
                }
                let next_arg = args.next_string()?;
                parsed.label = Some(next_arg);
            }
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
    Label
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
        "label" => LabelNameSearchToken::Label
    }
}

fn resolve_ordering(
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

fn parse_bool(value: &str) -> ValkeyResult<bool> {
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
}
