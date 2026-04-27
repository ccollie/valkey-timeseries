use crate::commands::fanout::LabelSearchType;
use crate::commands::parse_metadata_command_args;
use crate::common::SortDir;
use crate::error_consts;
use crate::series::index::{
    FuzzyAlgorithm, FuzzyFilter, LabelNameSearchFilter, LabelQuerier, LabelSearcher,
    SEARCH_RESULT_DEFAULT_LIMIT, SearchHints, SearchResult, SearchResultOrdering, SelectHints,
    SortBy,
};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

#[derive(Debug, Clone)]
pub(crate) struct LabelNameSearchArgs {
    pub(crate) search_type: LabelSearchType,
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
            search_type: LabelSearchType::Name,
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
        })
    }

    pub fn get_order_by(&self) -> SearchResultOrdering {
        resolve_ordering(self.sort_by, self.sort_dir)
            .unwrap_or(SearchResultOrdering::OrderByValueAsc)
    }

    pub fn get_limit(&self) -> usize {
        self.metadata.limit.unwrap_or(SEARCH_RESULT_DEFAULT_LIMIT)
    }
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
    Label,
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

pub(super) fn parse_label_name_search_args(
    args: Vec<ValkeyString>,
    search_type: LabelSearchType,
) -> ValkeyResult<LabelNameSearchArgs> {
    let mut args = args.into_iter().skip(1).peekable();
    let mut parsed = LabelNameSearchArgs::default();
    let mut metadata_args: Vec<ValkeyString> = Vec::new();

    let allow_label_name = matches!(search_type, LabelSearchType::Value);

    while let Some(arg) = args.next() {
        match parse_label_name_search_token(arg.as_slice()) {
            Some(LabelNameSearchToken::Label) => {
                if !allow_label_name {
                    return Err(ValkeyError::Str(
                        "TSDB: LABEL is not allowed in this context",
                    ));
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
    parsed.search_type = search_type;

    Ok(parsed)
}

pub(crate) fn process_label_search_request(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<Vec<SearchResult>> {
    let querier = LabelQuerier::default();
    let raw_hints = parsed.build_search_hints()?;
    let select_hints = if parsed.metadata.matchers.is_empty() && parsed.metadata.date_range.is_none() {
        None
    } else {
        Some(SelectHints::new(
            parsed.metadata.matchers.clone(),
            parsed.metadata.date_range,
        ))
    };
    match parsed.search_type {
        LabelSearchType::Name => Ok(querier.search_label_names(ctx, &raw_hints, select_hints)?),
        LabelSearchType::Value => {
            let label = parsed.label.as_ref().ok_or_else(|| {
                ValkeyError::Str("TSDB: label name must be provided for label value search")
            })?;
            Ok(querier.search_label_values(ctx, label.as_str(), &raw_hints, select_hints)?)
        }
        _ => Err(ValkeyError::Str(
            "TSDB: label search type not supported for label name search",
        )),
    }
}
