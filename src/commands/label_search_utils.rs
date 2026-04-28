use crate::commands::fanout::LabelSearchType;
use crate::commands::parse_metadata_command_args;
use crate::commands::ts_label_search_fanout_command::LabelSearchFanoutCommand;
use crate::common::SortDir;
use crate::common::threads::spawn;
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::{
    DefaultLabelQuerier, FuzzyAlgorithm, FuzzyFilter, LabelNameSearchFilter, LabelQuerier,
    LabelSearchResult, SEARCH_RESULT_DEFAULT_LIMIT, SearchHints, SearchResultOrdering, SelectHints,
    SortBy,
};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{
    Context, NextArg, Status, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString,
    ValkeyValue,
};

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
    pub(crate) series_filter: MatchFilterOptions,
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
            series_filter: Default::default(),
        }
    }
}

impl LabelNameSearchArgs {
    pub fn build_search_hints(&self) -> ValkeyResult<SearchHints<'static>> {
        self.validate()?;

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
        resolve_ordering(self.sort_by, self.sort_dir).unwrap_or(SearchResultOrdering::ValueAsc)
    }

    pub fn get_limit(&self) -> usize {
        self.series_filter
            .limit
            .unwrap_or(SEARCH_RESULT_DEFAULT_LIMIT)
    }

    pub fn validate(&self) -> ValkeyResult<()> {
        if let Some(sort_by) = self.sort_by
            && sort_by == SortBy::Score
            && self.sort_dir == SortDir::Asc
        {
            return Err(ValkeyError::Str(
                "TSDB: SORT_DIR asc is not supported for SORT_BY score",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LabelNameSearchToken {
    Search,
    FuzzThreshold,
    FuzzAlgorithm,
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
        "fuzz_alg" => LabelNameSearchToken::FuzzAlgorithm,
        "fuzz_algo" => LabelNameSearchToken::FuzzAlgorithm,
        "ignore_case" => LabelNameSearchToken::IgnoreCase,
        "include_score" => LabelNameSearchToken::IncludeScore,
        "sort_by" => LabelNameSearchToken::SortBy,
        "sortby" => LabelNameSearchToken::SortBy,
        "sort_dir" => LabelNameSearchToken::SortDir,
        "sortdir" => LabelNameSearchToken::SortDir,
        "label" => LabelNameSearchToken::Label
    }
}

fn resolve_ordering(
    sort_by: Option<SortBy>,
    sort_dir: SortDir,
) -> ValkeyResult<SearchResultOrdering> {
    match sort_by {
        None => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::ValueAsc,
            SortDir::Desc => SearchResultOrdering::ValueDesc,
        }),
        Some(SortBy::Alpha) => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::ValueAsc,
            SortDir::Desc => SearchResultOrdering::ValueDesc,
        }),
        Some(SortBy::Score) => match sort_dir {
            SortDir::Desc => Ok(SearchResultOrdering::ScoreDesc),
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
            Some(LabelNameSearchToken::FuzzAlgorithm) => {
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

    parsed.series_filter = parse_metadata_command_args(&mut args, false)?;
    parsed.search_type = search_type;

    // Search APIs default to a bounded response to keep metadata discovery interactive.
    if parsed.series_filter.limit.is_none() {
        parsed.series_filter.limit = Some(SEARCH_RESULT_DEFAULT_LIMIT);
    }

    Ok(parsed)
}

pub(crate) fn process_label_search_request(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<Vec<LabelSearchResult>> {
    let querier = DefaultLabelQuerier;
    let raw_hints = parsed.build_search_hints()?;
    let select_hints =
        if parsed.series_filter.matchers.is_empty() && parsed.series_filter.date_range.is_none() {
            None
        } else {
            Some(SelectHints::new(
                parsed.series_filter.matchers.clone(),
                parsed.series_filter.date_range,
            ))
        };
    match parsed.search_type {
        LabelSearchType::Name => {
            Ok(querier.get_label_names(ctx, select_hints, Some(&raw_hints))?)
        }
        LabelSearchType::Value => {
            let label = parsed.label.as_ref().ok_or({
                ValkeyError::Str("TSDB: label name must be provided for label value search")
            })?;
            Ok(querier.get_label_values(ctx, label.as_str(), select_hints, Some(&raw_hints))?)
        }
        LabelSearchType::MetricName => {
            Ok(querier.get_metric_names(ctx, select_hints, Some(&raw_hints))?)
        }
    }
}

pub(super) fn label_search_result_to_valkey_value(
    results: Vec<LabelSearchResult>,
    include_score: bool,
) -> ValkeyValue {
    if include_score {
        ValkeyValue::Array(
            results
                .into_iter()
                .map(|r| {
                    ValkeyValue::Array(vec![
                        ValkeyValue::BulkString(r.value),
                        ValkeyValue::BulkString(r.score.to_string()),
                    ])
                })
                .collect::<Vec<_>>(),
        )
    } else {
        ValkeyValue::Array(
            results
                .into_iter()
                .map(|r| ValkeyValue::BulkString(r.value))
                .collect::<Vec<_>>(),
        )
    }
}

pub(super) fn reply_label_search_results(
    ctx: &Context,
    results: Vec<LabelSearchResult>,
    include_score: bool,
) -> Status {
    let converted = label_search_result_to_valkey_value(results, include_score);
    ctx.reply(Ok(converted))
}

pub(super) fn run_label_search(
    ctx: &Context,
    args: Vec<ValkeyString>,
    cmd_type: LabelSearchType,
) -> ValkeyResult {
    let parsed = parse_label_name_search_args(args, LabelSearchType::Name)?;

    if is_clustered(ctx) {
        let name = match cmd_type {
            LabelSearchType::Name => "TS.LABELNAMESEARCH",
            LabelSearchType::Value => "TS.LABELVALUESEARCH",
            LabelSearchType::MetricName => "TS.METRICNAMES",
        };
        if parsed.series_filter.matchers.is_empty() {
            return Err(ValkeyError::String(format!(
                "{name} in cluster mode requires at least one matcher"
            )));
        }
        let operation = LabelSearchFanoutCommand::new(parsed)?;
        return operation.exec(ctx);
    }

    // todo: if we have series filter with date range, put this onto a thread so we don't block the main thread with a potentially long search.
    if parsed.series_filter.date_range.is_some() {
        let blocked_client = ctx.block_client();

        spawn(move || {
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            let ctx = thread_ctx.lock();
            let results = process_label_search_request(&ctx, &parsed)
                .map(|r| label_search_result_to_valkey_value(r, parsed.include_score));
            thread_ctx.reply(results);
        });

        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }

    let results = process_label_search_request(ctx, &parsed)?;
    Ok(label_search_result_to_valkey_value(
        results,
        parsed.include_score,
    ))
}
