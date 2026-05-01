use crate::commands::command_parser::parse_filter_by_range_options;
use crate::commands::fanout::LabelSearchType;
use crate::commands::ts_label_search_fanout_command::LabelSearchFanoutCommand;
use crate::common::SortDir;
use crate::common::threads::spawn;
use crate::error_consts;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::parser::series_selector::parse_series_selector;
use crate::series::index::{
    DefaultLabelQuerier, FuzzyAlgorithm, FuzzyFilter, LabelNameSearchFilter, LabelQuerier,
    LabelQueryResult, SEARCH_RESULT_DEFAULT_LIMIT, SEARCH_RESULT_LIMIT_MAX, SearchHints,
    SearchResultOrdering, SelectHints, SortBy,
};
use crate::series::request_types::MatchFilterOptions;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{
    Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

#[derive(Debug, Clone)]
pub(crate) struct LabelNameSearchArgs {
    pub(crate) search_type: LabelSearchType,
    pub(crate) label: Option<String>,
    pub(crate) search_terms: Vec<String>,
    pub(crate) fuzzy_threshold: f64,
    pub(crate) fuzzy_algorithm: FuzzyAlgorithm,
    pub(crate) ignore_case: bool,
    pub(crate) include_metadata: bool,
    pub(crate) sort_order: SearchResultOrdering,
    pub(crate) series_filter: MatchFilterOptions,
}

impl Default for LabelNameSearchArgs {
    fn default() -> Self {
        Self {
            search_type: LabelSearchType::Name,
            label: None,
            search_terms: Vec::new(),
            fuzzy_threshold: 0.0,
            fuzzy_algorithm: FuzzyAlgorithm::JaroWinkler,
            ignore_case: false,
            include_metadata: false,
            sort_order: SearchResultOrdering::ValueAsc,
            series_filter: Default::default(),
        }
    }
}

impl LabelNameSearchArgs {
    pub fn build_search_hints(&self) -> ValkeyResult<SearchHints<'static>> {
        self.validate()?;

        // ...existing code... (debug logging removed)

        let limit = self.get_limit();

        let filter = if self.search_terms.is_empty() {
            None
        } else {
            Some(Box::new(LabelNameSearchFilter::new(
                self.search_terms.clone(),
                self.fuzzy_algorithm,
                self.fuzzy_threshold,
                !self.ignore_case,
            )) as Box<dyn FuzzyFilter>)
        };

        Ok(SearchHints {
            filter,
            limit,
            order_by: self.sort_order,
            include_meta: self.include_metadata,
        })
    }

    pub fn get_limit(&self) -> usize {
        self.series_filter
            .limit
            .unwrap_or(SEARCH_RESULT_DEFAULT_LIMIT)
            .min(SEARCH_RESULT_LIMIT_MAX)
    }

    pub fn validate(&self) -> ValkeyResult<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LabelNameSearchToken {
    Search,
    FuzzyThreshold,
    FuzzyAlgorithm,
    IgnoreCase,
    IncludeMetadata,
    SortBy,
    Limit,
    Filter,
    FilterByRange,
}

fn parse_label_name_search_token(value: &[u8]) -> Option<LabelNameSearchToken> {
    hashify::tiny_map_ignore_case! {
        value,
        "search" => LabelNameSearchToken::Search,
        "fuzzy_threshold" => LabelNameSearchToken::FuzzyThreshold,
        "fuzzy_algo" => LabelNameSearchToken::FuzzyAlgorithm,
        "fuzzy_algorithm" => LabelNameSearchToken::FuzzyAlgorithm,
        "ignore_case" => LabelNameSearchToken::IgnoreCase,
        "include_metadata" => LabelNameSearchToken::IncludeMetadata,
        "sortby" => LabelNameSearchToken::SortBy,
        "limit" => LabelNameSearchToken::Limit,
        "filter" => LabelNameSearchToken::Filter,
        "filter_by_range" => LabelNameSearchToken::FilterByRange,
    }
}

fn resolve_label_search_ordering(
    sort_by: Option<SortBy>,
    sort_dir: SortDir,
) -> ValkeyResult<SearchResultOrdering> {
    match sort_by {
        None => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::ValueAsc,
            SortDir::Desc => SearchResultOrdering::ValueDesc,
        }),
        Some(SortBy::Value) => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::ValueAsc,
            SortDir::Desc => SearchResultOrdering::ValueDesc,
        }),
        Some(SortBy::Score) => match sort_dir {
            SortDir::Desc => Ok(SearchResultOrdering::ScoreDesc),
            SortDir::Asc => Err(ValkeyError::Str("TSDB: SORTBY score ASC is not supported")),
        },
        Some(SortBy::Cardinality) => Ok(match sort_dir {
            SortDir::Asc => SearchResultOrdering::CardinalityAsc,
            SortDir::Desc => SearchResultOrdering::CardinalityDesc,
        }),
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
}

pub(super) fn parse_label_name_search_args(
    args: Vec<ValkeyString>,
    search_type: LabelSearchType,
) -> ValkeyResult<LabelNameSearchArgs> {
    let mut args = args.into_iter().skip(1).peekable();
    let mut parsed = LabelNameSearchArgs::default();
    let mut sorting_specified = false;

    if search_type == LabelSearchType::Value {
        // For label value search, we require the label name to be specified as the first argument
        if args.peek().is_none() {
            return Err(ValkeyError::Str(
                "TSDB: Label name is required for label value search",
            ));
        }
        parsed.label = Some(args.next_string()?);
    }

    parsed.search_type = search_type;

    while let Some(arg) = args.next() {
        let Some(token) = parse_label_name_search_token(arg.as_slice()) else {
            return Err(ValkeyError::String(format!(
                "TSDB: unrecognized argument '{}'",
                arg.to_string_lossy()
            )));
        };
        match token {
            LabelNameSearchToken::Search => {
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
            LabelNameSearchToken::FuzzyThreshold => {
                let value = args.next_str()?;
                let threshold = value.parse::<f64>().map_err(|_| {
                    ValkeyError::Str("TSDB: FUZZY_THRESHOLD must be a number in [0.0..1.0]")
                })?;
                if !(0.0..=1.0).contains(&threshold) {
                    return Err(ValkeyError::Str(
                        "TSDB: FUZZY_THRESHOLD must be in [0.0..1.0]",
                    ));
                }
                parsed.fuzzy_threshold = threshold;
            }
            LabelNameSearchToken::FuzzyAlgorithm => {
                let value = args.next_str()?;
                parsed.fuzzy_algorithm =
                    FuzzyAlgorithm::try_from(value).map_err(ValkeyError::String)?;
            }
            LabelNameSearchToken::IgnoreCase => {
                let value = args.next_str()?;
                parsed.ignore_case = parse_bool(value)?;
            }
            LabelNameSearchToken::SortBy => {
                let value = args.next_str()?;
                let sort_by = SortBy::try_from(value).map_err(|_| {
                    ValkeyError::Str("TSDB: SORTBY must be value, score, or cardinality")
                })?;
                if let Some(dir) = args.peek()
                    && let Some(sort_dir) = SortDir::try_from(dir.as_slice()).ok()
                {
                    args.next();
                    parsed.sort_order = resolve_label_search_ordering(Some(sort_by), sort_dir)?;
                } else {
                    let order = if sort_by == SortBy::Score {
                        SearchResultOrdering::ScoreDesc
                    } else {
                        resolve_label_search_ordering(Some(sort_by), SortDir::Asc)?
                    };
                    parsed.sort_order = order;
                }
                sorting_specified = true;
            }
            LabelNameSearchToken::IncludeMetadata => {
                parsed.include_metadata = true;
            }
            LabelNameSearchToken::Limit => {
                let limit = args
                    .next_u64()
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_LIMIT_VALUE))?
                    as usize;
                parsed.series_filter.limit = Some(limit);
            }
            LabelNameSearchToken::Filter => {
                while let Some(next) = args.peek() {
                    if is_search_token(next.as_slice()) {
                        break;
                    }
                    let arg = args.next_str()?;
                    let selector = parse_series_selector(arg)
                        .map_err(|_| ValkeyError::Str(error_consts::MISSING_FILTER))?;
                    parsed.series_filter.matchers.push(selector);
                }
            }
            LabelNameSearchToken::FilterByRange => {
                let filter = parse_filter_by_range_options(&mut args)?;
                parsed.series_filter.date_range = Some(filter);
            }
        }
    }

    if !sorting_specified {
        // if the user specified a search term but didn't specify sorting, we default to sorting by relevance score
        if !parsed.search_terms.is_empty() {
            parsed.sort_order = SearchResultOrdering::ScoreDesc;
        } else {
            // otherwise, we default to sorting by value ascending
            parsed.sort_order = SearchResultOrdering::ValueAsc;
        }
    }
    // Search APIs default to a bounded response to keep metadata discovery interactive.
    if parsed.series_filter.limit.is_none() {
        parsed.series_filter.limit = Some(SEARCH_RESULT_DEFAULT_LIMIT);
    }

    Ok(parsed)
}

pub(crate) fn process_label_search_request(
    ctx: &Context,
    parsed: &LabelNameSearchArgs,
) -> ValkeyResult<LabelQueryResult> {
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
    query_result: LabelQueryResult,
    include_meta: bool,
) -> ValkeyValue {
    let results_array = if include_meta {
        ValkeyValue::Array(
            query_result
                .results
                .into_iter()
                .map(|r| {
                    ValkeyValue::Array(vec![
                        ValkeyValue::BulkString(r.value),
                        ValkeyValue::BulkString(r.score.to_string()),
                        ValkeyValue::Integer(r.cardinality as i64),
                    ])
                })
                .collect::<Vec<_>>(),
        )
    } else {
        ValkeyValue::Array(
            query_result
                .results
                .into_iter()
                .map(|r| ValkeyValue::BulkString(r.value))
                .collect::<Vec<_>>(),
        )
    };

    let mut map = HashMap::with_capacity(2);
    map.insert(
        ValkeyValueKey::BulkString("has_more".into()),
        ValkeyValue::Bool(query_result.has_more),
    );
    map.insert(ValkeyValueKey::BulkString("results".into()), results_array);
    ValkeyValue::Map(map)
}

pub(super) fn run_label_search(
    ctx: &Context,
    args: Vec<ValkeyString>,
    cmd_type: LabelSearchType,
) -> ValkeyResult {
    let parsed = parse_label_name_search_args(args, cmd_type)?;

    if is_clustered(ctx) {
        let operation = LabelSearchFanoutCommand::new(parsed)?;
        return operation.exec(ctx);
    }

    if parsed.series_filter.date_range.is_some() {
        let blocked_client = ctx.block_client();

        spawn(move || {
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            let ctx = thread_ctx.lock();
            let results = process_label_search_request(&ctx, &parsed)
                .map(|r| label_search_result_to_valkey_value(r, parsed.include_metadata));
            thread_ctx.reply(results);
        });

        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }

    let query_result = process_label_search_request(ctx, &parsed)?;
    Ok(label_search_result_to_valkey_value(
        query_result,
        parsed.include_metadata,
    ))
}
