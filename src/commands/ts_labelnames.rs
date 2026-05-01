use crate::commands::fanout::LabelSearchType;
use crate::commands::label_search_utils::run_label_search;
use valkey_module::{Context, ValkeyResult, ValkeyString};

// see https://github.com/tcp13equals2/proposals/blob/c56fb4b25f1151de148f58f0a51799c339185922/proposals/0074-new-labels-values-api.md

/// TS.LABELNAMES
/// [SEARCH term [term...]]
/// [FUZZY_THRESHOLD 0.0..1.0]
/// [FUZZY_ALGORITHM jarowinkler|subsequence]
/// [IGNORE_CASE true|false]
/// [INCLUDE_SCORE true|false]
/// [INCLUDE_META true|false]
/// [SORTBY <value|score|cardinality> [ASC|DESC]]
/// [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
/// [LIMIT limit]
/// [FILTER seriesMatcher...]
pub fn ts_labelnames_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    run_label_search(ctx, args, LabelSearchType::Name)
}
