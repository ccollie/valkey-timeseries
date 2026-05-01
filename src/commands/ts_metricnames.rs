use crate::commands::fanout::LabelSearchType;
use crate::commands::label_search_utils::run_label_search;
use valkey_module::{Context, ValkeyResult, ValkeyString};

/// TS.METRICNAMES
/// [SEARCH term [term...]]
/// [FUZZY_THRESHOLD 0.0..1.0]
/// [FUZZY_ALGORITHM jarowinkler|subsequence]
/// [IGNORE_CASE true|false]
/// [INCLUDE_METADATA true|false]
/// [SORTBY <value|score|cardinality> [ASC|DESC]]
/// [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
/// [LIMIT limit]
/// [FILTER seriesMatcher...]
pub fn ts_metricnames_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    run_label_search(ctx, args, LabelSearchType::MetricName)
}
