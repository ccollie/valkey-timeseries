use crate::commands::query_index_fanout_operation::exec_index_query_fanout_request;
use crate::fanout::is_clustered;
use crate::labels::parse_series_selector;
use crate::series::index::series_keys_by_selectors;
use crate::series::request_types::MatchFilterOptions;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

pub fn query_index(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    let mut matcher_list = Vec::with_capacity(args.len());
    while let Ok(arg) = args.next_str() {
        let matchers = parse_series_selector(arg)?;
        matcher_list.push(matchers);
    }
    if matcher_list.is_empty() {
        return Err(WrongArity);
    }

    if is_clustered(ctx) {
        let options = MatchFilterOptions {
            date_range: None,
            matchers: matcher_list,
            limit: None,
        };
        // in cluster mode, we need to send the request to all nodes
        return exec_index_query_fanout_request(ctx, options);
    }
    let keys = series_keys_by_selectors(ctx, &matcher_list, None)?;

    Ok(ValkeyValue::from(keys))
}
