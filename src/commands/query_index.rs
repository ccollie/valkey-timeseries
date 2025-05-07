use crate::fanout::cluster::is_cluster_mode;
use crate::fanout::{perform_remote_index_query_request, IndexQueryResponse};
use crate::labels::matchers::Matchers;
use crate::labels::parse_series_selector;
use crate::series::index::series_keys_by_matchers;
use crate::series::request_types::MatchFilterOptions;
use crate::series::TimestampRange;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{
    BlockedClient, Context, NextArg, ThreadSafeContext, ValkeyResult, ValkeyString, ValkeyValue,
};

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

    if is_cluster_mode(ctx) {
        let options = MatchFilterOptions {
            date_range: None,
            matchers: matcher_list,
            limit: None,
        };
        // in cluster mode, we need to send the request to all nodes
        perform_remote_index_query_request(ctx, &options, on_query_index_request_done)?;
        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }
    let keys = series_keys_by_matchers(ctx, &matcher_list, None)?;

    Ok(ValkeyValue::from(keys))
}

pub fn process_query_index_request(
    ctx: &Context,
    filters: &[Matchers],
    range: Option<TimestampRange>,
) -> ValkeyResult<Vec<ValkeyString>> {
    series_keys_by_matchers(ctx, filters, range)
}

fn on_query_index_request_done(
    ctx: &ThreadSafeContext<BlockedClient>,
    results: Vec<IndexQueryResponse>,
) {
    let count = results.iter().map(|result| result.keys.len()).sum();
    let mut keys = Vec::with_capacity(count);
    for result in results.into_iter() {
        // Handle the results from the remote nodes
        keys.extend(result.keys.into_iter().map(|x| ValkeyValue::BulkString(x)));
    }
    // Handle the results from the remote nodes
    ctx.reply(Ok(ValkeyValue::Array(keys)));
}
