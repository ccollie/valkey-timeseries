use crate::commands::arg_parse::parse_metadata_command_args;
use crate::fanout::cluster::is_clustered;
use crate::fanout::{LabelNamesResponse, perform_remote_label_names_request};
use crate::series::index::with_matched_series;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{
    AclPermissions, BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
/// TS.LABELNAMES [START startTimestamp] [END endTimestamp] [LIMIT limit] FILTER seriesMatcher...
pub fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, true)?;

    if is_clustered(ctx) {
        if options.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.LABELNAMES in cluster mode requires at least one matcher",
            ));
        }
        // in cluster mode, we need to send the request to all nodes
        perform_remote_label_names_request(ctx, options, on_label_names_request_done)?;
        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }
    let mut names = process_label_names_request(ctx, &options)?;
    names.sort();

    let labels = names
        .into_iter()
        .map(ValkeyValue::BulkString)
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(labels))
}

pub fn process_label_names_request(
    ctx: &Context,
    options: &MatchFilterOptions,
) -> ValkeyResult<Vec<String>> {
    let mut names: BTreeSet<String> = BTreeSet::new();

    with_matched_series(
        ctx,
        &mut names,
        options,
        Some(AclPermissions::ACCESS),
        |acc, ts, _| {
            for label in ts.labels.iter() {
                acc.insert(label.name.into());
            }
        },
    )?;

    let limit = options.limit.unwrap_or(names.len());
    let names = names.into_iter().take(limit).collect::<Vec<_>>();

    Ok(names)
}

fn on_label_names_request_done(
    ctx: &ThreadSafeContext<BlockedClient>,
    _req: MatchFilterOptions,
    res: Vec<LabelNamesResponse>,
) {
    let count = res.iter().map(|result| result.names.len()).sum();
    let mut names = Vec::with_capacity(count);
    for result in res.into_iter() {
        let list = result.names.into_iter().map(ValkeyValue::BulkString);
        names.extend(list);
    }

    ctx.reply(Ok(ValkeyValue::Array(names)));
}
