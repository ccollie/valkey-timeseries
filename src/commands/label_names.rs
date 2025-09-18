use super::label_names_fanout_operation::exec_label_names_fanout_request;
use crate::commands::{
    arg_parse::parse_metadata_command_args, fanout::generated::LabelNamesResponse,
};
use crate::fanout::is_clustered;
use crate::series::index::with_matched_series;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{
    AclPermissions, BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
/// TS.LABELNAMES [FILTER_BY_RANGE fromTimestamp toTimestamp] [LIMIT limit] FILTER seriesMatcher...
pub fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, true)?;

    if is_clustered(ctx) {
        if options.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.LABELNAMES in cluster mode requires at least one matcher",
            ));
        }

        return exec_label_names_fanout_request(ctx, options);
    }
    let mut names = process_label_names_request(ctx, &options)?;

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