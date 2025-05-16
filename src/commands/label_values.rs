use crate::commands::arg_parse::parse_metadata_command_args;
use crate::error_consts;
use crate::fanout::cluster::is_clustered;
use crate::fanout::{perform_remote_label_values_request, LabelValuesRequest, LabelValuesResponse};
use crate::series::index::with_matched_series;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{
    AclPermissions, BlockedClient, Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult,
    ValkeyString, ValkeyValue,
};

// TS.LABELVALUES label [START fromTimestamp] [END fromTimestamp] [LIMIT limit] FILTER seriesMatcher...
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub fn label_values(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();
    let label_name = args.next_arg()?.to_string_lossy();
    let label_args = parse_metadata_command_args(&mut args, true)?;

    if is_clustered(ctx) {
        if label_args.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.LABELVALUES in cluster mode requires at least one matcher",
            ));
        }

        let options = LabelValuesRequest {
            label_name,
            range: label_args.date_range,
            filters: label_args.matchers,
        };
        // in cluster mode, we need to send the request to all nodes
        perform_remote_label_values_request(ctx, options, on_label_values_request_done)?;
        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }
    let mut names = process_label_values_request(ctx, &label_name, &label_args)?;
    names.sort();

    let label_values = names
        .into_iter()
        .map(ValkeyValue::BulkString)
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(label_values))
}

pub fn process_label_values_request(
    ctx: &Context,
    label_name: &str,
    options: &MatchFilterOptions,
) -> ValkeyResult<Vec<String>> {
    if label_name.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_LABEL_VALUE));
    }

    let mut names: BTreeSet<String> = BTreeSet::new();

    with_matched_series(
        ctx,
        &mut names,
        options,
        Some(AclPermissions::ACCESS),
        |acc, ts, _| {
            if let Some(label) = ts.get_label(label_name) {
                acc.insert(label.value.into());
            }
        },
    )?;

    let limit = options.limit.unwrap_or(names.len());
    let names = names.into_iter().take(limit).collect::<Vec<_>>();

    Ok(names)
}

fn on_label_values_request_done(
    ctx: &ThreadSafeContext<BlockedClient>,
    _req: LabelValuesRequest,
    res: Vec<LabelValuesResponse>,
) {
    let count = res.iter().map(|result| result.values.len()).sum();
    let mut values = Vec::with_capacity(count);
    for result in res.into_iter() {
        // Handle the results from the remote nodes
        values.extend(result.values.into_iter().map(ValkeyValue::BulkString));
    }
    // Sort the values
    values.sort_by(|a, b| {
        if let (ValkeyValue::BulkString(a), ValkeyValue::BulkString(b)) = (a, b) {
            a.cmp(b)
        } else {
            panic!("BUG: Unexpected value type in TS.LABELVALUES response");
        }
    });
    // Handle the results from the remote nodes
    ctx.reply(Ok(ValkeyValue::Array(values)));
}
