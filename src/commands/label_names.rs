use crate::commands::arg_parse::parse_metadata_command_args;
use crate::series::index::with_matched_series;
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString, ValkeyValue};
use crate::series::request_types::MatchFilterOptions;

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
/// TS.LABELNAMES [START startTimestamp] [END endTimestamp] [LIMIT limit] FILTER seriesMatcher...
pub fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, true)?;

    let mut names = process_label_names_request(ctx, &options)?;
    names.sort();

    let labels = names
        .into_iter()
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(labels))
}

pub fn process_label_names_request(
    ctx: &Context,
    options: &MatchFilterOptions
) -> ValkeyResult<Vec<String>> {
    let mut names: Vec<String> = vec![];

    with_matched_series(
        ctx,
        &mut names,
        options,
        Some(AclPermissions::ACCESS),
        |acc, ts, _| {
            for label in ts.labels.iter() {
                acc.push(label.name.into());
            }
        },
    )?;

    if let Some(limit) = options.limit {
        names.truncate(limit);
    }
    
    Ok(names)
}