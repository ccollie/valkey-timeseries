use crate::commands::arg_parse::parse_metadata_command_args;
use crate::series::index::with_matched_series;
use std::collections::BTreeSet;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

/// https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names
/// TS.LABELNAMES [START startTimestamp] [END endTimestamp] [LIMIT limit] FILTER seriesMatcher...
pub fn label_names(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, true)?;
    let limit = options.limit.unwrap_or(usize::MAX);

    let mut names: BTreeSet<String> = BTreeSet::new();

    with_matched_series(ctx, &mut names, &options, |acc, ts, _| {
        for label in ts.labels.iter() {
            acc.insert(label.name.into());
        }
    })?;

    let labels = names
        .into_iter()
        .take(limit)
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(labels))
}
