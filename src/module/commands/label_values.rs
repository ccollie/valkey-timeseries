use crate::module::arg_parse::parse_metadata_command_args;
use crate::series::index::with_matched_series;
use std::collections::BTreeSet;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

// TS.LABEL_VALUES label [FILTER seriesMatcher] [START fromTimestamp] [END fromTimestamp]
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub fn label_values(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let label_args = parse_metadata_command_args(args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let mut names: BTreeSet<String> = BTreeSet::new();
    with_matched_series(ctx, &mut names, &label_args, |acc, ts, _| {
        for label in ts.labels.iter() {
            acc.insert(label.value.into());
        }
    })?;

    let label_values = names
        .into_iter()
        .take(limit)
        .map(ValkeyValue::from)
        .collect::<Vec<_>>();

    Ok(ValkeyValue::Array(label_values))
}
