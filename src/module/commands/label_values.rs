use crate::error_consts;
use crate::module::arg_parse::parse_metadata_command_args;
use crate::series::index::with_matched_series;
use std::collections::BTreeSet;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

// TS.LABELVALUES label [START fromTimestamp] [END fromTimestamp] [LIMIT limit] FILTER seriesMatcher...
// https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values
pub fn label_values(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let label_name = args.next_arg()?.to_string_lossy();
    if label_name.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_LABEL_VALUE));
    }
    let label_args = parse_metadata_command_args(&mut args, true)?;
    let limit = label_args.limit.unwrap_or(usize::MAX);

    let mut names: BTreeSet<String> = BTreeSet::new();
    // todo: ensure ACL checks
    with_matched_series(ctx, &mut names, &label_args, move |acc, ts, _| {
        if let Some(label) = ts.get_label(&label_name) {
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
