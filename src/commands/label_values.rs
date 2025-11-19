use super::label_values_fanout_operation::LabelValuesFanoutOperation;
use crate::commands::arg_parse::parse_metadata_command_args;
use crate::error_consts;
use crate::fanout::{FanoutOperation, is_clustered};
use crate::series::index::with_matched_series;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

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

        // in cluster mode, we need to send the request to all nodes
        let operation = LabelValuesFanoutOperation::new(label_name, label_args);
        return operation.exec(ctx);
    }

    let names = process_label_values_request(ctx, &label_name, &label_args)?;

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

    with_matched_series(ctx, &mut names, options, |acc, ts, _| {
        if let Some(label) = ts.get_label(label_name) {
            acc.insert(label.value.into());
        }
    })?;

    let limit = options.limit.unwrap_or(names.len());
    let names = names.into_iter().take(limit).collect::<Vec<_>>();

    Ok(names)
}
