use crate::commands::arg_parse::parse_metadata_command_args;
use crate::error_consts;
use crate::series::request_types::MatchFilterOptions;
use crate::series::index::with_matched_series;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{
    AclPermissions, Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
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
    
    let mut names = process_label_values_request(ctx, &label_name, &label_args)?;
    names.sort();

    let label_values = names
        .into_iter()
        .map(ValkeyValue::from)
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
    
    let mut names: Vec<String> = vec![];

    with_matched_series(
        ctx,
        &mut names,
        options,
        Some(AclPermissions::ACCESS),
        |acc, ts, _| {
            if let Some(label) = ts.get_label(label_name) {
                acc.push(label.value.into());
            }
        },
    )?;

    if let Some(limit) = options.limit {
        names.truncate(limit);
    }

    Ok(names)
}