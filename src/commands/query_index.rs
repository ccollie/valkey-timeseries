use crate::labels::matchers::Matchers;
use crate::labels::parse_series_selector;
use crate::series::index::series_keys_by_matchers;
use crate::series::TimestampRange;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

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
    let keys = series_keys_by_matchers(ctx, &matcher_list, None)?;

    Ok(ValkeyValue::from(keys))
}


pub fn handle_query_index(
    ctx: &Context,
    filters: &[Matchers],
    range: Option<TimestampRange>,
) -> ValkeyResult<Vec<ValkeyString>> {
    series_keys_by_matchers(ctx, filters, range)
}