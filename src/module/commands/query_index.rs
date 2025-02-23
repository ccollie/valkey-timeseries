use crate::labels::parse_series_selector;
use crate::series::index::{get_keys_by_matchers, with_timeseries_index};
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
    let keys = with_timeseries_index(ctx, move |index| {
        get_keys_by_matchers(ctx, index, &matcher_list)
    })?;

    Ok(ValkeyValue::from(keys))
}
