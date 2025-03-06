use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};
use crate::module::arg_parse::parse_metadata_command_args;
use crate::series::index::{get_cardinality_by_matchers_list, with_matched_series, with_timeseries_index};

pub fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let options = parse_metadata_command_args(args, true)?;
    let mut counter: usize = 0;

    // if we don't have a date range, we can simply count postings...
    if options.date_range.is_none() {
        counter = with_timeseries_index(ctx, |index| {
            get_cardinality_by_matchers_list(index, &options.matchers)
        })? as usize;
    } else {
        with_matched_series(ctx, &mut counter, &options, |count, _, _| { *count += 1; })?;
    }
    Ok(ValkeyValue::from(counter as i64))
}
