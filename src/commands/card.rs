use crate::commands::arg_parse::parse_metadata_command_args;
use crate::series::index::{
    get_cardinality_by_matchers_list, with_matched_series, with_timeseries_index,
};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.CARD [START fromTimestamp] [END toTimestamp] [FILTER filter...]
///
/// returns the number of unique time series that match a certain label set.
pub fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, false)?;
    let mut counter: usize = 0;

    match (options.date_range, options.matchers.is_empty()) {
        (None, true) => {
            // a bare TS.CARD is a request for the cardinality of the entire index
            counter = with_timeseries_index(ctx, |index| index.count());
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            counter = with_timeseries_index(ctx, |index| {
                get_cardinality_by_matchers_list(index, &options.matchers)
            })? as usize;
        }
        (Some(_), false) => {
            with_matched_series(ctx, &mut counter, &options, |count, _, _| {
                *count += 1;
            })?;
        }
        _ => {
            // if we don't have a date range, we need at least one matcher, otherwise we
            // end up scanning the entire index
            return Err(ValkeyError::Str(
                "TSDB: TS.CARD requires at least one matcher or a date range",
            ));
        }
    }

    Ok(ValkeyValue::from(counter))
}
