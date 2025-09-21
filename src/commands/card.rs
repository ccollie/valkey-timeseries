use super::card_fanout_operation::exec_cardinality_fanout_request;
use crate::commands::arg_parse::parse_metadata_command_args;
use crate::fanout::is_clustered;
use crate::labels::matchers::Matchers;
use crate::series::TimestampRange;
use crate::series::index::{
    get_cardinality_by_matchers_list, with_matched_series, with_timeseries_index,
};
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

///
/// TS.CARD [FILTER_BY_RANGE fromTimestamp toTimestamp] [FILTER filter...]
///
/// returns the number of unique time series that match a certain label set.
pub fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, false)?;

    if is_clustered(ctx) {
        if options.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.CARD in cluster mode requires at least one matcher",
            ));
        }

        return exec_cardinality_fanout_request(ctx, options);
    }
    let counter = calculate_cardinality(ctx, options.date_range, &options.matchers)?;

    Ok(ValkeyValue::from(counter))
}

pub fn calculate_cardinality(
    ctx: &Context,
    date_range: Option<TimestampRange>,
    matchers: &[Matchers],
) -> ValkeyResult<usize> {
    let count = match (date_range, matchers.is_empty()) {
        (None, true) => {
            // todo: check to see if user can read all keys, otherwise error
            // a bare TS.CARD is a request for the cardinality of the entire index
            with_timeseries_index(ctx, |index| index.count())
        }
        (None, false) => {
            // if we don't have a date range, we can simply count postings...
            with_timeseries_index(ctx, |index| {
                get_cardinality_by_matchers_list(index, matchers)
            })? as usize
        }
        (Some(_), false) => {
            let options = MatchFilterOptions {
                date_range,
                matchers: matchers.to_vec(),
                ..Default::default()
            };
            let mut counter = 0;
            with_matched_series(ctx, &mut counter, &options, |count: &mut usize, _, _| {
                *count += 1;
            })?;
            counter
        }
        _ => {
            // if we don't have a date range, we need at least one matcher, otherwise we
            // end up scanning the entire index
            return Err(ValkeyError::Str(
                "TSDB: TS.CARD requires at least one matcher or a date range",
            ));
        }
    };
    Ok(count)
}
