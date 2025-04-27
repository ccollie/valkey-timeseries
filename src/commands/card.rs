use crate::commands::arg_parse::parse_metadata_command_args;
use crate::series::index::{
    get_cardinality_by_matchers_list, with_matched_series, with_timeseries_index,
};
use valkey_module::{AclPermissions, BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};
use crate::commands::arg_types::MatchFilterOptions;
use crate::fanout::{perform_card, CardinalityRequest, CardinalityResponse};
use crate::fanout::cluster::is_cluster_mode;
use crate::labels::matchers::Matchers;
use crate::series::TimestampRange;

///
/// TS.CARD [START fromTimestamp] [END toTimestamp] [FILTER filter...]
///
/// returns the number of unique time series that match a certain label set.
pub fn cardinality(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let mut options = parse_metadata_command_args(&mut args, false)?;

    if is_cluster_mode(ctx) {
        if options.matchers.is_empty() {
            return Err(ValkeyError::Str(
                "TS.CARD in cluster mode requires at least one matcher",
            ));
        }
        let filter = options.matchers.pop().unwrap();
        // in cluster mode, we need to send the request to all nodes
        let request = CardinalityRequest {
            range: options.date_range,
            filter,
        };
        perform_card(ctx, &request, on_cardinality_request_done);
        // We will reply later, from the thread
        return Ok(ValkeyValue::NoReply);
    }
    let counter = calculate_cardinality(ctx, options.date_range, &options.matchers)?;

    Ok(ValkeyValue::from(counter))
}


pub fn calculate_cardinality(
    ctx: &Context,
    date_range: Option<TimestampRange>,
    matchers: &[Matchers],
) -> ValkeyResult<usize> {
    const PERMISSIONS: Option<AclPermissions> = Some(AclPermissions::ACCESS);

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
            with_matched_series(
                ctx,
                &mut counter,
                &options,
                PERMISSIONS,
                |count: &mut usize, _, _| {
                    *count += 1;
                },
            )?;
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

fn on_cardinality_request_done(ctx: &ThreadSafeContext<BlockedClient>, res: &[CardinalityResponse]) {
    let count: usize = res.iter().map(|r| r.count).sum();
    ctx.reply(Ok(ValkeyValue::from(count)));
}