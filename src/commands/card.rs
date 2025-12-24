use super::card_fanout_operation::CardFanoutOperation;
use crate::commands::arg_parse::parse_metadata_command_args;
use crate::fanout::{FanoutOperation, is_clustered};
use crate::series::index::count_matched_series;
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
        let operation = CardFanoutOperation::new(options);
        return operation.exec(ctx);
    }

    let counter = count_matched_series(ctx, options.date_range, &options.matchers)?;

    Ok(ValkeyValue::from(counter))
}
