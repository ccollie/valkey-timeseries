use crate::module::arg_parse::parse_timestamp_range;
use crate::series::with_timeseries_mut;
use valkey_module::{
    Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

///
/// TS.DEL key fromTimestamp toTimestamp
///
pub fn del(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let key = args.next_arg()?;

    let date_range = parse_timestamp_range(&mut args)?;
    let count = with_timeseries_mut(ctx, &key, |series| {
        let (start_ts, end_ts) = date_range.get_series_range(series, true);

        let deleted = series
            .remove_range(start_ts, end_ts)
            .map_err(|_e| ValkeyError::String("TS: error deleting range".to_string()))?; // todo: better error

        Ok(deleted)
    })?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.DEL", &key);

    Ok(ValkeyValue::from(count))
}
