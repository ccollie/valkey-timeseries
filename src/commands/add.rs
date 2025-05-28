use crate::commands::arg_parse::{parse_timestamp, parse_value_arg};
use crate::commands::parse_series_options;
use crate::common::Timestamp;
use crate::error_consts;
use crate::series::{
    create_and_store_series, get_timeseries_mut, SampleAddResult, TimeSeries, TimeSeriesOptions,
};
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

///
/// TS.ADD key timestamp value
///     [RETENTION duration]
///     [DUPLICATE_POLICY policy]
///     [ON_DUPLICATE policy_ovr]
///     [ENCODING <COMPRESSED|UNCOMPRESSED>]
///     [CHUNK_SIZE chunkSize]
///     [METRIC metric | LABELS labelName labelValue ...]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///     [LABELS label1=value1 label2=value2 ...]
///
pub fn add(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let timestamp_str = args[2].try_as_str()?;
    let timestamp = parse_timestamp(timestamp_str)?;

    let value = parse_value_arg(&args[3])?;

    if let Some(mut guard) = get_timeseries_mut(ctx, &args[1], false, Some(AclPermissions::UPDATE))?
    {
        // args.done()?;
        let series = guard.get_series_mut();
        return handle_add(ctx, series, args, timestamp, timestamp_str, value);
    }

    // clones because of replicate_and_notify
    let original_args = args.clone();
    let mut args = args.into_iter().skip(4).peekable();

    let options = parse_series_options(&mut args, TimeSeriesOptions::from_config(), &[])?;

    let key = &original_args[1];
    create_and_store_series(ctx, key, options)?; // todo: ACL ?

    if let Some(mut series) = get_timeseries_mut(ctx, key, true, Some(AclPermissions::INSERT))? {
        handle_add(
            ctx,
            &mut series,
            original_args,
            timestamp,
            timestamp_str,
            value,
        )
    } else {
        Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND))
    }
}

fn handle_add(
    ctx: &Context,
    series: &mut TimeSeries,
    args: Vec<ValkeyString>,
    timestamp: Timestamp,
    timestamp_str: &str,
    value: f64,
) -> ValkeyResult {
    match series.add(timestamp, value, None) {
        SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => {
            let timestamp = if timestamp_str == "*" { Some(ts) } else { None };
            replicate_and_notify(ctx, args, timestamp);
            Ok(ValkeyValue::Integer(ts))
        }
        other => other.into(),
    }
}

fn replicate_and_notify(ctx: &Context, args: Vec<ValkeyString>, timestamp: Option<Timestamp>) {
    if let Some(ts) = timestamp {
        // "*" could have a completely different value on a replica, so send the current value instead
        let ts_str = ts.to_string();
        let mut args = args;
        args.remove(0);
        args[1] = ctx.create_string(ts_str.as_bytes());
        let replication_args = args.iter().collect::<Vec<_>>();
        ctx.replicate("TS.ADD", &*replication_args);
        let key = args.swap_remove(0);
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.ADD", &key);
    } else {
        ctx.replicate_verbatim();
        ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.ADD", &args[2]);
    }
}
