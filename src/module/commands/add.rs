use crate::common::Timestamp;
use crate::module::arg_parse::parse_timestamp;
use crate::module::commands::create_series::create_series;
use crate::module::commands::parse_series_options;
use crate::module::{get_timeseries_mut, VK_TIME_SERIES_TYPE};
use crate::series::{SampleAddResult, TimeSeriesOptions};
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

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
    let value = args[3].parse_float()?;

    if let Some(mut guard) = get_timeseries_mut(ctx, &args[1], false)? {
        // args.done()?;
        let series = guard.get_series_mut();
        return match series.add(timestamp, value, None) {
            SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => {
                let timestamp = if timestamp_str == "*" { Some(ts) } else { None };
                replicate_and_notify(ctx, args, timestamp);
                Ok(ValkeyValue::Integer(ts))
            }
            _ => Ok(ValkeyValue::Null),
        };
    }

    let original_args = args.clone();
    let mut args = args.into_iter().skip(4).peekable();

    let options = parse_series_options(&mut args, TimeSeriesOptions::default(), &[])?;

    let key = &original_args[1];
    let mut ts = create_series(key, options, ctx)?;

    match ts.add(timestamp, value, None) {
        SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => {
            let redis_key = ValkeyKeyWritable::open(ctx.ctx, key);
            redis_key.set_value(&VK_TIME_SERIES_TYPE, ts)?;

            replicate_and_notify(ctx, original_args, Some(timestamp));
            Ok(ValkeyValue::Integer(ts))
        }
        _ => Ok(ValkeyValue::Null),
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
