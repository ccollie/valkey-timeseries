use crate::common::Timestamp;
use crate::module::arg_parse::parse_timestamp;
use crate::module::commands::{create_series, parse_series_options};
use crate::module::get_timeseries_mut;
use crate::series::{SampleAddResult, TimeSeries, TimeSeriesOptions};
use valkey_module::{Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

pub fn incrby(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    incr_decr(ctx, args, true)
}

pub fn decrby(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    incr_decr(ctx, args, false)
}

fn incr_decr(ctx: &Context, args: Vec<ValkeyString>, is_increment: bool) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args;
    let delta = args[2]
        .parse_float()
        .map_err(|_e| ValkeyError::Str("TSDB: invalid delta value"))?;

    let timestamp = handle_parse_timestamp(&mut args)?;

    let key_name = args[1].clone();
    if let Some(mut series) = get_timeseries_mut(ctx, &key_name, false)? {
        handle_update(ctx, &mut series, &key_name, timestamp, delta, is_increment)
    } else {
        let mut args = args.into_iter().skip(3).peekable();
        let options = parse_series_options(&mut args, TimeSeriesOptions::default(), &[])?;
        let mut series = create_series(&key_name, options, ctx)?;
        handle_update(ctx, &mut series, &key_name, timestamp, delta, is_increment)
    }
}

fn handle_parse_timestamp(args: &mut Vec<ValkeyString>) -> ValkeyResult<Option<Timestamp>> {
    if let Some(index) = args
        .iter()
        .position(|x| x.eq_ignore_ascii_case(b"timestamp"))
    {
        return if index < args.len() - 1 {
            args.remove(index);
            let timestamp_str = args.remove(index).to_string_lossy();
            let value = parse_timestamp(&timestamp_str)?;
            Ok(Some(value))
        } else {
            Err(ValkeyError::Str("TSDB: missing timestamp value"))
        };
    }
    Ok(None)
}

fn handle_update(
    ctx: &Context,
    series: &mut TimeSeries,
    key_name: &ValkeyString,
    timestamp: Option<Timestamp>,
    delta: f64,
    is_increment: bool,
) -> ValkeyResult {
    let mut delta = delta;
    if !series.is_empty() && !is_increment {
        delta = -delta;
    }
    let result = series.increment_sample_value(timestamp, delta)?;
    match result {
        SampleAddResult::Ok(ts) | SampleAddResult::Ignored(ts) => {
            let event = if is_increment {
                "ts.incrby"
            } else {
                "ts.decrby"
            };
            ctx.replicate_verbatim();
            ctx.notify_keyspace_event(NotifyEvent::MODULE, event, key_name);
            Ok(ValkeyValue::Integer(ts))
        }
        _ => Ok(ValkeyValue::Null),
    }
}
