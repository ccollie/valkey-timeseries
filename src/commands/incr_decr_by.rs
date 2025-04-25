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

    let delta = parse_value_arg(&args[2])?;
    let timestamp = handle_parse_timestamp(&mut args)?;
    let key_name = &args[1];

    if let Some(mut series) = get_timeseries_mut(
        ctx,
        key_name,
        false,
        Some(AclPermissions::UPDATE | AclPermissions::ACCESS),
    )? {
        handle_update(ctx, &mut series, key_name, timestamp, delta, is_increment)
    } else {
        let key_name = args.remove(1);
        let mut args = args.into_iter().skip(2).peekable();
        let options = parse_series_options(&mut args, TimeSeriesOptions::from_config(), &[])?;
        create_and_store_series(ctx, &key_name, options)?; // todo: ACL ?

        if let Some(mut series) =
            get_timeseries_mut(ctx, &key_name, true, Some(AclPermissions::INSERT))?
        {
            handle_update(ctx, &mut series, &key_name, timestamp, delta, is_increment)
        } else {
            Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND))
        }
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
    let delta = if !is_increment { -delta } else { delta };

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
        SampleAddResult::Duplicate => Err(ValkeyError::Str(error_consts::DUPLICATE_SAMPLE_BLOCKED)),
        SampleAddResult::Error(err) => Err(ValkeyError::Str(err)),
        _ => {
            unreachable!("BUG: invalid return value from TimeSeries::add() in TS.INCRBY/TS.DECRBY")
        }
    }
}
