use crate::module::with_timeseries_mut;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

pub fn get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if let Ok(key) = args.next_arg() {
        with_timeseries_mut(ctx, &key, |series| {
            args.done()?;

            let result = if let Some(latest) = series.last_sample {
                vec![
                    ValkeyValue::from(latest.timestamp),
                    ValkeyValue::from(latest.value),
                ]
            } else {
                vec![]
            };

            Ok(ValkeyValue::Array(result))
        })
    } else {
        Err(WrongArity)
    }
}
