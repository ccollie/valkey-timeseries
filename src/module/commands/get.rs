use crate::module::with_timeseries;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};

pub fn get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if let Ok(key) = args.next_arg() {
        let sample = with_timeseries(ctx, &key, true, move |series| Ok(series.last_sample))?;
        Ok(match sample {
            Some(sample) => sample.into(),
            None => ValkeyValue::Array(vec![]),
        })
    } else {
        Err(WrongArity)
    }
}
