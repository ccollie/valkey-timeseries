use crate::series::with_timeseries;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};
use valkey_module_macros::command;

/// TS.GET key
#[command(
    {
        name: "TS.GET",
        flags: [ReadOnly],
        arity: 2,
        key_spec: [
            {
                notes: "Get the latest sample from a time series",
                flags: [ReadOnly, Access],
                begin_search: Index({ index : 1 }),
                find_keys: Keynum({ key_num_idx : 0, first_key : 1, key_step : 0 }),
            }
        ]
    }
)]
pub fn get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if let Ok(key) = args.next_arg() {
        args.done()?;

        let sample = with_timeseries(ctx, &key, true, move |series| Ok(series.last_sample))?;
        Ok(match sample {
            Some(sample) => sample.into(),
            None => ValkeyValue::Array(vec![]),
        })
    } else {
        Err(WrongArity)
    }
}
