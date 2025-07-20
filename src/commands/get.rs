use crate::series::{get_latest_compaction_sample, with_timeseries};
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.GET key [LATEST]
pub fn get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1);
    if let Ok(key) = args.next_arg() {
        let latest = match args.next_arg() {
            Ok(s) if s.eq_ignore_ascii_case("latest".as_ref()) => true,
            Ok(s) => {
                let msg = format!("TSDB: invalid argument \"{s}\"");
                return Err(ValkeyError::String(msg));
            }
            _ => return Err(WrongArity),
        };
        args.done()?;

        let sample = with_timeseries(ctx, &key, true, move |series| {
            Ok(if latest {
                get_latest_compaction_sample(ctx, series)
            } else {
                series.last_sample
            })
        })?;
        Ok(match sample {
            Some(sample) => sample.into(),
            None => ValkeyValue::Array(vec![]),
        })
    } else {
        Err(WrongArity)
    }
}
