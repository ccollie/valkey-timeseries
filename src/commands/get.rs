use crate::series::{get_latest_compaction_sample, with_timeseries};
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// TS.GET key [LATEST]
pub fn get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 || args.len() > 3 {
        return Err(WrongArity);
    }

    let latest = if args.len() == 3 {
        let arg = args[2].as_slice();
        if arg.len() != 6 || !arg.eq_ignore_ascii_case("latest".as_ref()) {
            return Err(ValkeyError::Str("TSDB: wrong 3rd argument"));
        }
        true
    } else {
        false
    };

    let key = &args[1];
    let sample = with_timeseries(ctx, key, true, move |series| {
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
}
