use crate::commands::arg_parse::parse_integer_arg;
use crate::commands::stats_fanout_operation::StatsFanoutOperation;
use crate::fanout::{is_clustered, FanoutOperation};
use crate::series::index::with_timeseries_index;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

pub const DEFAULT_STATS_RESULTS_LIMIT: usize = 10;

/// https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats
pub fn stats(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let limit = match args.len() {
        0 => DEFAULT_STATS_RESULTS_LIMIT,
        1 => {
            let next = parse_integer_arg(&args[0], "LIMIT", false)?;
            if next > usize::MAX as i64 {
                return Err(ValkeyError::Str("TSDB: LIMIT too large"));
            } else if next == 0 {
                return Err(ValkeyError::Str("TSDB: LIMIT must be greater than 0"));
            }
            next as usize
        }
        _ => {
            return Err(ValkeyError::WrongArity);
        }
    };

    if is_clustered(ctx) {
        let operation = StatsFanoutOperation::new(limit);
        return operation.exec(ctx)
    }

    with_timeseries_index(ctx, |index| {
        let stats = index.stats("", limit);
        Ok(stats.into())
    })
}
