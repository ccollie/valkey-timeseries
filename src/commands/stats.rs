use crate::commands::parse_stats_command_args;
use crate::commands::stats_fanout_operation::StatsFanoutOperation;
use crate::fanout::{FanoutOperation, is_clustered};
use crate::series::index::get_timeseries_index;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString};

/// https://prometheus.io/docs/prometheus/latest/querying/api/#tsdb-stats
pub fn stats(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() > 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();
    let (label, limit) = parse_stats_command_args(&mut args)?;

    if is_clustered(ctx) {
        let operation = StatsFanoutOperation::new(limit, label);
        return operation.exec(ctx);
    }

    let index = get_timeseries_index(ctx);
    let selected_label = label.as_deref().unwrap_or("");
    let stats = index.stats(selected_label, limit);

    Ok(stats.into())
}
