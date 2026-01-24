use crate::commands::command_args::parse_query_index_command_args;
use crate::commands::query_index_fanout_operation::QueryIndexFanoutOperation;
use crate::fanout::{FanoutOperation, is_clustered};
use crate::series::index::series_keys_by_selectors;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

pub fn query_index(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_query_index_command_args(&mut args)?;

    if is_clustered(ctx) {
        // in cluster mode, we need to send the request to all nodes
        let operation = QueryIndexFanoutOperation::new(options);
        return operation.exec(ctx);
    }

    let mut keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;

    keys.sort_unstable();
    Ok(ValkeyValue::from(keys))
}
