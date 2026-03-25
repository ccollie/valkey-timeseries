use crate::commands::command_parser::parse_query_index_command_args;
use crate::commands::ts_queryindex_fanout_command::QueryIndexFanoutCommand;
use crate::fanout::{FanoutClientCommand, is_clustered};
use crate::series::index::series_keys_by_selectors;
use valkey_module::ValkeyError::WrongArity;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

pub fn ts_queryindex_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    let options = parse_query_index_command_args(&mut args)?;

    if is_clustered(ctx) {
        // in cluster mode, we need to send the request to all nodes
        let operation = QueryIndexFanoutCommand::new(options);
        return operation.exec(ctx);
    }

    let mut keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;

    keys.sort_unstable();
    Ok(ValkeyValue::from(keys))
}
