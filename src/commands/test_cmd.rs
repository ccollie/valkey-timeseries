use crate::commands::parse_metadata_command_args;
use crate::series::index::series_keys_by_matchers;
use valkey_module::{Context, ValkeyResult, ValkeyString, ValkeyValue};

/// Stub function for easier testing of arbitrary code
pub fn test_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_metadata_command_args(&mut args, true)?;

    let keys = series_keys_by_matchers(ctx, &options.matchers, None)?;

    Ok(ValkeyValue::from(keys))
}
