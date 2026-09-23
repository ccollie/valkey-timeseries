//! Internal command a `STORE` write is replicated as.
//!
//! The analysis commands that can `STORE` their output compute it on the primary only and
//! replicate the resulting write (see `store_target.rs`), as
//! `TS._STORE key <samples> [store options...]`: `<samples>` is the encoded input to the write,
//! and the options are the client's `STORE` clause tokens after the key, `MERGE` included. The
//! replica applies them with the same primitive the primary used, so both end in the same state
//! (series ids aside, which are always node-local). The same command lands in the AOF.

use crate::commands::command_parser::{CommandArgIterator, parse_store_options};
use crate::commands::store_target::decode_store_samples;
use crate::common::context::is_real_user_client;
use crate::config::is_debug_mode_enabled;
use crate::series::create_or_update_series_with_samples;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// `TS._STORE key <samples> [store options...]`
///
/// Internal-only: fed by replication and AOF replay. Like `TS._RESTORE`, a direct client call is
/// rejected unless `ts.debug-mode` is enabled — it would write a series with no ACL or key-spec
/// checks beyond the command's own.
pub fn ts_store_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if is_real_user_client(ctx) && !is_debug_mode_enabled() {
        return Err(ValkeyError::Str(
            "ERR TS._STORE is an internal command and cannot be invoked directly",
        ));
    }
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args: CommandArgIterator = args.into_iter().skip(1).peekable();
    let key = args.next_arg()?;
    let payload = args.next_arg()?;
    let samples = decode_store_samples(payload.as_slice())?;
    let (options, write_mode) = parse_store_options(&mut args)?;
    args.done()?;

    let outcome = create_or_update_series_with_samples(ctx, &key, options, write_mode, &samples)?;
    Ok(ValkeyValue::Integer(outcome.written as i64))
}
