use super::ts_debug_configs::list_configs_cmd;
use super::utils::{
    reply_with_array, reply_with_bulk_string, reply_with_double, reply_with_str, reply_with_usize,
};
use crate::commands::CommandArgIterator;
use crate::common::string_interner::{BucketStats, InternedString, TopKEntry};
// use crate::config::get_config;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString};

/// Dumps a bucket's statistics to the reply.
fn dump_bucket(ctx: &Context, bucket: &BucketStats) {
    reply_with_array(ctx, 12);

    reply_with_str(ctx, "count");
    reply_with_usize(ctx, bucket.count);

    reply_with_str(ctx, "bytes");
    reply_with_usize(ctx, bucket.bytes);

    reply_with_str(ctx, "avgSize");
    reply_with_double(ctx, bucket.get_avg_size());

    reply_with_str(ctx, "allocated");
    reply_with_usize(ctx, bucket.allocated);

    reply_with_str(ctx, "avgAllocated");
    reply_with_double(ctx, bucket.get_avg_allocated());

    let utilization = bucket.get_utilization() * 100.0;
    reply_with_str(ctx, "utilization");
    reply_with_usize(ctx, utilization as usize);
}

/// Dumps a TopKEntry to the reply.
fn dump_top_k_entry(ctx: &Context, entry: &TopKEntry) {
    reply_with_array(ctx, 8);

    reply_with_str(ctx, "value");
    reply_with_bulk_string(ctx, &entry.value);

    reply_with_str(ctx, "refCount");
    reply_with_usize(ctx, entry.ref_count);

    reply_with_str(ctx, "bytes");
    reply_with_usize(ctx, entry.bytes);

    reply_with_str(ctx, "allocated");
    reply_with_usize(ctx, entry.allocated);
}

/// Returns statistics about the string pool.
///
/// TS._DEBUG STRINGPOOLSTATS
fn string_pool_stats(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    // Parse optional k parameter (default: 0 for backward compatibility)
    let k = if args.peek().is_some() {
        args.next_u64()? as usize
    } else {
        0
    };

    args.done()?;

    // todo: currently we're local only. Support cluster mode
    let stats = InternedString::get_stats_with_top_k(k);

    let arr_len = if k > 0 { 6 } else { 4 };
    reply_with_array(ctx, arr_len);

    // Reply[0] -> GlobalStats
    dump_bucket(ctx, &stats.total_stats);

    // Reply[1] -> ByRefcount
    reply_with_array(ctx, stats.by_ref_stats.len());
    for (&ref_count, bucket) in &stats.by_ref_stats {
        reply_with_array(ctx, 2);
        reply_with_usize(ctx, ref_count);
        dump_bucket(ctx, bucket);
    }

    // Reply[2] -> BySize
    reply_with_array(ctx, stats.by_size_stats.len());
    for (&size, bucket) in &stats.by_size_stats {
        reply_with_array(ctx, 2);
        reply_with_usize(ctx, size);
        dump_bucket(ctx, bucket);
    }

    // Reply[3] -> MemorySavings
    reply_with_array(ctx, 4);
    reply_with_str(ctx, "memorySavedBytes");
    reply_with_usize(ctx, stats.memory_saved_bytes);
    reply_with_str(ctx, "memorySavedPct");
    reply_with_double(ctx, stats.memory_saved_pct);

    if k > 0 {
        // Reply[4] -> TopK by RefCount
        reply_with_array(ctx, stats.top_k_by_ref.len());
        for entry in &stats.top_k_by_ref {
            dump_top_k_entry(ctx, entry);
        }

        // Reply[5] -> TopK by Size
        reply_with_array(ctx, stats.top_k_by_size.len());
        for entry in &stats.top_k_by_size {
            dump_top_k_entry(ctx, entry);
        }
    }

    Ok(())
}

/// Displays help text for the TS._DEBUG command.
fn help_cmd(ctx: &Context, args: &mut CommandArgIterator) -> ValkeyResult<()> {
    args.done()?;

    const HELP_TEXT: &[(&str, &str)] = &[
        ("TS._DEBUG SHOW_INFO", "Show Info Variable Information"),
        (
            "TS._DEBUG STRINGPOOLSTATS [TOPK]",
            "Show String Interner Stats",
        ),
        (
            "TS._DEBUG LIST_CONFIGS [VERBOSE] [APP|DEV|HIDDEN]",
            "List config names (default) or VERBOSE details, optionally filtered by visibility",
        ),
    ];

    reply_with_array(ctx, HELP_TEXT.len() * 2);
    for &(command, description) in HELP_TEXT {
        reply_with_bulk_string(ctx, command);
        reply_with_bulk_string(ctx, description);
    }

    Ok(())
}

/// Main entry point for TS._DEBUG command.
pub fn debug_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult<()> {
    // if !get_config().is_debug_mode_enabled {
    //     // Pretend like we don't exist.
    //     let mut msg = format!(
    //         "ERR unknown command '{}', with args beginning with:",
    //         args[0]
    //     );
    //     for arg in &args[1..] {
    //         msg.push_str(&format!(" '{}'", arg));
    //     }
    //     ctx.reply_error_string(msg.as_str());
    //     return Ok(());
    // }
    // skip the command name and parse the subcommand keyword
    let mut itr = args.into_iter().skip(1).peekable();

    let keyword = itr.next_str()?.to_ascii_uppercase();

    match keyword.as_str() {
        "STRINGPOOLSTATS" => string_pool_stats(ctx, &mut itr),
        "HELP" => help_cmd(ctx, &mut itr),
        "LIST_CONFIGS" => list_configs_cmd(ctx, &mut itr),
        _ => Err(ValkeyError::String(format!(
            "Unknown subcommand: {} try HELP subcommand",
            keyword
        ))),
    }
}
