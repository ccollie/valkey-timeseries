use crate::series::get_timeseries_mut;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK};

///
/// TS.DELETERULE sourceKey destKey
///
/// Deletes a compaction rule.
/// The user must be authorized to write to both sourceKey and destKey.
/// The rule is removed from the sourceKey, and the src_series field in destKey is cleared, but
/// the destination series is not deleted.
///
pub fn delete_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Check for minimum number of arguments: command, sourceKey, destKey
    if args.len() != 3 {
        return Err(ValkeyError::WrongArity);
    }

    let source_key = &args[1];
    let dest_key = &args[2];

    // Get source time series (must exist, writable)
    let mut source_series = get_timeseries_mut(ctx, source_key, true, Some(AclPermissions::UPDATE))?
        .expect("BUG in delete_rule: should have returned a value before this point (must_exist = true)");

    // Get destination time series (must exist, writable)
    let mut dest_series = get_timeseries_mut(ctx, dest_key, true, Some(AclPermissions::UPDATE))?
        .expect("BUG in delete_rule: should have returned a value before this point (must_exist = true)");

    let dest_id = dest_series.id;
    
    if dest_series.src_series.unwrap_or_default() != source_series.id {
        return Err(ValkeyError::Str("TSDB: source series is not the source of the compaction rule"));
    }

    // Check if the rule exists
    let rule_index = source_series.rules.iter().position(|rule| rule.dest_id == dest_id);
    
    if let Some(index) = rule_index {
        // Remove the rule from the source series
        source_series.rules.remove(index);
        
        // Clear the src_series field in the destination series
        dest_series.src_series = None;
        
        // Replicate the command
        ctx.replicate_verbatim();
        
        VALKEY_OK
    } else {
        Err(ValkeyError::Str("TSDB: compaction rule does not exist"))
    }
}