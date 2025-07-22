use crate::aggregators::AggregationType;
use crate::commands::{parse_duration, CommandArgIterator};
use crate::parser::timestamp::parse_timestamp;
use crate::series::{
    check_new_rule_circular_dependency, get_timeseries_mut, CompactionRule, SeriesRef,
};
use valkey_module::{
    AclPermissions, Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
    VALKEY_OK,
};

///
/// TS.CREATERULE sourceKey destKey AGGREGATION aggregator bucketDuration [alignTimestamp]
///
/// Creates a compaction rule from sourceKey to destKey with a specific aggregator and bucket duration.
/// sourceKey must be different from destKey, and the user must be authorized to read from sourceKey and write to destKey.
///
pub fn create_rule(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Check for minimum number of arguments: command, sourceKey, destKey, AGGREGATION, aggregator, bucketDuration
    if args.len() < 6 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();

    // expect is fine here because we already checked the length above
    let source_key = args.next().expect("BUG: reading source_key in CREATERULE"); // Skip the command name
    let dest_key = args.next().expect("BUG: reading source_key in CREATERULE");

    // Validate that sourceKey is different from destKey
    if source_key == dest_key {
        return Err(ValkeyError::Str(
            "TSDB: source and destination key cannot be the same",
        ));
    }

    // Get source time series (must exist, writable)
    let mut source_series = get_timeseries_mut(
        ctx,
        &source_key,
        true,
        Some(AclPermissions::UPDATE),
    )?
    .expect(
        "BUG in create_rule: should have returned a value before this point (must_exist = true)",
    );

    let source_id = source_series.id;

    // Get destination time series (must exist, writable)
    let mut dest_series = get_timeseries_mut(ctx, &dest_key, true, Some(AclPermissions::UPDATE))?
        .expect(
        "BUG in create_rule: should have returned a value before this point (must_exist = true)",
    );

    let dest_id = dest_series.id;

    if source_series.is_compaction() {
        return Err(ValkeyError::Str(
            "TSDB: source series is already the destination of a compaction rule",
        ));
    }

    if dest_series.is_compaction() {
        return Err(ValkeyError::Str(
            "TSDB: destination series is already the destination of a compaction rule",
        ));
    }

    // Parse aggregation options
    let rule = parse_args(&mut args, dest_id)?;

    check_new_rule_circular_dependency(ctx, &mut source_series, &rule)?;

    // add or replace in the list of compaction rules in `source_series`
    if let Some(existing) = source_series
        .rules
        .iter_mut()
        .find(|rule| rule.dest_id == dest_id)
    {
        existing.align_timestamp = rule.align_timestamp;
        existing.bucket_duration = rule.bucket_duration;
        existing.aggregator = rule.aggregator;
    } else {
        source_series.rules.push(rule)
    }
    // Add the rule to the destination series
    dest_series.src_series = Some(source_id);

    // Replicate the command
    ctx.replicate_verbatim();

    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.createrule:src", &source_key);
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.createrule:dest", &dest_key);

    VALKEY_OK
}

fn parse_args(args: &mut CommandArgIterator, dest_id: SeriesRef) -> ValkeyResult<CompactionRule> {
    let aggregation = args.next_str()?;
    if !aggregation.eq_ignore_ascii_case("AGGREGATION") {
        return Err(ValkeyError::Str("TSDB: missing AGGREGATION keyword"));
    }
    let aggregation_type = AggregationType::try_from(args.next_str()?)?;
    let duration_str = args
        .next_str()
        .map_err(|_| ValkeyError::Str("TSDB: missing bucket duration"))?;
    let duration = parse_duration(duration_str)
        .map_err(|_| ValkeyError::Str("TSDB: invalid bucket duration"))?;
    let align_timestamp = if let Ok(align_str) = args.next_str() {
        parse_timestamp(align_str, false)
            .map_err(|_| ValkeyError::Str("TSDB: invalid align timestamp"))?
    } else {
        0
    };

    Ok(CompactionRule {
        dest_id,
        aggregator: aggregation_type.into(),
        bucket_duration: duration.as_millis() as u64,
        align_timestamp,
        bucket_start: None,
    })
}
