use crate::aggregators::{calc_bucket_start, AggregationHandler, AggregationType, Aggregator};
use crate::common::parallel::{Parallel, ParallelExt};
use crate::common::rdb::{rdb_load_timestamp, rdb_save_timestamp};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::parser::parse_positive_duration_value;
use crate::series::index::{get_series_by_id, get_series_key_by_id};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesGuardMut, SeriesRef, TimeSeries};
use get_size::GetSize;
use smallvec::SmallVec;
use std::sync::{Arc, Mutex};
use valkey_module::{
    raw, BlockedClient, Context, NotifyEvent, ThreadSafeContext, ValkeyError, ValkeyResult,
};

const PARALLEL_THRESHOLD: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub end_ts: Option<Timestamp>,
}

impl GetSize for CompactionRule {
    fn get_size(&self) -> usize {
        self.dest_id.get_size()
            + self.aggregator.get_size()
            + self.bucket_duration.get_size()
            + self.align_timestamp.get_size()
            + self.end_ts.get_size()
    }
}

impl CompactionRule {
    pub fn save_to_rdb(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.dest_id);
        self.aggregator.save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        rdb_save_timestamp(rdb, self.align_timestamp);
        rdb_save_timestamp(rdb, self.end_ts.unwrap_or(-1));
    }

    pub fn load_from_rdb(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let bucket_end = rdb_load_timestamp(rdb)?;
        let end_ts = if bucket_end == -1 {
            None
        } else {
            Some(bucket_end)
        };

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
            end_ts,
        })
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        calc_bucket_start(ts, self.align_timestamp, self.bucket_duration)
    }

    fn calc_bucket_end(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        ts + (delta - (diff % delta))
    }

    fn get_bucket_range(&self, ts: Timestamp) -> (Timestamp, Timestamp) {
        let start = self.calc_bucket_start(ts);
        let end = start + self.bucket_duration as i64;
        (start, end)
    }

    pub fn calculate_range(
        &self,
        start: Timestamp,
        end: Timestamp,
        series: &TimeSeries,
    ) -> (Timestamp, Timestamp) {
        let iter = series.range_iter(start, end);
        let mut aggregator = self.aggregator.clone();
        aggregator.reset();
        for sample in iter {
            aggregator.update(sample.value);
        }
        (start, end)
    }

    fn reset(&mut self) {
        self.aggregator.reset();
        self.end_ts = None;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionPolicy {
    pub aggregator: AggregationType,
    pub bucket_duration_ms: u64,
    pub retention_ms: u64,
    pub align_timestamp: Timestamp,
}

/// A compaction policy is a set of rules that define how to compact time series data.
/// The value from configuration is expected to be in the format:
///  `aggregation_type:bucket_duration:retention:[align_timestamp]`
///
/// E.g., `avg:60:2h:0`
///
/// Where:
///  - `aggregation_type` is the type of aggregation to use (e.g., `avg`, `sum`, etc.)
///  - `bucket_duration` is the duration of each bucket in seconds
///  - `retention` is the retention period in seconds
///  - The `align_timestamp` is optional and defaults to 0 if not provided.
///
fn parse_compaction_policy(val: &str) -> ValkeyResult<CompactionPolicy> {
    let args: Vec<&str> = val.split(':').collect();
    if args.len() < 3 || args.len() > 4 {
        let msg = format!(
            "TSDB: invalid compaction policy format: '{val}'. Expected: aggregation_type:bucket_duration:retention[:align_timestamp]",
        );
        return Err(ValkeyError::String(msg));
    }
    let aggregator = AggregationType::try_from(args[0])?;
    let Ok(bucket_duration_ms) = parse_positive_duration_value(args[1]) else {
        return Err(ValkeyError::String(
            "TSDB: invalid bucket duration in compaction policy".to_string(),
        ));
    };
    if bucket_duration_ms == 0 {
        return Err(ValkeyError::String(
            "TSDB: compaction policy bucket duration cannot be zero".to_string(),
        ));
    }
    let Ok(retention_ms) = parse_positive_duration_value(args[2]) else {
        return Err(ValkeyError::Str(
            "TSDB: Invalid retention in compaction policy",
        ));
    };
    let align_timestamp = if args.len() == 4 {
        match parse_positive_duration_value(args[3]) {
            Ok(ts) => ts,
            _ => {
                return Err(ValkeyError::String(
                    "Invalid align timestamp in compaction policy".to_string(),
                ))
            }
        }
    } else {
        0 // Default align timestamp
    };
    if align_timestamp >= bucket_duration_ms {
        return Err(ValkeyError::Str(
            "TSDB: align timestamp must be less than than bucket duration",
        ));
    }

    Ok(CompactionPolicy {
        aggregator,
        bucket_duration_ms: bucket_duration_ms as u64,
        retention_ms: retention_ms as u64,
        align_timestamp,
    })
}

pub(crate) fn parse_compaction_policies(val: &str) -> ValkeyResult<Vec<CompactionPolicy>> {
    let policies: Vec<&str> = val.split(';').collect();
    if policies.is_empty() {
        return Err(ValkeyError::String(
            "No compaction policies provided".to_string(),
        ));
    }
    let mut resolved_policies: Vec<CompactionPolicy> = Vec::with_capacity(policies.len());
    for policy in policies.iter() {
        if policy.is_empty() {
            return Err(ValkeyError::String(
                "Empty compaction policy found".to_string(),
            ));
        }
        let parsed_policy = parse_compaction_policy(policy)?;
        resolved_policies.push(parsed_policy);
    }
    Ok(resolved_policies)
}

#[derive(Clone)]
struct CompactionWorker {
    errors: Arc<Mutex<SmallVec<TsdbError, 4>>>,
}

impl CompactionWorker {
    fn new() -> Self {
        Self {
            errors: Arc::new(Mutex::new(SmallVec::new())),
        }
    }
}

impl Parallel for CompactionWorker {
    fn create(&self) -> Self {
        Self {
            /* fields */
            errors: self.errors.clone(),
        }
    }

    fn merge(&mut self, _other: Self) {
        // state is shared behind an Arc, so no need to merge
    }
}

pub fn run_compaction_parallel(
    ctx: &Context,
    series: &mut TimeSeries,
    sample: Sample,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }

    let last_ts = series.last_timestamp();

    if sample.timestamp < last_ts {
        run_parallel(
            ctx,
            series,
            sample,
            |thread_ctx, parent, dest, rule, value| {
                handle_compaction_upsert(thread_ctx, parent, rule, dest, value)
            },
        )
    } else {
        run_parallel(
            ctx,
            series,
            sample,
            |thread_ctx, parent, dest_series, rule, sample| {
                handle_sample_compaction(thread_ctx, parent, rule, dest_series, sample)
            },
        )
    }
}

fn null_ts_filter(ts: Timestamp) -> bool {
    ts != 0
}

/// Handle compaction for a genuinely new sample (timestamp > last sample timestamp)
fn handle_sample_compaction(
    ctx: &ThreadSafeContext<BlockedClient>,
    series: &TimeSeries,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    sample: Sample,
) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let bucket_start = rule.calc_bucket_start(ts);
    let bucket_end = bucket_start + rule.bucket_duration as i64;

    let Some(current_end_ts) = rule.end_ts else {
        // First sample for this rule - initialize the aggregation
        rule.end_ts = Some(bucket_end);
        rule.aggregator.update(sample.value);
        return Ok(());
    };

    let current_bucket_start = current_end_ts.saturating_sub(rule.bucket_duration as i64);

    if bucket_start == current_bucket_start {
        // Sample belongs to the current aggregation bucket
        rule.aggregator.update(sample.value);
    } else if bucket_start > current_bucket_start {
        // Sample starts a new bucket - finalize the current bucket first
        finalize_current_bucket_and_start_new(ctx, rule, dest_series, sample, bucket_end)?;
    } else {
        // Sample is in an older bucket (shouldn't happen for new samples, but handle gracefully)
        recalculate_bucket(
            ctx,
            series,
            rule,
            dest_series,
            bucket_start,
            bucket_end,
            null_ts_filter,
        )?;
    }

    Ok(())
}

/// Finalize the current aggregation bucket and start a new one
fn finalize_current_bucket_and_start_new(
    ctx: &ThreadSafeContext<BlockedClient>,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    new_sample: Sample,
    new_bucket_end: Timestamp,
) -> TsdbResult<()> {
    // Finalize the current bucket
    let aggregated_value = rule.aggregator.finalize();
    let current_bucket_start = rule
        .end_ts
        .unwrap_or(new_bucket_end)
        .saturating_sub(rule.bucket_duration as i64);
    add_dest_bucket(
        ctx,
        dest_series,
        rule,
        current_bucket_start,
        aggregated_value,
    )?;

    // Start a new bucket with the new sample
    rule.aggregator.reset();
    rule.aggregator.update(new_sample.value);
    rule.end_ts = Some(new_bucket_end);

    Ok(())
}

/// Handle upsert compaction for a destination series
/// This is called when a sample is being inserted/updated with a timestamp that's <= the last sample timestamp
fn handle_compaction_upsert(
    ctx: &ThreadSafeContext<BlockedClient>,
    series: &TimeSeries,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    sample: Sample,
) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let bucket_start = rule.calc_bucket_start(ts);
    let bucket_end = bucket_start + rule.bucket_duration as i64;

    // Check if this affects the current ongoing aggregation bucket
    if let Some(current_end_ts) = rule.end_ts {
        let current_bucket_start = current_end_ts.saturating_sub(rule.bucket_duration as i64);

        if bucket_start == current_bucket_start {
            // This sample belongs to the current aggregation bucket
            // We need to recalculate the entire bucket since we don't know what changed
            recalculate_current_bucket(series, rule, current_bucket_start, current_end_ts)?;
            return Ok(());
        }
    }

    // This is a historical upsert - need to recalculate the affected bucket
    recalculate_bucket(
        ctx,
        series,
        rule,
        dest_series,
        bucket_start,
        bucket_end,
        null_ts_filter,
    )
}

/// Recalculate the current ongoing aggregation bucket
fn recalculate_current_bucket(
    series: &TimeSeries,
    rule: &mut CompactionRule,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
) -> TsdbResult<()> {
    // Reset the aggregator and recalculate from all samples in the bucket
    rule.aggregator.reset();

    for sample in series.range_iter(bucket_start, bucket_end) {
        rule.aggregator.update(sample.value);
    }

    Ok(())
}

/// Recalculate a historical bucket and update the destination series
fn recalculate_bucket<F>(
    ctx: &ThreadSafeContext<BlockedClient>,
    series: &TimeSeries,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
    filter: F,
) -> TsdbResult<()>
where
    F: Fn(Timestamp) -> bool,
{
    // Create a new aggregator for this bucket
    let mut bucket_aggregator = rule.aggregator.clone();
    bucket_aggregator.reset();

    // Aggregate all samples in this bucket
    let mut has_samples = false;
    let sample_iter = series
        .range_iter(bucket_start, bucket_end)
        .filter(|sample| filter(sample.timestamp));

    for sample in sample_iter {
        bucket_aggregator.update(sample.value);
        has_samples = true;
    }

    if has_samples {
        let aggregated_value = bucket_aggregator.finalize();
        add_dest_bucket(ctx, dest_series, rule, bucket_start, aggregated_value)?;
    } else {
        // No samples in this bucket anymore, remove it from destination
        dest_series.remove_range(bucket_start, bucket_start)?;
    }

    Ok(())
}

/// When a range of samples is removed, we need to remove samples in the corresponding
/// rule destination series that overlap with the range.
///
/// We need to handle the following scenarios:
/// - `Single Bucket Partial Removal`: When the removal range affects only part of a single aggregation bucket,
///   it recalculates the aggregation for the remaining samples.
/// - `Multiple Bucket Removal`: When the removal spans multiple buckets, we need to handle each bucket appropriately:
///   completely removing middle buckets and recalculating partial buckets at the boundaries.
/// - `Complete Bucket Removal`: When entire buckets are removed, remove the corresponding aggregated
///   samples from the destination series.
/// - `Current Aggregation State`: If there's an ongoing aggregation (indicated by `hi_ts`), adjust the
///   current aggregation state to account for the removed samples.
/// - `Error Handling`: Properly handle cases where destination series are missing or inaccessible.
///
fn handle_compaction_range_removal(
    ctx: &ThreadSafeContext<BlockedClient>,
    series: &TimeSeries,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<()> {
    // Calculate the bucket boundaries that are affected by the removed range
    let first_bucket_start = rule.calc_bucket_start(start);
    let last_bucket_start = rule.calc_bucket_start(end);

    // Handle different scenarios based on how the removal affects buckets
    if first_bucket_start == last_bucket_start {
        // Scenario 1: Removal is within a single bucket - need to recalculate that bucket
        handle_single_bucket_removal(
            ctx,
            series,
            rule,
            dest_series,
            first_bucket_start,
            start,
            end,
        )
    } else {
        // Scenario 2: Removal spans multiple buckets
        handle_multiple_bucket_removal(
            ctx,
            series,
            rule,
            dest_series,
            first_bucket_start,
            last_bucket_start,
            start,
            end,
        )
    }
}

fn handle_single_bucket_removal(
    ctx: &ThreadSafeContext<BlockedClient>,
    series: &TimeSeries,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    bucket_start: Timestamp,
    removal_start: Timestamp,
    removal_end: Timestamp,
) -> TsdbResult<()> {
    let bucket_end = bucket_start + rule.bucket_duration as i64;

    // Check if the entire bucket is being removed
    if removal_start <= bucket_start && removal_end >= bucket_end {
        // Remove the entire bucket from destination
        dest_series.remove_range(bucket_start, bucket_end)?;
        return Ok(());
    }

    recalculate_bucket(
        ctx,
        series,
        rule,
        dest_series,
        bucket_start,
        bucket_end,
        |ts| ts < removal_start || ts > removal_end,
    )
}

#[allow(clippy::too_many_arguments)]
fn handle_multiple_bucket_removal(
    ctx: &ThreadSafeContext<BlockedClient>,
    series: &TimeSeries,
    rule: &mut CompactionRule,
    dest_series: &mut TimeSeries,
    first_bucket_start: Timestamp,
    last_bucket_start: Timestamp,
    removal_start: Timestamp,
    removal_end: Timestamp,
) -> TsdbResult<()> {
    let mut current_bucket_start = first_bucket_start;

    while current_bucket_start <= last_bucket_start {
        let bucket_end = current_bucket_start + rule.bucket_duration as i64;

        // Determine the overlap between this bucket and the removal range
        let overlap_start = removal_start.max(current_bucket_start);
        let overlap_end = removal_end.min(bucket_end);

        if overlap_start <= overlap_end {
            if current_bucket_start == first_bucket_start
                || current_bucket_start == last_bucket_start
            {
                // The first or last bucket might be partial
                handle_single_bucket_removal(
                    ctx,
                    series,
                    rule,
                    dest_series,
                    current_bucket_start,
                    removal_start,
                    removal_end,
                )?;
            } else {
                // Middle buckets are completely removed
                dest_series.remove_range(current_bucket_start, current_bucket_start)?;
            }
        }

        current_bucket_start += rule.bucket_duration as i64;
    }

    Ok(())
}

/// Handle the case where range removal affects the current aggregation bucket
fn handle_current_bucket_adjustment(
    series: &TimeSeries,
    rule: &mut CompactionRule,
    removal_start: Timestamp,
    removal_end: Timestamp,
) {
    // If we have an active aggregation bucket
    let Some(end_ts) = rule.end_ts else {
        // No active aggregation, nothing to adjust
        return;
    };

    let current_bucket_start = end_ts.saturating_sub(rule.bucket_duration as i64);
    let current_bucket_end = end_ts;

    // Check if the removal affects the current aggregation bucket
    if removal_start < current_bucket_end && removal_end > current_bucket_start {
        // The current bucket is affected, we need to recalculate
        let mut new_aggregator = rule.aggregator.clone();
        new_aggregator.reset();

        // Re-aggregate samples that are not in the removal range
        let bucket_samples = series
            .get_range(current_bucket_start, current_bucket_end)
            .into_iter()
            .filter(|sample| sample.timestamp < removal_start || sample.timestamp > removal_end);

        for sample in bucket_samples {
            new_aggregator.update(sample.value);
        }

        rule.aggregator = new_aggregator;
    }
}

fn get_destination_series(ctx: &'_ Context, dest_id: SeriesRef) -> Option<SeriesGuardMut<'_>> {
    if let Ok(Some(res)) = get_series_by_id(ctx, dest_id, false, None) {
        if res.is_compaction() {
            return Some(res);
        }
    };
    ctx.log_verbose("Destination series for compaction not found or not a compaction series");
    None
}

fn get_compaction_series<'a>(
    ctx: &'a Context,
    series: &mut TimeSeries,
) -> SmallVec<SeriesGuardMut<'a>, 4> {
    if series.rules.is_empty() {
        return SmallVec::new();
    }

    let mut missing: SmallVec<_, 4> = SmallVec::new();
    let mut destinations: SmallVec<_, 4> = SmallVec::new();

    for rule in series.rules.iter() {
        if let Some(dest_series) = get_destination_series(ctx, rule.dest_id) {
            // Destination series exists, add it to the list
            destinations.push(dest_series);
        } else {
            // Destination series doesn't exist, mark rule for removal
            missing.push(rule.dest_id);
        }
    }

    if !missing.is_empty() {
        series.rules.retain(|r| !missing.contains(&r.dest_id));
    }
    destinations
}

fn run_parallel<F>(ctx: &Context, series: &mut TimeSeries, value: Sample, f: F) -> TsdbResult<()>
where
    F: Fn(
            &ThreadSafeContext<BlockedClient>,
            &TimeSeries,
            &mut TimeSeries,
            &mut CompactionRule,
            Sample,
        ) -> TsdbResult<()>
        + Send
        + Sync,
{
    let destinations = get_compaction_series(ctx, series);

    if series.rules.is_empty() {
        // No destination series available, nothing to do
        return Ok(());
    }

    fn run_internal<F>(
        thread_ctx: &ThreadSafeContext<BlockedClient>,
        series: &TimeSeries,
        rules: &mut [CompactionRule],
        child_series: SmallVec<SeriesGuardMut, 4>,
        value: Sample,
        f: F,
    ) -> TsdbResult<()>
    where
        F: Fn(
                &ThreadSafeContext<BlockedClient>,
                &TimeSeries,
                &mut TimeSeries,
                &mut CompactionRule,
                Sample,
            ) -> TsdbResult<()>
            + Send
            + Sync,
    {
        let destinations = rules.iter_mut().zip(child_series).collect::<Vec<_>>();

        let mut worker = CompactionWorker::new();
        worker.maybe_par(PARALLEL_THRESHOLD, destinations, |worker, item| {
            let rule = item.0;
            let mut dest_guard = item.1;
            match f(thread_ctx, series, &mut dest_guard, rule, value) {
                Ok(_) => {}
                Err(error) => {
                    // todo: handle error properly
                    let log_ctx = thread_ctx.lock();
                    let msg = format!(
                        "Failed to handle compaction rule for series {}: {}",
                        dest_guard.id, error
                    );
                    log_ctx.log_warning(&msg);
                    let mut lock = worker.errors.lock().unwrap();
                    lock.push(error);
                }
            }
        });

        let errors = worker.errors.lock().unwrap();
        let Some(first_error) = errors.first().cloned() else {
            // No errors, we can safely return
            return Ok(());
        };

        Err(first_error)
    }

    let mut rules = std::mem::take(&mut series.rules);
    let thread_ctx = ThreadSafeContext::with_blocked_client(ctx.block_client());
    let res = run_internal(&thread_ctx, series, &mut rules, destinations, value, f);
    series.rules = rules;
    // todo: recurse into child series if needed
    res
}

fn add_dest_bucket(
    ctx: &ThreadSafeContext<BlockedClient>,
    dest_series: &mut TimeSeries,
    rule: &mut CompactionRule,
    ts: Timestamp,
    value: f64,
) -> TsdbResult<()> {
    let bucket_start = rule.calc_bucket_start(ts);
    // Add the sample to the destination series
    match dest_series.add(bucket_start, value, Some(DuplicatePolicy::KeepLast)) {
        SampleAddResult::Ok(_) => {
            let ctx = ctx.lock();
            let Some(key) = get_series_key_by_id(&ctx, dest_series.id) else {
                ctx.log_verbose("Destination series key not found for compaction notification");
                return Ok(());
            };
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add:dest", &key);
            Ok(())
        }
        SampleAddResult::Ignored(_) => Ok(()), // duplicate sample, ignore it
        SampleAddResult::TooOld => {
            // bucket start is too old, we cannot add it
            let ctx = ctx.lock();
            ctx.log_verbose("Sample is too old for compaction rule, ignoring");
            Ok(())
        }
        SampleAddResult::Duplicate => {
            let ctx = ctx.lock();
            ctx.log_verbose("Duplicate sample detected in compaction rule, ignoring");
            Ok(())
        }
        x => {
            let base_msg = format!("TSDB: failed to add sample @{ts} to destination bucket: {x}",);
            Err(TsdbError::General(base_msg))
        }
    }
}

impl TimeSeries {
    pub fn add_compaction_rule(&mut self, rule: CompactionRule) {
        self.rules.push(rule);
    }

    pub fn remove_compaction_rule(&mut self, dest_id: SeriesRef) {
        self.rules.retain(|rule| rule.dest_id != dest_id);
    }

    pub fn get_rule_by_dest_id(&self, dest_id: SeriesRef) -> Option<&CompactionRule> {
        self.rules.iter().find(|rule| rule.dest_id == dest_id)
    }

    pub fn remove_range_with_compaction(
        &mut self,
        ctx: &Context,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<usize> {
        // First, handle the compaction rule adjustments
        self.remove_compaction_range(ctx, start_ts, end_ts)?;

        // Then remove the actual data from the source series
        let deleted_count = self.remove_range(start_ts, end_ts)?;

        if deleted_count == 0 {
            return Ok(0);
        }

        if self.rules.is_empty() {
            // No compaction rules, nothing to do
            return Ok(deleted_count);
        }

        // Update any ongoing aggregations that might be affected
        let mut rules = std::mem::take(&mut self.rules);
        for rule in &mut rules {
            handle_current_bucket_adjustment(self, rule, start_ts, end_ts);
        }
        self.rules = rules;

        Ok(deleted_count)
    }

    pub fn run_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        run_compaction_parallel(ctx, self, value)
    }

    pub fn upsert_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        run_parallel(ctx, self, value, |thread_ctx, parent, dest, rule, item| {
            handle_compaction_upsert(thread_ctx, parent, rule, dest, item)
        })
    }

    /// Remove a range from all compactions
    pub(super) fn remove_compaction_range(
        &mut self,
        ctx: &Context,
        start: Timestamp,
        end: Timestamp,
    ) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        let unused = Sample::new(0, 0.0);
        // Process all contexts in parallel
        run_parallel(
            ctx,
            self,
            unused,
            |thread_ctx, src_series, dest_series, rule, _| {
                handle_compaction_range_removal(
                    thread_ctx,
                    src_series,
                    rule,
                    dest_series,
                    end,
                    start,
                )
            },
        )
    }
}

pub(crate) fn get_latest_compaction_sample(ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    let src_id = series.src_series?;
    let parent = get_destination_series(ctx, src_id)?;
    if parent.is_empty() {
        return None;
    }
    let rule = parent.get_rule_by_dest_id(series.id)?;
    let Some(end_ts) = rule.end_ts else {
        // no data
        return None;
    };
    let value = rule.aggregator.current()?;
    let start = rule.calc_bucket_start(end_ts - 1);
    let sample = Sample::new(start, value);
    Some(sample)
}
