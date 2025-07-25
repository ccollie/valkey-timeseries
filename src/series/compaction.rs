use crate::aggregators::{calc_bucket_start, AggregationHandler, Aggregator};
use crate::common::parallel::{Parallel, ParallelExt};
use crate::common::rdb::{rdb_load_timestamp, rdb_save_timestamp};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::index::{get_series_by_id, with_timeseries_postings};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesGuardMut, SeriesRef, TimeSeries};
use get_size::GetSize;
use logger_rust::*;
use smallvec::SmallVec;
use topologic::AcyclicDependencyGraph;
use valkey_module::{raw, Context, DetachedContext, NotifyEvent, ValkeyError, ValkeyResult};

const PARALLEL_THRESHOLD: usize = 2;
const TEMP_VEC_LEN: usize = 6;

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub bucket_start: Option<Timestamp>,
}

impl GetSize for CompactionRule {
    fn get_size(&self) -> usize {
        self.dest_id.get_size()
            + self.aggregator.get_size()
            + self.bucket_duration.get_size()
            + self.align_timestamp.get_size()
            + self.bucket_start.get_size()
    }
}

impl CompactionRule {
    pub fn save_to_rdb(&self, rdb: *mut raw::RedisModuleIO) {
        raw::save_unsigned(rdb, self.dest_id);
        self.aggregator.save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        rdb_save_timestamp(rdb, self.align_timestamp);
        rdb_save_timestamp(rdb, self.bucket_start.unwrap_or(-1));
    }

    pub fn load_from_rdb(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let start_ts = rdb_load_timestamp(rdb)?;
        let bucket_start = if start_ts == -1 { None } else { Some(start_ts) };

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
            bucket_start,
        })
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        calc_bucket_start(ts, self.align_timestamp, self.bucket_duration)
    }

    fn get_bucket_range(&self, ts: Timestamp) -> (Timestamp, Timestamp) {
        let start = self.calc_bucket_start(ts);
        let end = start.saturating_add_unsigned(self.bucket_duration);
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
        self.bucket_start = None;
    }
}

#[derive(Clone)]
struct CompactionWorker {
    added: SmallVec<SeriesRef, TEMP_VEC_LEN>,
    errors: SmallVec<TsdbError, 2>,
}

impl CompactionWorker {
    fn new() -> Self {
        Self {
            added: SmallVec::new(),
            errors: SmallVec::new(),
        }
    }
}

impl Parallel for CompactionWorker {
    fn create(&self) -> Self {
        Self {
            added: SmallVec::new(),
            errors: self.errors.clone(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.added.extend(other.added);
        self.errors.extend(other.errors);
    }
}

struct CompactionContext<'a> {
    parent: &'a TimeSeries,
    dest: &'a mut TimeSeries,
    rule: &'a mut CompactionRule,
    value: Sample,
    log_ctx: &'a DetachedContext,
    added: bool,
}

impl<'a> CompactionContext<'a> {
    fn new(
        log_ctx: &'a DetachedContext,
        parent: &'a TimeSeries,
        dest: &'a mut TimeSeries,
        rule: &'a mut CompactionRule,
        value: Sample,
    ) -> Self {
        Self {
            parent,
            dest,
            rule,
            value,
            log_ctx,
            added: false,
        }
    }
}

pub fn run_compaction(ctx: &Context, series: &mut TimeSeries, sample: Sample) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    process_series_with_compaction(ctx, series, sample, handle_sample_compaction)
}

pub fn upsert_compaction(ctx: &Context, series: &mut TimeSeries, sample: Sample) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }

    process_series_with_compaction(ctx, series, sample, handle_compaction_upsert)
}

pub fn remove_compaction_range(
    ctx: &Context,
    series: &mut TimeSeries,
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }
    let unused = Sample::new(0, 0.0);
    // Process all compactions in parallel
    process_series_with_compaction(ctx, series, unused, |context, _sample| {
        handle_compaction_range_removal(context, start, end)
    })?;
    // Update any ongoing aggregations that might be affected
    let mut rules = std::mem::take(&mut series.rules);
    for rule in &mut rules {
        handle_current_bucket_adjustment(series, rule, start, end);
    }
    series.rules = rules;
    Ok(())
}

fn null_ts_filter(ts: Timestamp) -> bool {
    ts != 0
}

/// Handle compaction for a genuinely new sample (timestamp > last sample timestamp)
fn handle_sample_compaction(ctx: &mut CompactionContext, sample: Sample) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let sample_bucket_start = ctx.rule.calc_bucket_start(ts);

    log_info!(
            "handle_sample_compaction({}): series_id:{}, sample: {} @ {}, sample_bucket_start: {}, bucket_start: {:?}",
            ctx.rule.aggregator.aggregation_type(), ctx.parent.id, sample.value, sample.timestamp, sample_bucket_start, ctx.rule.bucket_start
        );

    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // First sample for this rule - initialize the aggregation
        ctx.rule.bucket_start = Some(sample_bucket_start);
        ctx.rule.aggregator.update(sample.value);
        return Ok(());
    };

    if sample_bucket_start == current_bucket_start {
        // Sample belongs to the current aggregation bucket
        log_info!("Sample belongs to the current aggregation bucket: {current_bucket_start}");
        ctx.rule.aggregator.update(sample.value);
    } else if sample_bucket_start > current_bucket_start {
        // Sample starts a new bucket - finalize the current bucket first
        log_info!("Sample starts a new bucket ({sample_bucket_start} > {current_bucket_start}). Finalize the current bucket first");
        finalize_current_bucket(ctx, sample, sample_bucket_start)?;
    } else {
        let bucket_end = sample_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);
        // Sample is in an older bucket (shouldn't happen for new samples, but handle gracefully)
        recalculate_bucket(ctx, sample_bucket_start, bucket_end, null_ts_filter)?;
    }

    Ok(())
}

/// Finalize the current aggregation bucket and start a new one
fn finalize_current_bucket(
    ctx: &mut CompactionContext<'_>,
    new_sample: Sample,
    new_bucket_start: Timestamp,
) -> TsdbResult<()> {
    // Finalize the current bucket
    let aggregated_value = ctx.rule.aggregator.finalize();
    let current_bucket_start = ctx.rule.bucket_start.expect(
        "finalize_current_bucket should be called when current bucket start is already set",
    );

    add_dest_bucket(ctx, current_bucket_start, aggregated_value)?;

    // Start a new bucket with the new sample
    ctx.rule.aggregator.reset();
    ctx.rule.aggregator.update(new_sample.value);
    ctx.rule.bucket_start = Some(new_bucket_start);

    Ok(())
}

/// Handle upsert compaction for a destination series
/// This is called when a sample is being inserted/updated with a timestamp that's <= the last sample timestamp
fn handle_compaction_upsert(ctx: &mut CompactionContext, sample: Sample) -> TsdbResult<()> {
    let ts = sample.timestamp;
    let bucket_start = ctx.rule.calc_bucket_start(ts);

    log_info!(
        "handle_compaction_upsert({}, {} @ {}",
        ctx.parent.id,
        sample.value,
        sample.timestamp
    );

    // Check if this affects the current ongoing aggregation bucket
    let Some(current_bucket_start) = ctx.rule.bucket_start else {
        // No current bucket, this is the first sample for this rule
        ctx.rule.bucket_start = Some(bucket_start);
        ctx.rule.aggregator.update(sample.value);
        return Ok(());
    };

    let duration = ctx.rule.bucket_duration;
    let bucket_end = current_bucket_start.saturating_add_unsigned(duration);

    if bucket_start == current_bucket_start {
        // This sample belongs to the current aggregation bucket
        // We need to recalculate the entire bucket since we don't know what changed
        recalculate_current_bucket(ctx, current_bucket_start, bucket_end)?;
        return Ok(());
    }

    // This is a historical upsert - need to recalculate the affected bucket
    recalculate_bucket(ctx, bucket_start, bucket_end, null_ts_filter)
}

/// Recalculate the current ongoing aggregation bucket
fn recalculate_current_bucket(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
) -> TsdbResult<()> {
    // Reset the aggregator and recalculate from all samples in the bucket
    ctx.rule.aggregator.reset();

    for sample in ctx.parent.range_iter(bucket_start, bucket_end) {
        ctx.rule.aggregator.update(sample.value);
    }

    // reset would have cleared the bucket_start, so we need to set it again
    ctx.rule.bucket_start = Some(bucket_start);

    log_debug!(
        "recalculate_current_bucket({}, {bucket_start}, {bucket_end})",
        ctx.rule.aggregator.aggregation_type()
    );

    Ok(())
}

/// Recalculate a historical bucket and update the destination series
fn recalculate_bucket<F>(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    bucket_end: Timestamp,
    filter: F,
) -> TsdbResult<()>
where
    F: Fn(Timestamp) -> bool,
{
    log_debug!(
        "recalculate_bucket({}, {}, {bucket_start}, {bucket_end})",
        ctx.rule.aggregator.aggregation_type(),
        ctx.dest.id
    );

    // Create a new aggregator for this bucket
    let mut bucket_aggregator = ctx.rule.aggregator.clone();
    bucket_aggregator.reset();

    // Aggregate all samples in this bucket
    let mut has_samples = false;
    let sample_iter = ctx
        .parent
        .range_iter(bucket_start, bucket_end)
        .filter(|sample| filter(sample.timestamp));

    for sample in sample_iter {
        bucket_aggregator.update(sample.value);
        log_debug!(
            "Updated {} aggregator with sample value: {} during range calculation.",
            ctx.rule.aggregator.aggregation_type(),
            sample.value
        );

        has_samples = true;
    }

    if has_samples {
        let aggregated_value = bucket_aggregator.finalize();
        log_debug!("Bucket aggregation finalized with value: {aggregated_value}");
        add_dest_bucket(ctx, bucket_start, aggregated_value)?;
    } else {
        // No samples in this bucket anymore, remove it from destination
        log_debug!("No samples in this bucket anymore, remove it from destination");
        ctx.dest.remove_range(bucket_start, bucket_start)?;
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
    ctx: &mut CompactionContext,
    start: Timestamp,
    end: Timestamp,
) -> TsdbResult<()> {
    // Calculate the bucket boundaries that are affected by the removed range
    let first_bucket_start = ctx.rule.calc_bucket_start(start);
    let last_bucket_start = ctx.rule.calc_bucket_start(end);

    // Handle different scenarios based on how the removal affects buckets
    if first_bucket_start == last_bucket_start {
        // Scenario 1: Removal is within a single bucket - need to recalculate that bucket
        handle_single_bucket_removal(ctx, first_bucket_start, start, end)
    } else {
        // Scenario 2: Removal spans multiple buckets
        handle_multiple_bucket_removal(ctx, first_bucket_start, last_bucket_start, start, end)
    }
}

fn handle_single_bucket_removal(
    ctx: &mut CompactionContext,
    bucket_start: Timestamp,
    removal_start: Timestamp,
    removal_end: Timestamp,
) -> TsdbResult<()> {
    let bucket_end = bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

    // Check if the entire bucket is being removed
    if removal_start <= bucket_start && removal_end >= bucket_end {
        // Remove the entire bucket from destination
        ctx.dest.remove_range(bucket_start, bucket_end)?;
        return Ok(());
    }

    recalculate_bucket(ctx, bucket_start, bucket_end, |ts| {
        ts < removal_start || ts > removal_end
    })
}

fn handle_multiple_bucket_removal(
    ctx: &mut CompactionContext,
    first_bucket_start: Timestamp,
    last_bucket_start: Timestamp,
    removal_start: Timestamp,
    removal_end: Timestamp,
) -> TsdbResult<()> {
    let mut current_bucket_start = first_bucket_start;

    while current_bucket_start <= last_bucket_start {
        let bucket_end = current_bucket_start.saturating_add_unsigned(ctx.rule.bucket_duration);

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
                    current_bucket_start,
                    removal_start,
                    removal_end,
                )?;
            } else {
                // Middle buckets are completely removed
                ctx.dest
                    .remove_range(current_bucket_start, current_bucket_start)?;
            }
        }

        current_bucket_start = bucket_end;
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
    let Some(current_bucket_start) = rule.bucket_start else {
        // No active aggregation, nothing to adjust
        return;
    };

    let current_bucket_end = current_bucket_start.saturating_add_unsigned(rule.bucket_duration);

    // Check if the removal affects the current aggregation bucket
    if removal_start < current_bucket_end && removal_end > current_bucket_start {
        // The current bucket is affected, we need to recalculate
        let mut new_aggregator = rule.aggregator.clone();
        new_aggregator.reset();

        // Re-aggregate samples that are not in the removal range
        let bucket_samples = series
            .range_iter(current_bucket_start, current_bucket_end)
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
) -> SmallVec<SeriesGuardMut<'a>, TEMP_VEC_LEN> {
    if series.rules.is_empty() {
        return SmallVec::new();
    }

    let mut missing: SmallVec<_, TEMP_VEC_LEN> = SmallVec::new();
    let mut destinations: SmallVec<_, TEMP_VEC_LEN> = SmallVec::new();

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

fn notify_compaction(ctx: &Context, ids: &[SeriesRef]) {
    with_timeseries_postings(ctx, |postings| {
        for &id in ids {
            let Some(key) = postings.get_key_by_id(id) else {
                ctx.log_warning("Compaction notification failed: series key not found");
                continue;
            };
            let key = ctx.create_string(key.as_ref());
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.add:dest", &key);
        }
    });
}

/// Iterates through compaction rules (possibly in parallel) and applies the provided function `f` to process
/// the specified `TimeSeries` and its associated child series. This function handles
/// the traversal and execution of compaction logic on the series hierarchy, allowing
/// recursive processing of any child `TimeSeries` derived from the rules.
///
/// ## Arguments
///
/// - `ctx`: the current context
/// - `series`: A mutable reference to the `TimeSeries` being processed.
/// - `value`: A `Sample` object containing the value or data being processed
///   during the current iteration.
/// - `f`: A closure or function pointer that implements the processing logic
///   for each compaction rule. This function is called with:
///   - A detached context for logging or thread-dependent operations.
///   - The current `TimeSeries` being processed.
///   - A mutable reference to a child `TimeSeries` derived from compaction rules.
///   - A mutable reference to a `CompactionRule` for the current rule being processed.
///   - The sample value (`value`) to be processed.
///
/// ## Returns
///
/// This function returns a `TsdbResult<()>`:
/// - `Ok(())` if all compaction operations and recursive calls are successful.
/// - `Err` if any part of the compaction operation fails at any level.
///
fn process_series_with_compaction<F>(
    ctx: &Context,
    series: &mut TimeSeries,
    value: Sample,
    f: F,
) -> TsdbResult<()>
where
    F: Fn(&mut CompactionContext, Sample) -> TsdbResult<()> + Send + Sync,
{
    iterate_compactions(ctx, series, value, &f)
}

/// Processes a single series and its compaction rules, then recursively processes child series
fn iterate_compactions<F>(
    ctx: &Context,
    series: &mut TimeSeries,
    value: Sample,
    f: &F,
) -> TsdbResult<()>
where
    F: Fn(&mut CompactionContext, Sample) -> TsdbResult<()> + Send + Sync,
{
    // Process current series compaction rules
    let parent_result = execute_compaction_rules(ctx, series, value, f);

    // Collect child series
    let child_series: Vec<_> = series
        .rules
        .iter()
        .filter_map(|rule| get_destination_series(ctx, rule.dest_id))
        .collect();

    // Handle the processing result after collecting children to avoid borrow conflicts
    parent_result?;

    // Recursively process child series
    for mut child in child_series {
        execute_compaction_rules(ctx, &mut child, value, f)?;
    }

    Ok(())
}

fn execute_compaction_rules<F>(
    ctx: &Context,
    series: &mut TimeSeries,
    value: Sample,
    f: &F,
) -> TsdbResult<()>
where
    F: Fn(&mut CompactionContext, Sample) -> TsdbResult<()> + Send + Sync,
{
    let destinations = get_compaction_series(ctx, series);
    if destinations.is_empty() || series.rules.is_empty() {
        return Ok(());
    }

    let mut rules = std::mem::take(&mut series.rules);
    let result = run_compaction_internal(ctx, series, &mut rules, destinations, value, f);
    series.rules = rules;
    result
}

/// Internal function that handles the parallel execution of compaction rules
fn run_compaction_internal<F>(
    ctx: &Context,
    series: &TimeSeries,
    rules: &mut [CompactionRule],
    child_series: SmallVec<SeriesGuardMut, TEMP_VEC_LEN>,
    value: Sample,
    f: &F,
) -> TsdbResult<()>
where
    F: Fn(&mut CompactionContext, Sample) -> TsdbResult<()> + Send + Sync,
{
    if rules.is_empty() {
        return Ok(());
    }

    let destinations = rules.iter_mut().zip(child_series).collect::<Vec<_>>();
    let mut worker = CompactionWorker::new();
    let log_ctx = DetachedContext::new();

    worker.maybe_par(PARALLEL_THRESHOLD, destinations, |worker, item| {
        let rule = item.0;
        let mut dest_guard = item.1;
        let mut cctx = CompactionContext {
            parent: series,
            dest: &mut dest_guard,
            rule,
            value,
            log_ctx: &log_ctx,
            added: false,
        };
        match f(&mut cctx, value) {
            Ok(_) => {
                if cctx.added {
                    worker.added.push(dest_guard.id);
                }
            }
            Err(error) => {
                let msg = format!(
                    "Failed to handle compaction rule for series {}: {error}",
                    dest_guard.id,
                );
                log_ctx.log_warning(&msg);
                worker.errors.push(error);
            }
        }
    });

    // Notify about the compaction results
    if !worker.added.is_empty() {
        notify_compaction(ctx, &worker.added);
    }

    let Some(first_error) = worker.errors.first().cloned() else {
        return Ok(());
    };
    Err(first_error)
}

fn add_dest_bucket(ctx: &mut CompactionContext, ts: Timestamp, value: f64) -> TsdbResult<()> {
    let bucket_start = ctx.rule.calc_bucket_start(ts);
    // Add the sample to the destination series
    // todo: specify to ignore whatever adjustments
    log_debug!("add_dest_bucket: {value} @ {ts} during range calculation.");

    match ctx
        .dest
        .add(bucket_start, value, Some(DuplicatePolicy::KeepLast))
    {
        SampleAddResult::Ok(_) => {
            ctx.added = true;
            log_debug!("add_dest_bucket: Added value {value} @ {ts}.",);
            Ok(())
        }
        SampleAddResult::Ignored(_) => Ok(()), // duplicate sample, (ignored)
        SampleAddResult::TooOld => {
            // bucket start is too old, we cannot add it
            ctx.log_ctx
                .log_verbose("Sample is too old for compaction rule, ignoring");
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
        run_compaction(ctx, self, value)
    }

    pub fn upsert_compaction(&mut self, ctx: &Context, value: Sample) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }
        upsert_compaction(ctx, self, value)
    }

    /// Remove a range from all compactions
    pub fn remove_compaction_range(
        &mut self,
        ctx: &Context,
        start: Timestamp,
        end: Timestamp,
    ) -> TsdbResult<()> {
        remove_compaction_range(ctx, self, start, end)
    }
}

pub(crate) fn get_latest_compaction_sample(ctx: &Context, series: &TimeSeries) -> Option<Sample> {
    let src_id = series.src_series?;
    let parent = get_destination_series(ctx, src_id)?;
    if parent.is_empty() {
        return None;
    }
    let rule = parent.get_rule_by_dest_id(series.id)?;
    let Some(start) = rule.bucket_start else {
        // no data
        return None;
    };
    let value = rule.aggregator.current()?;
    let sample = Sample::new(start, value);
    Some(sample)
}

pub fn check_circular_dependencies(ctx: &Context, series: &mut TimeSeries) -> ValkeyResult<()> {
    let graph = build_compaction_dependency_graph(ctx, series)?;
    if graph.is_empty() {
        return Ok(());
    }
    Ok(())
}

/// Check if adding a new compaction rule would create a circular dependency
pub fn check_new_rule_circular_dependency(
    ctx: &Context,
    series: &mut TimeSeries,
    rule: &CompactionRule,
) -> ValkeyResult<()> {
    if series.rules.is_empty() {
        return Ok(());
    }

    let mut graph = build_compaction_dependency_graph(ctx, series)?;
    if graph.is_empty() {
        return Ok(());
    }

    // Check if the new rule would create a circular dependency
    if graph.depend_on(series.id, rule.dest_id).is_err() {
        return Err(ValkeyError::Str(
            error_consts::COMPACTION_CIRCULAR_DEPENDENCY,
        ));
    }

    Ok(())
}

pub fn build_compaction_dependency_graph(
    ctx: &Context,
    series: &mut TimeSeries,
) -> ValkeyResult<AcyclicDependencyGraph<SeriesRef>> {
    fn build(
        ctx: &Context,
        source_series: &mut TimeSeries,
        graph: &mut AcyclicDependencyGraph<SeriesRef>,
    ) -> ValkeyResult<()> {
        let src_id = source_series.id;
        let mut destinations = get_compaction_series(ctx, source_series);
        for dest in destinations.iter_mut() {
            graph
                .depend_on(src_id, dest.id)
                .map_err(|_| ValkeyError::Str(error_consts::COMPACTION_CIRCULAR_DEPENDENCY))?;
            build(ctx, dest, graph)?;
        }
        Ok(())
    }

    let mut graph = AcyclicDependencyGraph::new();
    build(ctx, series, &mut graph)?;

    Ok(graph)
}
