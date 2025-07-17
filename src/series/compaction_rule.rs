use crate::aggregators::{calc_bucket_start, AggregationHandler, Aggregator};
use crate::common::parallel::join;
use crate::common::rdb::{rdb_load_timestamp, rdb_save_timestamp};
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::series::index::get_series_by_id;
use crate::series::{DuplicatePolicy, SeriesGuardMut, SeriesRef, TimeSeries};
use get_size::GetSize;
use rayon::iter::{IntoParallelRefMutIterator, ParallelIterator};
use smallvec::SmallVec;
use valkey_module::{raw, Context, ValkeyResult};

#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub hi_ts: Option<Timestamp>,
}

impl GetSize for CompactionRule {

    fn get_size(&self) -> usize {
        self.dest_id.get_size() +
        self.aggregator.get_size() +
        self.bucket_duration.get_size() +
        self.align_timestamp.get_size() +
        self.hi_ts.get_size()
    }
}

impl CompactionRule {
    pub fn save_to_rdb(
        &self,
        rdb: *mut raw::RedisModuleIO,
    ) {
        raw::save_unsigned(rdb, self.dest_id);
        self.aggregator.save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        rdb_save_timestamp(rdb, self.align_timestamp);
        rdb_save_timestamp(rdb, self.hi_ts.unwrap_or(-1));
    }
    
    pub fn load_from_rdb(
        rdb: *mut raw::RedisModuleIO,
    ) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let bucket_end = rdb_load_timestamp(rdb)?;
        let hi_ts = if bucket_end == -1 {
            None
        } else {
            Some(bucket_end)
        };

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
            hi_ts,
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
        self.hi_ts = None;
    }
}

struct CompactionUpdateContext<'a> {
    series: SeriesGuardMut<'a>,
    rule: CompactionRule,
}

fn upsert_compaction_internal(rule_ctx: &mut CompactionUpdateContext, sample: Sample) {
    let ts = sample.timestamp;
    
    match rule_ctx.rule.hi_ts {
        Some(hi_ts) => {
            if ts > hi_ts {
                // we need to add a new bucket
                let value = rule_ctx.rule.aggregator.finalize();
                rule_ctx.rule.aggregator.reset();
                let rule = &rule_ctx.rule;
                let start = hi_ts.saturating_sub(rule.bucket_duration as i64);
                // do we need to replicate ?
                rule_ctx.series.add(start, value, Some(DuplicatePolicy::KeepLast));
                rule_ctx.rule.hi_ts = Some(rule.calc_bucket_end(ts));
            }
        }
        None => {
            // the first time we add a sample, we need to align the timestamps
            rule_ctx.rule.hi_ts = Some(rule_ctx.rule.calc_bucket_end(ts));
        }
    }
    
    rule_ctx.rule.aggregator.update(sample.value);
}

fn upsert_compactions_internal(rule_contexts: &mut [CompactionUpdateContext], value: Sample) {
    match rule_contexts {
        [first] => {
            upsert_compaction_internal(first, value);
        }
        [first, second ] => {
            join(|| upsert_compaction_internal(first, value),
                 || upsert_compaction_internal(second, value));
        }
        _ => {
            let (left, right) = rule_contexts.split_at_mut(rule_contexts.len() / 2);
            join(|| upsert_compactions_internal(left, value),
                 || upsert_compactions_internal(right, value));
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

    pub fn remove_range_with_compaction_update(
        &mut self,
        ctx: &Context,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> TsdbResult<usize> {
        // First, handle the compaction rule adjustments
        self.remove_compaction_range(ctx, start_ts, end_ts)?;

        // Then remove the actual data from the source series
        let deleted_count = self.remove_range(start_ts, end_ts)?;

        // Update any ongoing aggregations that might be affected
        let mut rules = std::mem::take(&mut self.rules);
        for rule in &mut rules {
            self.handle_current_bucket_adjustment(rule, start_ts, end_ts);
        }
        self.rules = rules;

        Ok(deleted_count)
    }

    fn upsert_compactions(&mut self, ctx: &Context, value: Sample) {
        let mut rule_contexts = Vec::new();

        let rules = std::mem::take(&mut self.rules);
        for rule in rules.into_iter() {
            // note: permissions would have been checked for parent
            let Ok(Some(dest)) = get_series_by_id(ctx, rule.dest_id, false, None) else {
                continue;
            };
            if !dest.is_compaction() {
                continue;
            }
            rule_contexts.push(CompactionUpdateContext {
                series: dest,
                rule,
            });
        }

        // todo: find way to avoid cloning here, though in most case all we have are Copy types
        self.rules = rule_contexts.iter().map(|x| x.rule.clone()).collect();
        upsert_compactions_internal(&mut rule_contexts, value);
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
    fn remove_compaction_range(&mut self, ctx: &Context, start: Timestamp, end: Timestamp) -> TsdbResult<()> {
        if self.rules.is_empty() {
            return Ok(());
        }

        let mut rule_contexts: SmallVec<_, 4> = SmallVec::new();

        let rules = std::mem::take(&mut self.rules);
        // Process rules and collect contexts
        for rule in rules {
            // Get the destination series
            let Ok(Some(dest_series)) = get_series_by_id(ctx, rule.dest_id, true, None) else {
                // Destination series doesn't exist, mark rule for removal
                continue;
            };
            if !dest_series.is_compaction() {
                continue;
            }
        
            rule_contexts.push(CompactionUpdateContext {
                series: dest_series,
                rule,
            });
        }
    
        // Process all contexts in parallel
        let res = rule_contexts.par_iter_mut().try_for_each(|ctx| {
            self.handle_compaction_range_removal(&mut ctx.rule, &mut ctx.series, start, end)
        });

        self.rules = rule_contexts.into_iter().map(|x| x.rule).collect();

        if let Err(e) = res {
            return Err(e);
        }
    
        Ok(())
    }

    fn handle_compaction_range_removal(
        &self,
        rule: &mut CompactionRule,
        dest_series: &mut TimeSeries,
        start: Timestamp,
        end: Timestamp,
    ) -> TsdbResult<()> {
        // Calculate the bucket boundaries that are affected by the removed range
        let first_affected_bucket_start = rule.calc_bucket_start(start);
        let last_affected_bucket_start = rule.calc_bucket_start(end);

        // Handle different scenarios based on how the removal affects buckets
        if first_affected_bucket_start == last_affected_bucket_start {
            // Scenario 1: Removal is within a single bucket - need to recalculate that bucket
            self.handle_single_bucket_removal(rule, dest_series, first_affected_bucket_start, start, end)?;
        } else {
            // Scenario 2: Removal spans multiple buckets
            self.handle_multiple_bucket_removal(rule, dest_series, first_affected_bucket_start, last_affected_bucket_start, start, end)?;
        }

        Ok(())
    }

    fn handle_single_bucket_removal(
        &self,
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

        // Partial bucket removal - need to recalculate the aggregation
        let mut new_aggregator = rule.aggregator.clone();
        new_aggregator.reset();

        // Iterate through remaining samples in the bucket and re-aggregate
        let bucket_samples = self.range_iter(bucket_start, bucket_end)
            .into_iter()
            .filter(|sample| sample.timestamp < removal_start || sample.timestamp > removal_end);

        let mut has_samples = false;

        for sample in bucket_samples {
            new_aggregator.update(sample.value);
            has_samples = true;
        }

        if has_samples {
            // Update the destination with the new aggregated value
            let new_value = new_aggregator.finalize();
            dest_series.add(bucket_start, new_value, Some(DuplicatePolicy::KeepLast));
        } else {
            // No samples left in the bucket, remove it from destination
            dest_series.remove_range(bucket_start, bucket_start)?;
        }

        Ok(())
    }

    fn handle_multiple_bucket_removal(
        &self,
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
                if current_bucket_start == first_bucket_start || current_bucket_start == last_bucket_start {
                    // The first or last bucket might be partial
                    self.handle_single_bucket_removal(rule, dest_series, current_bucket_start, removal_start, removal_end)?;
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
        &mut self,
        rule: &mut CompactionRule,
        removal_start: Timestamp,
        removal_end: Timestamp,
    ) {
        // If we have an active aggregation bucket
        if let Some(hi_ts) = rule.hi_ts {
            let current_bucket_start = hi_ts.saturating_sub(rule.bucket_duration as i64);
            let current_bucket_end = hi_ts;

            // Check if the removal affects the current aggregation bucket
            if removal_start < current_bucket_end && removal_end > current_bucket_start {
                // The current bucket is affected, we need to recalculate
                let mut new_aggregator = rule.aggregator.clone();
                new_aggregator.reset();

                // Re-aggregate samples that are not in the removal range
                let bucket_samples = self.get_range(current_bucket_start, current_bucket_end)
                    .into_iter()
                    .filter(|sample| sample.timestamp < removal_start || sample.timestamp > removal_end);

                for sample in bucket_samples {
                    new_aggregator.update(sample.value);
                }

                rule.aggregator = new_aggregator;
            }
        }
    }


}