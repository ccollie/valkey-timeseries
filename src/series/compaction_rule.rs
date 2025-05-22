use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::{Sample, Timestamp};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesRef, TimeSeries};
use get_size::GetSize;
use smallvec::SmallVec;
use valkey_module::{raw, AclPermissions, Context, ThreadSafeContext, ValkeyResult};
use crate::common::db::get_current_db;
use crate::common::parallel::join;
use crate::common::rdb::{rdb_load_timestamp, rdb_save_timestamp};
use crate::series::index::{get_series_by_id, mark_series_for_removal, with_timeseries_index, TIMESERIES_INDEX};

#[derive(Debug, Clone, PartialEq, GetSize)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub hi_ts: Option<Timestamp>,
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
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        ts - ((diff % delta + delta) % delta)
    }

    fn calc_bucket_end(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        ts + (delta - (diff % delta))
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
}


#[derive(PartialEq)]
pub enum CompactionResult {
    Ok,
    Error,
}

struct CompactionUpdateContext<'a> {
    series: &'a mut TimeSeries,
    rule: &'a mut CompactionRule,
}

fn run_compaction_internal(rule_ctx: &mut CompactionUpdateContext, sample: Sample) {
    let ts = sample.timestamp;
    
    match rule_ctx.rule.hi_ts {
        Some(hi_ts) => {
            if ts > hi_ts {
                // we need to add a new bucket
                let value = rule_ctx.rule.aggregator.finalize();
                rule_ctx.rule.aggregator.reset();
                let rule = &rule_ctx.rule;
                let start = hi_ts.saturating_sub(rule.bucket_duration as i64);
                let res = rule_ctx.series.add(start, value, Some(DuplicatePolicy::KeepLast));
                if res.is_ok() {
                    // replicate
                } else {
                    // log error
                }
                rule_ctx.rule.hi_ts = Some(rule.calc_bucket_end(ts));
            }
        }
        None => {
            // first time we add a sample, we need to align the timestamps
            rule_ctx.rule.hi_ts = Some(rule_ctx.rule.calc_bucket_end(ts));
        }
    }
    
    rule_ctx.rule.aggregator.update(sample.value);
}

fn run_compactions_internal(rule_contexts: &mut [CompactionUpdateContext], value: Sample) {
    match rule_contexts {
        [first] => {
            run_compaction_internal(first, value);
        }
        [first, second ] => {
            join(|| run_compaction_internal(first, value),
                || run_compaction_internal(second, value));
        }
        _ => {
            let (left, right) = rule_contexts.split_at_mut(rule_contexts.len() / 2);
            join(|| run_compactions_internal(left, value),
                || run_compactions_internal(right, value));
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

    fn run_compactions(&mut self, ctx: &Context, value: Sample) {
        let mut rule_contexts = Vec::new();

        let mut to_remove: SmallVec<_, 4> = SmallVec::new();
        for rule in self.rules.iter_mut() {
            // note: permissions would have been checked for parent
            let Ok(Some(mut dest)) = get_series_by_id(ctx, rule.dest_id, false, None) else {
                // mark the id for removal
                mark_series_for_removal(ctx, rule.dest_id);
                to_remove.push(rule.dest_id);
                continue;
            };
            if !dest.is_compaction() {
                mark_series_for_removal(ctx, dest.id);
                to_remove.push(dest.id);
                continue;
            }
            rule_contexts.push(CompactionUpdateContext {
                series: &mut dest,
                rule,
            });
        }
        // remove the rules that are invalid
        for id in to_remove {
            self.remove_compaction_rule(id);
        }
        let blocked_client = ctx.block_client();
        run_compactions_internal(ctx, &rule_contexts, value);
    }
}