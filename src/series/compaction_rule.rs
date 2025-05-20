use crate::aggregators::{AggregationHandler, Aggregator};
use crate::common::{Sample, Timestamp};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesRef, TimeSeries};
use get_size::GetSize;
use valkey_module::{raw, AclPermissions, Context, ValkeyResult};
use crate::common::rdb::{rdb_load_timestamp, rdb_save_timestamp};
use crate::series::index::{get_series_by_id, with_timeseries_index};

#[derive(Debug, Clone, PartialEq, GetSize)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
    pub bucket_end: Timestamp,
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
        rdb_save_timestamp(rdb, self.bucket_end);
    }
    
    pub fn load_from_rdb(
        rdb: *mut raw::RedisModuleIO,
    ) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = rdb_load_timestamp(rdb)?;
        let bucket_end = rdb_load_timestamp(rdb)?;

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
            bucket_end,
        })
    }

    fn calc_bucket_start(&self, ts: Timestamp) -> Timestamp {
        let diff = ts - self.align_timestamp;
        let delta = self.bucket_duration as i64;
        ts - ((diff % delta + delta) % delta)
    }
    
    pub fn update(
        &mut self,
        sample: Sample,
    ) {
        let (start, end) = self.get_bucket_range(sample);
        self.aggregator.update(sample, start, end);
    }
}


fn run_compaction(series: &TimeSeries, sample: Sample) {
    
    todo!()
}

fn mark_series_for_removal(ctx: &Context, id: SeriesRef) {
    with_timeseries_index(ctx, |index| {
        index.mark_id_as_stale(id);
    })
}

#[derive(PartialEq)]
pub enum CompactionResult {
    Ok,
    Error,
    InvalidDestination(SeriesRef),
}

fn run_compaction_internal(ctx: &CompactionUpdateContext, sample: Sample) -> CompactionResult {
    let start = ctx.rule.calc_bucket_start(sample.timestamp);
    let end = start + ctx.rule.bucket_duration as i64;
    rule.aggregator.update(sample.value);
    
    let res = get_series_by_id(ctx, rule.dest_id, false, Some(AclPermissions::UPDATE));
    if res.is_err() {
        // could be permissions related
        // mark the id for removal, signal to src_series to remove it
    } else {
        let Some(mut dest) = res.unwrap() else {
            // todo: mark the id for removal, signal to src_series to remove it
            mark_series_for_removal(ctx, rule.dest_id);
            return CompactionResult::InvalidDestination(rule.dest_id);
        };
        if !dest.is_compaction() {
            mark_series_for_removal(ctx, rule.dest_id);
            return CompactionResult::InvalidDestination(rule.dest_id);
        }
        match dest.add(sample.timestamp, sample.value, Some(DuplicatePolicy::KeepLast)) {
            SampleAddResult::Ok(_) => {
                
            }
        }
        CompactionResult::Ok
    }
}

struct CompactionUpdateContext<'a> {
    series: &'a mut TimeSeries,
    rule: &'a mut CompactionRule,
}

fn run_compactions_internal(ctx: &Context, rules: &[&mut CompactionUpdateContext], value: Sample) {

}

fn run_compactions(ctx: &Context, series: &mut TimeSeries, value: Sample) {
    let mut compaction_rules = Vec::new();
    for rule in series.rules.iter_mut() {
        if rule.dest_id == 0 {
            continue;
        }
        let res = get_series_by_id(ctx, rule.dest_id, false, Some(AclPermissions::UPDATE));
        if res.is_err() {
            // could be permissions related
            // mark the id for removal, signal to src_series to remove it
        } else {
            let Some(mut dest) = res.unwrap() else {
                // todo: mark the id for removal, signal to src_series to remove it
                mark_series_for_removal(ctx, rule.dest_id);
                return CompactionResult::InvalidDestination(rule.dest_id);
            };
            compaction_rules.push(CompactionUpdateContext {
                series,
                rule,
            });
        }
    }
    run_compactions_internal(ctx, &compaction_rules, value);
}