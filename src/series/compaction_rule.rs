use crate::aggregators::Aggregator;
use crate::common::{Sample, Timestamp};
use crate::series::{DuplicatePolicy, SampleAddResult, SeriesRef, TimeSeries};
use get_size::GetSize;
use valkey_module::{raw, AclPermissions, Context, ValkeyResult};
use crate::series::index::{get_series_by_id, with_timeseries_index};

#[derive(Debug, Clone, PartialEq, GetSize)]
pub struct CompactionRule {
    pub dest_id: SeriesRef,
    pub aggregator: Aggregator,
    pub bucket_duration: u64,
    pub align_timestamp: Timestamp,
}

impl CompactionRule {
    pub fn save_to_rdb(
        &self,
        rdb: *mut raw::RedisModuleIO,
    ) {
        raw::save_unsigned(rdb, self.dest_id);
        self.aggregator.save(rdb);
        raw::save_unsigned(rdb, self.bucket_duration);
        raw::save_unsigned(rdb, self.align_timestamp as u64);
    }
    
    pub fn load_from_rdb(
        rdb: *mut raw::RedisModuleIO,
    ) -> ValkeyResult<Self> {
        let dest_id = raw::load_unsigned(rdb)? as SeriesRef;
        let aggregator = Aggregator::load(rdb)?;
        let bucket_duration = raw::load_unsigned(rdb)?;
        let align_timestamp = raw::load_unsigned(rdb)? as Timestamp;

        Ok(CompactionRule {
            dest_id,
            aggregator,
            bucket_duration,
            align_timestamp,
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

fn run_compaction_internal(ctx: &Context, rule: &mut CompactionRule, sample: Sample) -> CompactionResult {
    let start = rule.calc_bucket_start(sample.timestamp);
    let end = start + rule.bucket_duration as i64;
    rule.aggregator.update(sample, start, end);
    
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

fn run_compactions(ctx: &Context, rules: &[&mut CompactionRule], value: Sample) {

}