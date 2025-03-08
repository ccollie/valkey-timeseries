use crate::aggregators::Aggregator;
use crate::common::Timestamp;
use crate::series::SeriesRef;

pub struct CompactionRule {
    pub dest: SeriesRef,
    pub aggregate: Aggregator,
    pub bucket_duration: u64, // ms
    pub ts_alignment: Timestamp,
    pub bucket_start: Timestamp //
}