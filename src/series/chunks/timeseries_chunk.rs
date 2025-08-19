use crate::common::rdb::rdb_load_string;
use crate::common::{Sample, Timestamp};
use crate::config::SPLIT_FACTOR;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::{FilteredSampleIterator, SampleIter};
use crate::series::chunks::utils::{filter_samples_by_value, filter_timestamp_slice};
use crate::series::types::ValueFilter;
use crate::series::{
    DuplicatePolicy, SampleAddResult,
    chunks::{Chunk, ChunkEncoding, GorillaChunk, PcoChunk, UncompressedChunk},
};
use core::mem::size_of;
use get_size::GetSize;
use std::cmp::Ordering;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, raw};

#[derive(Debug, Clone, Hash, PartialEq, GetSize)]
pub enum TimeSeriesChunk {
    Uncompressed(UncompressedChunk),
    Gorilla(GorillaChunk),
    Pco(PcoChunk),
}

impl TimeSeriesChunk {
    pub fn new(compression: ChunkEncoding, chunk_size: usize) -> Self {
        use TimeSeriesChunk::*;
        match compression {
            ChunkEncoding::Uncompressed => {
                let chunk = UncompressedChunk::with_max_size(chunk_size);
                Uncompressed(chunk)
            }
            ChunkEncoding::Gorilla => {
                let chunk = GorillaChunk::with_max_size(chunk_size);
                Gorilla(chunk)
            }
            ChunkEncoding::Pco => Pco(PcoChunk::with_max_size(chunk_size)),
        }
    }

    pub fn is_full(&self) -> bool {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.is_full(),
            Gorilla(chunk) => chunk.is_full(),
            Pco(chunk) => chunk.is_full(),
        }
    }

    pub fn bytes_per_sample(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.bytes_per_sample(),
            Gorilla(chunk) => chunk.bytes_per_sample(),
            Pco(chunk) => chunk.bytes_per_sample(),
        }
    }

    pub fn utilization(&self) -> f64 {
        let used = self.size();
        let total = self.max_size();
        (used / total) as f64
    }

    /// Get an estimate of the remaining capacity in number of samples
    pub fn estimate_remaining_sample_capacity(&self) -> usize {
        let used = self.size();
        let total = self.max_size();
        if used >= total {
            return 0;
        }
        let remaining = total - used;
        let bytes_per_sample = self.bytes_per_sample();
        if bytes_per_sample == 0 {
            return 0;
        }
        remaining / bytes_per_sample
    }

    pub fn clear(&mut self) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.clear(),
            Gorilla(chunk) => chunk.clear(),
            Pco(chunk) => chunk.clear(),
        }
    }

    pub fn is_timestamp_in_range(&self, ts: Timestamp) -> bool {
        ts >= self.first_timestamp() && ts <= self.last_timestamp()
    }

    pub fn is_contained_by_range(&self, start_ts: Timestamp, end_ts: Timestamp) -> bool {
        self.first_timestamp() >= start_ts && self.last_timestamp() <= end_ts
    }

    pub fn overlaps(&self, start_time: i64, end_time: i64) -> bool {
        let first_time = self.first_timestamp();
        let last_time = self.last_timestamp();
        first_time <= end_time && last_time >= start_time
    }

    pub fn has_samples_in_range(&self, start_time: Timestamp, end_time: Timestamp) -> bool {
        if self.is_empty() || !self.overlaps(start_time, end_time) {
            return false;
        }

        if self.range_iter(start_time, end_time).next().is_some() {
            return true;
        }

        false
    }

    // todo: make this a trait method
    pub fn iter(&self) -> Box<dyn Iterator<Item = Sample> + '_> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Box::new(chunk.iter()),
            Gorilla(chunk) => Box::new(chunk.iter()),
            Pco(chunk) => Box::new(chunk.iter()),
        }
    }

    pub fn range_iter(&self, start: Timestamp, end: Timestamp) -> SampleIter {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.range_iter(start, end),
            Gorilla(chunk) => chunk.range_iter(start, end),
            Pco(chunk) => chunk.range_iter(start, end),
        }
    }

    // NOTE! this function assumes that timestamps are sorted ascending and de-duped
    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.len() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }

        let mut samples = Vec::with_capacity(timestamps.len());

        let first_ts = timestamps[0];
        let mut index: usize = 0;

        let first_timestamp = first_ts.max(self.first_timestamp());
        let last_timestamp = timestamps[timestamps.len() - 1].min(self.last_timestamp());

        for sample in self.range_iter(first_timestamp, last_timestamp) {
            if index >= timestamps.len() {
                break;
            }
            let mut first_ts = timestamps[index];
            match sample.timestamp.cmp(&first_ts) {
                Ordering::Less => continue,
                Ordering::Equal => {
                    samples.push(sample);
                    index += 1;
                }
                Ordering::Greater => {
                    while first_ts < sample.timestamp && index < timestamps.len() {
                        index += 1;
                        first_ts = timestamps[index];
                        if first_ts == sample.timestamp {
                            samples.push(sample);
                            index += 1;
                            break;
                        }
                    }
                }
            }
        }

        Ok(samples)
    }

    pub(crate) fn get_range_filtered(
        &self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: &Option<Vec<Timestamp>>,
        value_filter: &Option<ValueFilter>,
    ) -> Vec<Sample> {
        let mut samples = if let Some(ts_filter) = timestamp_filter {
            let filtered_ts = filter_timestamp_slice(ts_filter, start_timestamp, end_timestamp);
            self.samples_by_timestamps(&filtered_ts)
                .unwrap_or_default()
                .into_iter()
                .collect()
        } else {
            self.get_range(start_timestamp, end_timestamp)
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

        if let Some(value_filter) = value_filter {
            filter_samples_by_value(&mut samples, value_filter);
        }
        samples
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.set_data(samples),
            Gorilla(chunk) => chunk.set_data(samples),
            Pco(chunk) => chunk.set_data(samples),
        }
    }

    /// Merge a range of samples into this chunk.
    /// If the chunk is full or the other chunk is empty, it returns 0.
    /// Duplicate values are handled according to `duplicate_policy`.
    /// Samples with timestamps before `retention_threshold` will be ignored, whether
    /// they fall with the given range [start_ts..end_ts].
    /// Returns the number of samples merged.
    pub fn merge_range(
        &mut self,
        sample_iter: impl Iterator<Item = Sample>,
        duplicate_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<usize> {
        if self.is_full() {
            return Ok(0);
        }
        let samples = sample_iter.collect::<Vec<Sample>>();
        // todo: handle error
        let res = self.merge_samples(&samples, duplicate_policy)?;
        Ok(res.iter().filter(|s| s.is_ok()).count())
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    pub fn should_split(&self) -> bool {
        self.utilization() >= SPLIT_FACTOR
    }

    pub(crate) fn upsert(
        &mut self,
        sample: Sample,
        policy: DuplicatePolicy,
    ) -> (usize, SampleAddResult) {
        match self.upsert_sample(sample, policy) {
            Ok(size) => (size, SampleAddResult::Ok(sample)),
            Err(TsdbError::DuplicateSample(_)) => (0, SampleAddResult::Duplicate),
            Err(_) => (0, SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE)),
        }
    }

    pub fn last_sample(&self) -> Option<Sample> {
        if self.is_empty() {
            None
        } else {
            Some(Sample {
                timestamp: self.last_timestamp(),
                value: self.last_value(),
            })
        }
    }

    pub fn filtered_iter<'a>(
        &'a self,
        start_timestamp: Timestamp,
        end_timestamp: Timestamp,
        timestamp_filter: Option<&'a Vec<Timestamp>>,
        value_filter: Option<ValueFilter>,
    ) -> FilteredSampleIterator<'a> {
        // determine the range of timestamps to filter
        let (start_timestamp, end_timestamp) = if let Some(ts_filter) = timestamp_filter {
            if ts_filter.is_empty() {
                (start_timestamp, end_timestamp)
            } else {
                let first_ts = ts_filter[0];
                let last_ts = ts_filter[ts_filter.len() - 1];
                (start_timestamp.max(first_ts), end_timestamp.min(last_ts))
            }
        } else {
            (start_timestamp, end_timestamp)
        };

        FilteredSampleIterator::new(
            self.range_iter(start_timestamp, end_timestamp),
            value_filter,
            timestamp_filter,
        )
    }
}

impl Chunk for TimeSeriesChunk {
    fn first_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(uncompressed) => uncompressed.first_timestamp(),
            Gorilla(gorilla) => gorilla.first_timestamp(),
            Pco(compressed) => compressed.first_timestamp(),
        }
    }

    fn last_timestamp(&self) -> Timestamp {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_timestamp(),
            Gorilla(chunk) => chunk.last_timestamp(),
            Pco(chunk) => chunk.last_timestamp(),
        }
    }

    fn len(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.len(),
            Gorilla(chunk) => chunk.len(),
            Pco(chunk) => chunk.len(),
        }
    }

    fn last_value(&self) -> f64 {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.last_value(),
            Gorilla(chunk) => chunk.last_value(),
            Pco(chunk) => chunk.last_value(),
        }
    }

    fn size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.size(),
            Gorilla(chunk) => chunk.size(),
            Pco(chunk) => chunk.size(),
        }
    }

    fn max_size(&self) -> usize {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.max_size(),
            Gorilla(chunk) => chunk.max_size(),
            Pco(chunk) => chunk.max_size(),
        }
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.remove_range(start_ts, end_ts),
            Gorilla(chunk) => chunk.remove_range(start_ts, end_ts),
            Pco(chunk) => chunk.remove_range(start_ts, end_ts),
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.add_sample(sample),
            Gorilla(chunk) => chunk.add_sample(sample),
            Pco(chunk) => chunk.add_sample(sample),
        }
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        debug_assert!(start <= end);
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.get_range(start, end),
            Gorilla(chunk) => chunk.get_range(start, end),
            Pco(chunk) => chunk.get_range(start, end),
        }
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.upsert_sample(sample, dp_policy),
            Gorilla(chunk) => chunk.upsert_sample(sample, dp_policy),
            Pco(chunk) => chunk.upsert_sample(sample, dp_policy),
        }
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        use TimeSeriesChunk::*;

        debug_assert!(!samples.is_empty());

        match self {
            Uncompressed(chunk) => chunk.merge_samples(samples, dp_policy),
            Gorilla(chunk) => chunk.merge_samples(samples, dp_policy),
            Pco(chunk) => chunk.merge_samples(samples, dp_policy),
        }
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => Ok(Uncompressed(chunk.split()?)),
            Gorilla(chunk) => Ok(Gorilla(chunk.split()?)),
            Pco(chunk) => Ok(Pco(chunk.split()?)),
        }
    }

    fn optimize(&mut self) -> TsdbResult {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.optimize(),
            Gorilla(chunk) => chunk.optimize(),
            Pco(chunk) => chunk.optimize(),
        }
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        use TimeSeriesChunk::*;
        save_chunk_type(self, rdb);
        match self {
            Uncompressed(chunk) => chunk.save_rdb(rdb),
            Gorilla(chunk) => chunk.save_rdb(rdb),
            Pco(chunk) => chunk.save_rdb(rdb),
        }
    }

    fn load_rdb(rdb: *mut RedisModuleIO, enc_ver: i32) -> ValkeyResult<Self> {
        use TimeSeriesChunk::*;
        let chunk_type = rdb_load_string(rdb)?;
        let chunk = match chunk_type.as_str() {
            "uncompressed" => Uncompressed(UncompressedChunk::load_rdb(rdb, enc_ver)?),
            "gorilla" => Gorilla(GorillaChunk::load_rdb(rdb, enc_ver)?),
            "pco" => Pco(PcoChunk::load_rdb(rdb, enc_ver)?),
            _ => return Err(ValkeyError::Str("Invalid chunk type")),
        };
        Ok(chunk)
    }

    fn debug_digest(&self, dig: &mut Digest) {
        use TimeSeriesChunk::*;
        match self {
            Uncompressed(chunk) => chunk.debug_digest(dig),
            Gorilla(chunk) => chunk.debug_digest(dig),
            Pco(chunk) => chunk.debug_digest(dig),
        }
    }
}

fn save_chunk_type(chunk: &TimeSeriesChunk, rdb: *mut RedisModuleIO) {
    let chunk_type = match chunk {
        TimeSeriesChunk::Uncompressed(_) => "uncompressed",
        TimeSeriesChunk::Gorilla(_) => "gorilla",
        TimeSeriesChunk::Pco(_) => "pco",
    };
    raw::save_string(rdb, chunk_type);
}

impl PartialOrd for TimeSeriesChunk {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.first_timestamp().partial_cmp(&other.first_timestamp())
    }
}
