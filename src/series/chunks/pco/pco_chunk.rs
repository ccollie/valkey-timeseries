use crate::common::binary_search::{find_last_ge_index, ExponentialSearch};
use crate::common::constants::VEC_BASE_SIZE;
use crate::common::parallel::join;
use crate::common::pool::{get_pooled_vec_f64, get_pooled_vec_i64, PooledVecF64, PooledVecI64};
use crate::common::serialization::{rdb_load_usize, rdb_save_usize};
use crate::common::{Sample, Timestamp};
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::merge::merge_samples;
use crate::series::chunks::pco::pco_utils::{
    compress_timestamps, compress_values, decompress_timestamps, decompress_values,
};
use crate::series::chunks::pco::PcoSampleIterator;
use crate::series::chunks::utils::get_timestamp_index_bounds;
use crate::series::chunks::Chunk;
use crate::series::{DuplicatePolicy, SampleAddResult};
use ahash::AHashSet;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use valkey_module::{raw, RedisModuleIO, ValkeyResult};

/// items above this count will cause value and timestamp encoding/decoding to happen in parallel
pub(in crate::series) const COMPRESSION_PARALLELIZATION_THRESHOLD: usize = 1024;

/// `PcoChunk` holds sample data encoded using Pco compression.
/// https://github.com/mwlon/pcodec
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, GetSize)]
pub struct PcoChunk {
    pub min_time: Timestamp,
    pub max_time: Timestamp,
    pub max_size: usize,
    pub last_value: f64,
    /// number of compressed samples
    pub count: usize,
    pub timestamps: Vec<u8>,
    pub values: Vec<u8>,
}

impl Default for PcoChunk {
    fn default() -> Self {
        Self {
            min_time: 0,
            max_time: i64::MAX,
            max_size: DEFAULT_CHUNK_SIZE_BYTES,
            last_value: 0.0,
            count: 0,
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl PcoChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        PcoChunk {
            max_size,
            ..Self::default()
        }
    }

    pub fn is_full(&self) -> bool {
        self.data_size() >= self.max_size
    }

    pub fn clear(&mut self) {
        self.count = 0;
        self.timestamps.clear();
        self.values.clear();
        self.min_time = 0;
        self.max_time = 0;
        self.last_value = f64::NAN; // todo - use option instead
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        if samples.is_empty() {
            self.clear();
            return Ok(());
        }
        let mut timestamps = get_pooled_vec_i64(samples.len());
        let mut values = get_pooled_vec_f64(samples.len());

        for sample in samples {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
        }

        self.compress(&timestamps, &values)?;
        self.min_time = samples[0].timestamp;
        // todo: complain if size > max_size
        Ok(())
    }

    fn compress(&mut self, timestamps: &[Timestamp], values: &[f64]) -> TsdbResult {
        if timestamps.is_empty() {
            self.clear();
            return Ok(());
        }
        debug_assert_eq!(timestamps.len(), values.len());
        // todo: validate range
        self.min_time = timestamps[0];
        self.max_time = timestamps[timestamps.len() - 1];
        self.count = timestamps.len();
        self.last_value = values[values.len() - 1];
        if timestamps.len() > COMPRESSION_PARALLELIZATION_THRESHOLD {
            // use rayon to run compression in parallel
            // first we steal the result buffers to avoid allocation and issues with the BC
            let mut t_data = std::mem::take(&mut self.timestamps);
            let mut v_data = std::mem::take(&mut self.values);

            t_data.clear();
            v_data.clear();

            // then we compress in parallel
            // TODO: handle errors
            let _ = join(
                || compress_timestamps(&mut t_data, timestamps).ok(),
                || compress_values(&mut v_data, values).ok(),
            );
            // then we put the buffers back
            self.timestamps = t_data;
            self.values = v_data;
        } else {
            self.timestamps.clear();
            self.values.clear();
            compress_timestamps(&mut self.timestamps, timestamps)?;
            compress_values(&mut self.values, values)?;
        }
        self.count = timestamps.len();
        self.timestamps.shrink_to_fit();
        self.values.shrink_to_fit();
        Ok(())
    }

    fn decompress(&self) -> TsdbResult<Option<(PooledVecI64, PooledVecF64)>> {
        if self.is_empty() {
            return Ok(None);
        }
        let mut timestamps = get_pooled_vec_i64(self.count);
        let mut values = get_pooled_vec_f64(self.count);

        self.decompress_internal(&mut timestamps, &mut values)?;
        Ok(Some((timestamps, values)))
    }

    fn decompress_internal(
        &self,
        timestamps: &mut Vec<Timestamp>,
        values: &mut Vec<f64>,
    ) -> TsdbResult<()> {
        timestamps.reserve(self.count);
        values.reserve(self.count);
        // todo: dynamically calculate cutoff or just use chili
        if self.values.len() > 2048 {
            // todo: return errors as appropriate
            let _ = join(
                || decompress_timestamps(&self.timestamps, timestamps).ok(),
                || decompress_values(&self.values, values).ok(),
            );
        } else {
            decompress_timestamps(&self.timestamps, timestamps)?;
            decompress_values(&self.values, values)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn decompress_samples(&self) -> TsdbResult<Vec<Sample>> {
        if let Some((timestamps, values)) = self.decompress()? {
            let samples = timestamps
                .iter()
                .zip(values.iter())
                .map(|(ts, value)| Sample {
                    timestamp: *ts,
                    value: *value,
                })
                .collect();
            Ok(samples)
        } else {
            Ok(vec![])
        }
    }

    pub fn timestamp_compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.timestamps.len() as f64;
        let uncompressed_size = (self.count * size_of::<i64>()) as f64;
        uncompressed_size / compressed_size
    }

    pub fn value_compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.values.len() as f64;
        let uncompressed_size = (self.count * size_of::<f64>()) as f64;
        uncompressed_size / compressed_size
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = (self.timestamps.len() + self.values.len()) as f64;
        let uncompressed_size = (self.count * size_of::<Sample>()) as f64;
        uncompressed_size / compressed_size
    }

    pub fn data_size(&self) -> usize {
        self.timestamps.get_heap_size() + self.values.get_heap_size() + 2 * VEC_BASE_SIZE
    }

    pub fn bytes_per_sample(&self) -> usize {
        let count = if self.count == 0 {
            // estimate 50%
            2
        } else {
            self.count
        };
        self.data_size() / count
    }

    /// estimate remaining capacity based on the current data size and chunk max_size
    pub fn remaining_capacity(&self) -> usize {
        self.max_size - self.data_size()
    }

    /// Estimate the number of samples that can be stored in the remaining capacity
    /// Note that for low sample counts this will be very inaccurate
    pub fn remaining_samples(&self) -> usize {
        if self.count == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        match PcoSampleIterator::new(&self.timestamps, &self.values) {
            Ok(iter) => iter.into(),
            Err(_) => {
                // todo: log error
                SampleIter::Empty
            }
        }
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter {
        let iter = PcoSampleIterator::new_range(&self.timestamps, &self.values, start_ts, end_ts);
        match iter {
            Ok(iter) => iter.into(),
            Err(_) => {
                // todo: log error
                SampleIter::Empty
            }
        }
    }

    fn merge_internal(
        &mut self,
        current: &[Sample],
        samples: &[Sample],
        dp_policy: DuplicatePolicy,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        struct MergeState {
            values: PooledVecF64,
            timestamps: PooledVecI64,
            result: Vec<SampleAddResult>,
        }

        // we don't do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        let capacity = current.len() + samples.len();

        let mut merge_state = MergeState {
            values: get_pooled_vec_f64(capacity),
            timestamps: get_pooled_vec_i64(capacity),
            result: Vec::with_capacity(samples.len()),
        };

        // eliminate results not related to the new samples
        let mut sample_set: AHashSet<Timestamp> = AHashSet::with_capacity(samples.len());
        for sample in samples.iter() {
            sample_set.insert(sample.timestamp);
        }

        let left = SampleIter::Slice(current.iter());
        let right = SampleIter::Slice(samples.iter());

        merge_samples(
            left,
            right,
            Some(dp_policy),
            &mut merge_state,
            |state, sample, is_duplicate| {
                let is_new = sample_set.remove(&sample.timestamp);
                state.values.push(sample.value);
                state.timestamps.push(sample.timestamp);
                if is_new {
                    if is_duplicate {
                        state.result.push(SampleAddResult::Duplicate);
                    } else {
                        state.result.push(SampleAddResult::Ok(sample.timestamp));
                    }
                }
                Ok(())
            },
        )?;

        self.compress(&merge_state.timestamps, &merge_state.values)?;

        Ok(merge_state.result)
    }
}

impl Chunk for PcoChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.min_time
    }
    fn last_timestamp(&self) -> Timestamp {
        self.max_time
    }
    fn len(&self) -> usize {
        self.count
    }
    fn last_value(&self) -> f64 {
        self.last_value
    }
    fn size(&self) -> usize {
        self.data_size()
    }
    fn max_size(&self) -> usize {
        self.max_size
    }
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }
        if start_ts > self.max_time || end_ts < self.min_time {
            return Ok(0);
        }
        if let Some((mut timestamps, mut values)) = self.decompress()? {
            let count = timestamps.len();
            remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

            let removed_count = count - timestamps.len();
            if timestamps.is_empty() {
                self.clear();
            } else {
                self.compress(&timestamps, &values)?;
            }
            Ok(removed_count)
        } else {
            Ok(0)
        }
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }

        if let Some((mut timestamps, mut values)) = self.decompress()? {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
            self.compress(&timestamps, &values)
        } else {
            let timestamps = vec![sample.timestamp];
            let values = vec![sample.value];
            self.compress(&timestamps, &values)
        }
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        // todo: use iterator instead
        if let Some((timestamps, values)) = self.decompress()? {
            if let Some((start_index, end_index)) =
                get_timestamp_index_bounds(&timestamps, start, end)
            {
                let stamps = &timestamps[start_index..=end_index];
                let values = &values[start_index..=end_index];
                return Ok(stamps
                    .iter()
                    .cloned()
                    .zip(values.iter().cloned())
                    .map(|(timestamp, value)| Sample { timestamp, value })
                    .collect());
            }
        }
        Ok(vec![])
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        // we don't do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old one
        if let Some((mut timestamps, mut values)) = self.decompress()? {
            let ts = sample.timestamp;
            let (pos, duplicate_found) = get_timestamp_index(&timestamps, ts, 0);

            if duplicate_found {
                values[pos] = dp_policy.duplicate_value(ts, values[pos], sample.value)?;
            } else if pos == timestamps.len() {
                timestamps.push(ts);
                values.push(sample.value);
            } else {
                timestamps.insert(pos, ts);
                values.insert(pos, sample.value);
            }

            self.compress(&timestamps, &values)?;
            Ok(timestamps.len())
        } else {
            self.add_sample(&sample)?;
            Ok(1)
        }
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }
        let first = samples[0];
        let dp_policy = dp_policy.unwrap_or(DuplicatePolicy::Block);

        let mut result = Vec::with_capacity(samples.len());

        if self.is_empty() || first.timestamp > self.last_timestamp() {
            // we don't do streaming compression, so we have to accumulate all the samples
            // in a new chunk and then swap it with the old one
            let mut timestamps = get_pooled_vec_i64(self.count + samples.len());
            let mut values = get_pooled_vec_f64(self.count + samples.len());

            if self.count > 0 {
                self.decompress_internal(&mut timestamps, &mut values)?;
            }

            for sample in samples {
                timestamps.push(sample.timestamp);
                values.push(sample.value);
                result.push(SampleAddResult::Ok(sample.timestamp));
            }

            self.compress(&timestamps, &values)?;
            return Ok(result);
        }

        if let Some((mut timestamps, mut values)) = self.decompress()? {
            let first = samples[0];

            if first.timestamp > self.last_timestamp() {
                timestamps.reserve(samples.len());
                values.reserve(samples.len());
                for sample in samples {
                    timestamps.push(sample.timestamp);
                    values.push(sample.value);
                    result.push(SampleAddResult::Ok(sample.timestamp));
                }
                self.compress(&timestamps, &values)?;
                return Ok(result);
            }

            let current: Vec<Sample> = timestamps
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .map(|(ts, value)| Sample::new(ts, value))
                .collect::<Vec<Sample>>();

            return self.merge_internal(&current, samples, dp_policy);
        }

        Ok(result)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut result = Self::with_max_size(self.max_size);

        if self.is_empty() {
            return Ok(result);
        }

        let mid = self.len() / 2;

        // this compression method does not do streaming compression, so we have to accumulate all the samples
        // in a new chunk and then swap it with the old
        if let Some((timestamps, mut values)) = self.decompress()? {
            let (left_timestamps, right_timestamps) = timestamps.split_at(mid);
            let (left_values, right_values) = values.split_at_mut(mid);

            let (res_a, res_b) = join(
                || self.compress(left_timestamps, left_values).ok(),
                || result.compress(right_timestamps, right_values).ok(),
            );
            if res_a.is_none() || res_b.is_none() {
                return Err(TsdbError::CannotSerialize("ERR splitting node".to_string()));
            }
        }

        Ok(result)
    }

    fn optimize(&mut self) -> TsdbResult<()> {
        self.timestamps.shrink_to_fit();
        self.values.shrink_to_fit();
        Ok(())
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        raw::save_signed(rdb, self.min_time);
        raw::save_signed(rdb, self.max_time);
        rdb_save_usize(rdb, self.max_size);
        raw::save_double(rdb, self.last_value);
        rdb_save_usize(rdb, self.count);
        raw::save_slice(rdb, &self.timestamps);
        raw::save_slice(rdb, &self.values);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let min_time = raw::load_signed(rdb)?;
        let max_time = raw::load_signed(rdb)?;
        let max_size = rdb_load_usize(rdb)?;
        let last_value = raw::load_double(rdb)?;
        let count = rdb_load_usize(rdb)?;
        let ts = raw::load_string_buffer(rdb)?;
        let vals = raw::load_string_buffer(rdb)?;
        let timestamps: Vec<u8> = Vec::from(ts.as_ref());
        let values: Vec<u8> = Vec::from(vals.as_ref());

        Ok(PcoChunk {
            min_time,
            max_time,
            max_size,
            last_value,
            count,
            timestamps,
            values,
        })
    }
}

fn get_timestamp_index(timestamps: &[Timestamp], ts: Timestamp, start_ofs: usize) -> (usize, bool) {
    let stamps = &timestamps[start_ofs..];
    // for regularly spaced timestamps, we can get extreme levels of compression. Also since pco is
    // intended for "cold" storage we can have larger than normal numbers of samples.
    // if we pass a threshold, see if we should use an exponential search
    let idx = if should_use_exponential_search(stamps, ts) {
        stamps
            .exponential_search_by(|s| s.cmp(&ts))
            .unwrap_or_else(|x| x.saturating_sub(1))
    } else {
        // binary search
        find_last_ge_index(stamps, &ts)
    };
    if idx > timestamps.len() - 1 {
        return (timestamps.len() - 1, false);
    }
    // todo: get_unchecked
    (idx + start_ofs, stamps[idx] == ts)
}

const EXPONENTIAL_SEARCH_THRESHOLD: usize = 65536;
fn should_use_exponential_search(timestamps: &[Timestamp], ts: Timestamp) -> bool {
    if timestamps.len() < EXPONENTIAL_SEARCH_THRESHOLD {
        return false;
    }
    // exponential search is only worth it if we're near the beginning, so we use the following heuristic:
    // 1. Assume timestamps are equally spaced
    // 2. Get delta of `ts` from the start
    // 3. Calculate delta as a percentage of the range
    // 4. If delta is less than 20% of the range, return true. Otherwise, return false.
    // This heuristic assumes that timestamps are evenly spaced. If they aren't, this heuristic may not be accurate.
    let start_ts = timestamps[0];
    let end_ts = timestamps[timestamps.len() - 1];
    let delta = (ts - start_ts) as f64 / (end_ts - start_ts) as f64;
    delta < 0.20
}

fn remove_values_in_range(
    timestamps: &mut Vec<Timestamp>,
    values: &mut Vec<f64>,
    start_ts: Timestamp,
    end_ts: Timestamp,
) {
    debug_assert_eq!(
        timestamps.len(),
        values.len(),
        "Timestamps and scores vectors must be of the same length"
    );
    if let Some((start_index, end_index)) = get_timestamp_index_bounds(timestamps, start_ts, end_ts)
    {
        timestamps.drain(start_index..=end_index);
        values.drain(start_index..=end_index);
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{Sample, Timestamp};
    use std::time::Duration;

    use crate::error::TsdbError;
    use crate::series::chunks::pco::pco_chunk::remove_values_in_range;
    use crate::series::chunks::Chunk;
    use crate::series::chunks::PcoChunk;
    use crate::series::{DuplicatePolicy, SampleAddResult};
    use crate::tests::generators::DataGenerator;

    fn decompress(chunk: &PcoChunk) -> Vec<Sample> {
        chunk.iter().collect()
    }

    fn generate_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .interval(Duration::from_millis(1000))
            .build()
            .generate()
    }

    fn saturate_chunk(chunk: &mut PcoChunk) {
        loop {
            let samples = generate_samples(250);
            for sample in samples {
                match chunk.add_sample(&sample) {
                    Ok(_) => {}
                    Err(TsdbError::CapacityFull(_)) => break,
                    Err(e) => panic!("unexpected error: {:?}", e),
                }
            }
        }
    }

    fn compare_chunks(chunk1: &PcoChunk, chunk2: &PcoChunk) {
        assert_eq!(chunk1.min_time, chunk2.min_time, "min_time");
        assert_eq!(chunk1.max_time, chunk2.max_time);
        assert_eq!(chunk1.max_size, chunk2.max_size);
        assert_eq!(chunk1.last_value, chunk2.last_value);
        assert_eq!(
            chunk1.count, chunk2.count,
            "mismatched counts {} vs {}",
            chunk1.count, chunk2.count
        );
        assert_eq!(chunk1.timestamps, chunk2.timestamps);
        assert_eq!(chunk1.values, chunk2.values);
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = PcoChunk::default();
        let data = generate_samples(1000);

        chunk.set_data(&data).unwrap();
        assert_eq!(chunk.len(), data.len());
        assert_eq!(chunk.min_time, data[0].timestamp);
        assert_eq!(chunk.max_time, data[data.len() - 1].timestamp);
        assert_eq!(chunk.last_value, data[data.len() - 1].value);
        assert!(!chunk.timestamps.is_empty());
        assert!(!chunk.values.is_empty());
    }

    #[test]
    fn test_compress_decompress() {
        let mut chunk = PcoChunk::default();
        let data = generate_samples(1000);
        chunk.set_data(&data).unwrap();
        let actual = chunk.decompress_samples().unwrap();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_clear() {
        let mut chunk = PcoChunk::default();
        let data = generate_samples(1000);

        chunk.set_data(&data).unwrap();
        assert_eq!(chunk.len(), data.len());
        chunk.clear();
        assert_eq!(chunk.len(), 0);
        assert_eq!(chunk.min_time, 0);
        assert_eq!(chunk.max_time, 0);
        assert!(chunk.last_value.is_nan());
        assert_eq!(chunk.timestamps.len(), 0);
        assert_eq!(chunk.values.len(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            let data = generate_samples(500);
            let mut chunk = PcoChunk::with_max_size(chunk_size);

            let data_len = data.len();
            for sample in data.into_iter() {
                chunk
                    .upsert_sample(sample, DuplicatePolicy::KeepLast)
                    .unwrap();
            }
            assert_eq!(chunk.len(), data_len);
        }
    }

    #[test]
    fn test_upsert_while_at_capacity() {
        let mut chunk = PcoChunk::with_max_size(4096);
        saturate_chunk(&mut chunk);

        let timestamp = chunk.last_timestamp();

        // return an error on insert
        let mut sample = Sample {
            timestamp: 0,
            value: 1.0,
        };

        assert!(chunk
            .upsert_sample(sample, DuplicatePolicy::KeepLast)
            .is_err());

        // should update value for duplicate timestamp
        sample.timestamp = timestamp;
        let res = chunk.upsert_sample(sample, DuplicatePolicy::KeepLast);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 0);
    }

    #[test]
    fn test_split() {
        let mut chunk = PcoChunk::default();
        let data = generate_samples(500);
        chunk.set_data(&data).unwrap();

        let count = data.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid);

        let (left_samples, right_samples) = data.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_split_odd() {
        let mut chunk = PcoChunk::default();
        let samples = generate_samples(51);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid + 1);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_remove_values_in_range_single_timestamp() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 3;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2, 4, 5]);
        assert_eq!(values, vec![1.0, 2.0, 4.0, 5.0]);
    }

    #[test]
    fn test_remove_values_in_range_all_encompassing() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 0;
        let end_ts = 6;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert!(timestamps.is_empty(), "All timestamps should be removed");
        assert!(values.is_empty(), "All values should be removed");
    }

    #[test]
    fn test_remove_values_in_range_33_elements() {
        let mut timestamps: Vec<Timestamp> = (0..33).collect();
        let mut values: Vec<f64> = (0..33).map(|x| x as f64).collect();

        let start_ts = 10;
        let end_ts = 20;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps.len(), 22);
        assert_eq!(values.len(), 22);

        for i in 0..10 {
            assert_eq!(timestamps[i], i as Timestamp);
            assert_eq!(values[i], i as f64);
        }

        for i in 10..22 {
            assert_eq!(timestamps[i], (i + 11) as Timestamp);
            assert_eq!(values[i], (i + 11) as f64);
        }
    }

    #[test]
    fn test_remove_values_in_range_start_equals_end() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 3;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2, 4, 5]);
        assert_eq!(values, vec![1.0, 2.0, 4.0, 5.0]);
    }

    #[test]
    fn test_remove_values_in_range_end_after_last_timestamp() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 10;

        remove_values_in_range(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2]);
        assert_eq!(values, vec![1.0, 2.0]);
    }

    #[test]
    fn test_range_iter_partial_overlap() {
        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 200,
                value: 2.0,
            },
            Sample {
                timestamp: 300,
                value: 3.0,
            },
            Sample {
                timestamp: 400,
                value: 4.0,
            },
            Sample {
                timestamp: 500,
                value: 5.0,
            },
        ];

        let mut chunk = PcoChunk::default();
        chunk.set_data(&samples).unwrap();

        let result: Vec<Sample> = chunk.range_iter(150, 450).collect();

        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            Sample {
                timestamp: 200,
                value: 2.0
            }
        );
        assert_eq!(
            result[1],
            Sample {
                timestamp: 300,
                value: 3.0
            }
        );
        assert_eq!(
            result[2],
            Sample {
                timestamp: 400,
                value: 4.0
            }
        );
    }

    #[test]
    fn test_range_iter_exact_boundaries() {
        let mut chunk = PcoChunk::default();
        let samples = vec![
            Sample {
                timestamp: 100,
                value: 1.0,
            },
            Sample {
                timestamp: 200,
                value: 2.0,
            },
            Sample {
                timestamp: 300,
                value: 3.0,
            },
            Sample {
                timestamp: 400,
                value: 4.0,
            },
            Sample {
                timestamp: 500,
                value: 5.0,
            },
        ];
        chunk.set_data(&samples).unwrap();

        let result: Vec<Sample> = chunk.range_iter(200, 400).collect();

        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            Sample {
                timestamp: 200,
                value: 2.0
            }
        );
        assert_eq!(
            result[1],
            Sample {
                timestamp: 300,
                value: 3.0
            }
        );
        assert_eq!(
            result[2],
            Sample {
                timestamp: 400,
                value: 4.0
            }
        );
    }

    #[test]
    fn test_range_iter_performance_on_large_chunks() {
        let mut chunk = PcoChunk::default();
        let num_samples = 1_000_000;
        let samples: Vec<Sample> = (0..num_samples)
            .map(|i| Sample {
                timestamp: i as i64,
                value: i as f64,
            })
            .collect();
        chunk.set_data(&samples).unwrap();

        let start_time = std::time::Instant::now();
        let range_samples: Vec<Sample> = chunk.range_iter(250_000, 750_000).collect();
        let duration = start_time.elapsed();

        assert_eq!(range_samples.len(), 500_001);
        assert!(
            duration.as_millis() < 1000,
            "Range iteration took too long: {:?}",
            duration
        );
    }

    #[test]
    fn test_range_iter_empty_chunk() {
        let chunk = PcoChunk::default();
        let start_ts = 0;
        let end_ts = 100;

        let samples: Vec<Sample> = chunk.range_iter(start_ts, end_ts).collect();

        assert!(
            samples.is_empty(),
            "Range iterator should return no samples for an empty chunk"
        );
    }

    #[test]
    fn test_merge_samples_with_greater_timestamps() {
        let mut chunk = PcoChunk::default();
        let mut initial_samples = generate_samples(5);
        chunk.set_data(&initial_samples).unwrap();

        let mut timestamp = chunk.last_timestamp() + 5000;
        let mut new_samples = generate_samples(5);

        for sample in new_samples.iter_mut() {
            sample.timestamp = timestamp;
            timestamp += 1000;
        }

        let result = chunk.merge_samples(&new_samples, None).unwrap();

        // Check that all new samples were added successfully
        for (i, sample) in new_samples.iter().enumerate() {
            assert_eq!(result[i], SampleAddResult::Ok(sample.timestamp));
        }

        // Verify the chunk now contains the initial and new samples
        initial_samples.extend_from_slice(&new_samples);
        let actual = decompress(&chunk);
        assert_eq!(initial_samples, actual);
    }

    #[test]
    fn test_merge_samples_with_timestamps_less_than_first() {
        let mut chunk = PcoChunk::default();
        let existing_samples = generate_samples(1000);
        let (left_samples, right_samples) = existing_samples.split_at(500);
        chunk.set_data(right_samples).unwrap();

        // Merge the new samples into the chunk
        let result = chunk
            .merge_samples(left_samples, Some(DuplicatePolicy::Block))
            .unwrap();

        // Check that all new samples were added successfully
        for (i, sample) in left_samples.iter().enumerate() {
            assert_eq!(result[i], SampleAddResult::Ok(sample.timestamp));
        }

        // Verify that the chunk now contains both the new and existing samples
        let expected_samples: Vec<Sample> = left_samples
            .iter()
            .chain(right_samples.iter())
            .cloned()
            .collect();
        let actual_samples = decompress(&chunk);
        assert_eq!(actual_samples, expected_samples);
    }

    #[test]
    fn test_merge_samples_with_duplicates() {
        let mut chunk = PcoChunk::default();
        let initial_samples = vec![
            Sample {
                timestamp: 1,
                value: 10.0,
            },
            Sample {
                timestamp: 2,
                value: 20.0,
            },
            Sample {
                timestamp: 3,
                value: 30.0,
            },
        ];
        chunk.set_data(&initial_samples).unwrap();

        let new_samples = vec![
            Sample {
                timestamp: 2,
                value: 25.0,
            }, // Duplicate timestamp
            Sample {
                timestamp: 4,
                value: 40.0,
            }, // New timestamp
        ];

        let result = chunk
            .merge_samples(&new_samples, Some(DuplicatePolicy::KeepLast))
            .unwrap();

        // Check that the merge result indicates successful addition
        assert_eq!(
            result,
            vec![
                SampleAddResult::Ok(2), // Duplicate, but should be overwritten
                SampleAddResult::Ok(4), // New sample
            ]
        );

        // Decompress and verify the final state of the chunk
        let expected_samples = vec![
            Sample {
                timestamp: 1,
                value: 10.0,
            },
            Sample {
                timestamp: 2,
                value: 25.0,
            }, // Overwritten value
            Sample {
                timestamp: 3,
                value: 30.0,
            },
            Sample {
                timestamp: 4,
                value: 40.0,
            },
        ];
        let actual_samples = chunk.decompress_samples().unwrap();
        assert_eq!(actual_samples, expected_samples);
    }

    #[test]
    fn test_merge_samples_with_mixed_duplicates() {
        let mut chunk = PcoChunk::default();
        let initial_samples = vec![
            Sample {
                timestamp: 1,
                value: 1.0,
            },
            Sample {
                timestamp: 2,
                value: 2.0,
            },
            Sample {
                timestamp: 3,
                value: 3.0,
            },
        ];
        chunk.set_data(&initial_samples).unwrap();

        let new_samples = vec![
            Sample {
                timestamp: 2,
                value: 2.5,
            }, // duplicate
            Sample {
                timestamp: 3,
                value: 3.5,
            }, // duplicate
            Sample {
                timestamp: 4,
                value: 4.0,
            }, // non-duplicate
        ];

        let result = chunk
            .merge_samples(&new_samples, Some(DuplicatePolicy::Block))
            .unwrap();

        // Expecting Duplicate for duplicates and Ok for non-duplicates
        assert_eq!(
            result,
            vec![
                SampleAddResult::Duplicate,
                SampleAddResult::Duplicate,
                SampleAddResult::Ok(4),
            ]
        );

        // Verify the chunk's data hasn't changed for duplicates
        let expected_samples = vec![
            Sample {
                timestamp: 1,
                value: 1.0,
            },
            Sample {
                timestamp: 2,
                value: 2.0,
            },
            Sample {
                timestamp: 3,
                value: 3.0,
            },
            Sample {
                timestamp: 4,
                value: 4.0,
            },
        ];
        let actual_samples = chunk.decompress_samples().unwrap();
        assert_eq!(actual_samples, expected_samples);
    }

    #[test]
    fn test_merge_samples_empty_chunk_default_policy() {
        let mut chunk = PcoChunk::default();
        let samples = generate_samples(10);

        // Merge samples into an empty chunk with DuplicatePolicy set to None
        let result = chunk.merge_samples(&samples, None).unwrap();

        // Ensure all samples are added successfully
        for (i, sample) in samples.iter().enumerate() {
            match result[i] {
                SampleAddResult::Ok(ts) => assert_eq!(ts, sample.timestamp),
                _ => panic!("Expected SampleAddResult::Ok, got {:?}", result[i]),
            }
        }

        // Verify the chunk now contains the samples
        let decompressed_samples = chunk.decompress_samples().unwrap();
        assert_eq!(decompressed_samples, samples);
    }

    #[test]
    fn test_merge_samples_on_boundary() {
        let mut chunk = PcoChunk::default();
        let initial_samples = generate_samples(10);

        // Set initial data to the chunk
        chunk.set_data(&initial_samples).unwrap();

        // Create samples with timestamps on the boundary of the current min_time and max_time
        let boundary_samples = vec![
            Sample {
                timestamp: chunk.min_time,
                value: 42.0,
            },
            Sample {
                timestamp: chunk.max_time,
                value: 84.0,
            },
        ];

        // Merge samples with DuplicatePolicy::KeepLast
        let result = chunk
            .merge_samples(&boundary_samples, Some(DuplicatePolicy::KeepLast))
            .unwrap();

        // Check results
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], SampleAddResult::Ok(chunk.min_time));
        assert_eq!(result[1], SampleAddResult::Ok(chunk.max_time));

        // Decompress and verify the samples
        let decompressed_samples = chunk.decompress_samples().unwrap();
        assert_eq!(decompressed_samples.len(), initial_samples.len());

        // Verify that the values at the boundary timestamps have been updated
        assert_eq!(
            decompressed_samples
                .iter()
                .find(|s| s.timestamp == chunk.min_time)
                .unwrap()
                .value,
            42.0
        );
        assert_eq!(
            decompressed_samples
                .iter()
                .find(|s| s.timestamp == chunk.max_time)
                .unwrap()
                .value,
            84.0
        );
    }

    #[test]
    fn test_merge_samples_with_empty_array() {
        let mut chunk = PcoChunk::default();
        let result = chunk.merge_samples(&[], None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<SampleAddResult>::new());
    }
}
