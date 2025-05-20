use super::{GorillaEncoder, GorillaIterator};
use crate::common::rdb::{rdb_load_usize, rdb_save_usize};
use crate::common::{Sample, Timestamp};
use crate::config::DEFAULT_CHUNK_SIZE_BYTES;
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::SampleIter;
use crate::series::chunks::chunk::Chunk;
use crate::series::chunks::merge::merge_samples;
use crate::series::{DuplicatePolicy, SampleAddResult};
use ahash::AHashSet;
use get_size::GetSize;
use std::mem::size_of;
use valkey_module::{RedisModuleIO, ValkeyResult};

/// `GorillaChunk` is a chunk of timeseries data encoded using Gorilla XOR encoding.
#[derive(Debug, Clone, PartialEq, GetSize)]
pub struct GorillaChunk {
    pub(crate) encoder: GorillaEncoder,
    pub max_size: usize,
}

impl Default for GorillaChunk {
    fn default() -> Self {
        Self::with_max_size(DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl GorillaChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            encoder: GorillaEncoder::new(),
            max_size,
        }
    }

    pub fn is_full(&self) -> bool {
        let data_size = self.encoder.buf().len();
        data_size >= self.max_size
    }

    pub fn clear(&mut self) {
        self.encoder.clear();
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult {
        debug_assert!(!samples.is_empty());
        self.compress(samples)?;
        // todo: complain if size > max_size
        Ok(())
    }

    fn compress(&mut self, samples: &[Sample]) -> TsdbResult {
        let mut encoder = GorillaEncoder::new();
        for sample in samples {
            push_sample(&mut encoder, sample)?;
        }
        self.encoder = encoder;
        Ok(())
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let compressed_size = self.encoder.buf().len();
        let uncompressed_size = self.len() * (size_of::<i64>() + size_of::<f64>());
        (uncompressed_size / compressed_size) as f64
    }

    pub fn data_size(&self) -> usize {
        self.encoder.get_size()
    }

    pub fn bytes_per_sample(&self) -> usize {
        let mut count = self.len();
        if count == 0 {
            // estimate 50%
            count = 2;
        }
        self.data_size() / count
    }

    /// estimate remaining capacity based on the current data size and chunk max_size
    pub fn remaining_capacity(&self) -> usize {
        self.max_size - self.data_size()
    }

    /// Estimate the number of samples that can be stored in the remaining capacity
    /// Note that for low sample counts this will be very inaccurate
    pub fn remaining_samples(&self) -> usize {
        if self.len() == 0 {
            return 0;
        }
        self.remaining_capacity() / self.bytes_per_sample()
    }

    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.get_heap_size()
    }

    pub fn iter(&self) -> SampleIter {
        self.range_iter(i64::MIN, i64::MAX)
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter {
        GorillaChunkIterator::new(self, start_ts, end_ts).into()
    }
}

impl Chunk for GorillaChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.encoder.first_ts
    }
    fn last_timestamp(&self) -> Timestamp {
        self.encoder.last_ts
    }
    fn len(&self) -> usize {
        self.encoder.num_samples
    }
    fn last_value(&self) -> f64 {
        self.encoder.last_value
    }
    fn size(&self) -> usize {
        self.data_size()
    }
    fn max_size(&self) -> usize {
        self.max_size
    }
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() || start_ts > self.last_timestamp() || end_ts < self.first_timestamp() {
            return Ok(0);
        }

        let mut new_encoder = GorillaEncoder::new();
        let saved_count = self.len();

        for value in self.encoder.iter() {
            let sample = value?;
            if sample.timestamp >= start_ts && sample.timestamp <= end_ts {
                continue;
            }
            push_sample(&mut new_encoder, &sample)?;
        }

        self.encoder = new_encoder;
        Ok(saved_count - self.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(self.max_size));
        }

        push_sample(&mut self.encoder, sample)?;
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }

        let samples = self.range_iter(start, end).collect();
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;

        if self.is_empty() {
            self.add_sample(&sample)?;
            return Ok(1);
        }

        let count = self.len();
        let mut xor_encoder = GorillaEncoder::new();

        let mut iter = self.encoder.iter();

        if ts < self.first_timestamp() {
            // add sample to the beginning
            push_sample(&mut xor_encoder, &sample)?;
        } else {
            let mut current = Sample::default();

            // add previous samples
            for item in iter.by_ref() {
                current = item?;
                if current.timestamp >= ts {
                    break;
                }
                push_sample(&mut xor_encoder, &current)?;
            }

            if current.timestamp == ts {
                duplicate_found = true;
                current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                push_sample(&mut xor_encoder, &current)?;
                iter.next();
            } else {
                push_sample(&mut xor_encoder, &sample)?;
            }
        }

        for item in iter {
            let current = item?;
            push_sample(&mut xor_encoder, &current)?;
        }

        // todo: do a self.encoder.buf.take()
        self.encoder = xor_encoder;
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        fn add_sample(
            chunk: &mut GorillaChunk,
            sample: &Sample,
            res: &mut Vec<SampleAddResult>,
        ) -> TsdbResult<()> {
            match chunk.add_sample(sample) {
                Ok(_) => {
                    res.push(SampleAddResult::Ok(sample.timestamp));
                    Ok(())
                }
                err @ Err(TsdbError::CapacityFull(_)) => Err(err.unwrap_err()),
                Err(_e) => {
                    // todo: log error
                    res.push(SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE));
                    Ok(())
                }
            }
        }

        let mut result = Vec::with_capacity(samples.len());

        // We assume that samples are sorted. Try to optimize by seeing if all samples are past the
        // current chunk's last timestamp.
        let first = samples[0];
        if self.is_empty() || first.timestamp > self.last_timestamp() {
            // set_data
            for sample in samples.iter() {
                add_sample(self, sample, &mut result)?;
            }
            return Ok(result);
        }

        let mut sample_set: AHashSet<Timestamp> = AHashSet::with_capacity(samples.len());
        for sample in samples.iter() {
            sample_set.insert(sample.timestamp);
        }

        struct MergeState {
            xor_encoder: GorillaEncoder,
            result: Vec<SampleAddResult>,
        }

        let mut merge_state = MergeState {
            xor_encoder: GorillaEncoder::new(),
            result: Vec::with_capacity(samples.len()),
        };

        let left = self.iter();
        let right = SampleIter::Slice(samples.iter());

        merge_samples(
            left,
            right,
            dp_policy,
            &mut merge_state,
            |state, sample, is_duplicate| {
                push_sample(&mut state.xor_encoder, &sample)?;
                if sample_set.remove(&sample.timestamp) {
                    if is_duplicate {
                        state.result.push(SampleAddResult::Duplicate);
                    } else {
                        state.result.push(SampleAddResult::Ok(sample.timestamp));
                    }
                }
                Ok(())
            },
        )?;

        self.encoder = merge_state.xor_encoder;
        Ok(merge_state.result)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut left_chunk = GorillaEncoder::new();
        let mut right_chunk = GorillaChunk::default();

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mut first_ts: Timestamp = 0;
        let mut other_first_ts: Timestamp = 0;

        let mid = self.len() / 2;
        for (i, value) in self.encoder.iter().enumerate() {
            let sample = value?;
            if i < mid {
                if first_ts == 0 {
                    first_ts = sample.timestamp;
                }
                // todo: handle min and max timestamps
                push_sample(&mut left_chunk, &sample)?;
            } else {
                if other_first_ts == 0 {
                    other_first_ts = sample.timestamp;
                }
                push_sample(&mut right_chunk.encoder, &sample)?;
            }
        }
        self.encoder = left_chunk;
        Ok(right_chunk)
    }

    fn optimize(&mut self) -> TsdbResult {
        self.encoder.shrink_to_fit();
        Ok(())
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        self.encoder.rdb_save(rdb);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let encoder = GorillaEncoder::rdb_load(rdb)?;
        let chunk = GorillaChunk { encoder, max_size };
        Ok(chunk)
    }
}

#[inline]
fn push_sample(encoder: &mut GorillaEncoder, sample: &Sample) -> TsdbResult {
    encoder.add_sample(sample).map_err(|e| {
        eprintln!("Error adding sample: {:?}", e);
        TsdbError::CannotAddSample(*sample)
    })
}

pub struct GorillaChunkIterator<'a> {
    inner: GorillaIterator<'a>,
    start: Timestamp,
    end: Timestamp,
    init: bool,
}

impl<'a> GorillaChunkIterator<'a> {
    pub fn new(chunk: &'a GorillaChunk, start: Timestamp, end: Timestamp) -> Self {
        let inner = GorillaIterator::new(&chunk.encoder);
        Self {
            inner,
            start,
            end,
            init: false,
        }
    }

    fn next_internal(&mut self) -> Option<Sample> {
        match self.inner.next() {
            Some(Ok(sample)) => {
                if sample.timestamp > self.end {
                    return None;
                }
                Some(sample)
            }
            #[cfg(debug_assertions)]
            Some(Err(err)) => {
                eprintln!("Error decoding sample: {:?}", err);
                None
            }
            #[cfg(not(debug_assertions))]
            Some(Err(_)) => None,
            None => None,
        }
    }
}

impl Iterator for GorillaChunkIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.init {
            self.init = true;

            while let Some(sample) = self.next_internal() {
                if sample.timestamp < self.start {
                    continue;
                }
                if sample.timestamp <= self.end {
                    return Some(sample);
                }
            }

            return None;
        }
        self.next_internal()
    }
}

#[cfg(test)]
mod tests {
    use crate::common::Sample;
    use crate::series::chunks::chunk::Chunk;
    use crate::series::chunks::gorilla::gorilla_chunk::GorillaChunk;
    use crate::series::DuplicatePolicy;
    use crate::tests::generators::DataGenerator;
    use std::time::Duration;

    fn generate_samples(count: usize) -> Vec<Sample> {
        DataGenerator::builder()
            .samples(count)
            .start(1000)
            .decimal_digits(3)
            .interval(Duration::from_millis(1000))
            .build()
            .generate()
    }

    fn decompress(chunk: &GorillaChunk) -> Vec<Sample> {
        chunk.iter().collect()
    }

    #[test]
    fn test_chunk_compress() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let data = generate_samples(1000);

        for sample in data.iter() {
            chunk.add_sample(sample).unwrap();
        }
        assert_eq!(chunk.len(), data.len());
        assert_eq!(chunk.first_timestamp(), data[0].timestamp);
        assert_eq!(chunk.last_timestamp(), data[data.len() - 1].timestamp);
        assert_eq!(chunk.last_value(), data[data.len() - 1].value);
    }

    #[test]
    fn test_clear() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let data = generate_samples(500);

        for datum in data.iter() {
            chunk.add_sample(datum).unwrap();
        }

        assert_eq!(chunk.len(), data.len());
        chunk.clear();
        assert_eq!(chunk.len(), 0);
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
    }

    #[test]
    fn test_upsert() {
        for chunk_size in (64..8192).step_by(64) {
            const SAMPLE_COUNT: usize = 200;
            let samples = generate_samples(SAMPLE_COUNT);
            let mut chunk = GorillaChunk::with_max_size(chunk_size);

            let sample_count = samples.len();
            for sample in samples.into_iter() {
                chunk
                    .upsert_sample(sample, DuplicatePolicy::KeepLast)
                    .unwrap();
            }
            assert_eq!(chunk.len(), sample_count);
        }
    }

    #[test]
    fn test_split() {
        const COUNT: usize = 500;
        let samples = generate_samples(COUNT);
        let mut chunk = GorillaChunk::with_max_size(16384);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        let count = samples.len();
        let mid = count / 2;

        let right = chunk.split().unwrap();
        assert_eq!(chunk.len(), mid);
        assert_eq!(right.len(), mid);

        let (left_samples, right_samples) = samples.split_at(mid);

        let right_decompressed = decompress(&right);
        assert_eq!(right_decompressed, right_samples);

        let left_decompressed = decompress(&chunk);
        assert_eq!(left_decompressed, left_samples);
    }

    #[test]
    fn test_split_odd() {
        const COUNT: usize = 51;
        let samples = generate_samples(COUNT);
        let mut chunk = GorillaChunk::default();

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
    fn test_iter() {
        let mut chunk = GorillaChunk::default();
        let data = generate_samples(1000);

        chunk.set_data(&data).unwrap();

        let actual: Vec<_> = chunk.iter().collect();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_remove_range() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_samples(100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Remove a range that covers the first half of the samples
        let mid = samples.len() / 2;
        let start_ts = samples[0].timestamp;
        let mid_ts = samples[mid].timestamp;
        let removed_count = chunk.remove_range(start_ts, mid_ts).unwrap();
        // range is inclusive, so we would have deleted mid + 1
        assert_eq!(removed_count, mid + 1);

        // Ensure the remaining samples are correct
        let remaining_samples: Vec<_> = chunk.iter().collect();
        let expected_samples = &samples[mid + 1..];
        assert_eq!(remaining_samples, expected_samples);

        // Remove a range that covers the remaining samples
        let end_ts = samples[samples.len() - 1].timestamp;
        let removed_count = chunk.remove_range(mid_ts, end_ts).unwrap();
        assert_eq!(removed_count, mid - 1);

        // Ensure the chunk is empty
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_remove_range_no_overlap() {
        let mut chunk = GorillaChunk::with_max_size(16384);
        let samples = generate_samples(100);

        for sample in samples.iter() {
            chunk.add_sample(sample).unwrap();
        }

        // Attempt to remove a range that does not overlap with any samples
        let start_ts = samples[samples.len() - 1].timestamp + 1;
        let end_ts = start_ts + 1000;
        let removed_count = chunk.remove_range(start_ts, end_ts).unwrap();
        assert_eq!(removed_count, 0);

        // Ensure all samples are still present
        let remaining_samples: Vec<_> = chunk.iter().collect();
        assert_eq!(remaining_samples, samples);
    }
}
