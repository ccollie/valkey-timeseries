use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::logging::log_warning;
use crate::common::rdb::{rdb_load_f64, rdb_load_timestamp, rdb_load_usize, rdb_save_f64, rdb_save_timestamp, rdb_save_usize};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::SampleIter;
use crate::series::chunks::tsxor::tsxor_compressor::CompressorTSXor;
use crate::series::chunks::tsxor::tsxor_decompressor::DecompressorTSXor;
use crate::series::chunks::{merge_samples, Chunk};
use crate::series::{DuplicatePolicy, SampleAddResult};
use ahash::AHashSet;
use get_size2::GetSize;
use std::hash::{Hash, Hasher};
use crate::common::rdb::RdbSerializable;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

/// A TSXor-based chunk implementation. This is a simple wrapper around the
/// compressor that stores the encoded buffer and performs
/// operations by decoding/re-encoding as needed.
#[derive(Debug, Clone, GetSize)]
pub struct TsXorChunk {
    compressor: CompressorTSXor,
    max_size: usize,
    first_timestamp: Timestamp,
    last_timestamp: Timestamp,
    last_value: f64,
}

impl PartialEq for TsXorChunk {
    fn eq(&self, other: &Self) -> bool {
        self.max_size == other.max_size
            && self.first_timestamp == other.first_timestamp
            && self.last_timestamp == other.last_timestamp
            && self.last_value.to_bits() == other.last_value.to_bits()
            && self.buf() == other.buf()
    }
}

impl Hash for TsXorChunk {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.max_size.hash(state);
        self.first_timestamp.hash(state);
        self.last_timestamp.hash(state);
        self.last_value.to_bits().hash(state);
        self.buf().hash(state);
    }
}

impl Default for TsXorChunk {
    fn default() -> Self {
        Self::with_max_size(crate::config::DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl TsXorChunk {
    fn reset_state(&mut self) {
        self.first_timestamp = 0;
        self.last_timestamp = 0;
        self.last_value = f64::NAN;
    }

    pub fn with_max_size(max_size: usize) -> Self {
        TsXorChunk {
            compressor: CompressorTSXor::new(),
            max_size,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
        }
    }

    pub fn is_full(&self) -> bool {
        self.compressor.len() >= self.max_size
    }

    pub fn clear(&mut self) {
        self.compressor.clear();
        self.reset_state();
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        if samples.is_empty() {
            self.clear();
            return Ok(());
        }
        // Invariant: samples are sorted.
        let first = samples.first().unwrap();
        let last = samples.last().unwrap();
        self.first_timestamp = first.timestamp;
        self.last_timestamp = last.timestamp;
        self.last_value = last.value;
        let mut c = CompressorTSXor::new();
        for s in samples.iter() {
            c.add(s.timestamp as u64, s.value);
        }
        self.compressor = c;
        Ok(())
    }

    pub fn data_size(&self) -> usize {
        self.compressor.get_size()
    }

    /// Borrow the internal encoded buffer
    pub fn buf(&self) -> &[u8] {
        self.compressor.as_ref()
    }

    pub fn bytes_per_sample(&self) -> usize {
        let count = self.len();
        if count == 0 { return size_of::<Sample>() / 2; }
        self.data_size() / count
    }

    pub fn get_iter(&self, start: Timestamp, end: Timestamp) -> TsXorChunkIterator<'_> {
        TsXorChunkIterator::new(self.buf(), self.len(), start, end)
    }
}

pub struct TsXorChunkIterator<'a> {
    decoder: DecompressorTSXor<'a>,
    is_init: bool,
    start: Timestamp,
    end: Timestamp,
}

impl<'a> TsXorChunkIterator<'a> {
    pub(crate) fn new(buf: &'a [u8], count: usize, start: Timestamp, end: Timestamp) -> Self {
        Self { decoder: DecompressorTSXor::new(buf, count), start, end, is_init: false }
    }

    fn skip_to_start(&mut self) {
        while let Some((ts, _)) = self.decoder.next() {
            let ts_i = ts as i64;
            if ts_i >= self.start {
                // we have reached the start timestamp, we can stop skipping
                break;
            }
        }
        self.is_init = true;
    }
}

impl<'a> Iterator for TsXorChunkIterator<'a> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.skip_to_start();
        }
        let (ts, v) = self.decoder.next()?;
        if ts > self.end as u64 {
            return None;
        }
        Some(Sample::new(ts as i64, v))
    }
}

impl Chunk for TsXorChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }

    fn last_timestamp(&self) -> Timestamp {
        self.last_timestamp
    }

    fn len(&self) -> usize {
        self.compressor.len()
    }

    fn last_value(&self) -> f64 {
        self.last_value
    }

    fn size(&self) -> usize {
        self.data_size()
    }

    fn max_size(&self) -> usize { self.max_size }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() {
            return Ok(0);
        }
        if start_ts > self.last_timestamp() || end_ts < self.first_timestamp() {
            return Ok(0);
        }

        let old_len = self.len();
        let mut decoder = self.compressor.get_decompressor();
        let mut encoder = CompressorTSXor::new();
        let mut first_ts: Option<Timestamp> = None;
        let mut last_sample: Option<Sample> = None;
        while let Some((ts, value)) = decoder.next() {
            let ts = ts as Timestamp;
            if ts < start_ts || ts > end_ts {
                let sample = Sample::new(ts, value);
                encoder.add_sample(sample);
                if first_ts.is_none() {
                    first_ts = Some(ts);
                }
                last_sample = Some(sample);
            }
        }
        let new_len = encoder.len();

        self.first_timestamp = first_ts.unwrap_or(self.first_timestamp);
        if let Some(last) = last_sample {
            self.last_timestamp = last.timestamp;
            self.last_value = last.value;
        } else {
            // all samples were removed, reset state
            self.reset_state();
        }

        self.compressor = encoder;
        Ok(old_len - new_len)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_empty() {
            self.first_timestamp = sample.timestamp;
        }
        self.compressor.add_sample(*sample);
        self.last_timestamp = sample.timestamp;
        self.last_value = sample.value;
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        // Use streaming iterator to produce the requested range without decoding entire chunk
        let iter = self.get_iter(start, end);
        let samples: Vec<Sample> = iter.collect();
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        if self.is_empty() {
            self.add_sample(&sample)?;
            self.first_timestamp = sample.timestamp;
            self.last_timestamp = sample.timestamp;
            self.last_value = sample.value;
            return Ok(1);
        }

        let ts = sample.timestamp;
        let mut duplicate_found = false;
        let count = self.len();
        if count == 0 {
            self.add_sample(&sample)?;
            return Ok(1);
        }
        let mut encoder = CompressorTSXor::new();
        let mut iter = self.get_iter(0, self.last_timestamp + 1);
        let mut last_sample: Option<Sample> = None;
        let mut first_ts: Option<Timestamp> = None;

        if ts < self.first_timestamp() {
            // add a sample to the beginning
            encoder.add(sample.timestamp as u64, sample.value);
            first_ts = Some(sample.timestamp);
            // Add all existing samples after the new one
            for current in iter {
                encoder.add_sample(current);
                last_sample = Some(current);
            }
        } else {
            let mut current = Sample::default();
            // add previous samples
            for current in iter.by_ref() {
                if current.timestamp >= ts {
                    break;
                }
                encoder.add_sample(current);
                if first_ts.is_none() {
                    first_ts = Some(current.timestamp);
                }
                last_sample = Some(current);
            }
            if current.timestamp == ts {
                duplicate_found = true;
                current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                encoder.add_sample(current);
            } else {
                encoder.add(sample.timestamp as u64, sample.value);
                // Add the current sample that caused the break (if it exists and is valid)
                if current.timestamp > ts {
                    encoder.add_sample(current);
                }
            }

            for current in iter {
                encoder.add_sample(current);
                last_sample = Some(current);
            }
        }

        self.compressor = encoder;
        if let Some(last) = last_sample {
            self.last_timestamp = last.timestamp;
            self.last_value = last.value;
        }
        if let Some(first) = first_ts {
            self.first_timestamp = first;
        }
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        fn add_sample(
            chunk: &mut TsXorChunk,
            sample: &Sample,
            res: &mut Vec<SampleAddResult>,
        ) -> TsdbResult<()> {
            match chunk.add_sample(sample) {
                Ok(_) => {
                    res.push(SampleAddResult::Ok(*sample));
                    Ok(())
                }
                err @ Err(TsdbError::CapacityFull(_)) => Err(err.unwrap_err()),
                Err(e) => {
                    log_warning(format!("error in gorilla chunk merge : {e:?}"));
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

        // todo: halfbrown
        let mut sample_set: AHashSet<Timestamp> = AHashSet::with_capacity(samples.len());
        for sample in samples.iter() {
            sample_set.insert(sample.timestamp);
        }

        struct MergeState {
            encoder: CompressorTSXor,
            result: Vec<SampleAddResult>,
        }

        let mut merge_state = MergeState {
            encoder: CompressorTSXor::new(),
            result: Vec::with_capacity(samples.len()),
        };

        let left = SampleIter::Tsxor(self.get_iter(0, self.last_timestamp() + 1));
        let right = SampleIter::Slice(samples.iter());

        merge_samples(
            left,
            right,
            dp_policy,
            &mut merge_state,
            |state, sample, is_duplicate| {
                state.encoder.add(sample.timestamp as u64, sample.value);
                if sample_set.remove(&sample.timestamp) {
                    if is_duplicate {
                        state.result.push(SampleAddResult::Duplicate);
                    } else {
                        state.result.push(SampleAddResult::Ok(sample));
                    }
                }
                Ok(())
            },
        )?;

        self.compressor = merge_state.encoder;
        // self.refresh_state_from_compressor();
        Ok(merge_state.result)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut encoder = CompressorTSXor::new();
        let mut right_chunk = Self::with_max_size(self.max_size);

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.len() / 2;
        let iter = self.get_iter(self.first_timestamp(), self.last_timestamp() + 1);
        for (i, sample) in iter.enumerate() {
            if i < mid {
                // todo: handle min and max timestamps
                encoder.add(sample.timestamp as u64, sample.value);
            } else {
                right_chunk.add_sample(&sample)?;
            }
        }
        self.compressor = encoder;
        // self.refresh_state_from_compressor();
        Ok(right_chunk)
    }

    fn optimize(&mut self) -> TsdbResult<()> { Ok(()) }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        rdb_save_timestamp(rdb, self.first_timestamp);
        rdb_save_timestamp(rdb, self.last_timestamp);
        rdb_save_f64(rdb, self.last_value);
        let mut state = Vec::new();
        self.compressor.serialize(&mut state);
        valkey_module::raw::save_slice(rdb, &state);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let first_timestamp = rdb_load_timestamp(rdb)?;
        let last_timestamp = rdb_load_timestamp(rdb)?;
        let last_value = rdb_load_f64(rdb)?;
        let state = valkey_module::raw::load_string_buffer(rdb)?;
        let mut chunk = TsXorChunk::with_max_size(max_size);
        chunk.compressor = CompressorTSXor::deserialize(state.as_ref());
        chunk.last_timestamp = last_timestamp;
        chunk.last_value = last_value;
        chunk.first_timestamp = first_timestamp;
        Ok(chunk)
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        write_uvarint(dest, self.first_timestamp as u64);
        write_uvarint(dest, self.last_timestamp as u64);
        write_uvarint(dest, f64::to_bits(self.last_value));
        let mut state = Vec::new();
        self.compressor.serialize(&mut state);
        write_uvarint(dest, state.len() as u64);
        dest.extend_from_slice(&state);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut b = buf;
        let max_size = try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as usize;
        let first_timestamp = try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as Timestamp;
        let last_timestamp = try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as Timestamp;
        let last_value = f64::from_bits(try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)?);
        let state_len = try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as usize;
        if b.len() < state_len {
            return Err(TsdbError::ChunkDecoding);
        }

        let (state, rest) = b.split_at(state_len);
        if !rest.is_empty() {
            return Err(TsdbError::ChunkDecoding);
        }

        let mut chunk = TsXorChunk::with_max_size(max_size);
        chunk.last_timestamp = last_timestamp;
        chunk.last_value = last_value;
        chunk.first_timestamp = first_timestamp;
        chunk.compressor = CompressorTSXor::deserialize(state);
        Ok(chunk)
    }

    fn debug_digest(&self, dig: &mut Digest) {
        dig.add_string_buffer(self.buf());
        dig.add_long_long(self.max_size as i64);
    }
}

impl RdbSerializable for TsXorChunk {
    fn rdb_save(&self, rdb: *mut RedisModuleIO) {
        <Self as Chunk>::save_rdb(self, rdb);
    }

    fn rdb_load(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        <Self as Chunk>::load_rdb(rdb, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeseries_chunk_range_iter_streaming() {
        // build some samples
        let block = 100_000u64;
        let mut samples = Vec::new();
        for i in 0..50 {
            let ts = (block + (i * 60) as u64) as i64;
            let v = (i as f64) * 2.0;
            samples.push(Sample::new(ts, v));
        }

        let mut chunk = TsXorChunk::with_max_size(1024 * 1024);
        chunk.set_data(&samples).expect("set_data");

        // pick range: 10..30
        let start = samples[10].timestamp;
        let end = samples[29].timestamp;

        // get via TimeSeriesChunk::range_iter dispatch
        let ts_chunk = crate::series::chunks::TimeSeriesChunk::Tsxor(chunk.clone());
        let iter = ts_chunk.range_iter(start, end);
        let got: Vec<Sample> = iter.collect();

        // expected via decoding the chunk iterator directly
        let expected: Vec<Sample> = TsXorChunkIterator::new(chunk.buf(), chunk.len(), start, end).collect();

        assert_eq!(got.len(), expected.len());
        for (g, e) in got.iter().zip(expected.iter()) {
            assert_eq!(g.timestamp, e.timestamp);
            assert_eq!(g.value.to_bits(), e.value.to_bits());
        }
    }

    #[test]
    fn tsxor_chunk_serialize_deserialize_roundtrip_restores_bounds() {
        let mut samples = Vec::new();
        for i in 0..24 {
            samples.push(Sample::new(1_700_000_000 + (i * 60), (i as f64) * 1.5));
        }

        let mut chunk = TsXorChunk::with_max_size(8192);
        chunk.set_data(&samples).expect("set_data");

        let mut serialized = Vec::new();
        chunk.serialize(&mut serialized);

        let restored = TsXorChunk::deserialize(&serialized).expect("deserialize");
        assert_eq!(restored.max_size(), 8192);
        assert_eq!(restored.len(), samples.len());
        assert_eq!(restored.first_timestamp(), samples.first().unwrap().timestamp);
        assert_eq!(restored.last_timestamp(), samples.last().unwrap().timestamp);
        assert_eq!(restored.last_value().to_bits(), samples.last().unwrap().value.to_bits());

        let mut decoder = DecompressorTSXor::new(restored.buf(), restored.len());
        let mut actual = Vec::new();
        while let Some((ts, value)) = decoder.next() {
            actual.push(Sample::new(ts as i64, value));
        }
        assert_eq!(actual.len(), samples.len());
        for (a, e) in actual.iter().zip(samples.iter()) {
            assert_eq!(a.timestamp, e.timestamp);
            assert_eq!(a.value.to_bits(), e.value.to_bits());
        }
    }

    #[test]
    fn tsxor_chunk_updates_state_as_data_changes() {
        let mut chunk = TsXorChunk::with_max_size(4096);
        let initial = vec![
            Sample::new(1_000, 1.0),
            Sample::new(1_060, 2.0),
            Sample::new(1_120, 3.0),
        ];
        chunk.set_data(&initial).expect("set_data");

        chunk.add_sample(&Sample::new(1_180, 4.0)).expect("add_sample");
        assert_eq!(chunk.first_timestamp(), 1_000);
        assert_eq!(chunk.last_timestamp(), 1_180);
        assert_eq!(chunk.last_value().to_bits(), 4.0f64.to_bits());

        chunk
            .upsert_sample(Sample::new(900, 0.5), DuplicatePolicy::KeepLast)
            .expect("upsert_sample");
        assert_eq!(chunk.first_timestamp(), 900);
        assert_eq!(chunk.last_timestamp(), 1_180);
        assert_eq!(chunk.last_value().to_bits(), 4.0f64.to_bits());

        chunk.clear();
        assert_eq!(chunk.first_timestamp(), 0);
        assert_eq!(chunk.last_timestamp(), 0);
        assert!(chunk.last_value().is_nan());
    }

    #[test]
    fn tsxor_chunk_deserialize_rejects_truncated_payload() {
        let samples = vec![
            Sample::new(1_000, 1.0),
            Sample::new(1_060, 2.0),
            Sample::new(1_120, 3.0),
        ];

        let mut chunk = TsXorChunk::with_max_size(4096);
        chunk.set_data(&samples).expect("set_data");

        let mut serialized = Vec::new();
        chunk.serialize(&mut serialized);
        serialized.pop();

        assert!(matches!(
            TsXorChunk::deserialize(&serialized),
            Err(TsdbError::ChunkDecoding)
        ));
    }
}
