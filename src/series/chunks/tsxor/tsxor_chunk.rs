use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::mem::size_of;

use crate::common::encoding::{
    try_read_f64_le, try_read_signed_varint, try_read_uvarint, write_f64_le, write_signed_varint,
    write_uvarint, zigzag_encode,
};
use crate::common::logging::log_warning;
use crate::common::rdb::RdbSerializable;
use crate::common::rdb::{
    rdb_load_f64, rdb_load_timestamp, rdb_load_usize, rdb_save_f64, rdb_save_timestamp,
    rdb_save_usize,
};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::stream::bitstream::BitStream;
use crate::series::chunks::tsxor::tsxor_decompressor::TsXorDecompressor;
use crate::series::chunks::{Chunk, ChunkOps, append_samples, merge_chunk_samples};
use crate::series::{DuplicatePolicy, SampleAddResult};
use get_size2::GetSize;
use valkey_module::{RedisModuleIO, ValkeyResult};

pub(super) const WINDOW_SIZE: usize = 127;
pub(super) const FIRST_DELTA_BITS: u32 = 32;

#[derive(Debug, Clone, GetSize, Hash)]
pub(super) struct CacheWindow {
    buffer: VecDeque<u64>,
}

impl Default for CacheWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheWindow {
    fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(WINDOW_SIZE),
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn insert(&mut self, val: u64) {
        if self.len() == WINDOW_SIZE {
            self.buffer.pop_back();
        }
        self.buffer.push_front(val);
    }

    fn get_index_of(&self, val: u64) -> Option<usize> {
        self.buffer.iter().position(|v| *v == val)
    }

    pub fn get(&self, offset: usize) -> u64 {
        self.buffer[offset]
    }

    fn get_candidate(&self, val: u64) -> u64 {
        if self.buffer.is_empty() {
            return 0;
        }

        let mut best_score: i32 = -1;
        let mut best_idx: usize = 0;
        for (i, &b) in self.buffer.iter().enumerate() {
            let x = val ^ b;
            let score = if x != 0 {
                (x.leading_zeros() + x.trailing_zeros()) as i32
            } else {
                64i32
            };
            if score > best_score {
                best_score = score;
                best_idx = i;
            }
        }
        self.buffer[best_idx]
    }

    pub fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.len() as u64);
        for &val in self.buffer.iter() {
            write_uvarint(dest, val);
        }
    }

    fn deserialize(src: &mut &[u8]) -> TsdbResult<Self> {
        let len = match try_read_uvarint(src) {
            Ok(l) => l as usize,
            Err(_) => {
                log_warning(
                    "TSXorChunk deserialization failed: unable to read cache window length",
                );
                return Err(TsdbError::ChunkDecoding);
            }
        };
        if len > WINDOW_SIZE {
            log_warning("TSXorChunk deserialization failed: window size exceeded");
            return Err(TsdbError::ChunkDecoding);
        }

        let mut buffer = VecDeque::with_capacity(WINDOW_SIZE);
        for _ in 0..len {
            let val = match try_read_uvarint(src) {
                Ok(v) => v,
                Err(_) => {
                    log_warning(
                        "TSXorChunk deserialization failed: unable to read cache window value",
                    );
                    return Err(TsdbError::ChunkDecoding);
                }
            };
            buffer.push_back(val);
        }

        Ok(Self { buffer })
    }
}

/// A TSXor-based chunk implementation with inlined TSXor compression state.
#[derive(Debug, Clone, GetSize)]
pub struct TsXorChunk {
    writer: BitStream,
    window: CacheWindow,
    stored_timestamp: u64,
    stored_delta: i64,
    block_timestamp: u64,
    count: usize,
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
    fn new_encoder(max_size: usize) -> Self {
        TsXorChunk {
            writer: BitStream::new(),
            window: CacheWindow::new(),
            stored_timestamp: 0,
            stored_delta: 0,
            block_timestamp: 0,
            count: 0,
            max_size,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
        }
    }

    fn reset_state(&mut self) {
        self.first_timestamp = 0;
        self.last_timestamp = 0;
        self.last_value = f64::NAN;
    }

    pub fn with_max_size(max_size: usize) -> Self {
        Self::new_encoder(max_size)
    }

    pub fn data_size(&self) -> usize {
        self.get_size()
    }

    /// Borrow the internal encoded buffer
    pub fn buf(&self) -> &[u8] {
        self.writer.get_ref()
    }

    pub fn get_iter(&self, start: Timestamp, end: Timestamp) -> TsXorChunkIterator<'_> {
        TsXorChunkIterator::new(self.buf(), self.len(), start, end)
    }

    pub(crate) fn append_sample(&mut self, sample: Sample) {
        debug_assert!(
            sample.timestamp >= 0,
            "negative timestamps should have been handled by caller"
        );
        self.append(sample.timestamp as u64, sample.value);
    }

    pub(crate) fn append(&mut self, timestamp: u64, val: f64) {
        if self.count == 0 {
            self.block_timestamp = timestamp;
            self.stored_delta = (timestamp as i128 - self.block_timestamp as i128) as i64;
            self.stored_timestamp = timestamp;
            self.writer.write_u64(timestamp);
            let _ = self
                .writer
                .write_bits(FIRST_DELTA_BITS, self.stored_delta as u64);
            self.writer.write_u64(val.to_bits());
            self.window.insert(val.to_bits());
        } else {
            self.compress_timestamp(timestamp);
            self.compress_value(val);
        }
        self.count += 1;
    }

    fn compress_timestamp(&mut self, timestamp: u64) {
        let new_delta = (timestamp as i128 - self.stored_timestamp as i128) as i64;
        let delta_d = new_delta - self.stored_delta;

        if delta_d == 0 {
            self.writer.write_bit(false);
        } else {
            let mut enc = zigzag_encode(delta_d);
            let mut length = 64 - enc.leading_zeros();
            if length == 0 {
                length = 1;
            }

            if length <= 7 {
                enc |= (0x02u64) << 7;
                let _ = self.writer.write_bits(9, enc);
            } else if (8..=9).contains(&length) {
                enc |= (0x06u64) << 9;
                let _ = self.writer.write_bits(12, enc);
            } else if (10..=12).contains(&length) {
                enc |= (0x0Eu64) << 12;
                let _ = self.writer.write_bits(16, enc);
            } else {
                let _ = self.writer.write_bits(4, 0x0Fu64);
                let _ = self.writer.write_bits(32, enc);
            }
        }

        self.stored_delta = (timestamp as i128 - self.stored_timestamp as i128) as i64;
        self.stored_timestamp = timestamp;
    }

    fn write_full_value(&mut self, v: u64) {
        self.writer.write_byte(255);
        for b in v.to_be_bytes().iter() {
            self.writer.write_byte(*b);
        }
    }

    fn compress_value(&mut self, value: f64) {
        let v = value.to_bits();

        if let Some(offset) = self.window.get_index_of(v) {
            self.writer.write_byte(offset as u8);
        } else {
            let candidate = self.window.get_candidate(v);
            let xor = candidate ^ v;
            let lead_bytes = (xor.leading_zeros() / 8) as usize;
            let trail_bytes = (xor.trailing_zeros() / 8) as usize;

            if (lead_bytes + trail_bytes) > 1 {
                let offset = self.window.get_index_of(candidate).unwrap_or_default();

                let off = (offset as u8) | 0x80u8;
                self.writer.write_byte(off);

                let xor_len_bytes = 8 - lead_bytes - trail_bytes;
                let xor_shifted = xor >> (trail_bytes * 8);
                let head = ((trail_bytes as u8) << 4) | (xor_len_bytes as u8 & 0x0F);
                self.writer.write_byte(head);

                let arr = xor_shifted.to_be_bytes();
                let start = 8 - xor_len_bytes;
                for &b in arr.iter().skip(start) {
                    self.writer.write_byte(b);
                }
            } else {
                self.write_full_value(v);
            }
        }

        self.window.insert(v);
    }

    fn serialize_encoder_state(&self, dest: &mut Vec<u8>) {
        self.writer.serialize(dest);
        self.window.serialize(dest);
        write_uvarint(dest, self.stored_timestamp);
        write_signed_varint(dest, self.stored_delta);
        write_uvarint(dest, self.block_timestamp);
        write_uvarint(dest, self.count as u64);
    }

    fn deserialize_encoder_state(&mut self, src: &[u8]) -> TsdbResult<()> {
        let mut src = src;

        let writer = BitStream::deserialize(&mut src).map_err(|_| TsdbError::ChunkDecoding)?;

        let window = CacheWindow::deserialize(&mut src).map_err(|_| TsdbError::ChunkDecoding)?;

        let stored_timestamp = try_read_uvarint(&mut src).map_err(|_| TsdbError::ChunkDecoding)?;

        let stored_delta =
            try_read_signed_varint(&mut src).map_err(|_| TsdbError::ChunkDecoding)?;

        let block_timestamp = try_read_uvarint(&mut src).map_err(|_| TsdbError::ChunkDecoding)?;

        let count = try_read_uvarint(&mut src).map_err(|_| TsdbError::ChunkDecoding)? as usize;

        self.writer = writer;
        self.window = window;
        self.stored_timestamp = stored_timestamp;
        self.stored_delta = stored_delta;
        self.block_timestamp = block_timestamp;
        self.count = count;
        Ok(())
    }
}

pub struct TsXorChunkIterator<'a> {
    decoder: TsXorDecompressor<'a>,
    is_init: bool,
    start: Timestamp,
    end: Timestamp,
    pending: Option<(u64, f64)>,
}

impl<'a> TsXorChunkIterator<'a> {
    pub(crate) fn new(buf: &'a [u8], count: usize, start: Timestamp, end: Timestamp) -> Self {
        Self {
            decoder: TsXorDecompressor::new(buf, count),
            start,
            end,
            is_init: false,
            pending: None,
        }
    }

    fn skip_to_start(&mut self) {
        while let Some((ts, value)) = self.decoder.next() {
            let ts_i = ts as i64;
            if ts_i >= self.start {
                // we've found the first sample at or after start; store it as pending
                self.pending = Some((ts, value));
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

        if let Some((ts, v)) = self.pending.take() {
            if ts > self.end as u64 {
                return None;
            }
            return Some(Sample::new(ts as i64, v));
        }

        let (ts, v) = self.decoder.next()?;
        if ts > self.end as u64 {
            return None;
        }
        Some(Sample::new(ts as i64, v))
    }
}

impl ChunkOps for TsXorChunk {
    fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }

    fn last_timestamp(&self) -> Timestamp {
        self.last_timestamp
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
        if start_ts > self.last_timestamp() || end_ts < self.first_timestamp() {
            return Ok(0);
        }

        let old_len = self.len();
        let mut decoder = TsXorDecompressor::new(self.buf(), self.len());
        let mut encoder = Self::new_encoder(self.max_size);
        let mut first_ts: Option<Timestamp> = None;
        let mut last_sample: Option<Sample> = None;
        while let Some((ts, value)) = decoder.next() {
            let ts = ts as Timestamp;
            if ts < start_ts || ts > end_ts {
                let sample = Sample::new(ts, value);
                encoder.append_sample(sample);
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

        self.writer = encoder.writer;
        self.window = encoder.window;
        self.stored_timestamp = encoder.stored_timestamp;
        self.stored_delta = encoder.stored_delta;
        self.block_timestamp = encoder.block_timestamp;
        self.count = encoder.count;
        Ok(old_len - new_len)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_empty() {
            self.first_timestamp = sample.timestamp;
        }
        self.append_sample(*sample);
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
        let mut encoder = Self::new_encoder(self.max_size);
        let mut iter = self.get_iter(0, self.last_timestamp + 1);
        let mut last_sample: Option<Sample> = None;
        let mut first_ts: Option<Timestamp> = None;

        if ts < self.first_timestamp() {
            // add a sample to the beginning
            encoder.append(sample.timestamp as u64, sample.value);
            first_ts = Some(sample.timestamp);
            // Add all existing samples after the new one
            for current in iter {
                encoder.append_sample(current);
                last_sample = Some(current);
            }
        } else {
            let mut current = Sample::default();
            // add previous samples
            for current in iter.by_ref() {
                if current.timestamp >= ts {
                    break;
                }
                encoder.append_sample(current);
                if first_ts.is_none() {
                    first_ts = Some(current.timestamp);
                }
                last_sample = Some(current);
            }
            if current.timestamp == ts {
                duplicate_found = true;
                current.value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                encoder.append_sample(current);
            } else {
                encoder.append(sample.timestamp as u64, sample.value);
                // Add the current sample that caused the break (if it exists and is valid)
                if current.timestamp > ts {
                    encoder.append_sample(current);
                }
            }

            for current in iter {
                encoder.append_sample(current);
                last_sample = Some(current);
            }
        }

        self.writer = encoder.writer;
        self.window = encoder.window;
        self.stored_timestamp = encoder.stored_timestamp;
        self.stored_delta = encoder.stored_delta;
        self.block_timestamp = encoder.block_timestamp;
        self.count = encoder.count;
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
        if samples.is_empty() {
            return Ok(Vec::new());
        }

        // We assume that samples are sorted. Try to optimize by seeing if all samples are past the
        // current chunk's last timestamp.
        let first = samples[0];
        if self.is_empty() || first.timestamp > self.last_timestamp() {
            return append_samples(self, samples);
        }

        let mut encoder = TsXorChunk::new_encoder(self.max_size);
        // Track first and last samples seen so we can set the resulting chunk's metadata
        // (first_timestamp, last_timestamp and last_value) after the merge.
        let mut first_ts: Option<Timestamp> = None;
        let mut last_sample: Option<Sample> = None;

        let left = SampleIter::TSXor(self.get_iter(0, self.last_timestamp() + 1));
        let result = merge_chunk_samples(left, samples, dp_policy, |sample| {
            encoder.append(sample.timestamp as u64, sample.value);
            if first_ts.is_none() {
                first_ts = Some(sample.timestamp);
            }
            last_sample = Some(sample);
            Ok(())
        })?;

        self.writer = encoder.writer;
        self.window = encoder.window;
        self.stored_timestamp = encoder.stored_timestamp;
        self.stored_delta = encoder.stored_delta;
        self.block_timestamp = encoder.block_timestamp;
        self.count = encoder.count;
        // Update first/last metadata from tracked samples
        if let Some(first) = first_ts {
            self.first_timestamp = first;
        }
        if let Some(last) = last_sample {
            self.last_timestamp = last.timestamp;
            self.last_value = last.value;
        }

        Ok(result)
    }

    fn optimize(&mut self) -> TsdbResult<()> {
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.buf().len() >= self.max_size
    }

    fn bytes_per_sample(&self) -> usize {
        use crate::series::chunks::MIN_SAMPLES_FOR_BPS_ESTIMATE;
        let count = self.len();
        if count < MIN_SAMPLES_FOR_BPS_ESTIMATE {
            return size_of::<Sample>() / 2;
        }
        self.data_size() / count
    }

    fn clear(&mut self) {
        self.writer.clear();
        self.window = CacheWindow::new();
        self.stored_timestamp = 0;
        self.stored_delta = 0;
        self.block_timestamp = 0;
        self.count = 0;
        self.reset_state();
    }

    fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
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
        let mut c = Self::new_encoder(self.max_size);
        for s in samples.iter() {
            c.append_sample(*s);
        }
        *self = c;
        self.first_timestamp = first.timestamp;
        self.last_timestamp = last.timestamp;
        self.last_value = last.value;
        Ok(())
    }
}

impl Chunk for TsXorChunk {
    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut encoder = Self::new_encoder(self.max_size);
        let mut right_chunk = Self::with_max_size(self.max_size);

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.len() / 2;
        let iter = self.get_iter(self.first_timestamp(), self.last_timestamp() + 1);
        // Track first/last metadata for the left partition.
        let mut left_first_ts: Option<Timestamp> = None;
        let mut left_last_sample: Option<Sample> = None;
        for (i, sample) in iter.enumerate() {
            if i < mid {
                encoder.append(sample.timestamp as u64, sample.value);
                if left_first_ts.is_none() {
                    left_first_ts = Some(sample.timestamp);
                }
                left_last_sample = Some(sample);
            } else {
                right_chunk.add_sample(&sample)?;
            }
        }
        self.writer = encoder.writer;
        self.window = encoder.window;
        self.stored_timestamp = encoder.stored_timestamp;
        self.stored_delta = encoder.stored_delta;
        self.block_timestamp = encoder.block_timestamp;
        self.count = encoder.count;
        // Update metadata for the left (self) partition.
        if let Some(first_ts) = left_first_ts {
            self.first_timestamp = first_ts;
        }
        if let Some(last) = left_last_sample {
            self.last_timestamp = last.timestamp;
            self.last_value = last.value;
        }
        Ok(right_chunk)
    }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        rdb_save_timestamp(rdb, self.first_timestamp);
        rdb_save_timestamp(rdb, self.last_timestamp);
        rdb_save_f64(rdb, self.last_value);
        let mut state = Vec::new();
        self.serialize_encoder_state(&mut state);
        valkey_module::raw::save_slice(rdb, &state);
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let first_timestamp = rdb_load_timestamp(rdb)?;
        let last_timestamp = rdb_load_timestamp(rdb)?;
        let last_value = rdb_load_f64(rdb)?;
        let state = valkey_module::raw::load_string_buffer(rdb)?;
        let mut chunk = TsXorChunk::with_max_size(max_size);
        chunk.first_timestamp = first_timestamp;
        chunk.last_timestamp = last_timestamp;
        chunk.last_value = last_value;
        chunk.deserialize_encoder_state(state.as_ref())?;
        Ok(chunk)
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        write_signed_varint(dest, self.first_timestamp);
        write_signed_varint(dest, self.last_timestamp);
        write_f64_le(dest, self.last_value);
        self.serialize_encoder_state(dest);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut b = buf;
        let max_size = try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as usize;
        let first_timestamp =
            try_read_signed_varint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as Timestamp;
        let last_timestamp =
            try_read_signed_varint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as Timestamp;
        // read last_value as f64 little-endian (matches serialize)
        let last_value = try_read_f64_le(&mut b).map_err(|_| TsdbError::ChunkDecoding)?;

        // The remaining bytes are the encoder state; pass them to the state deserializer
        let mut chunk = TsXorChunk::with_max_size(max_size);
        chunk.last_timestamp = last_timestamp;
        chunk.last_value = last_value;
        chunk.first_timestamp = first_timestamp;
        chunk.deserialize_encoder_state(b)?;
        Ok(chunk)
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
        let ts_chunk = crate::series::chunks::TimeSeriesChunk::TsXor(chunk.clone());
        let iter = ts_chunk.range_iter(start, end);
        let got: Vec<Sample> = iter.collect();

        // expected via decoding the chunk iterator directly
        let expected: Vec<Sample> =
            TsXorChunkIterator::new(chunk.buf(), chunk.len(), start, end).collect();

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
        assert_eq!(
            restored.first_timestamp(),
            samples.first().unwrap().timestamp
        );
        assert_eq!(restored.last_timestamp(), samples.last().unwrap().timestamp);
        assert_eq!(
            restored.last_value().to_bits(),
            samples.last().unwrap().value.to_bits()
        );

        let mut decoder = TsXorDecompressor::new(restored.buf(), restored.len());
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

        chunk
            .add_sample(&Sample::new(1_180, 4.0))
            .expect("add_sample");
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

    #[test]
    fn cache_window_respects_capacity_with_unique_values() {
        let mut window = CacheWindow::new();
        for i in 0..(WINDOW_SIZE + 5) {
            window.insert(i as u64);
        }

        assert_eq!(window.len(), WINDOW_SIZE);
        assert_eq!(window.get(0), (WINDOW_SIZE + 4) as u64);
        assert_eq!(window.get(WINDOW_SIZE - 1), 5);
    }
}
