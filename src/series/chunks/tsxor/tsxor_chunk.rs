use std::collections::VecDeque;
use std::hash::{Hash, Hasher};

use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::logging::log_warning;
use crate::common::rdb::{rdb_load_f64, rdb_load_timestamp, rdb_load_usize, rdb_save_f64, rdb_save_timestamp, rdb_save_usize};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::iterators::SampleIter;
use crate::series::chunks::buffered_writer::BufferedWriter;
use crate::series::chunks::gorilla::utils::{zigzag_decode, zigzag_encode};
use crate::series::chunks::traits::BitWrite;
use crate::series::chunks::tsxor::tsxor_decompressor::DecompressorTSXor;
use crate::series::chunks::{merge_samples, Chunk};
use crate::series::{DuplicatePolicy, SampleAddResult};
use ahash::AHashSet;
use get_size2::GetSize;
use crate::common::rdb::RdbSerializable;
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};

pub(super) const WINDOW_SIZE: usize = 127;
pub(super) const FIRST_DELTA_BITS: u32 = 32;

#[derive(Debug, Clone, GetSize)]
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
        let mut buffer = VecDeque::with_capacity(WINDOW_SIZE);
        for _ in 0..WINDOW_SIZE {
            buffer.push_back(0);
        }
        Self { buffer }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn insert(&mut self, val: u64) {
        self.buffer.pop_back();
        self.buffer.push_front(val);
    }

    fn get_index_of(&self, val: u64) -> Option<usize> {
        self.buffer.iter().position(|v| *v == val)
    }

    pub fn get(&self, offset: usize) -> u64 {
        self.buffer[offset]
    }

    fn get_candidate(&self, val: u64) -> u64 {
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

    fn deserialize_from(src: &mut &[u8]) -> Option<Self> {
        let len = try_read_uvarint(src).ok()? as usize;
        if len > WINDOW_SIZE {
            return None;
        }

        let mut buffer = VecDeque::with_capacity(WINDOW_SIZE);
        for _ in 0..len {
            let val = try_read_uvarint(src).ok()?;
            buffer.push_back(val);
        }

        for _ in len..WINDOW_SIZE {
            buffer.push_back(0);
        }

        Some(Self { buffer })
    }
}

/// A TSXor-based chunk implementation with inlined TSXor compression state.
#[derive(Debug, Clone, GetSize)]
pub struct TsXorChunk {
    writer: BufferedWriter,
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
            writer: BufferedWriter::new(),
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

    pub fn is_full(&self) -> bool {
        self.count >= self.max_size
    }

    pub fn clear(&mut self) {
        self.writer.clear();
        self.window = CacheWindow::new();
        self.stored_timestamp = 0;
        self.stored_delta = 0;
        self.block_timestamp = 0;
        self.count = 0;
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

    pub fn data_size(&self) -> usize {
        self.get_size()
    }

    /// Borrow the internal encoded buffer
    pub fn buf(&self) -> &[u8] {
        self.writer.get_ref()
    }

    pub fn bytes_per_sample(&self) -> usize {
        let count = self.len();
        if count == 0 { return size_of::<Sample>() / 2; }
        self.data_size() / count
    }

    pub fn get_iter(&self, start: Timestamp, end: Timestamp) -> TsXorChunkIterator<'_> {
        TsXorChunkIterator::new(self.buf(), self.len(), start, end)
    }

    pub(crate) fn append_sample(&mut self, sample: Sample) {
        self.append(sample.timestamp as u64, sample.value);
    }

    pub(crate) fn append(&mut self, timestamp: u64, val: f64) {
        if self.count == 0 {
            self.block_timestamp = timestamp;
            self.stored_delta = (timestamp as i128 - self.block_timestamp as i128) as i64;
            self.stored_timestamp = timestamp;
            self.writer.write_u64(timestamp);
            let _ = self.writer.write_bits(FIRST_DELTA_BITS, self.stored_delta as u64);
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
            let _ = self.writer.write_bit(false);
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
                if let Some(offset) = self.window.get_index_of(candidate) {
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
            } else {
                self.write_full_value(v);
            }
        }

        self.window.insert(v);
    }

    pub(crate) fn serialize_encoder_state(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.writer.position() as u64);
        write_uvarint(dest, self.writer.get_ref().len() as u64);
        dest.extend_from_slice(self.writer.get_ref());
        self.window.serialize(dest);
        write_uvarint(dest, self.stored_timestamp);
        write_uvarint(dest, zigzag_encode(self.stored_delta));
        write_uvarint(dest, self.block_timestamp);
        write_uvarint(dest, self.count as u64);
    }

    pub(crate) fn deserialize_encoder_state(&mut self, src: &[u8]) {
        let mut src = src;

        let writer_pos = match try_read_uvarint(&mut src) {
            Ok(pos) if pos <= 8 => pos as u32,
            _ => return,
        };

        let writer_len = match try_read_uvarint(&mut src) {
            Ok(len) => len as usize,
            Err(_) => return,
        };

        if src.len() < writer_len {
            return;
        }

        let writer_buf = src[..writer_len].to_vec();
        src = &src[writer_len..];

        let window = match CacheWindow::deserialize_from(&mut src) {
            Some(window) => window,
            None => return,
        };

        let stored_timestamp = match try_read_uvarint(&mut src) {
            Ok(ts) => ts,
            Err(_) => return,
        };

        let stored_delta = match try_read_uvarint(&mut src) {
            Ok(delta) => zigzag_decode(delta),
            Err(_) => return,
        };

        let block_timestamp = match try_read_uvarint(&mut src) {
            Ok(ts) => ts,
            Err(_) => return,
        };

        let count = match try_read_uvarint(&mut src) {
            Ok(count) => count as usize,
            Err(_) => return,
        };

        self.writer = BufferedWriter::hydrate(writer_buf, writer_pos);
        self.window = window;
        self.stored_timestamp = stored_timestamp;
        self.stored_delta = stored_delta;
        self.block_timestamp = block_timestamp;
        self.count = count;
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
        self.count
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
        let mut decoder = DecompressorTSXor::new(self.buf(), self.len());
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
            encoder: TsXorChunk,
            result: Vec<SampleAddResult>,
        }

        let mut merge_state = MergeState {
            encoder: TsXorChunk::new_encoder(self.max_size),
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
                state.encoder.append(sample.timestamp as u64, sample.value);
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

        self.writer = merge_state.encoder.writer;
        self.window = merge_state.encoder.window;
        self.stored_timestamp = merge_state.encoder.stored_timestamp;
        self.stored_delta = merge_state.encoder.stored_delta;
        self.block_timestamp = merge_state.encoder.block_timestamp;
        self.count = merge_state.encoder.count;
        // self.refresh_state_from_compressor();
        Ok(merge_state.result)
    }

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
        for (i, sample) in iter.enumerate() {
            if i < mid {
                // todo: handle min and max timestamps
                encoder.append(sample.timestamp as u64, sample.value);
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
        chunk.deserialize_encoder_state(state.as_ref());
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
        self.serialize_encoder_state(&mut state);
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
        chunk.deserialize_encoder_state(state);
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
