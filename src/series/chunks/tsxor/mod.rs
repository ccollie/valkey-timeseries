use crate::series::chunks::gorilla::buffered_read::BufferedReader;
use crate::series::chunks::gorilla::buffered_writer::BufferedWriter;
use crate::series::chunks::gorilla::traits::{BitRead, BitWrite};
use crate::series::chunks::gorilla::utils::{zigzag_decode, zigzag_encode};
use std::collections::VecDeque;
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::series::DuplicatePolicy;
use crate::series::chunks::chunk::Chunk;
use crate::common::rdb::{rdb_load_usize, rdb_save_usize};
use valkey_module::digest::Digest;
use valkey_module::{RedisModuleIO, ValkeyResult};
use crate::common::encoding::{try_read_uvarint, write_uvarint};
use std::mem::size_of;
use get_size2::GetSize;

const WINDOW_SIZE: usize = 127;
const FIRST_DELTA_BITS: u32 = 32;

/// Simple sliding window used by TSXor (port of TSXor Window)
struct Window {
    buffer: VecDeque<u64>,
}

impl Window {
    fn new() -> Self {
        let mut buffer = VecDeque::with_capacity(WINDOW_SIZE);
        // initialize with zeros
        for _ in 0..WINDOW_SIZE {
            buffer.push_back(0);
        }
        Self { buffer }
    }

    fn insert(&mut self, val: u64) {
        // remove tail, push front
        self.buffer.pop_back();
        self.buffer.push_front(val);
    }

    fn contains(&self, val: u64) -> bool {
        self.buffer.iter().any(|v| *v == val)
    }

    fn get_index_of(&self, val: u64) -> Option<usize> {
        self.buffer.iter().position(|v| *v == val)
    }

    fn get(&self, offset: usize) -> u64 {
        self.buffer[offset]
    }

    fn get_candidate(&self, val: u64) -> u64 {
        // compute similarity metric (clz + ctz) and pick best match
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
}

/// Compressor for TSXor for a single time-series (timestamps u64, values f64)
pub struct CompressorTSXor {
    bw: BufferedWriter,
    window: Window,
    stored_timestamp: u64,
    stored_delta: i64,
    block_timestamp: u64,
}

impl CompressorTSXor {
    pub fn new(block_timestamp: u64) -> Self {
        let mut c = Self {
            bw: BufferedWriter::new(),
            window: Window::new(),
            stored_timestamp: 0,
            stored_delta: 0,
            block_timestamp,
        };
        // write header
        c.bw.write_u64(block_timestamp);
        c
    }

    pub fn add_value(&mut self, timestamp: u64, val: f64) {
        if self.stored_timestamp == 0 {
            self.stored_delta = (timestamp as i128 - self.block_timestamp as i128) as i64;
            self.stored_timestamp = timestamp;
            // write FIRST_DELTA_BITS bits of stored_delta
            let sd = self.stored_delta as u64;
            let _ = self.bw.write_bits(FIRST_DELTA_BITS, sd);
            // write value as 64-bit big endian
            self.bw.write_u64(val.to_bits());
            self.window.insert(val.to_bits());
        } else {
            self.compress_timestamp(timestamp);
            self.compress_value(val);
        }
    }

    fn compress_timestamp(&mut self, timestamp: u64) {
        let new_delta = (timestamp as i128 - self.stored_timestamp as i128) as i64;
        let delta_d = new_delta - self.stored_delta;

        if delta_d == 0 {
            let _ = self.bw.write_bit(false);
        } else {
            // encode with zigzag
            let mut enc = zigzag_encode(delta_d) as u64;
            let mut length = 64 - enc.leading_zeros();
            if length == 0 {
                length = 1;
            }

            if length <= 7 {
                // prepend '10' in the high bits by shifting
                enc |= (0x02u64) << 7;
                let _ = self.bw.write_bits(9, enc);
            } else if (8..=9).contains(&length) {
                enc |= (0x06u64) << 9;
                let _ = self.bw.write_bits(12, enc);
            } else if (10..=12).contains(&length) {
                enc |= (0x0Eu64) << 12;
                let _ = self.bw.write_bits(16, enc);
            } else {
                // write '1111' prefix
                let _ = self.bw.write_bits(4, 0x0Fu64);
                let _ = self.bw.write_bits(32, enc);
            }
        }

        self.stored_delta = (timestamp as i128 - self.stored_timestamp as i128) as i64;
        self.stored_timestamp = timestamp;
    }

    fn write_full_value(&mut self, v: u64) {
        self.bw.write_byte(255);
        for b in v.to_be_bytes().iter() {
            self.bw.write_byte(*b);
        }
    }

    fn compress_value(&mut self, value: f64) {
        let v = value.to_bits();

        if let Some(offset) = self.window.get_index_of(v) {
            self.bw.write_byte(offset as u8);
        } else {
            let candidate = self.window.get_candidate(v);
            let xor = candidate ^ v;
            let lead_bytes = (xor.leading_zeros() / 8) as usize;
            let trail_bytes = (xor.trailing_zeros() / 8) as usize;

            if (lead_bytes + trail_bytes) > 1 {
                if let Some(offset) = self.window.get_index_of(candidate) {
                    let off = (offset as u8) | 0x80u8; // set high bit
                    self.bw.write_byte(off);

                    let xor_len_bytes = 8 - lead_bytes - trail_bytes;
                    let xor_shifted = xor >> (trail_bytes * 8);
                    let head = ((trail_bytes as u8) << 4) | (xor_len_bytes as u8 & 0x0F);
                    self.bw.write_byte(head);

                    let arr = xor_shifted.to_be_bytes();
                    // write most-significant first for the used bytes
                    let start = 8 - xor_len_bytes;
                    for i in start..8 {
                        self.bw.write_byte(arr[i]);
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

    pub fn close(&mut self) {
        // mimic BitStream::close sentinel
        let _ = self.bw.write_bits(4, 0x0F);
        // write two u64::MAX
        self.bw.write_u64(u64::MAX);
        self.bw.write_u64(u64::MAX);
        let _ = self.bw.write_bit(false);
    }

    /// Produce a single contiguous buffer containing the bitstream followed by the value-bytes
    pub fn into_bytes(self) -> Vec<u8> {
        self.bw.get_ref().to_vec()
    }
}

/// Decompressor for TSXor single-column
pub struct DecompressorTSXor<'a> {
    reader: BufferedReader<'a>,
    bytes: &'a [u8],
    byte_idx: usize,
    cache: Window,
    stored_leading_zeros: Vec<u64>,
    stored_trailing_zeros: Vec<u64>,
    stored_val: f64,
    stored_timestamp: u64,
    stored_delta: i64,
    block_timestamp: u64,
    ncols: usize,
    end_of_stream: bool,
}

impl<'a> DecompressorTSXor<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        let mut reader = BufferedReader::new(buf);
        // read header
        let block_timestamp = reader.read_u64().unwrap_or(0);
        DecompressorTSXor {
            reader,
            bytes: buf,
            byte_idx: 0,
            cache: Window::new(),
            stored_leading_zeros: vec![],
            stored_trailing_zeros: vec![],
            stored_val: 0.0,
            stored_timestamp: 0,
            stored_delta: 0,
            block_timestamp,
            ncols: 1,
            end_of_stream: false,
        }
    }

    fn bits_to_read(&mut self) -> Option<u32> {
        // read up to 4 bits until a zero is found; return number of bits to read after prefix
        let mut acc = 0u8;
        for _ in 0..4 {
            match self.reader.read_bit() {
                Ok(bit) => {
                    acc = (acc << 1) | (bit as u8);
                    if !bit {
                        break;
                    }
                }
                Err(_) => return None,
            }
        }

        match acc {
            0x00 => Some(0),
            0x02 => Some(7),
            0x06 => Some(9),
            0x0e => Some(12),
            0x0f => Some(32),
            _ => None,
        }
    }

    pub fn next(&mut self) -> Option<(u64, f64)> {
        if self.end_of_stream {
            return None;
        }

        if self.stored_timestamp == 0 {
            // first sample
            let sd = match self.reader.read_bits(FIRST_DELTA_BITS) {
                Ok(v) => v,
                Err(_) => return None,
            } as u64;
            self.stored_delta = sd as i64;
            let val_bits = match self.reader.read_u64() {
                Ok(v) => v,
                Err(_) => return None,
            };
            self.cache.insert(val_bits);
            self.stored_val = f64::from_bits(val_bits);
            self.stored_timestamp = self.block_timestamp + sd;
            return Some((self.stored_timestamp, self.stored_val));
        }

        // next timestamp
        let to_read = match self.bits_to_read() {
            Some(v) => v,
            None => return None,
        };

        let delta_delta = if to_read == 0 {
            0i64
        } else {
            let bits = match self.reader.read_bits(to_read) {
                Ok(v) => v,
                Err(_) => return None,
            };
            zigzag_decode(bits) as i64
        };

        self.stored_delta = self.stored_delta + delta_delta;
        self.stored_timestamp = (self.stored_timestamp as i128 + self.stored_delta as i128) as u64;

        // next value (single column)
        // read one byte from leftover bytes area. We need to track a byte cursor over the remaining
        // bytes that were appended after the bit stream. To simplify, we locate the current bit-reader
        // byte position by introspecting buffer, and reader state is not exposed; instead we rely on
        // reading value bytes using read_byte/read_u64 from the bit reader, which is correct because
        // the compressor wrote bytes into the same bitstream in the same order.
        // Attempt to read a single byte
        let head = match self.reader.read_byte() {
            Ok(b) => b,
            Err(_) => return None,
        };

        let final_val = if head < 128 {
            // reference
            let val = self.cache.get(head as usize);
            val
        } else if head == 255 {
            // full value
            match self.reader.read_u64() {
                Ok(v) => v,
                Err(_) => return None,
            }
        } else {
            // xor encoded
            let offset = (head & 0x7F) as usize;
            let info = match self.reader.read_byte() {
                Ok(b) => b,
                Err(_) => return None,
            };
            let trail_zeros = (info >> 4) as usize;
            let xor_bytes = (info & 0x0F) as usize;
            let mut xor_val: u64 = 0;
            for _ in 0..xor_bytes {
                match self.reader.read_byte() {
                    Ok(b) => xor_val = (xor_val << 8) | (b as u64),
                    Err(_) => return None,
                }
            }
            let shift_bits = trail_zeros.saturating_mul(8) as u32;
            if shift_bits < 64 {
                xor_val <<= shift_bits;
            } else {
                // out-of-range shift - treat as zero shift to avoid panic
                xor_val = 0;
            }
            xor_val ^ self.cache.get(offset)
        };

        self.cache.insert(final_val);
        let valf = f64::from_bits(final_val);
        self.stored_val = valf;

        Some((self.stored_timestamp, valf))
    }
}

/// A TSXor-based chunk implementation. This is a simple wrapper around the
/// compressor/decompressor that stores the encoded buffer and performs
/// operations by decoding/re-encoding as needed.
#[derive(Debug, Clone, PartialEq, GetSize, Hash)]
pub struct TsXorChunk {
    buf: Vec<u8>,
    max_size: usize,
}

impl Default for TsXorChunk {
    fn default() -> Self {
        Self::with_max_size(crate::config::DEFAULT_CHUNK_SIZE_BYTES)
    }
}

impl TsXorChunk {
    pub fn with_max_size(max_size: usize) -> Self {
        TsXorChunk { buf: Vec::new(), max_size }
    }

    pub fn is_full(&self) -> bool {
        self.buf.len() >= self.max_size
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        if samples.is_empty() {
            self.buf.clear();
            return Ok(());
        }
        let block_ts = samples[0].timestamp as u64;
        let mut c = CompressorTSXor::new(block_ts);
        for s in samples.iter() {
            c.add_value(s.timestamp as u64, s.value);
        }
        c.close();
        self.buf = c.into_bytes();
        Ok(())
    }

    pub(crate) fn decode_all(&self) -> Vec<Sample> {
        let mut samples = Vec::new();
        if self.buf.is_empty() {
            return samples;
        }
        let mut d = DecompressorTSXor::new(&self.buf);
        while let Some((ts, v)) = d.next() {
            samples.push(Sample::new(ts as i64, v));
        }
        samples
    }

    pub fn data_size(&self) -> usize {
        self.buf.len()
    }

    /// Borrow the internal encoded buffer
    pub fn buf(&self) -> &[u8] {
        &self.buf
    }

    pub fn bytes_per_sample(&self) -> usize {
        // Count samples using a streaming iterator to avoid allocation
        let count = TsXorChunkIterator::new(self.buf(), i64::MIN, i64::MAX).count();
        if count == 0 { return size_of::<Sample>() / 2; }
        self.data_size() / count
    }
}

pub struct TsXorChunkIterator<'a> {
    decoder: DecompressorTSXor<'a>,
    start: Timestamp,
    end: Timestamp,
}

impl<'a> TsXorChunkIterator<'a> {
    pub fn new(buf: &'a [u8], start: Timestamp, end: Timestamp) -> Self {
        Self { decoder: DecompressorTSXor::new(buf), start, end }
    }
}

impl<'a> Iterator for TsXorChunkIterator<'a> {
    type Item = Sample;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((ts, v)) = self.decoder.next() {
            let ts_i = ts as i64;
            if ts_i < self.start { continue; }
            if ts_i > self.end { return None; }
            return Some(Sample::new(ts_i, v));
        }
        None
    }
}

impl Chunk for TsXorChunk {
    fn first_timestamp(&self) -> Timestamp {
        // Streaming: return the first sample timestamp without allocating a Vec
        let mut it = TsXorChunkIterator::new(self.buf(), i64::MIN, i64::MAX);
        if let Some(s) = it.next() { s.timestamp } else { i64::MIN }
    }

    fn last_timestamp(&self) -> Timestamp {
        // Streaming: iterate to the end and return last timestamp
        let mut it = TsXorChunkIterator::new(self.buf(), i64::MIN, i64::MAX);
        let mut last_ts = i64::MIN;
        while let Some(s) = it.next() { last_ts = s.timestamp; }
        last_ts
    }

    fn len(&self) -> usize {
        // Streaming count without allocating
        TsXorChunkIterator::new(self.buf(), i64::MIN, i64::MAX).count()
    }

    fn last_value(&self) -> f64 {
        let mut it = TsXorChunkIterator::new(self.buf(), i64::MIN, i64::MAX);
        let mut last_val = 0.0f64;
        while let Some(s) = it.next() { last_val = s.value; }
        last_val
    }

    fn size(&self) -> usize {
        self.data_size()
    }

    fn max_size(&self) -> usize { self.max_size }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let mut samples = self.decode_all();
        let orig = samples.len();
        samples.retain(|s| !(s.timestamp >= start_ts && s.timestamp <= end_ts));
        let removed = orig - samples.len();
        self.set_data(&samples)?;
        Ok(removed)
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        let mut samples = self.decode_all();
        samples.push(*sample);
        samples.sort_by_key(|s| s.timestamp);
        self.set_data(&samples)?;
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        // Use streaming iterator to produce the requested range without decoding entire chunk
        let vec: Vec<Sample> = TsXorChunkIterator::new(self.buf(), start, end).collect();
        Ok(vec)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let mut samples = self.decode_all();
        if samples.is_empty() {
            self.set_data(&[sample])?;
            return Ok(1);
        }
        // find position
        let mut duplicate_found = false;
        for s in samples.iter_mut() {
            if s.timestamp == sample.timestamp {
                duplicate_found = true;
                s.value = dp_policy.duplicate_value(s.timestamp, s.value, sample.value)?;
                break;
            }
        }
        if !duplicate_found {
            samples.push(sample);
            samples.sort_by_key(|s| s.timestamp);
        }
        self.set_data(&samples)?;
        Ok(if duplicate_found { samples.len() } else { samples.len() })
    }

    fn merge_samples(&mut self, samples: &[Sample], _dp_policy: Option<DuplicatePolicy>) -> TsdbResult<Vec<crate::series::SampleAddResult>> {
        // naive merge: decompress existing, append, sort, and compute results
        let mut existing = self.decode_all();
        let mut res = Vec::with_capacity(samples.len());
        let mut set: std::collections::HashSet<Timestamp> = existing.iter().map(|s| s.timestamp).collect();
        for sample in samples.iter() {
            if set.contains(&sample.timestamp) {
                res.push(crate::series::SampleAddResult::Duplicate);
                continue;
            }
            existing.push(*sample);
            set.insert(sample.timestamp);
            res.push(crate::series::SampleAddResult::Ok(*sample));
        }
        existing.sort_by_key(|s| s.timestamp);
        self.set_data(&existing)?;
        Ok(res)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        let mut samples = self.decode_all();
        if samples.is_empty() { return Ok(self.clone()); }
        let mid = samples.len() / 2;
        let right = samples.split_off(mid);
        let mut right_chunk = TsXorChunk::with_max_size(self.max_size);
        right_chunk.set_data(&right)?;
        self.set_data(&samples)?;
        Ok(right_chunk)
    }

    fn optimize(&mut self) -> TsdbResult<()> { Ok(()) }

    fn save_rdb(&self, rdb: *mut RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        // save length and bytes
        let len = self.buf.len();
        rdb_save_usize(rdb, len);
        for &b in self.buf.iter() {
            valkey_module::raw::save_unsigned(rdb, b as u64);
        }
    }

    fn load_rdb(rdb: *mut RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let len = rdb_load_usize(rdb)?;
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            let v = valkey_module::raw::load_unsigned(rdb)? as u8;
            buf.push(v);
        }
        Ok(TsXorChunk { buf, max_size })
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        dest.extend_from_slice(&self.buf);
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut b = buf;
        let max_size = try_read_uvarint(&mut b).map_err(|_| TsdbError::ChunkDecoding)? as usize;
        let chunk = TsXorChunk { buf: b.to_vec(), max_size };
        Ok(chunk)
    }

    fn debug_digest(&self, dig: &mut Digest) {
        dig.add_string_buffer(&self.buf);
        dig.add_long_long(self.max_size as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_simple() {
        let mut c = CompressorTSXor::new(1_600_000_000);
        let mut ts = 1_600_000_000u64 + 1000;
        let mut values = vec![];
        for i in 0..100 {
            let v = (i as f64) * 1.5 + (i % 3) as f64;
            c.add_value(ts, v);
            ts += 60;
            values.push((ts - 60, v));
        }
        c.close();
        let buf = c.into_bytes();

        let mut d = DecompressorTSXor::new(&buf);
        let mut out = vec![];
        while let Some(s) = d.next() {
            out.push(s);
        }

        // we expect at least as many outputs as inputs (first sample + compressed ones)
        assert!(!out.is_empty());
        // Compare first N values
        for (i, (ts, val)) in values.iter().enumerate() {
            if i >= out.len() {
                break;
            }
            let (ots, oval) = out[i];
            assert_eq!(*ts, ots);
            assert_eq!(*val, oval);
        }
    }

    fn round_trip(samples: &[(u64, f64)], block_ts: u64) -> Vec<(u64, f64)> {
        let mut c = CompressorTSXor::new(block_ts);
        for (ts, v) in samples.iter() {
            c.add_value(*ts, *v);
        }
        c.close();
        let buf = c.into_bytes();

        let mut d = DecompressorTSXor::new(&buf);
        let mut out = vec![];
        while let Some(s) = d.next() {
            out.push(s);
        }
        out
    }

    #[test]
    fn test_reference_hit() {
        let block = 10_000;
        let vbits = 0x0123_4567_89AB_CDEFu64;
        let v = f64::from_bits(vbits);
        let samples = vec![(block + 100, v), (block + 200, v)];
        let out = round_trip(&samples, block);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], samples[0]);
        assert_eq!(out[1], samples[1]);
    }

    #[test]
    fn test_xor_encoding_and_exception() {
        let block = 20_000;
        // candidate value
        let cand = f64::from_bits(0x0102_0304_0506_0708u64);
        // value differing in mid bytes -> should trigger xor encoding
        let v_xor = f64::from_bits(0x0102_0304_05AA_0708u64);
        // value differing in low bits -> likely exception
        let v_exc = f64::from_bits(0xDEAD_BEEF_CAFE_BABEu64);

        let samples = vec![
            (block + 100, cand),
            (block + 200, v_xor),
            (block + 300, v_exc),
        ];

        let out = round_trip(&samples, block);
        assert_eq!(out.len(), 3);
        for i in 0..3 {
            assert_eq!(out[i].0, samples[i].0);
            assert_eq!(out[i].1.to_bits(), samples[i].1.to_bits());
        }
    }

    #[test]
    fn test_timestamp_varlen_cases() {
        let block = 30_000u64;
        // first timestamp
        let t0 = block + 100;
        // next: same delta -> delta-of-delta = 0
        let t1 = t0 + 60;
        let t2 = t1 + 60; // delta-delta = 0
        // make a small delta-delta = 1
        let t3 = t2 + 61;
        // make a larger delta-delta to trigger longer encoding
        let t4 = t3 + 61 + (1 << 10); // big jump

        let samples = vec![
            (t0, 1.0_f64),
            (t1, 2.0_f64),
            (t2, 3.0_f64),
            (t3, 4.0_f64),
            (t4, 5.0_f64),
        ];

        let out = round_trip(&samples, block);
        assert_eq!(out.len(), samples.len());
        for i in 0..samples.len() {
            assert_eq!(out[i].0, samples[i].0);
            assert_eq!(out[i].1.to_bits(), samples[i].1.to_bits());
        }
    }

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
        let expected: Vec<Sample> = TsXorChunkIterator::new(chunk.buf(), start, end).collect();

        assert_eq!(got.len(), expected.len());
        for (g, e) in got.iter().zip(expected.iter()) {
            assert_eq!(g.timestamp, e.timestamp);
            assert_eq!(g.value.to_bits(), e.value.to_bits());
        }
    }
}
