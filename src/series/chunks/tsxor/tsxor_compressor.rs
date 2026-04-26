use std::collections::VecDeque;
use get_size2::GetSize;
use crate::common::encoding::{write_uvarint, try_read_uvarint};
use crate::common::Sample;
use crate::series::chunks::buffered_writer::BufferedWriter;
use crate::series::chunks::DecompressorTSXor;
use crate::series::chunks::gorilla::utils::{zigzag_decode, zigzag_encode};
use crate::series::chunks::traits::BitWrite;

pub(super) const WINDOW_SIZE: usize = 127;
pub(super) const FIRST_DELTA_BITS: u32 = 32;


/// Simple sliding window used by TSXor (port of TSXor Window)
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
        // initialize with zeros
        for _ in 0..WINDOW_SIZE {
            buffer.push_back(0);
        }
        Self { buffer }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn insert(&mut self, val: u64) {
        // remove tail, push front
        self.buffer.pop_back();
        self.buffer.push_front(val);
    }

    pub fn contains(&self, val: u64) -> bool {
        self.buffer.iter().any(|v| *v == val)
    }

    fn get_index_of(&self, val: u64) -> Option<usize> {
        self.buffer.iter().position(|v| *v == val)
    }

    pub fn get(&self, offset: usize) -> u64 {
        self.buffer[offset]
    }

    /// Get the best candidate from the window for the given value based on the similarity metric (clz + ctz)
    /// TODO: This is a linear search and could be optimized with SIMD or by keeping track of leading/trailing
    /// zeros for each entry in the window
    pub fn get_candidate(&self, val: u64) -> u64 {
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

    pub fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.len() as u64);
        for &val in self.buffer.iter() {
            write_uvarint(dest, val);
        }
    }

    pub fn deserialize(src: &[u8]) -> Self {
        let mut src = src;
        Self::deserialize_from(&mut src).unwrap_or_default()
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

/// Compressor for TSXor for a single time-series (timestamps u64, values f64)
#[derive(Debug, Clone, GetSize)]
pub struct CompressorTSXor {
    writer: BufferedWriter,
    window: CacheWindow,
    stored_timestamp: u64,
    stored_delta: i64,
    block_timestamp: u64,
    count: usize,
}

impl Default for CompressorTSXor {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressorTSXor {
    pub fn new() -> Self {
        Self {
            writer: BufferedWriter::new(),
            window: CacheWindow::new(),
            stored_timestamp: 0,
            stored_delta: 0,
            block_timestamp: 0,
            count: 0,
        }
    }

    pub fn clear(&mut self) {
        self.writer.clear();
        self.window = CacheWindow::new();
        self.stored_timestamp = 0;
        self.stored_delta = 0;
        self.block_timestamp = 0;
        self.count = 0;
    }

    pub fn add_sample(&mut self, sample: Sample) {
        self.add(sample.timestamp as u64, sample.value);
    }

    pub fn add(&mut self, timestamp: u64, val: f64) {
        if self.count == 0 {
            self.block_timestamp = timestamp;
            self.stored_delta = (timestamp as i128 - self.block_timestamp as i128) as i64;
            self.stored_timestamp = timestamp;

            // write header
            self.writer.write_u64(timestamp);

            // write FIRST_DELTA_BITS bits of stored_delta
            let sd = self.stored_delta as u64;
            let _ = self.writer.write_bits(FIRST_DELTA_BITS, sd);

            // write value as 64-bit big endian
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
            // encode with zigzag
            let mut enc = zigzag_encode(delta_d);
            let mut length = 64 - enc.leading_zeros();
            if length == 0 {
                length = 1;
            }

            if length <= 7 {
                // prepend '10' in the high bits by shifting
                enc |= (0x02u64) << 7;
                let _ = self.writer.write_bits(9, enc);
            } else if (8..=9).contains(&length) {
                enc |= (0x06u64) << 9;
                let _ = self.writer.write_bits(12, enc);
            } else if (10..=12).contains(&length) {
                enc |= (0x0Eu64) << 12;
                let _ = self.writer.write_bits(16, enc);
            } else {
                // write '1111' prefix
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
                    let off = (offset as u8) | 0x80u8; // set high bit
                    self.writer.write_byte(off);

                    let xor_len_bytes = 8 - lead_bytes - trail_bytes;
                    let xor_shifted = xor >> (trail_bytes * 8);
                    let head = ((trail_bytes as u8) << 4) | (xor_len_bytes as u8 & 0x0F);
                    self.writer.write_byte(head);

                    let arr = xor_shifted.to_be_bytes();
                    // write most-significant first for the used bytes
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

    pub fn close(&mut self) {
        // mimic BitStream::close sentinel
        let _ = self.writer.write_bits(4, 0x0F);
        // write two u64::MAX
        self.writer.write_u64(u64::MAX);
        self.writer.write_u64(u64::MAX);
        let _ = self.writer.write_bit(false);
    }

    /// Produce a single contiguous buffer containing the bitstream followed by the value-bytes
    pub fn into_bytes(self) -> Vec<u8> {
        self.writer.get_ref().to_vec()
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.writer.position() as u64);
        write_uvarint(dest, self.writer.get_ref().len() as u64);
        dest.extend_from_slice(self.writer.get_ref());
        self.window.serialize(dest);
        write_uvarint(dest, self.stored_timestamp);
        write_uvarint(dest, zigzag_encode(self.stored_delta));
        write_uvarint(dest, self.block_timestamp);
        write_uvarint(dest, self.count as u64);
    }

    pub fn deserialize(src: &[u8]) -> Self {
        let mut src = src;

        let writer_pos = match try_read_uvarint(&mut src) {
            Ok(pos) if pos <= 8 => pos as u32,
            _ => return Self::new(),
        };

        let writer_len = match try_read_uvarint(&mut src) {
            Ok(len) => len as usize,
            Err(_) => return Self::new(),
        };

        if src.len() < writer_len {
            return Self::new();
        }

        let writer_buf = src[..writer_len].to_vec();
        src = &src[writer_len..];

        let window = match CacheWindow::deserialize_from(&mut src) {
            Some(window) => window,
            None => return Self::new(),
        };

        let stored_timestamp = match try_read_uvarint(&mut src) {
            Ok(ts) => ts,
            Err(_) => return Self::new(),
        };

        let stored_delta = match try_read_uvarint(&mut src) {
            Ok(delta) => zigzag_decode(delta),
            Err(_) => return Self::new(),
        };

        let block_timestamp = match try_read_uvarint(&mut src) {
            Ok(ts) => ts,
            Err(_) => return Self::new(),
        };

        let count = match try_read_uvarint(&mut src) {
            Ok(count) => count as usize,
            Err(_) => return Self::new(),
        };

        Self {
            writer: BufferedWriter::hydrate(writer_buf, writer_pos),
            window,
            stored_timestamp,
            stored_delta,
            block_timestamp,
            count,
        }
    }

    pub(super) fn get_decompressor(&self) -> DecompressorTSXor<'_> {
        DecompressorTSXor::new(self.writer.get_ref(), self.count)
    }
}

impl AsRef<[u8]> for CompressorTSXor {
    fn as_ref(&self) -> &[u8] {
        self.writer.get_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::series::chunks::DecompressorTSXor;
    use super::*;

    #[test]
    fn round_trip_simple() {
        let mut c = CompressorTSXor::new();
        let mut ts = 1_600_000_000u64 + 1000;
        let mut values = vec![];
        for i in 0..100 {
            let v = (i as f64) * 1.5 + (i % 3) as f64;
            c.add(ts, v);
            ts += 60;
            values.push((ts - 60, v));
        }

        let buf = c.as_ref();

        let mut d = c.get_decompressor();
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
        let mut c = CompressorTSXor::new();
        for (ts, v) in samples.iter() {
            c.add(*ts, *v);
        }
        let buf = c.as_ref();

        let mut d = c.get_decompressor();
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
    fn test_serialize_deserialize_continue_matches_full_stream() {
        let mut samples = Vec::new();
        let mut ts = 1_700_000_000u64;
        for i in 0..120 {
            let v = (i as f64) * 1.25 + ((i % 7) as f64) * 0.1;
            samples.push((ts, v));
            ts += if i % 5 == 0 { 61 } else { 60 };
        }

        let split = 57;

        let mut full = CompressorTSXor::new();
        for &(sample_ts, sample_val) in &samples {
            full.add(sample_ts, sample_val);
        }
        let expected = full.into_bytes();

        let mut partial = CompressorTSXor::new();
        for &(sample_ts, sample_val) in &samples[..split] {
            partial.add(sample_ts, sample_val);
        }

        let mut state = Vec::new();
        partial.serialize(&mut state);

        let mut restored = CompressorTSXor::deserialize(&state);
        for &(sample_ts, sample_val) in &samples[split..] {
            restored.add(sample_ts, sample_val);
        }
        let actual = restored.into_bytes();

        assert_eq!(actual, expected);

        let mut d = DecompressorTSXor::new(&actual, samples.len());
        let mut out = vec![];
        while let Some(sample) = d.next() {
            out.push(sample);
        }

        assert_eq!(out.len(), samples.len());
        for (i, (sample_ts, sample_val)) in samples.iter().enumerate() {
            assert_eq!(out[i].0, *sample_ts);
            assert_eq!(out[i].1.to_bits(), sample_val.to_bits());
        }
    }

    #[test]
    fn test_serialize_deserialize_empty_compressor() {
        let compressor = CompressorTSXor::new();
        let mut state = Vec::new();
        compressor.serialize(&mut state);

        let mut restored = CompressorTSXor::deserialize(&state);
        assert_eq!(restored.len(), 0);

        let expected_ts = 12345;
        let expected_val = 42.125;
        restored.add(expected_ts, expected_val);

        let restored_count = restored.len();

        let buf = restored.into_bytes();
        let mut d = DecompressorTSXor::new(&buf, restored_count);
        let first = d.next().expect("expected first sample after restoring empty compressor");
        assert_eq!(first.0, expected_ts);
        assert_eq!(first.1.to_bits(), expected_val.to_bits());
    }
}