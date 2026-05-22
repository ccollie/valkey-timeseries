// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! XOR2Chunk implements XOR encoding with joint timestamp+value control bits
//! and byte-packed dod encoding for efficient appending. It also has an extra
//! header byte after the sample count to allow for optionally encoding start
//! timestamp (ST).
//!
//! Control prefix for samples >= 2:
//!
//! ```text
//!     0     → dod=0 AND value unchanged              (1 bit)
//!     10    → dod=0, value changed                   (2 bits, then value encoding)
//!     110   → dod≠0, 13-bit signed [-4096, 4095]     (prefix+dod packed into 2 bytes)
//!     1110  → dod≠0, 20-bit signed [-524288, 524287] (prefix+dod packed into 3 bytes)
//!     11110 → dod≠0, 64-bit escape                   (5+64 bits, then value encoding)
//!     11111 → dod=0, stale NaN                       (5 bits, no value field)
//! ```
//!
//! The dod bins are widened so that prefix+dod aligns to byte boundaries,
//! replacing writeBit calls with writeByte for common cases.
//!
//! Value encoding for the dod≠0 cases (`<varbit_xor2>`):
//!
//! ```text
//!     0   → value unchanged
//!     10  → reuse previous leading/trailing window
//!     110 → new leading/trailing window
//!     111 → stale NaN
//! ```
//!
//! Value encoding for the dod=0, value-changed case (`<varbit_xor2_nn>`):
//!
//! ```text
//!     0 → reuse previous leading/trailing window
//!     1 → new leading/trailing window
//! ```
//!
//! Start timestamp (ST) encoding:
//!
//! 1-byte ST header (at b[chunkHeaderSize]) layout:
//!
//! ```text
//!     bit 7 (0x80): firstSTKnown   — ST for the first sample is present in the stream
//!     bits 6-0:    firstSTChangeOn — sample index where the first ST change begins
//! ```
//!
//! When no ST is provided (st == 0 always), the header stays 0x00 and the
//! chunk has no additional bits in it.
//!
//! When ST is present, the ST delta (prevT - st) is appended after each
//! sample's joint timestamp+value encoding using putVarbitIntFast.

use super::XOR2Iterator;
use crate::common::encoding::{try_read_uvarint, write_uvarint};
use crate::common::rdb::{
    RdbSerializable, rdb_load_bool, rdb_load_u8, rdb_load_usize, rdb_save_bool, rdb_save_u8,
    rdb_save_usize,
};
use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::bstream::{BStream, ONE, ZERO};
use crate::series::chunks::chunk::Chunk;
use crate::series::chunks::merge::merge_samples;
use crate::series::{DuplicatePolicy, SampleAddResult};
use ahash::AHashSet;
use get_size2::GetSize;
use std::cmp;
use valkey_module::{ValkeyError, ValkeyResult, digest::Digest, raw};

pub(super) const CHUNK_HEADER_SIZE: usize = 2;
// Number of bytes used for ST header (first byte: flag + low 7 bits, second byte: high bits)
pub(super) const ST_HEADER_SIZE: usize = 2;
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 1024;
pub(super) const MAX_FIRST_ST_CHANGE_ON: u8 = 0x7F;
pub(crate) const DEFAULT_MAX_CHUNK_SIZE: usize = 4 * 1024; // 4 KB

fn write_header_first_st_known(b: &mut [u8]) {
    b[0] |= 0x80;
}

fn write_header_first_st_change_on(b: &mut [u8], first_st_change_on: u16) {
    // Store low 7 bits in first byte (bits 0-6), high bits in second byte.
    b[0] |= (first_st_change_on as u8) & 0x7F;
    b[1] = (first_st_change_on >> 7) as u8;
}

pub(super) fn read_st_header(b: &[u8]) -> (bool, u16) {
    if b[0] == 0x00 && b.len() < 2 {
        return (false, 0);
    }
    let first_st_known = b[0] & 0x80 != 0;
    let low = (b[0] & 0x7F) as u16;
    let high = if b.len() > 1 { b[1] as u16 } else { 0 };
    let first_st_change_on = (high << 7) | low;
    (first_st_known, first_st_change_on)
}

#[derive(GetSize)]
pub struct XOR2Chunk {
    b: BStream,
    max_size: usize,
    st: i64,
    t: i64,
    v: f64,
    t_delta: u64,
    st_diff: i64,
    leading: u8,
    trailing: u8,
    num_total: u16,
    first_st_change_on: u16,
    first_st_known: bool,
    first_timestamp: i64,
    last_timestamp: i64,
    last_value: f64,
}

impl std::fmt::Debug for XOR2Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XOR2Chunk")
            .field("max_size", &self.max_size)
            .field("num_total", &self.num_total)
            .field("first_timestamp", &self.first_timestamp)
            .field("last_timestamp", &self.last_timestamp)
            .finish()
    }
}

impl PartialEq for XOR2Chunk {
    fn eq(&self, other: &Self) -> bool {
        self.max_size == other.max_size
            && self.b.bytes() == other.b.bytes()
            && self.st == other.st
            && self.t == other.t
            && self.v.to_bits() == other.v.to_bits()
            && self.t_delta == other.t_delta
            && self.st_diff == other.st_diff
            && self.leading == other.leading
            && self.trailing == other.trailing
            && self.num_total == other.num_total
            && self.first_st_change_on == other.first_st_change_on
            && self.first_st_known == other.first_st_known
            && self.first_timestamp == other.first_timestamp
            && self.last_timestamp == other.last_timestamp
            && self.last_value.to_bits() == other.last_value.to_bits()
    }
}

impl std::hash::Hash for XOR2Chunk {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.max_size.hash(state);
        self.b.bytes().hash(state);
        self.st.hash(state);
        self.t.hash(state);
        self.v.to_bits().hash(state);
        self.t_delta.hash(state);
        self.st_diff.hash(state);
        self.leading.hash(state);
        self.trailing.hash(state);
        self.num_total.hash(state);
        self.first_st_change_on.hash(state);
        self.first_st_known.hash(state);
        self.first_timestamp.hash(state);
        self.last_timestamp.hash(state);
        self.last_value.to_bits().hash(state);
    }
}

impl XOR2Chunk {
    pub(crate) fn with_max_size(max_size: usize) -> Self {
        let stream = vec![0u8; CHUNK_HEADER_SIZE + ST_HEADER_SIZE]; // reserve header bytes
        let mut b = BStream::new();
        b.reset(stream);

        Self {
            b,
            max_size,
            st: 0,
            t: i64::MIN,
            v: 0.0,
            t_delta: 0,
            st_diff: 0,
            leading: 0xFF,
            trailing: 0,
            num_total: 0,
            first_st_change_on: 0,
            first_st_known: false,
            first_timestamp: 0,
            last_timestamp: 0,
            last_value: f64::NAN,
        }
    }

    pub fn new() -> Self {
        Self::with_max_size(DEFAULT_MAX_CHUNK_SIZE)
    }

    pub fn bytes(&self) -> &[u8] {
        self.b.bytes()
    }

    pub fn num_samples(&self) -> usize {
        self.num_total as usize
    }

    pub fn compact(&mut self) {
        let l = self.b.len();
        if self.b.stream.capacity() > l + CHUNK_COMPACT_CAPACITY_THRESHOLD {
            let mut buf = vec![0u8; l];
            buf.copy_from_slice(&self.b.stream);
            self.b.stream = buf;
        }
    }

    pub fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    pub fn is_full(&self) -> bool {
        self.size() >= self.max_size
    }

    pub fn bytes_per_sample(&self) -> usize {
        // conservative constant estimate
        crate::common::SAMPLE_SIZE
    }

    pub fn clear(&mut self) {
        let stream = vec![0u8; CHUNK_HEADER_SIZE + ST_HEADER_SIZE];
        self.b.reset(stream);
        self.st = 0;
        self.t = i64::MIN;
        self.v = 0.0;
        self.t_delta = 0;
        self.st_diff = 0;
        self.leading = 0xFF;
        self.trailing = 0;
        self.num_total = 0;
        self.first_st_change_on = 0;
        self.first_st_known = false;
        self.first_timestamp = 0;
        self.last_timestamp = 0;
        self.last_value = f64::NAN;
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        self.clear();
        for sample in samples.iter() {
            self.add_sample(sample)?;
        }
        Ok(())
    }

    fn write_v_delta(&mut self, v: f64) {
        if is_stale_nan(v) {
            self.b.write_bits_fast(0b111, 3);
            return;
        }

        let delta = v.to_bits() ^ self.v.to_bits();

        if delta == 0 {
            self.b.write_bit(ZERO);
            return;
        }

        let new_leading = cmp::min(delta.leading_zeros() as u8, 31);
        let new_trailing = delta.trailing_zeros() as u8;

        if self.leading != 0xFF && new_leading >= self.leading && new_trailing >= self.trailing {
            self.b.write_bits_fast(0b10, 2);
            let sig_bits = 64 - self.leading as usize - self.trailing as usize;
            self.b.write_bits_fast(delta >> self.trailing, sig_bits);
            return;
        }

        self.leading = new_leading;
        self.trailing = new_trailing;

        self.b.write_bits_fast(0b110, 3);
        self.b.write_bits_fast(new_leading as u64, 5);

        let sig_bits = 64 - new_leading as usize - new_trailing as usize;
        self.b.write_bits_fast(sig_bits as u64, 6);
        self.b.write_bits_fast(delta >> new_trailing, sig_bits);
    }

    fn write_v_delta_known_non_zero(&mut self, delta: u64) {
        let new_leading = cmp::min(delta.leading_zeros() as u8, 31);
        let new_trailing = delta.trailing_zeros() as u8;

        if self.leading != 0xFF && new_leading >= self.leading && new_trailing >= self.trailing {
            self.b.write_bit(ZERO);
            let sig_bits = 64 - self.leading as usize - self.trailing as usize;
            self.b.write_bits_fast(delta >> self.trailing, sig_bits);
            return;
        }

        self.leading = new_leading;
        self.trailing = new_trailing;

        self.b.write_bit(ONE);
        self.b.write_bits_fast(new_leading as u64, 5);

        let sig_bits = 64 - new_leading as usize - new_trailing as usize;
        self.b.write_bits_fast(sig_bits as u64, 6);
        self.b.write_bits_fast(delta >> new_trailing, sig_bits);
    }

    fn encode_joint(&mut self, dod: i64, v: f64) {
        if dod == 0 {
            if is_stale_nan(v) {
                self.b.write_bits_fast(0b11111, 5);
                return;
            }
            let v_bits = v.to_bits() ^ self.v.to_bits();
            if v_bits == 0 {
                self.b.write_bit(ZERO);
                return;
            }
            self.b.write_bits_fast(0b10, 2);
            self.write_v_delta_known_non_zero(v_bits);
            return;
        }

        match dod {
            -4096..=4095 => {
                // 13-bit dod
                self.b
                    .write_byte(0b110_00000 | ((dod as u64 >> 8) & 0x1F) as u8);
                self.b.write_byte(dod as u8);
            }
            -524288..=524287 => {
                // 20-bit dod
                self.b
                    .write_byte(0b1110_0000 | ((dod as u64 >> 16) & 0x0F) as u8);
                self.b.write_byte((dod >> 8) as u8);
                self.b.write_byte(dod as u8);
            }
            _ => {
                // 64-bit escape
                self.b.write_bits_fast(0b11110, 5);
                self.b.write_bits_fast(dod as u64, 64);
            }
        }

        if v.to_bits() == self.v.to_bits() {
            self.b.write_bit(ZERO);
        } else {
            self.write_v_delta(v);
        }
    }

    fn update_header(&mut self) {
        if self.b.bytes().len() >= CHUNK_HEADER_SIZE + ST_HEADER_SIZE {
            let [hi, lo] = self.num_total.to_be_bytes();
            self.b.stream[0] = hi;
            self.b.stream[1] = lo;

            let mut st_header = [0u8; ST_HEADER_SIZE];
            if self.first_st_known {
                write_header_first_st_known(&mut st_header);
            }
            write_header_first_st_change_on(&mut st_header, self.first_st_change_on);
            // write both header bytes into reserved positions
            self.b.stream[CHUNK_HEADER_SIZE] = st_header[0];
            self.b.stream[CHUNK_HEADER_SIZE + 1] = st_header[1];
        }
    }

    pub fn append(&mut self, st: i64, t: i64, v: f64) {
        let mut t_delta = 0u64;
        let mut st_diff = 0i64;

        match self.num_total {
            0 => {
                self.b.write_signed_int(t);
                self.b.write_bits_fast(v.to_bits(), 64);
                self.first_timestamp = t;

                if st != 0 {
                    self.b.write_signed_int(t - st);
                    self.first_st_known = true;
                    let bytes = self.b.bytes();
                    if bytes.len() > CHUNK_HEADER_SIZE {
                        let mut header = [0u8; ST_HEADER_SIZE];
                        write_header_first_st_known(&mut header);
                        // Need to modify the actual buffer
                    }
                }
            }
            1 => {
                t_delta = (t - self.t) as u64;
                self.b.write_unsigned_int(t_delta);
                self.write_v_delta(v);

                if st != self.st {
                    st_diff = self.t - st;
                    self.first_st_change_on = 1;
                    let bytes = self.b.bytes();
                    if bytes.len() > CHUNK_HEADER_SIZE {
                        let mut header = [0u8; ST_HEADER_SIZE];
                        write_header_first_st_change_on(&mut header, 1);
                    }
                    self.b.write_signed_int(st_diff);
                }
            }
            _ => {
                t_delta = (t - self.t) as u64;
                let dod = (t_delta as i64) - (self.t_delta as i64);

                if self.first_st_change_on == 0 && st == self.st {
                    let v_bits = v.to_bits();
                    match (dod, v_bits == self.v.to_bits()) {
                        (0, true) => {
                            self.b.write_bit(ZERO);
                        }
                        (d, true) if (-(1 << 12)..=(1 << 12) - 1).contains(&d) => {
                            self.b
                                .write_byte(0b110_00000 | ((dod as u64 >> 8) & 0x1F) as u8);
                            self.b.write_byte(dod as u8);
                            self.b.write_bit(ZERO);
                        }
                        _ => {
                            self.encode_joint(dod, v);
                            if !is_stale_nan(v) {
                                self.v = v;
                            }
                        }
                    }
                    self.t = t;
                    self.t_delta = t_delta;
                    self.st_diff = st_diff;
                    self.num_total += 1;
                    self.st = st;
                    self.last_timestamp = t;
                    self.last_value = v;
                    if !is_stale_nan(v) {
                        self.v = v;
                    }
                    self.update_header();
                    return;
                }

                if self.first_st_change_on > 0 {
                    let new_st_diff = self.t - st;
                    let delta_st_diff = new_st_diff - self.st_diff;
                    let v_bits = v.to_bits();

                    match (dod, v_bits == self.v.to_bits()) {
                        (0, true) => match delta_st_diff {
                            0 => {
                                self.b.write_bit(ZERO);
                                self.b.write_bit(ZERO);
                            }
                            -3..=4 => {
                                self.b.write_bits_fast(
                                    (0b10 << 3) | ((delta_st_diff as u64) & 0x7),
                                    6,
                                );
                            }
                            -31..=32 => {
                                self.b.write_bits_fast(
                                    (0b110 << 6) | ((delta_st_diff as u64) & 0x3F),
                                    10,
                                );
                            }
                            -255..=256 => {
                                self.b.write_bits_fast(
                                    (0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF),
                                    14,
                                );
                            }
                            _ => {
                                self.b.write_bit(ZERO);
                                self.b.write_signed_int(delta_st_diff);
                            }
                        },
                        (d, true) if (-(1 << 12)..=(1 << 12) - 1).contains(&d) => {
                            self.b
                                .write_byte(0b110_00000 | ((dod as u64 >> 8) & 0x1F) as u8);
                            self.b.write_byte(dod as u8);
                            match delta_st_diff {
                                0 => {
                                    self.b.write_bit(ZERO);
                                    self.b.write_bit(ZERO);
                                }
                                -3..=4 => {
                                    self.b.write_bits_fast(
                                        (0b10 << 3) | ((delta_st_diff as u64) & 0x7),
                                        6,
                                    );
                                }
                                -31..=32 => {
                                    self.b.write_bits_fast(
                                        (0b110 << 6) | ((delta_st_diff as u64) & 0x3F),
                                        10,
                                    );
                                }
                                -255..=256 => {
                                    self.b.write_bits_fast(
                                        (0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF),
                                        14,
                                    );
                                }
                                _ => {
                                    self.b.write_bit(ZERO);
                                    self.b.write_signed_int(delta_st_diff);
                                }
                            }
                        }
                        _ => {
                            self.encode_joint(dod, v);
                            if !is_stale_nan(v) {
                                self.v = v;
                            }
                            match delta_st_diff {
                                0 => {
                                    self.b.write_bit(ZERO);
                                }
                                -3..=4 => {
                                    self.b.write_bits_fast(
                                        (0b10 << 3) | ((delta_st_diff as u64) & 0x7),
                                        5,
                                    );
                                }
                                -31..=32 => {
                                    self.b.write_bits_fast(
                                        (0b110 << 6) | ((delta_st_diff as u64) & 0x3F),
                                        9,
                                    );
                                }
                                -255..=256 => {
                                    self.b.write_bits_fast(
                                        (0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF),
                                        13,
                                    );
                                }
                                _ => {
                                    self.b.write_signed_int(delta_st_diff);
                                }
                            }
                        }
                    }
                    self.st_diff = new_st_diff;
                    self.st = st;
                    self.t = t;
                    self.t_delta = t_delta;
                    self.num_total += 1;

                    self.last_value = v;
                    self.last_timestamp = t;

                    self.update_header();
                    return;
                }

                self.encode_joint(dod, v);

                if st != self.st {
                    st_diff = self.t - st;
                    self.first_st_change_on = self.num_total;
                    let bytes = self.b.bytes();
                    if bytes.len() > CHUNK_HEADER_SIZE {
                        let mut header = [0u8; ST_HEADER_SIZE];
                        write_header_first_st_change_on(&mut header, self.num_total);
                    }
                    self.b.write_signed_int(st_diff);
                }
            }
        }

        self.st = st;
        self.t = t;
        self.last_timestamp = t;
        self.last_value = v;

        if !is_stale_nan(v) {
            self.v = v;
        }
        self.t_delta = t_delta;
        self.st_diff = st_diff;
        self.num_total += 1;
        self.update_header();
    }

    pub fn iterator(&self) -> XOR2Iterator<'_> {
        XOR2Iterator::new(self.b.bytes())
    }
}

impl RdbSerializable for XOR2Chunk {
    fn rdb_save(&self, rdb: *mut raw::RedisModuleIO) {
        rdb_save_usize(rdb, self.max_size);
        rdb_save_usize(rdb, self.num_total as usize);
        rdb_save_usize(rdb, self.first_st_change_on as usize);
        rdb_save_bool(rdb, self.first_st_known);
        raw::save_signed(rdb, self.first_timestamp);
        raw::save_signed(rdb, self.last_timestamp);
        raw::save_double(rdb, self.last_value);
        raw::save_signed(rdb, self.st);
        raw::save_signed(rdb, self.t);
        raw::save_double(rdb, self.v);
        raw::save_unsigned(rdb, self.t_delta);
        raw::save_signed(rdb, self.st_diff);
        rdb_save_u8(rdb, self.leading);
        rdb_save_u8(rdb, self.trailing);
        raw::save_slice(rdb, self.b.bytes());
        raw::save_unsigned(rdb, self.b.position() as u64);
    }

    fn rdb_load(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let max_size = rdb_load_usize(rdb)?;
        let num_total = u16::try_from(rdb_load_usize(rdb)?)
            .map_err(|_| ValkeyError::String("Invalid XOR2 chunk sample count".into()))?;
        let first_st_change_on = u16::try_from(rdb_load_usize(rdb)?)
            .map_err(|_| ValkeyError::String("Invalid XOR2 chunk first_st_change_on".into()))?;
        let first_st_known = rdb_load_bool(rdb)?;
        let first_timestamp = raw::load_signed(rdb)?;
        let last_timestamp = raw::load_signed(rdb)?;
        let last_value = raw::load_double(rdb)?;
        let st = raw::load_signed(rdb)?;
        let t = raw::load_signed(rdb)?;
        let v = raw::load_double(rdb)?;
        let t_delta = raw::load_unsigned(rdb)?;
        let st_diff = raw::load_signed(rdb)?;
        let leading = rdb_load_u8(rdb)?;
        let trailing = rdb_load_u8(rdb)?;
        let bytes = raw::load_string_buffer(rdb)?.as_ref().to_vec();
        let position = raw::load_unsigned(rdb)?;

        if bytes.len() < CHUNK_HEADER_SIZE + ST_HEADER_SIZE {
            return Err(ValkeyError::String("Invalid XOR2 chunk RDB payload".into()));
        }

        if position > 8 {
            return Err(ValkeyError::String(format!(
                "Invalid XOR2 chunk bit position: {position}"
            )));
        }

        let position = position as u8;

        let mut chunk = Self {
            b: BStream::hydrate(bytes, position),
            max_size,
            st,
            t,
            v,
            t_delta,
            st_diff,
            leading,
            trailing,
            num_total,
            first_st_change_on,
            first_st_known,
            first_timestamp,
            last_timestamp,
            last_value,
        };
        chunk.update_header();
        Ok(chunk)
    }
}

// Helper functions
pub(super) const STALE_NAN: u64 = 0x7FF0000000000002; // Prometheus stale NaN marker

pub(super) fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN
}

impl Clone for XOR2Chunk {
    fn clone(&self) -> Self {
        let stream = self.b.bytes().to_vec();

        Self {
            b: BStream::hydrate(stream, self.b.position()),
            max_size: self.max_size,
            st: self.st,
            t: self.t,
            v: self.v,
            t_delta: self.t_delta,
            st_diff: self.st_diff,
            leading: self.leading,
            trailing: self.trailing,
            num_total: self.num_total,
            first_st_change_on: self.first_st_change_on,
            first_st_known: self.first_st_known,
            first_timestamp: self.first_timestamp,
            last_timestamp: self.last_timestamp,
            last_value: self.last_value,
        }
    }
}

impl Chunk for XOR2Chunk {
    fn first_timestamp(&self) -> Timestamp {
        self.first_timestamp
    }

    fn last_timestamp(&self) -> Timestamp {
        self.last_timestamp
    }

    fn len(&self) -> usize {
        self.num_total as usize
    }

    fn last_value(&self) -> f64 {
        self.last_value
    }

    fn size(&self) -> usize {
        self.b.len()
    }

    fn max_size(&self) -> usize {
        self.max_size
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        if self.is_empty() || start_ts > self.last_timestamp() || end_ts < self.first_timestamp() {
            return Ok(0);
        }

        let mut new_chunk = XOR2Chunk::with_max_size(self.max_size);
        let saved_count = self.len();

        for sample in self.iterator() {
            if sample.timestamp >= start_ts && sample.timestamp <= end_ts {
                continue;
            }
            new_chunk.append(0, sample.timestamp, sample.value);
        }

        *self = new_chunk;
        Ok(saved_count - self.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        self.append(0, sample.timestamp, sample.value);
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        if self.is_empty() {
            return Ok(vec![]);
        }

        let samples = self
            .iterator()
            .filter(|s| s.timestamp >= start && s.timestamp <= end)
            .collect();
        Ok(samples)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;
        let mut duplicate_found = false;
        let count = self.len();

        if count == 0 {
            self.add_sample(&sample)?;
            return Ok(1);
        }

        let mut new_chunk = XOR2Chunk::with_max_size(self.max_size);
        let mut iter = self.iterator();

        if ts < self.first_timestamp() {
            // insert sample before the first existing sample
            // we need to read the first existing sample to get its `st`
            if let Some(first_current) = iter.next() {
                let st_for_first = iter.at_st();
                // use the first existing sample's st for the newly inserted sample
                new_chunk.append(st_for_first, sample.timestamp, sample.value);
                // append the first existing sample with its st
                new_chunk.append(st_for_first, first_current.timestamp, first_current.value);

                // append the rest using each sample's st from the iterator
                while let Some(cur) = iter.next() {
                    let st_cur = iter.at_st();
                    new_chunk.append(st_cur, cur.timestamp, cur.value);
                }
            }
        } else {
            let mut current = Sample::default();

            // append all samples strictly before ts
            while let Some(item) = iter.next() {
                current = item;
                if current.timestamp >= ts {
                    break;
                }
                let st_cur = iter.at_st();
                new_chunk.append(st_cur, current.timestamp, current.value);
            }

            if current.timestamp == ts {
                // duplicate found: keep/merge value according to policy
                duplicate_found = true;
                let new_value = dp_policy.duplicate_value(ts, current.value, sample.value)?;
                let st_cur = iter.at_st();
                new_chunk.append(st_cur, current.timestamp, new_value);
            } else {
                // insert the new sample before `current` (which is the first sample >= ts)
                // use the iterator's st (current sample's st) for the inserted sample
                let st_for_insert = iter.at_st();
                new_chunk.append(st_for_insert, sample.timestamp, sample.value);

                // if `current` actually exists and is > ts, append it with its st
                if current.timestamp > ts {
                    let st_cur = iter.at_st();
                    new_chunk.append(st_cur, current.timestamp, current.value);
                }
            }

            // append the remaining samples (if any) using their iterator st
            while let Some(cur) = iter.next() {
                let st_cur = iter.at_st();
                new_chunk.append(st_cur, cur.timestamp, cur.value);
            }
        }

        *self = new_chunk;
        let size = if duplicate_found { count } else { count + 1 };
        Ok(size)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        let mut result = Vec::with_capacity(samples.len());

        if self.is_empty() || (!samples.is_empty() && samples[0].timestamp > self.last_timestamp()) {
            for sample in samples.iter() {
                match self.add_sample(sample) {
                    Ok(_) => result.push(SampleAddResult::Ok(*sample)),
                    Err(TsdbError::CapacityFull(_)) => {
                        return Err(TsdbError::CapacityFull(self.max_size));
                    }
                    Err(_) => result.push(SampleAddResult::Error(
                        crate::error_consts::CANNOT_ADD_SAMPLE,
                    )),
                }
            }
            return Ok(result);
        }

        // Materialize the XOR2 iterator into a Vec<Sample> and create a SampleIter::Vec
        let left = SampleIter::vec(self.iterator().collect::<Vec<Sample>>());
        let right = SampleIter::Slice(samples.iter());

        let mut new_chunk = XOR2Chunk::with_max_size(self.max_size);

        // Build a set of input timestamps so we only produce a single result per unique
        // input timestamp. This mirrors the behavior of the Uncompressed chunk.
        let mut sample_set: AHashSet<Timestamp> = AHashSet::with_capacity(samples.len());
        for sample in samples.iter() {
            sample_set.insert(sample.timestamp);
        }

        let mut state_res: Vec<SampleAddResult> = Vec::with_capacity(samples.len());

        merge_samples(
            left,
            right,
            dp_policy,
            &mut state_res,
            |state: &mut Vec<SampleAddResult>, sample: Sample, is_duplicate: bool| {
                new_chunk.append(0, sample.timestamp, sample.value);
                let is_new = sample_set.remove(&sample.timestamp);
                if is_new {
                    if is_duplicate {
                        state.push(SampleAddResult::Duplicate);
                    } else {
                        state.push(SampleAddResult::Ok(sample));
                    }
                }
                Ok(())
            },
        )?;

        *self = new_chunk;
        Ok(state_res)
    }

    fn split(&mut self) -> TsdbResult<Self> {
        let mut left_chunk = XOR2Chunk::with_max_size(self.max_size);
        let mut right_chunk = XOR2Chunk::with_max_size(self.max_size);

        if self.is_empty() {
            return Ok(self.clone());
        }

        let mid = self.len() / 2;
        let mut i = 0;
        let mut iter = self.iterator();

        while let Some(sample) = iter.next() {
            let st = iter.at_st();
            if i < mid {
                left_chunk.append(st, sample.timestamp, sample.value);
            } else {
                right_chunk.append(st, sample.timestamp, sample.value);
            }
            i += 1;
        }

        *self = left_chunk;
        Ok(right_chunk)
    }

    fn optimize(&mut self) -> TsdbResult<()> {
        self.compact();
        Ok(())
    }

    fn save_rdb(&self, rdb: *mut raw::RedisModuleIO) {
        self.rdb_save(rdb);
    }

    fn load_rdb(rdb: *mut raw::RedisModuleIO, _enc_ver: i32) -> ValkeyResult<Self> {
        Self::rdb_load(rdb)
    }

    fn serialize(&self, dest: &mut Vec<u8>) {
        write_uvarint(dest, self.max_size as u64);
        write_uvarint(dest, self.num_total as u64);
        write_uvarint(dest, self.first_st_change_on as u64);
        write_uvarint(dest, self.first_st_known as u64);
        write_uvarint(dest, self.first_timestamp as u64);
        write_uvarint(dest, self.last_timestamp as u64);
        write_uvarint(dest, f64::to_bits(self.last_value));
        write_uvarint(dest, self.st as u64);
        write_uvarint(dest, self.t as u64);
        write_uvarint(dest, f64::to_bits(self.v));
        write_uvarint(dest, self.t_delta);
        write_uvarint(dest, self.st_diff as u64);
        write_uvarint(dest, self.leading as u64);
        write_uvarint(dest, self.trailing as u64);
        write_uvarint(dest, self.b.position() as u64);
        write_uvarint(dest, self.b.bytes().len() as u64);
        dest.extend_from_slice(self.b.bytes());
    }

    fn deserialize(buf: &[u8]) -> TsdbResult<Self> {
        let mut remaining = buf;
        let max_size =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as usize;
        let num_total =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as u16;
        let first_st_change_on =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as u16;
        let first_st_known =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? != 0;
        let first_timestamp =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as i64;
        let last_timestamp =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as i64;
        let last_value =
            f64::from_bits(try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)?);
        let st = try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as i64;
        let t = try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as i64;
        let v =
            f64::from_bits(try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)?);
        let t_delta = try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)?;
        let st_diff =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as i64;
        let leading = try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as u8;
        let trailing =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as u8;
        let position = try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)?;
        let stream_len =
            try_read_uvarint(&mut remaining).map_err(|_| TsdbError::ChunkDecoding)? as usize;

        if position > 8 || remaining.len() != stream_len {
            return Err(TsdbError::ChunkDecoding);
        }

        let stream = remaining.to_vec();
        if stream.len() < CHUNK_HEADER_SIZE + ST_HEADER_SIZE {
            return Err(TsdbError::ChunkDecoding);
        }

        let mut chunk = Self {
            b: BStream::hydrate(stream, position as u8),
            max_size,
            st,
            t,
            v,
            t_delta,
            st_diff,
            leading,
            trailing,
            num_total,
            first_st_change_on,
            first_st_known,
            first_timestamp,
            last_timestamp,
            last_value,
        };
        chunk.update_header();

        Ok(chunk)
    }

    fn debug_digest(&self, dig: &mut Digest) {
        dig.add_long_long(self.num_total as i64);
        dig.add_long_long(self.first_timestamp);
        dig.add_long_long(self.last_timestamp);
        dig.add_long_long(self.max_size as i64);
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;

    #[test]
    fn debug_xor2_remove_range_internal() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];

        let mut c = XOR2Chunk::new();
        c.set_data(&samples).unwrap();
        eprintln!("[debug_xor2] chunk.len() = {}", c.len());
        let all: Vec<_> = c.iterator().collect();
        eprintln!("[debug_xor2] iterator samples = {:?}", all);
        c.remove_range(20, 30).unwrap();
        eprintln!(
            "[debug_xor2] after remove len = {} iter = {:?}",
            c.len(),
            c.iterator().collect::<Vec<_>>()
        );
    }
}
