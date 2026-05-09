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
use crate::common::rdb::{
    rdb_load_bool, rdb_load_u8, rdb_load_usize, rdb_save_bool, rdb_save_u8, rdb_save_usize,
    RdbSerializable,
};
use crate::series::chunks::bstream::{BStream, ONE, ZERO};
use std::cmp;
use valkey_module::{raw, ValkeyError, ValkeyResult};

pub(super) const CHUNK_HEADER_SIZE: usize = 2; // Placeholder, adjust as needed
const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 1024; // Placeholder
pub(super) const MAX_FIRST_ST_CHANGE_ON: u8 = 0x7F;


fn write_header_first_st_known(b: &mut [u8]) {
    b[0] = 0x80;
}

fn write_header_first_st_change_on(b: &mut [u8], first_st_change_on: u16) {
    if first_st_change_on > MAX_FIRST_ST_CHANGE_ON as u16 {
        return;
    }
    b[0] |= first_st_change_on as u8;
}

pub(super) fn read_st_header(b: &[u8]) -> (bool, u8) {
    if b[0] == 0x00 {
        return (false, 0);
    }
    let first_st_known = b[0] & 0x80 != 0;
    let first_st_change_on = b[0] & 0x7F;
    (first_st_known, first_st_change_on)
}


pub struct XOR2Chunk {
    b: BStream,
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

impl XOR2Chunk {
    pub(crate) fn new() -> Self {
        let stream = vec![0u8; CHUNK_HEADER_SIZE + 1]; // +1 for ST header
        let mut b = BStream::new();
        b.reset(stream);
        
        Self {
            b,
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
                self.b.write_byte(0b110_00000 | ((dod as u64 >> 8) & 0x1F) as u8);
                self.b.write_byte(dod as u8);
            }
            -524288..=524287 => {
                // 20-bit dod
                self.b.write_byte(0b1110_0000 | ((dod as u64 >> 16) & 0x0F) as u8);
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
        if self.b.bytes().len() >= CHUNK_HEADER_SIZE + 1 {
            let [hi, lo] = self.num_total.to_be_bytes();
            self.b.stream[0] = hi;
            self.b.stream[1] = lo;

            let mut st_header = [0u8; 1];
            if self.first_st_known {
                write_header_first_st_known(&mut st_header);
            }
            write_header_first_st_change_on(&mut st_header, self.first_st_change_on);
            self.b.stream[CHUNK_HEADER_SIZE] = st_header[0];
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
                        let mut header = [0u8; 1];
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
                        let mut header = [0u8; 1];
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
                            self.b.write_byte(0b110_00000 | ((dod as u64 >> 8) & 0x1F) as u8);
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
                    self.num_total += 1;
                    self.update_header();
                    return;
                }

                if self.first_st_change_on > 0 {
                    let new_st_diff = self.t - st;
                    let delta_st_diff = new_st_diff - self.st_diff;
                    let v_bits = v.to_bits();

                    match (dod, v_bits == self.v.to_bits()) {
                        (0, true) => {
                            match delta_st_diff {
                                0 => {
                                    self.b.write_bit(ZERO);
                                    self.b.write_bit(ZERO);
                                }
                                -3..=4 => {
                                    self.b.write_bits_fast((0b10 << 3) | ((delta_st_diff as u64) & 0x7), 6);
                                }
                                -31..=32 => {
                                    self.b.write_bits_fast((0b110 << 6) | ((delta_st_diff as u64) & 0x3F), 10);
                                }
                                -255..=256 => {
                                    self.b.write_bits_fast((0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF), 14);
                                }
                                _ => {
                                    self.b.write_bit(ZERO);
                                    self.b.write_signed_int(delta_st_diff);
                                }
                            }
                        }
                        (d, true) if (-(1 << 12)..=(1 << 12) - 1).contains(&d) => {
                            self.b.write_byte(0b110_00000 | ((dod as u64 >> 8) & 0x1F) as u8);
                            self.b.write_byte(dod as u8);
                            match delta_st_diff {
                                0 => {
                                    self.b.write_bit(ZERO);
                                    self.b.write_bit(ZERO);
                                }
                                -3..=4 => {
                                    self.b.write_bits_fast((0b10 << 3) | ((delta_st_diff as u64) & 0x7), 6);
                                }
                                -31..=32 => {
                                    self.b.write_bits_fast((0b110 << 6) | ((delta_st_diff as u64) & 0x3F), 10);
                                }
                                -255..=256 => {
                                    self.b.write_bits_fast((0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF), 14);
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
                                    self.b.write_bits_fast((0b10 << 3) | ((delta_st_diff as u64) & 0x7), 5);
                                }
                                -31..=32 => {
                                    self.b.write_bits_fast((0b110 << 6) | ((delta_st_diff as u64) & 0x3F), 9);
                                }
                                -255..=256 => {
                                    self.b.write_bits_fast((0b1110 << 9) | ((delta_st_diff as u64) & 0x1FF), 13);
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
                        let mut header = [0u8; 1];
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
        let num_total = u16::try_from(rdb_load_usize(rdb)?).map_err(|_| {
            ValkeyError::String("Invalid XOR2 chunk sample count".into())
        })?;
        let first_st_change_on = u16::try_from(rdb_load_usize(rdb)?).map_err(|_| {
            ValkeyError::String("Invalid XOR2 chunk first_st_change_on".into())
        })?;
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

        if bytes.len() < CHUNK_HEADER_SIZE + 1 {
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
