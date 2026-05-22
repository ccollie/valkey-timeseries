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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It received minor modifications to suit Prometheus's needs.

use std::io;

const MAX_VARINT_LEN64: usize = 10;

/// BStreamReader reads bits from a byte stream.
///
/// The internal buffer is filled from the stream and bits are read left-to-right.
#[derive(Debug, Clone)]
pub struct BStreamReader<'a> {
    stream: &'a [u8],
    stream_offset: usize,

    pub(crate) buffer: u64,
    pub(crate) valid: u8,
    last: u8,
}

impl<'a> BStreamReader<'a> {
    /// Creates a new BStreamReader from a byte slice.
    ///
    /// The last byte of the stream is copied to handle potential concurrent writes.
    pub fn new(b: &'a [u8]) -> Self {
        let last = if b.is_empty() { 0 } else { b[b.len() - 1] };

        BStreamReader {
            stream: b,
            stream_offset: 0,
            buffer: 0,
            valid: 0,
            last,
        }
    }

    /// Reads a single bit and returns it as a boolean.
    pub fn read_bit(&mut self) -> io::Result<bool> {
        if self.valid == 0 && !self.load_next_buffer(1) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        self.read_bit_fast()
    }

    /// Fast path for reading a bit; returns EOF if buffer is empty.
    ///
    /// This function must remain small for compiler inlining.
    fn read_bit_fast(&mut self) -> io::Result<bool> {
        if self.valid == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        self.valid -= 1;
        let bitmask = 1u64 << self.valid;
        Ok((self.buffer & bitmask) != 0)
    }

    /// Reads the specified number of bits and returns them as a u64.
    ///
    /// The result has the bits in the right-most positions, with other bits set to 0.
    pub fn read_bits(&mut self, nbits: u8) -> io::Result<u64> {
        if self.valid == 0 && !self.load_next_buffer(nbits) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        if nbits <= self.valid {
            return self.read_bits_fast(nbits);
        }

        // Read remaining valid bits from current buffer and part from next one.
        let bitmask = (1u64 << self.valid) - 1;
        let nbits_remaining = nbits - self.valid;
        let mut v = (self.buffer & bitmask) << nbits_remaining;
        self.valid = 0;

        if !self.load_next_buffer(nbits_remaining) {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        // If load_next_buffer loaded fewer valid bits than requested, treat as EOF
        if self.valid < nbits_remaining {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        let bitmask = (1u64 << nbits_remaining) - 1;
        v |= (self.buffer >> (self.valid - nbits_remaining)) & bitmask;
        self.valid -= nbits_remaining;

        Ok(v)
    }

    /// Fast path for reading bits; returns EOF if not enough valid bits.
    ///
    /// This function must remain small for compiler inlining.
    fn read_bits_fast(&mut self, nbits: u8) -> io::Result<u64> {
        if nbits > self.valid {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }

        let bitmask = (1u64 << nbits) - 1;
        self.valid -= nbits;

        Ok((self.buffer >> self.valid) & bitmask)
    }

    /// Reads a single byte (8 bits).
    pub fn read_byte(&mut self) -> io::Result<u8> {
        let v = self.read_bits(8)?;
        Ok(v as u8)
    }

    /// Reads XOR2 control prefix quickly.
    ///
    /// Returns (control_value, success). If success is false, the caller should
    /// retry with read_xor2_control(). This function must remain small for inlining.
    pub fn read_xor2_control_fast(&mut self) -> (u8, bool) {
        if self.valid < 4 {
            return (0, false);
        }

        let top4 = ((self.buffer >> (self.valid - 4)) & 0xf) as u8;

        if top4 < 8 {
            // '0xxx': dod=0, val=0 (case 0)
            self.valid -= 1;
            return (0, true);
        }

        if top4 < 12 {
            // '10xx': dod=0, val changed (case 1)
            self.valid -= 2;
            return (1, true);
        }

        if top4 < 14 {
            // '110x': small dod (case 2)
            self.valid -= 3;
            return (2, true);
        }

        if top4 == 14 {
            // '1110': medium dod (case 3)
            self.valid -= 4;
            return (3, true);
        }

        (0, false)
    }

    /// Reads XOR2 variable-length joint control prefix.
    ///
    /// Returns 0-5 mapping to the six encoding cases:
    /// - 0 → '0'     dod=0, val=0               (1 bit consumed)
    /// - 1 → '10'    dod=0, val≠0               (2 bits consumed)
    /// - 2 → '110'   dod≠0, 13-bit signed dod   (3 bits consumed)
    /// - 3 → '1110'  dod≠0, 20-bit signed dod   (4 bits consumed)
    /// - 4 → '11110' dod≠0, 64-bit escape       (5 bits consumed)
    /// - 5 → '11111' dod=0, stale NaN           (5 bits consumed)
    pub fn read_xor2_control(&mut self) -> io::Result<u8> {
        if self.valid >= 4 {
            let top4 = ((self.buffer >> (self.valid - 4)) & 0xf) as u8;

            if top4 < 8 {
                // '0xxx' → case 0
                self.valid -= 1;
                return Ok(0);
            }

            if top4 < 12 {
                // '10xx' → case 1
                self.valid -= 2;
                return Ok(1);
            }

            if top4 < 14 {
                // '110x' → case 2
                self.valid -= 3;
                return Ok(2);
            }

            if top4 == 14 {
                // '1110' → case 3
                self.valid -= 4;
                return Ok(3);
            }

            // '1111': need fifth bit to distinguish cases 4 and 5
            if self.valid >= 5 {
                let bit4 = ((self.buffer >> (self.valid - 5)) & 1) as u8;
                self.valid -= 5;
                return Ok(4 + bit4);
            }

            // Fifth bit spans a buffer boundary
            self.valid -= 4;
            let bit4 = self.read_bit()?;
            if !bit4 {
                return Ok(4);
            }
            return Ok(5);
        }

        // Slow path: read bits one at a time
        let bit0 = self.read_bit()?;
        if !bit0 {
            return Ok(0);
        }

        let bit1 = self.read_bit()?;
        if !bit1 {
            return Ok(1);
        }

        let bit2 = self.read_bit()?;
        if !bit2 {
            return Ok(2);
        }

        let bit3 = self.read_bit()?;
        if !bit3 {
            return Ok(3);
        }

        let bit4 = self.read_bit()?;
        if !bit4 {
            return Ok(4);
        }

        Ok(5)
    }

    /// Reads a varint-encoded u64.
    pub fn read_uvarint(&mut self) -> io::Result<u64> {
        let mut x: u64 = 0;
        let mut s: u32 = 0;

        for _ in 0..MAX_VARINT_LEN64 {
            let byte = self.read_byte()?;

            if byte < 0x80 {
                return Ok(x | ((byte as u64) << s));
            }

            x |= (byte as u64 & 0x7f) << s;
            s += 7;
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "varint overflow",
        ))
    }

    /// Reads a varint-encoded i64.
    pub fn read_varint(&mut self) -> io::Result<i64> {
        let ux = self.read_uvarint()?;
        let mut x = (ux >> 1) as i64;

        if (ux & 1) != 0 {
            x = !x;
        }

        Ok(x)
    }

    /// Loads the next bytes from the stream into the internal buffer.
    ///
    /// Returns true if data was loaded, false if at EOF.
    fn load_next_buffer(&mut self, nbits: u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false;
        }

        // Fast path: more than 8 bytes remain
        if self.stream_offset + 8 < self.stream.len() {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&self.stream[self.stream_offset..self.stream_offset + 8]);
            self.buffer = u64::from_be_bytes(bytes);
            self.stream_offset += 8;
            self.valid = 64;
            return true;
        }

        // Slow path: 8 or fewer bytes remain
        let mut nbytes = ((nbits / 8) + 1) as usize;
        if self.stream_offset + nbytes > self.stream.len() {
            nbytes = self.stream.len() - self.stream_offset;
        }

        let mut buffer: u64 = 0;
        let mut skip = 0;

        if self.stream_offset + nbytes == self.stream.len() {
            // Use cached last byte to handle concurrent writes
            buffer |= self.last as u64;
            skip = 1;
        }

        for i in 0..(nbytes - skip) {
            let byte = self.stream[self.stream_offset + i] as u64;
            buffer |= byte << (8 * (nbytes - i - 1));
        }

        self.buffer = buffer;
        self.stream_offset += nbytes;
        self.valid = (nbytes * 8) as u8;

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_single_bit() {
        let data = vec![0b10000000u8];
        let mut reader = BStreamReader::new(&data);

        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
    }

    #[test]
    fn test_read_byte() {
        let data = vec![0xA5u8]; // 10100101
        let mut reader = BStreamReader::new(&data);

        assert_eq!(reader.read_byte().unwrap(), 0xA5);
    }

    #[test]
    fn test_read_bits() {
        let data = vec![0b11110000u8];
        let mut reader = BStreamReader::new(&data);

        let bits = reader.read_bits(4).unwrap();
        assert_eq!(bits, 0xF);
    }

    #[test]
    fn test_read_uvarint() {
        // Encode 300 as varint: 0xAC 0x02
        let data = vec![0xACu8, 0x02u8];
        let mut reader = BStreamReader::new(&data);

        let val = reader.read_uvarint().unwrap();
        assert_eq!(val, 300);
    }
}
