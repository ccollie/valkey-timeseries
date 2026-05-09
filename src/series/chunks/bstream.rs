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

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

pub(in crate::series::chunks) const ZERO: bool = false;
pub(in crate::series::chunks) const ONE: bool = true;


/// A stream of bits for writing.
pub struct BStream {
    /// The data stream.
    pub(in crate::series::chunks) stream: Vec<u8>,
    /// How many right-most bits are available for writing in the current byte
    /// (the last byte of the stream).
    count: u8,
}

impl BStream {
    pub fn new() -> Self {
        Self {
            stream: Vec::new(),
            count: 0,
        }
    }

    /// Reset the stream around the provided byte slice.
    pub fn reset(&mut self, stream: Vec<u8>) {
        self.stream = stream;
        self.count = 0;
    }

    /// Get the underlying bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.stream
    }

    pub(crate) fn position(&self) -> u8 {
        self.count
    }

    pub(crate) fn hydrate(stream: Vec<u8>, count: u8) -> Self {
        Self { stream, count }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.stream
    }

    pub fn clear(&mut self) {
        self.stream.clear();
        self.count = 0;
    }

    pub fn len(&self) -> usize {
        self.stream.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stream.is_empty()
    }

    /// Write a single bit to the stream.
    pub fn write_bit(&mut self, bit: bool) {
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }

        let i = self.stream.len() - 1;

        if bit {
            self.stream[i] |= 1 << (self.count - 1);
        }

        self.count -= 1;
    }

    /// Write a single byte to the stream.
    pub(in crate::series::chunks) fn write_byte(&mut self, byt: u8) {
        if self.count == 0 {
            self.stream.push(byt);
            return;
        }

        let i = self.stream.len() - 1;

        // Complete the last byte with the leftmost count bits from byt.
        self.stream[i] |= byt >> (8 - self.count);

        // Write the remainder, if any.
        self.stream.push(byt << self.count);
    }

    /// Write the nbits right-most bits of u to the stream in left-to-right order.
    /// TODO: Once XOR2 stabilizes, replace write_bits with write_bits_fast implementation.
    pub fn write_bits(&mut self, mut u: u64, mut nbits: usize) {
        u <<= 64 - nbits;
        while nbits >= 8 {
            let byt = (u >> 56) as u8;
            self.write_byte(byt);
            u <<= 8;
            nbits -= 8;
        }

        while nbits > 0 {
            self.write_bit((u >> 63) == 1);
            u <<= 1;
            nbits -= 1;
        }
    }

    /// Like write_bits but handles the partial last byte inline to avoid per-byte
    /// write_byte calls and writes complete bytes directly to the stream.
    pub fn write_bits_fast(&mut self, mut u: u64, mut nbits: usize) {
        u <<= 64 - nbits;

        // If the last byte is partial, fill its remaining bits first.
        if self.count > 0 {
            let free = self.count as usize;
            let last = self.stream.len() - 1;
            self.stream[last] |= (u >> (64 - free)) as u8;
            if nbits < free {
                self.count = (free - nbits) as u8;
                return;
            }
            u <<= free;
            nbits -= free;
            self.count = 0;
        }

        // Write complete bytes directly, avoiding per-byte function call overhead.
        while nbits >= 8 {
            self.stream.push((u >> 56) as u8);
            u <<= 8;
            nbits -= 8;
        }

        // Write any remaining bits as a partial final byte.
        if nbits > 0 {
            self.stream.push((u >> 56) as u8);
            self.count = (8 - nbits) as u8;
        }
    }

    pub(in crate::series::chunks) fn write_unsigned_int(&mut self, mut n: u64) {
        while n >= 0x80 {
            self.write_byte((n as u8) | 0x80);
            n >>= 7;
        }
        self.write_byte(n as u8);
    }

    pub(in crate::series::chunks) fn write_signed_int(&mut self, n: i64) {
        let zigzag = ((n << 1) ^ (n >> 63)) as u64;
        let mut remaining = zigzag;
        while remaining >= 0x80 {
            let byte = ((remaining & 0x7F) | 0x80) as u8;
            self.write_byte(byte);
            remaining >>= 7;
        }
        self.write_byte(remaining as u8);
    }
}

impl Default for BStream {
    fn default() -> Self {
        Self::new()
    }
}
