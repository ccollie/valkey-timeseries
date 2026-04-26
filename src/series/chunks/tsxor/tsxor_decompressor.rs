use crate::series::chunks::buffered_read::BufferedReader;
use crate::series::chunks::gorilla::utils::zigzag_decode;
use crate::series::chunks::traits::BitRead;
use crate::series::chunks::tsxor::tsxor_compressor::{CacheWindow, FIRST_DELTA_BITS};

/// Decompressor for TSXor single-column
pub(crate) struct DecompressorTSXor<'a> {
    reader: BufferedReader<'a>,
    bytes: &'a [u8],
    byte_idx: usize,
    cache: CacheWindow,
    stored_leading_zeros: Vec<u64>, // todo: smallvec
    stored_trailing_zeros: Vec<u64>,
    stored_val: f64,
    stored_timestamp: u64,
    stored_delta: i64,
    block_timestamp: u64,
    end_of_stream: bool,
    index: usize,
    sample_count: usize,
}

impl<'a> DecompressorTSXor<'a> {
    pub(super) fn new(buf: &'a [u8], sample_count: usize) -> Self {
        let mut reader = BufferedReader::new(buf);
        // read header
        let block_timestamp = reader.read_u64().unwrap_or(0);
        let end_of_stream = sample_count == 0;
        DecompressorTSXor {
            reader,
            bytes: buf,
            byte_idx: 0,
            cache: CacheWindow::default(),
            stored_leading_zeros: vec![],
            stored_trailing_zeros: vec![],
            stored_val: 0.0,
            stored_timestamp: 0,
            stored_delta: 0,
            block_timestamp,
            end_of_stream,
            index: 0,
            sample_count,
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

        // first sample
        if self.index == 0 {
            self.index += 1;
            let sd = match self.reader.read_bits(FIRST_DELTA_BITS) {
                Ok(v) => v,
                Err(_) => return None,
            };
            self.stored_delta = sd as i64;
            let val_bits = match self.reader.read_u64() {
                Ok(v) => v,
                Err(_e) => {
                    // todo: proper logging
                    eprintln!("Error reading first value bits: {}", _e);
                    return None;
                }
            };
            self.cache.insert(val_bits);
            self.stored_val = f64::from_bits(val_bits);
            self.stored_timestamp = self.block_timestamp + sd;
            return Some((self.stored_timestamp, self.stored_val));
        }

        self.index += 1;
        if self.index > self.sample_count {
            // sanity check to prevent reading beyond expected sample count, in case of malformed input
            self.end_of_stream = true;
            return None;
        }

        // next timestamp
        let to_read = self.bits_to_read()?;

        let delta_delta = if to_read == 0 {
            0i64
        } else {
            let bits = match self.reader.read_bits(to_read) {
                Ok(v) => v,
                Err(e) => {
                    // TODO: proper logging
                    eprintln!("Error reading delta delta bits: {}", e);
                    return None;
                }
            };

            // End-of-stream marker written by CompressorTSXor::close().
            if to_read == 32 && bits == u32::MAX as u64 {
                self.end_of_stream = true;
                return None;
            }

            zigzag_decode(bits)
        };

        self.stored_delta += delta_delta;
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
            self.cache.get(head as usize)
        } else if head == 255 {
            // full value
            match self.reader.read_u64() {
                Ok(v) => v,
                Err(e) => {
                    // todo: proper logging
                    eprintln!("Error reading full value bits: {}", e);
                    return None;
                }
            }
        } else {
            // xor encoded
            let offset = (head & 0x7F) as usize;
            let info = match self.reader.read_byte() {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Error reading XOR info byte: {}", e);
                    return None;
                }
            };
            let trail_zeros = (info >> 4) as usize;
            let xor_bytes = (info & 0x0F) as usize;
            let mut xor_val: u64 = 0;
            for _ in 0..xor_bytes {
                match self.reader.read_byte() {
                    Ok(b) => xor_val = (xor_val << 8) | (b as u64),
                    Err(e) => {
                        eprintln!("Error reading XOR value byte: {}", e);
                        return None;
                    }
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
        let value = f64::from_bits(final_val);
        self.stored_val = value;

        Some((self.stored_timestamp, value))
    }
}