// MIT License
//
// Portions Copyright (c) 2016 Jerome Froelich
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use super::traits::{BitRead, Error};
use super::utils::{zigzag_decode, DROP_MSB, MSB};

/// BufferedReader
///
/// BufferedReader encapsulates a buffer of bytes which can be read from.
#[derive(Debug)]
pub struct BufferedReader<'a> {
    bytes: &'a [u8], // internal buffer of bytes
    index: usize,    // index into bytes
    pos: u32,        // position in the byte we are currently reading
}

impl<'a> BufferedReader<'a> {
    /// new creates a new `BufferedReader` from `bytes`
    pub fn new(bytes: &'a [u8]) -> Self {
        BufferedReader {
            bytes,
            index: 0,
            pos: 0,
        }
    }

    pub fn read_uvarint(&mut self) -> Result<u64, Error> {
        let mut result: u64 = 0;
        let mut shift = 0;

        #[allow(unused_assignments)]
        let mut success = false;
        loop {
            let b = self.read_byte()?;
            let msb_dropped = b & DROP_MSB;
            result |= (msb_dropped as u64) << shift;
            shift += 7;

            if b & MSB == 0 || shift > (9 * 7) {
                success = b & MSB == 0;
                break;
            }
        }

        if success {
            Ok(result)
        } else {
            Err(Error::Overflow)
        }
    }

    pub fn read_varint(&mut self) -> Result<i64, Error> {
        let uvarint_value = self.read_uvarint()?;
        Ok(zigzag_decode(uvarint_value))
    }

    pub fn read_u64(&mut self) -> Result<u64, Error> {
        let mut bytes = [0; 8];
        for byte in &mut bytes {
            *byte = self.read_byte()?;
        }
        Ok(u64::from_be_bytes(bytes))
    }

    pub fn read_f64(&mut self) -> Result<f64, Error> {
        let bits = self.read_u64()?;
        Ok(f64::from_bits(bits))
    }

    fn get_byte(&mut self) -> Result<u8, Error> {
        self.bytes.get(self.index).cloned().ok_or(Error::Eof)
    }
}

impl BitRead for BufferedReader<'_> {
    fn read_bit(&mut self) -> Result<bool, Error> {
        if self.pos == 8 {
            self.index += 1;
            self.pos = 0;
        }

        let byte = self.get_byte()?;
        let bit = (byte & (1 << (7 - self.pos))) != 0;

        self.pos += 1;
        Ok(bit)
    }

    fn read_byte(&mut self) -> Result<u8, Error> {
        if self.pos == 0 {
            self.pos += 8;
            return self.get_byte();
        }

        if self.pos == 8 {
            self.index += 1;
            return self.get_byte();
        }

        let mut byte = 0;
        let mut b = self.get_byte()?;

        byte |= b.wrapping_shl(self.pos);

        self.index += 1;
        b = self.get_byte()?;

        byte |= b.wrapping_shr(8 - self.pos);

        Ok(byte)
    }

    fn read_bits(&mut self, mut num: u32) -> Result<u64, Error> {
        // can't read more than 64 bits into a u64
        if num > 64 {
            num = 64;
        }

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.read_byte().map(u64::from)?;
            bits = bits.wrapping_shl(8) | byte;
            num -= 8;
        }

        while num > 0 {
            self.read_bit()
                .map(|bit| bits = bits.wrapping_shl(1) | u64::from(bit))?;

            num -= 1;
        }

        Ok(bits)
    }
}

#[cfg(test)]
mod tests {
    use super::BufferedReader;
    use crate::series::chunks::gorilla::traits::{BitRead, Error};

    #[test]
    fn read_bit() {
        let bytes = vec![0b01101100, 0b11101001];
        let mut b = BufferedReader::new(&bytes);

        assert!(!b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(!b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(!b.read_bit().unwrap());
        assert!(!b.read_bit().unwrap());

        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(!b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(!b.read_bit().unwrap());
        assert!(!b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());

        assert_eq!(b.read_bit().err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_byte() {
        let bytes = vec![100, 25, 0, 240, 240];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.read_byte().unwrap(), 100);
        assert_eq!(b.read_byte().unwrap(), 25);
        assert_eq!(b.read_byte().unwrap(), 0);

        // read some individual bits we can test `read_byte` when the position in the
        // byte we are currently reading is non-zero
        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());
        assert!(b.read_bit().unwrap());

        assert_eq!(b.read_byte().unwrap(), 15);

        assert_eq!(b.read_byte().err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_bits() {
        let bytes = vec![0b01010111, 0b00011101, 0b11110101, 0b00010100];
        let mut b = BufferedReader::new(&bytes);

        assert_eq!(b.read_bits(3).unwrap(), 0b010);
        assert_eq!(b.read_bits(1).unwrap(), 0b1);
        assert_eq!(b.read_bits(20).unwrap(), 0b01110001110111110101);
        assert_eq!(b.read_bits(8).unwrap(), 0b00010100);
        assert_eq!(b.read_bits(4).err().unwrap(), Error::Eof);
    }

    #[test]
    fn read_mixed() {
        let bytes = vec![0b01101101, 0b01101101];
        let mut b = BufferedReader::new(&bytes);

        assert!(!b.read_bit().unwrap());
        assert_eq!(b.read_bits(3).unwrap(), 0b110);
        assert_eq!(b.read_byte().unwrap(), 0b11010110);
        assert_eq!(b.read_bits(2).unwrap(), 0b11);
        assert!(!b.read_bit().unwrap());
        assert_eq!(b.read_bits(1).unwrap(), 0b1);
        assert_eq!(b.read_bit().err().unwrap(), Error::Eof);
    }
}
