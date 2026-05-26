use num_traits::PrimInt;
use std::io;

/// Read bits from an underlying byte stream.
pub trait BitRead {
    /// Read a single bit from the underlying stream.
    fn read_bit(&mut self) -> io::Result<bool>;

    /// Read a single byte from the underlying stream.
    fn read_byte(&mut self) -> io::Result<u8>;

    /// Read `num` bits from the underlying stream.
    fn read_bits(&mut self, num: u32) -> io::Result<u64>;
}

/// Write bits to an underlying byte stream.
pub trait BitWrite {
    /// Writes a single bit to the stream.
    fn write_bit(&mut self, bit: bool) -> io::Result<()>;

    /// Writes an unsigned value to the stream using the given number of bits.
    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: PrimInt;

    /// Writes an unsigned value to the stream using a const number of bits.
    fn write_out<const BITS: u32, U>(&mut self, value: U) -> io::Result<()>
    where
        U: PrimInt,
    {
        self.write(BITS, value)
    }

    /// Writes an unaligned byte to the stream.
    fn write_byte(&mut self, byte: u8);
}
