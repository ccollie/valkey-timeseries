use num_traits::PrimInt;
use std::fmt::Display;
use std::{error, fmt, io};

/// Error
///
/// Enum used to represent potential errors when interacting with a stream.
#[derive(Debug, PartialEq)]
pub enum Error {
    Eof,
    Overflow,
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Eof => write!(f, "Encountered the end of the stream"),
            Error::Overflow => write!(f, "Numeric overflow reading stream"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Eof => "Encountered the end of the stream",
            Error::Overflow => "Numeric overflow reading stream",
        }
    }
}

/// Read
///
/// Read is a trait that encapsulates the functionality required to read from a stream of bytes.
pub trait BitRead {
    /// Read a single bit from the underlying stream.
    fn read_bit(&mut self) -> Result<bool, Error>;

    /// Read a single byte from the underlying stream.
    fn read_byte(&mut self) -> Result<u8, Error>;

    /// Read `num` bits from the underlying stream.
    fn read_bits(&mut self, num: u32) -> Result<u64, Error>;
}

/// A trait for anything that can write a variable number of
/// potentially un-aligned values to an output stream
pub trait BitWrite {
    /// Writes a single bit to the stream.
    /// `true` indicates 1, `false` indicates 0
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    fn write_bit(&mut self, bit: bool) -> io::Result<()>;

    /// Writes an unsigned value to the stream using the given
    /// number of bits.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    /// Returns an error if the input type is too small
    /// to hold the given number of bits.
    /// Returns an error if the value is too large
    /// to fit the given number of bits.
    fn write<U>(&mut self, bits: u32, value: U) -> io::Result<()>
    where
        U: PrimInt;

    /// Writes an unsigned value to the stream using the given
    /// const number of bits.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    /// Returns an error if the value is too large
    /// to fit the given number of bits.
    /// A compile-time error occurs if the given number of bits
    /// is larger than the output type.
    fn write_out<const BITS: u32, U>(&mut self, value: U) -> io::Result<()>
    where
        U: PrimInt,
    {
        self.write(BITS, value)
    }

    fn write_byte(&mut self, byte: u8);

    /// Returns true if the stream is aligned at a whole byte.
    fn byte_aligned(&self) -> bool;

    /// Pads the stream with 0 bits until it is aligned at a whole byte.
    /// Does nothing if the stream is already aligned.
    ///
    /// # Errors
    ///
    /// Passes along any I/O error from the underlying stream.
    fn byte_align(&mut self) -> io::Result<()>;
}
