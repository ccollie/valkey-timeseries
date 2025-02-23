use super::traits::BitRead;
use std::io::{Error, ErrorKind, Result};

const BYTE_WIDTH: usize = size_of::<u64>();
const BIT_WIDTH: usize = BYTE_WIDTH * 8;

/// All bits except for the most significant. Can be used as bitmask to drop the most-significant
/// bit using `&` (binary-and).
pub const DROP_MSB: u8 = 0b0111_1111;
pub const MSB: u8 = 0x080;

// see: http://stackoverflow.com/a/2211086/56332
// casting required because operations like unary negation
// cannot be performed on unsigned integers
#[inline]
pub(crate) fn zigzag_decode(from: u64) -> i64 {
    ((from >> 1) ^ (-((from & 1) as i64)) as u64) as i64
}

#[inline]
pub(crate) fn zigzag_encode(from: i64) -> u64 {
    ((from << 1) ^ (from >> 63)) as u64
}

// from bitter crate
#[inline]
pub(crate) fn sign_extend(val: u64, bits: u32) -> i64 {
    // Branchless sign extension from bit twiddling hacks:
    // https://graphics.stanford.edu/~seander/bithacks.html#VariableSignExtend
    //
    // The 3 operation approach with division turned out to be significantly slower,
    // and so was not used.
    debug_assert!(val.leading_zeros() as usize >= (BIT_WIDTH - bits as usize));
    let m = 1i64.wrapping_shl(bits.wrapping_sub(1));
    #[allow(clippy::cast_possible_wrap)]
    let val = val as i64;
    (val ^ m) - m
}

#[inline]
pub(crate) fn read_bool<R: BitRead>(reader: &mut R) -> Result<bool> {
    match reader.read_bit() {
        Ok(v) => Ok(v),
        Err(_) => Err(unexpected_eof()),
    }
}

pub(crate) fn read_bits<R: BitRead>(reader: &mut R, bits: u32) -> Result<u64> {
    match reader.read_bits(bits) {
        Ok(v) => Ok(v),
        Err(_) => Err(unexpected_eof()),
    }
}

fn unexpected_eof() -> Error {
    Error::new(ErrorKind::UnexpectedEof, "unexpected end of stream")
}
