//! Prometheus' varbit encoding.
//!

use super::traits::{BitRead, BitWrite};
use super::utils::{read_bits, read_bool, sign_extend};

/// writes an i64 using varbit encoding with a bit bucketing
/// optimized for the dod's observed in histogram buckets, plus a few additional
/// buckets for large numbers.
///
/// For optimal space utilization, each branch didn't need to support any values
/// of the prior branches, so we could expand the range of each branch. Do
/// more with fewer bits. It would come at the price of more expensive encoding
/// and decoding (cutting out and later adding back that center-piece we
/// skip). With the distributions of values we see in practice, we would reduce
/// the size by around 1%. A more detailed study would be needed for precise
/// values, but it's appears quite certain that we would end up far below 10%,
/// which would maybe convince us to invest the increased coding/decoding cost.
pub(crate) fn write_varbit<W: BitWrite>(value: i64, writer: &mut W) -> std::io::Result<()> {
    match value {
        0 => writer.write_bit(false)?, // Precisely 0, needs 1 bit.
        // -3 <= val <= 4, needs 5 bits.
        -3..=3 => {
            writer.write_out::<2, u8>(0b10)?;
            writer.write_out::<5, u64>(value as u64 & 0x1F)?;
        }
        // -31 <= val <= 32, 9 bits.
        -31..=31 => {
            writer.write_out::<3, u8>(0b110)?;
            writer.write_out::<9, u64>(value as u64 & 0x1FF)?;
        }
        // -255 <= val <= 256, 13 bits.
        -255..=255 => {
            writer.write_out::<4, u8>(0b1110)?;
            writer.write_out::<13, u64>(value as u64 & 0x1FFF)?;
        }
        // -2047 <= val <= 2048, 17 bits.
        -2047..=2047 => {
            writer.write_out::<5, u8>(0b11110)?;
            writer.write_out::<17, u64>(value as u64 & 0x1FFFF)?;
        }
        // -131071 <= val <= 131072, 3 bytes.
        -131071..=131071 => {
            writer.write_out::<6, u8>(0b111110)?;
            writer.write_out::<24, u64>(value as u64 & 0x0FFFFFF)?;
        }
        // -16777215 <= val <= 16777216, 4 bytes.
        -16777215..=167772165 => {
            writer.write_out::<7, u8>(0b1111110)?;
            writer.write_out::<32, u64>(value as u64 & 0x0FFFFFFFF)?;
        }
        // -36028797018963967 <= val <= 36028797018963968, 8 bytes.
        -36028797018963967..=36028797018963967 => {
            writer.write_out::<8, u8>(0b11111110)?;
            writer.write_out::<56, u64>(value as u64 & 0xFFFFFFFFFFFFFF)?;
        }
        _ => {
            writer.write_out::<8, u8>(0b11111111)?; // Worst case, needs 9 bytes.
            writer.write_out::<64, u64>(value as u64)?; // ??? test this !!!
        }
    }
    Ok(())
}

/// Reads a varbit-encoded integer from the input.
///
/// Prometheus' varbitint starts with a bucket category of variable length.
/// It consists of '1' bits and a final 0, up to 8 bits.
/// When it's 8 bits long, the final 0 is skipped.
///
/// It consists of 9 categories.
fn read_varbit_int_bucket<R: BitRead>(reader: &mut R) -> std::io::Result<u8> {
    for i in 0..8 {
        let bit = read_bool(reader)?;
        // If we read a 0, it's a sign that we reached the end of the bucket category.
        if !bit {
            return Ok(i);
        }
    }

    // If we read 8 bits already, there is no final 0.
    Ok(8)
}

#[inline]
fn varbit_bucket_to_num_bits(bucket: u8) -> u8 {
    match bucket {
        0 => 0,
        1 => 5,
        2 => 9,
        3 => 13,
        4 => 17,
        5 => 24,
        6 => 32,
        7 => 56,
        8 => 64,
        _ => unreachable!("gorilla decoding: Invalid varbit bucket value"),
    }
}

/// Reads a Prometheus varbit-encoded integer from the input.
pub(crate) fn read_varbit_int<R: BitRead>(reader: &mut R) -> std::io::Result<i64> {
    let bucket = read_varbit_int_bucket(reader)?;
    let num_bits = varbit_bucket_to_num_bits(bucket);

    // Shortcut for the 0 use case as nothing more has to be read.
    if bucket == 0 {
        return Ok(0);
    }

    let value = read_bits(reader, num_bits as u32)?;
    if num_bits != 64 && value > (1 << (num_bits - 1)) {
        //value -= 1 << num_bits;
        return Ok(sign_extend(value, num_bits as u32));
    }

    Ok(value as i64)
}

#[cfg(test)]
mod tests {
    use super::{read_varbit_int, write_varbit};
    use crate::series::chunks::gorilla::buffered_read::BufferedReader;
    use crate::series::chunks::gorilla::buffered_writer::BufferedWriter;

    const NUMBERS: [i64; 33] = [
        i64::MIN,
        -36028797018963968,
        -36028797018963967,
        -16777216,
        -16777215,
        -131072,
        -131071,
        -2048,
        -2047,
        -256,
        -255,
        -32,
        -31,
        -4,
        -3,
        -1,
        0,
        1,
        4,
        5,
        32,
        33,
        256,
        257,
        2048,
        2049,
        131072,
        131073,
        16777216,
        16777217,
        36028797018963968,
        36028797018963969,
        i64::MAX,
    ];

    #[test]
    fn test_write_varbit() {
        let mut bit_writer = BufferedWriter::new();

        for number in NUMBERS.iter().cloned() {
            write_varbit(number, &mut bit_writer).unwrap();
        }

        let binding = bit_writer.get_ref();
        let mut reader = BufferedReader::new(binding);
        // Read again
        for want in NUMBERS {
            let got = read_varbit_int(&mut reader).unwrap();
            assert_eq!(want, got)
        }
    }
}
