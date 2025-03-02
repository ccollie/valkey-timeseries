use super::traits::{BitRead, BitWrite};
use super::utils::{read_bits, read_bool};

/// Writes a f64 as a Prometheus varbit xor encoded number.
///
/// It also needs the previous value, the previous leading and trailing bits count.
///
/// The first time it is called, use 0xff (or 255) for the leading bits counts,
/// and 0 for the trailing bits count.
pub fn write_varbit_xor<W: BitWrite>(
    value: f64,
    previous_value: f64,
    previous_leading_bits_count: u8,
    previous_trailing_bits_count: u8,
    bit_writer: &mut W,
) -> std::io::Result<(u8, u8)> {
    let delta = value.to_bits() ^ previous_value.to_bits();

    if delta == 0 {
        bit_writer.write_bit(false)?;
        return Ok((previous_leading_bits_count, previous_trailing_bits_count));
    }
    bit_writer.write_bit(true)?;

    let mut new_leading = delta.leading_zeros() as u8;
    let new_trailing = delta.trailing_zeros() as u8;

    // Weird clamping that I don't reproduce, but it's there
    if new_leading >= 32 {
        new_leading = 31;
    }

    // If we reuse the previous leading and trailing bit counts
    if previous_leading_bits_count != 0xff
        && new_leading >= previous_leading_bits_count
        && new_trailing >= previous_trailing_bits_count
    {
        let significant_digits = 64 - previous_leading_bits_count - previous_trailing_bits_count;
        bit_writer.write_bit(false)?;
        bit_writer.write(
            significant_digits as u32,
            delta >> previous_trailing_bits_count,
        )?;
        return Ok((previous_leading_bits_count, previous_trailing_bits_count));
    }

    bit_writer.write_bit(true)?;
    bit_writer.write(5, new_leading)?;
    let sig_bits = (64_u64 - new_leading as u64) - new_trailing as u64;
    // Overflow 64 to 0 is fine because if 0 sig_bits, we would have written a "same number"
    // bit a bit earlier.
    // The reason is that only 6 bits are available, and the maximum value is 63.
    let encoded_sig_bits = if sig_bits > 63 { 0 } else { sig_bits };
    bit_writer.write(6, encoded_sig_bits)?;
    bit_writer.write(sig_bits as u32, delta >> new_trailing)?;

    Ok((new_leading, new_trailing))
}

#[inline]
fn read_leading_bits_count<R: BitRead>(reader: &mut R) -> std::io::Result<u8> {
    // The leading bits count is 5 bits long.
    Ok(read_bits(reader, 5)? as u8)
}

#[inline]
fn read_middle_bits_count<R: BitRead>(reader: &mut R) -> std::io::Result<u8> {
    // The middle bits count is 6 bits long.
    let middle_bits_count: u8 = read_bits(reader, 6)? as u8;

    // As prometheus uses 64 bits floats, the number of middle bits can be up to 64.
    // However, the max value on 6 bits is 63.
    // There, prometheus has a small trick: it overflows and 0 actually means 64.
    // It works because numbers with zero bits are not serialized through this.
    // Every saved bit counts!
    if middle_bits_count == 0 {
        return Ok(64);
    }

    Ok(middle_bits_count)
}

/// Reads a Prometheus varbit xor encoded number from the input.
///
/// The first time it is called, use 0 for both leading and trailing bits count.
///
/// It returns the new value, and also the new leading and trailing bits count.
pub fn read_varbit_xor<R: BitRead>(
    reader: &mut R,
    previous_value: f64,
    previous_leading_bits_count: u8,
    previous_trailing_bits_count: u8,
) -> std::io::Result<(f64, u8, u8)> {
    // Read the bit saying whether we use the previous value or not
    let different_value_bit = read_bool(reader)?;
    if !different_value_bit {
        return Ok((
            previous_value,
            previous_leading_bits_count,
            previous_trailing_bits_count,
        ));
    }

    let leading_bits_count: u8;
    let middle_bits_count: u8;
    let trailing_bits_count: u8;

    // Read the bit saying whether we reuse the previous leading and trailing bits count or not
    let different_leading_and_trailing_bits_count = read_bool(reader)?;
    if different_leading_and_trailing_bits_count {
        leading_bits_count = read_leading_bits_count(reader)?;
        middle_bits_count = read_middle_bits_count(reader)?;
        trailing_bits_count = 64 - leading_bits_count - middle_bits_count;
    } else {
        leading_bits_count = previous_leading_bits_count;
        trailing_bits_count = previous_trailing_bits_count;
        middle_bits_count = 64 - leading_bits_count - trailing_bits_count;
    }

    // Read the right number of bits
    let value_bits: u64 = read_bits(reader, middle_bits_count as u32)?;

    // Compute the new value
    let new_value = f64::from_bits(previous_value.to_bits() ^ (value_bits << trailing_bits_count));

    Ok((new_value, leading_bits_count, trailing_bits_count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::chunks::gorilla::buffered_read::BufferedReader;
    use crate::series::chunks::gorilla::buffered_writer::BufferedWriter;
    use rand::{Rng, SeedableRng};

    fn generate_random_test_data(seed: u64) -> Vec<Vec<f64>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let mut test_cases = Vec::with_capacity(128);
        for _ in 0..128 {
            let vec_size = rng.random_range(1..129);
            let mut vec = Vec::with_capacity(vec_size);

            let mut value: f64 = rng.random();
            vec.push(value);

            for _ in 1..vec_size {
                if rng.random_bool(0.33) {
                    value += 1.0;
                } else if rng.random_bool(0.33) {
                    value = rng.random();
                }
                vec.push(value);
            }
            test_cases.push(vec);
        }
        test_cases
    }

    #[test]
    fn test_write_varbit_xor() {
        let mut test_cases = generate_random_test_data(42);

        // add just a test case with the weird clamping
        test_cases.push(vec![f64::MAX, 0.0, f64::MIN, f64::MAX, f64::MIN]);

        for test_case in test_cases {
            // Writing first
            let mut bit_writer = BufferedWriter::new();

            let mut value = 0.0;
            let mut leading = 0xff;
            let mut trailing = 0;

            for number in test_case.iter().cloned() {
                let (new_leading, new_trailing) =
                    write_varbit_xor(number, value, leading, trailing, &mut bit_writer).unwrap();
                value = number;
                leading = new_leading;
                trailing = new_trailing;
            }

            bit_writer.byte_align().unwrap();

            // Read again
            value = 0.0;
            leading = 0;
            trailing = 0;

            let buffer = bit_writer.get_ref();
            let mut reader = BufferedReader::new(&buffer);

            for (i, number) in test_case.iter().enumerate() {
                let (new_value, new_leading, new_trailing) =
                    read_varbit_xor(&mut reader, value, leading, trailing).unwrap();

                assert_eq!(new_value, *number, "Failed at index {}", i);
                value = new_value;
                leading = new_leading;
                trailing = new_trailing;
            }
        }
    }
}
