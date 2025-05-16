/// Writes an unsigned varint to the buffer
pub(crate) fn write_uvarint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Writes a signed varint using zigzag encoding
pub(crate) fn write_signed_varint(buf: &mut Vec<u8>, value: i64) {
    // Use zigzag encoding for signed values
    let unsigned = zigzag_encode(value);
    write_uvarint(buf, unsigned);
}

/// Reads an unsigned varint from the buffer
/// Returns the value and the number of bytes consumed, or None if invalid
pub(crate) fn read_uvarint(buf: &[u8], start_offset: usize) -> Option<(u64, usize)> {
    if start_offset >= buf.len() {
        return None;
    }

    let mut value: u64 = 0;
    let mut shift = 0;
    let mut current_offset = start_offset;

    loop {
        if current_offset >= buf.len() {
            return None; // Unexpected end of buffer
        }

        let byte = buf[current_offset];
        current_offset += 1;

        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift > 63 {
            // Protect against malicious inputs
            return None;
        }
    }

    Some((value, current_offset - start_offset))
}

/// Reads a signed varint from the buffer using zigzag encoding
pub(crate) fn read_signed_varint(buf: &[u8], start_offset: usize) -> Option<(i64, usize)> {
    read_uvarint(buf, start_offset).map(|(unsigned, bytes_read)| {
        // Decode zigzag encoding
        let signed = zigzag_decode(unsigned);
        (signed, bytes_read)
    })
}

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
