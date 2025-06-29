use super::response_generated::Label as ResponseLabel;
use crate::common::encoding::{
    read_signed_varint, read_uvarint, write_signed_varint, write_uvarint,
};
use crate::fanout::request::request_generated::{DateRange, DateRangeArgs};
use crate::fanout::CommandMessageType;
use crate::labels::Label;
use crate::series::TimestampRange;
use flatbuffers::{FlatBufferBuilder, Verifiable, WIPOffset};
use valkey_module::{ValkeyError, ValkeyResult};

pub struct MessageHeader {
    pub request_id: u64,
    pub db: i32,
    pub msg_type: CommandMessageType,
    pub reserved: u8, // Reserved for future use (e.g., for larger payloads, we may compress the data)
}

impl MessageHeader {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        // Encode request_id as uvarint
        write_uvarint(buf, self.request_id);

        // Encode db as signed varint
        write_signed_varint(buf, self.db as i64);

        buf.push(self.msg_type as u8);
        buf.push(self.reserved);
    }

    /// Returns the minimum size of the serialized header in bytes.
    pub const fn min_serialized_size() -> usize {
        1 + // request_id (minimum 1 byte with uvarint)
            1 + // db (minimum 1 byte with signed varint)
            1 + // msg_type (1 byte)
            1 // reserved (1 byte)
    }

    /// Deserializes a MessageHeader from the beginning of the buffer.
    /// Returns the deserialized header and the number of bytes consumed.
    /// Returns None if the buffer is too small.
    pub fn deserialize(buf: &[u8]) -> Option<(Self, usize)> {
        let mut current_offset = 0;

        if buf.len() < Self::min_serialized_size() {
            return None;
        }

        // Decode request_id as uvarint
        let (request_id, bytes_read) = read_uvarint(buf, current_offset)?;
        current_offset += bytes_read;

        let (db_i64, bytes_read) = read_signed_varint(buf, current_offset)?;
        let db = db_i64 as i32;
        current_offset += bytes_read;

        // Need at least 2 more bytes for msg_type and reserved
        if current_offset + 1 >= buf.len() {
            return None;
        }

        // Read msg_type and reserved as direct bytes
        let msg_type_byte = buf[current_offset];
        current_offset += 1;
        let reserved = buf[current_offset];
        current_offset += 1;

        let msg_type = CommandMessageType::from(msg_type_byte);

        Some((
            MessageHeader {
                request_id,
                db,
                msg_type,
                reserved,
            },
            current_offset,
        ))
    }
}

pub(super) fn load_flatbuffers_object<'buf, T>(
    buf: &'buf [u8],
    name: &'static str,
) -> ValkeyResult<T::Inner>
where
    T: flatbuffers::Follow<'buf> + Verifiable + 'buf,
{
    flatbuffers::root::<T>(buf).map_err(|_e| {
        let msg = format!("TSDB: failed to deserialize {name}");
        ValkeyError::String(msg)
    })
}

pub fn serialize_timestamp_range<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    range: Option<TimestampRange>,
) -> Option<WIPOffset<DateRange<'a>>> {
    if let Some(range) = range {
        let (start, end) = range.get_timestamps(None);
        return Some(DateRange::create(bldr, &DateRangeArgs { start, end }));
    }
    None
}

pub fn deserialize_timestamp_range(
    range: Option<DateRange>,
) -> ValkeyResult<Option<TimestampRange>> {
    if let Some(range) = range {
        let start = range.start();
        let end = range.end();
        match TimestampRange::from_timestamps(start, end) {
            Ok(range) => Ok(Some(range)),
            Err(e) => Err(ValkeyError::String(format!(
                "TSDB: failed to deserialize date range: {e}",
            ))),
        }
    } else {
        Ok(None)
    }
}

pub(super) fn decode_label(label: ResponseLabel) -> Label {
    let name = label.name().unwrap_or_default().to_string();
    let value = label.value().unwrap_or_default().to_string();
    Label { name, value }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_header_round_trip() {
        let original_header = MessageHeader {
            request_id: 9876543210123456789,
            db: 15,
            msg_type: CommandMessageType::MultiRangeQuery,
            reserved: 0xBA,
        };

        let mut buffer = Vec::new();
        original_header.serialize(&mut buffer);

        match MessageHeader::deserialize(&buffer) {
            Some((deserialized_header, consumed)) => {
                assert!(
                    consumed >= MessageHeader::min_serialized_size(),
                    "Consumed size mismatch in round trip"
                );
                assert_eq!(
                    deserialized_header.request_id, original_header.request_id,
                    "request_id mismatch in round trip"
                );
                assert_eq!(
                    deserialized_header.db, original_header.db,
                    "db mismatch in round trip"
                );
                assert_eq!(
                    deserialized_header.msg_type, original_header.msg_type,
                    "msg_type mismatch in round trip"
                );
                assert_eq!(
                    deserialized_header.reserved, original_header.reserved,
                    "reserved mismatch in round trip"
                );
            }
            None => {
                panic!("Deserialization failed during round trip test");
            }
        }
    }

    #[test]
    fn test_message_header_deserialize_buffer_too_small() {
        let size = MessageHeader::min_serialized_size();
        // Create buffers slightly smaller than required
        for i in 0..size {
            let buffer: Vec<u8> = vec![0; i]; // Create a buffer of size i
            assert!(
                MessageHeader::deserialize(&buffer).is_none(),
                "Deserialization should fail for buffer size {i}"
            );
        }
    }

    #[test]
    fn test_message_header_deserialize_empty_buffer() {
        let buffer: Vec<u8> = vec![];
        assert!(
            MessageHeader::deserialize(&buffer).is_none(),
            "Deserialization should fail for empty buffer"
        );
    }
}
