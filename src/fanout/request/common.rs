use super::response_generated::{
    ErrorResponse as FBErrorResponse, ErrorResponseArgs, Label as ResponseLabel,
};
use crate::common::encoding::{
    read_signed_varint, read_uvarint, write_signed_varint, write_uvarint,
};
use crate::fanout::request::request_generated::{DateRange, DateRangeArgs};
use crate::fanout::ClusterMessageType;
use crate::labels::Label;
use crate::series::TimestampRange;
use flatbuffers::{FlatBufferBuilder, Verifiable, WIPOffset};
use valkey_module::{ValkeyError, ValkeyResult};

pub struct MessageHeader {
    pub request_id: u64,
    pub db: i32,
    pub msg_type: ClusterMessageType,
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
            return None; // Buffer too small
        }

        // Decode request_id as uvarint
        let (request_id, bytes_read) = read_uvarint(buf, current_offset)?;
        current_offset += bytes_read;

        // Decode db as signed varint
        let (db_i64, bytes_read) = read_signed_varint(buf, current_offset)?;
        let db = db_i64 as i32; // Convert i64 to i32
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

        let msg_type = ClusterMessageType::from(msg_type_byte);

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

#[derive(Clone)]
pub struct ErrorResponse {
    pub error_message: String,
}

pub(super) fn load_flatbuffers_object<'buf, T>(
    buf: &'buf [u8],
    name: &'static str,
) -> ValkeyResult<T::Inner>
where
    T: flatbuffers::Follow<'buf> + Verifiable + 'buf,
{
    flatbuffers::root::<T>(buf).map_err(|_e| {
        let msg = format!("TSDB: failed to deserialize {}", name);
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
                "TSDB: failed to deserialize date range: {}",
                e
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

pub fn serialize_error_response(buf: &mut Vec<u8>, error: &str) {
    let mut bldr = FlatBufferBuilder::with_capacity(512); // todo: store this in a pool
    let error_message = bldr.create_string(error);

    let obj = FBErrorResponse::create(
        &mut bldr,
        &ErrorResponseArgs {
            error_code: 0,
            error_message: Some(error_message),
        },
    );
    bldr.finish(obj, None);

    let data = bldr.finished_data();
    buf.extend_from_slice(data)
}

pub fn deserialize_error_response(buf: &[u8]) -> ValkeyResult<ErrorResponse> {
    // Get access to the root:
    let req = flatbuffers::root::<FBErrorResponse>(buf)
        .map_err(|_e| ValkeyError::Str("TSDB: failed to deserialize error response"))?;
    let error_message = req.error_message().unwrap_or_default().to_string();
    Ok(ErrorResponse { error_message })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_header_round_trip() {
        let original_header = MessageHeader {
            request_id: 9876543210123456789,
            db: 15,
            msg_type: ClusterMessageType::MultiRangeQuery,
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
                "Deserialization should fail for buffer size {}",
                i
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

    #[test]
    fn test_serialize_error_response_basic() {
        let error_message = "Something went wrong!";
        let mut buf = Vec::new();

        serialize_error_response(&mut buf, error_message);

        // Basic check: Buffer should not be empty after serialization
        assert!(!buf.is_empty(), "Serialization produced an empty buffer");

        // Deserialize to verify content
        let result = deserialize_error_response(&buf);
        assert!(
            result.is_ok(),
            "Deserialization failed after basic serialization: {:?}",
            result.err()
        );

        let deserialized = result.unwrap();
        assert_eq!(
            deserialized.error_message, error_message,
            "Error message mismatch after serialization"
        );
    }

    #[test]
    fn test_serialize_error_response_empty_message() {
        let error_message = ""; // Empty message
        let mut buf = Vec::new();

        serialize_error_response(&mut buf, error_message);

        // Basic check: Buffer should not be empty even with an empty message
        assert!(
            !buf.is_empty(),
            "Serialization produced an empty buffer for empty message"
        );

        // Deserialize to verify content
        let result = deserialize_error_response(&buf);
        assert!(
            result.is_ok(),
            "Deserialization failed for empty message: {:?}",
            result.err()
        );

        let deserialized = result.unwrap();
        assert_eq!(
            deserialized.error_message, error_message,
            "Error message should be empty"
        );
    }

    #[test]
    fn test_error_response_round_trip() {
        let original_response = ErrorResponse {
            error_message: "A detailed error message with Unicode 😊✅".to_string(),
        };

        let mut buf = Vec::new();
        serialize_error_response(&mut buf, &original_response.error_message);

        let result = deserialize_error_response(&buf);

        assert!(
            result.is_ok(),
            "Round trip deserialization failed: {:?}",
            result.err()
        );
        let deserialized_response = result.unwrap();

        // Compare the structs (requires PartialEq derived on ErrorResponse)
        assert_eq!(
            deserialized_response.error_message,
            original_response.error_message
        );
    }

    #[test]
    fn test_deserialize_error_invalid_buffer() {
        // Create some random bytes that are unlikely to be a valid buffer
        let invalid_buf: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];

        let result = deserialize_error_response(invalid_buf);

        assert!(
            result.is_err(),
            "Deserialization should fail for invalid buffer"
        );

        // Check for the specific flatbuffers error mapped to ValkeyError::Str
        match result.err().unwrap() {
            ValkeyError::Str(msg) => {
                assert!(
                    msg.contains("failed to deserialize error response"),
                    "Unexpected error message: {}",
                    msg
                );
            }
            // Handle other potential ValkeyError variants if necessary, though Str is expected here
            _ => panic!("Expected ValkeyError::Str for invalid buffer"),
        }
    }

    #[test]
    fn test_deserialize_error_empty_buffer() {
        let empty_buf: &[u8] = &[];

        let result = deserialize_error_response(empty_buf);

        assert!(
            result.is_err(),
            "Deserialization should fail for empty buffer"
        );

        // Check for the specific flatbuffers error mapped to ValkeyError::Str
        match result.err().unwrap() {
            ValkeyError::Str(msg) => {
                assert!(
                    msg.contains("failed to deserialize error response"),
                    "Unexpected error message for empty buffer: {}",
                    msg
                );
            }
            _ => panic!("Expected ValkeyError::Str for empty buffer"),
        }
    }
}
