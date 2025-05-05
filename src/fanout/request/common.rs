use std::io::{Write, Read};
use super::response_generated::{
    ErrorResponse as FBErrorResponse, 
    ErrorResponseArgs, 
    Label as ResponseLabel, 
    LabelBuilder,
    Sample as ResponseSample,
};
use crate::common::{Sample, Serialized};
use crate::fanout::request::request_generated::{
    DateRange, 
    DateRangeArgs,
    MetadataRequest,
    MetadataRequestArgs,
};
use crate::fanout::types::ClusterMessageType;
use crate::labels::Label;
use crate::series::TimestampRange;
use flatbuffers::{FlatBufferBuilder, Vector, WIPOffset};
use valkey_module::{ValkeyError, ValkeyResult};
use crate::fanout::request::matchers::{deserialize_matchers, serialize_matchers};
use crate::series::request_types::MatchFilterOptions;

impl Serialized<MatchFilterOptions> for MatchFilterOptions {
    fn serialize<W: Write>(&self, buf: &mut W) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);

        let range = serialize_timestamp_range(&mut bldr, self.date_range);
        let matchers = self.matchers.iter()
            .map(|m| serialize_matchers(&mut bldr, m))
            .collect::<Vec<_>>();
        
        let filters = bldr.create_vector(&matchers);
        let args = MetadataRequestArgs {
            range,
            filters: Some(filters),
            limit: self.limit.unwrap_or_default() as u32,
        };

        let obj = MetadataRequest::create(&mut bldr, &args);
        
        bldr.finish(obj, None);
        let data = bldr.finished_data();
        buf.write_all(&data).unwrap();
    }

    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = flatbuffers::root::<MetadataRequest>(buf).unwrap();
        let range = deserialize_timestamp_range(req.range())?;
        let limit = if req.limit() > 0 {
            Some(req.limit() as usize)
        } else {
            None
        };
        let mut matchers = Vec::with_capacity(req.filters().unwrap_or_default().len());
        for filter in req.filters().unwrap_or_default() {
            let matcher = deserialize_matchers(&filter)?;
            matchers.push(matcher);
        }
        Ok(MatchFilterOptions { 
            date_range: range,
            matchers,
            limit,
        })
    }
}

pub struct MessageHeader {
    pub request_id: u64,
    pub db: i32,
    pub msg_type: ClusterMessageType,
}

impl MessageHeader {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        // Write request_id (u64) as little-endian
        buf.extend_from_slice(&self.request_id.to_le_bytes());

        // Write db (i32) as little-endian
        buf.extend_from_slice(&self.db.to_le_bytes());

        // Write msg_type (assuming it converts to u8)
        // If ClusterMessageType is #[repr(u8)], you can cast:
        // buf.push(self.msg_type as u8);
        // If it implements Into<u8>:
        buf.push(self.msg_type as u8);
    }

    /// Returns the size of the serialized header in bytes.
    pub const fn serialized_size() -> usize {
        size_of::<u64>() // request_id
            + size_of::<i32>() // db
            + size_of::<u8>() // msg_type (assuming u8 representation)
    }

    /// Deserializes a MessageHeader from the beginning of the buffer.
    /// Returns the deserialized header and the number of bytes consumed.
    /// Returns None if the buffer is too small.
    pub fn deserialize(buf: &[u8]) -> Option<(Self, usize)> {
        const U64_SIZE: usize = size_of::<u64>();
        const I32_SIZE: usize = size_of::<i32>();
        const U8_SIZE: usize = size_of::<u8>(); // For msg_type

        let required_size = U64_SIZE + I32_SIZE + U8_SIZE;
        if buf.len() < required_size {
            return None;
        }

        let mut current_offset = 0;

        // Read request_id (u64) as little-endian
        let request_id_bytes: [u8; U64_SIZE] = buf[current_offset..current_offset + U64_SIZE]
            .try_into()
            .ok()?; // Error if slicing fails
        let request_id = u64::from_le_bytes(request_id_bytes);
        current_offset += U64_SIZE;

        // Read db (i32) as little-endian
        let db_bytes: [u8; I32_SIZE] = buf[current_offset..current_offset + I32_SIZE]
            .try_into()
            .ok()?; // Error if slicing fails
        let db = i32::from_le_bytes(db_bytes);
        current_offset += I32_SIZE;

        // Read msg_type (u8)
        let msg_type_byte = buf[current_offset];

        // Convert u8 back to ClusterMessageType
        // This depends on how ClusterMessageType is defined (e.g., From<u8>)
        let msg_type = ClusterMessageType::from(msg_type_byte); // Assuming From<u8> is implemented

        Some((
            MessageHeader {
                request_id,
                db,
                msg_type,
            },
            required_size,
        ))
    }
}

#[derive(Clone)]
pub struct ErrorResponse {
    pub error_message: String,
}

#[inline]
pub(super) fn decode_sample(reader: ResponseSample) -> Sample {
    let timestamp = reader.timestamp();
    let value = reader.value();
    Sample { timestamp, value }
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

pub fn deserialize_timestamp_range(range: Option<DateRange>) -> ValkeyResult<Option<TimestampRange>> {
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
pub(super) fn serialize_label<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    label: &Label,
) -> WIPOffset<ResponseLabel<'a>> {
    let name = bldr.create_string(label.name.as_str());
    let value = bldr.create_string(label.value.as_str());
    let mut builder = LabelBuilder::new(bldr);
    builder.add_name(name);
    builder.add_value(value);
    builder.finish()
}

pub(super) fn decode_label(label: ResponseLabel) -> Label {
    let name = label.name().unwrap_or_default().to_string();
    let value = label.value().unwrap_or_default().to_string();
    Label { name, value }
}

#[inline]
pub(super) fn serialize_sample_vec<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    samples: &[Sample],
) -> WIPOffset<Vector<'a, ResponseSample>> {
    bldr.create_vector_from_iter(
        samples
            .iter()
            .map(|s| ResponseSample::new(s.timestamp, s.value)),
    )
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
    use crate::fanout::types::ClusterMessageType;

    #[test]
    fn test_message_header_serialized_size() {
        let expected_size = size_of::<u64>() + size_of::<i32>() + size_of::<u8>();
        assert_eq!(
            MessageHeader::serialized_size(),
            expected_size,
            "Serialized size calculation mismatch"
        );
    }

    #[test]
    fn test_message_header_serialize() {
        // Byte representation for db = 16384
        let buffer: Vec<u8> = vec![
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // request_id = 0x0102030405060708
            0x00, 0x40, 0x00, 0x00, // db = 16384
            0x01, // msg_type = Request
            0xAA, 0xBB, // Extra bytes
        ];

        let expected_header = MessageHeader {
            request_id: 0x0102030405060708,
            db: 16384,
            msg_type: ClusterMessageType::RangeQuery,
        };
        let expected_consumed = MessageHeader::serialized_size();

        match MessageHeader::deserialize(&buffer) {
            Some((deserialized_header, consumed)) => {
                assert!(
                    deserialized_header.db > 0,
                    "Deserialized db should be positive"
                );
                assert!(
                    deserialized_header.db <= 16384,
                    "Deserialized db should be <= 16384"
                );
                assert_eq!(deserialized_header.request_id, expected_header.request_id);
                assert_eq!(deserialized_header.db, expected_header.db);
                assert_eq!(deserialized_header.msg_type, expected_header.msg_type);
                assert_eq!(consumed, expected_consumed, "Bytes consumed mismatch");
            }
            None => {
                panic!("Deserialization failed for a valid buffer with db=16384");
            }
        }
    }

    #[test]
    fn test_message_header_deserialize_exact_size() {
        // Buffer with exactly the required size
        let buffer: Vec<u8> = vec![
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // request_id
            0xF6, 0xFF, 0xFF, 0xFF, // db
            0x01, // msg_type
        ];

        let expected_header = MessageHeader {
            request_id: 0x0102030405060708,
            db: -10,
            msg_type: ClusterMessageType::RangeQuery, // Represents 1u8
        };
        let expected_consumed = MessageHeader::serialized_size();

        match MessageHeader::deserialize(&buffer) {
            Some((deserialized_header, consumed)) => {
                assert_eq!(deserialized_header.request_id, expected_header.request_id);
                assert_eq!(deserialized_header.db, expected_header.db);
                assert_eq!(deserialized_header.msg_type, expected_header.msg_type);
                assert_eq!(
                    consumed, expected_consumed,
                    "Bytes consumed mismatch for exact size buffer"
                );
            }
            None => {
                panic!("Deserialization failed for exact size valid buffer");
            }
        }
    }

    #[test]
    fn test_message_header_round_trip() {
        let original_header = MessageHeader {
            request_id: 9876543210123456789,
            db: 15,
            msg_type: ClusterMessageType::MultiRangeQuery,
        };

        let mut buffer = Vec::new();
        original_header.serialize(&mut buffer);

        match MessageHeader::deserialize(&buffer) {
            Some((deserialized_header, consumed)) => {
                assert_eq!(
                    consumed,
                    MessageHeader::serialized_size(),
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
            }
            None => {
                panic!("Deserialization failed during round trip test");
            }
        }
    }

    #[test]
    fn test_message_header_deserialize_buffer_too_small() {
        let size = MessageHeader::serialized_size();
        // Create buffers slightly smaller than required
        for i in 0..size {
            let buffer: Vec<u8> = vec![0; i]; // Create buffer of size i
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

        // Basic check: Buffer should not be empty even with empty message
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

        let buf = Vec::new();

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
        // Create some random bytes that are unlikely to be a valid flatbuffer
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
            _ => panic!("Expected ValkeyError::Str for invalid flatbuffer"),
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
