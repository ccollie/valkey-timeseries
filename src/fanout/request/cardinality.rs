use super::response_generated::{
    CardinalityResponse as FBCardinalityResponse, CardinalityResponseArgs,
};
use crate::commands::calculate_cardinality;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{CommandMessageType, MultiShardCommand, TrackerEnum};
use crate::series::request_types::MatchFilterOptions;
use flatbuffers::FlatBufferBuilder;
use valkey_module::{Context, ValkeyResult};

pub struct CardinalityCommand;

impl MultiShardCommand for CardinalityCommand {
    type REQ = MatchFilterOptions;
    type RES = CardinalityResponse;
    fn request_type() -> CommandMessageType {
        CommandMessageType::Cardinality
    }
    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<CardinalityResponse> {
        let count = calculate_cardinality(ctx, req.date_range, &req.matchers)?;
        Ok(CardinalityResponse { count })
    }
    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::Cardinality(t) = tracker {
            t.update(res);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CardinalityResponse {
    pub count: usize,
}

impl Serialized for CardinalityResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(128);
        let resp = FBCardinalityResponse::create(
            &mut bldr,
            &CardinalityResponseArgs {
                count: self.count as u64,
            },
        );
        
        bldr.finish(resp, None);

        let data = bldr.finished_data(); // Of type `&[u8]`
        buf.extend_from_slice(data);
    }
}

impl Deserialized for CardinalityResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBCardinalityResponse>(buf, "CardinalityResponse")?;

        let count = req.count() as usize;

        Ok(CardinalityResponse { count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_response_serialization_deserialization() {
        // Create a test response
        let original_response = CardinalityResponse { count: 42 };

        // Serialize
        let mut buf = Vec::new();
        original_response.serialize(&mut buf);

        // Deserialize
        let deserialized_response = CardinalityResponse::deserialize(&buf).unwrap();

        // Assert equality
        assert_eq!(original_response.count, deserialized_response.count);
    }

    #[test]
    fn test_error_cases() {
        // Test with an empty buffer
        let empty_buf: &[u8] = &[];
        assert!(CardinalityResponse::deserialize(empty_buf).is_err());

        // Test with invalid buffer
        let invalid_buf: &[u8] = &[1, 2, 3, 4];
        assert!(CardinalityResponse::deserialize(invalid_buf).is_err());
    }
}
