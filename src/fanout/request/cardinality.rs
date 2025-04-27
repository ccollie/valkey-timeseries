use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{CardinalityRequest as FBCardinalityRequest, CardinalityRequestArgs};
use super::response_generated::{CardinalityResponse as FBCardinalityResponse, CardinalityResponseArgs};
use crate::fanout::coordinator::create_response_done_callback;
use crate::fanout::request::common::{deserialize_timestamp_range, serialize_timestamp_range};
use crate::fanout::request::{Request, Response};
use crate::fanout::types::{ClusterMessageType, TrackerEnum};
use crate::fanout::ResultsTracker;
use crate::labels::matchers::Matchers;
use crate::series::TimestampRange;
use flatbuffers::FlatBufferBuilder;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};
use crate::commands::calculate_cardinality;

#[derive(Clone, Debug, Default)]
pub struct CardinalityRequest {
    pub range: Option<TimestampRange>,
    pub filter: Matchers,
}

impl Request<CardinalityResponse> for CardinalityRequest {
    fn request_type() -> ClusterMessageType {
        ClusterMessageType::RangeQuery
    }
    fn serialize<'a>(&self, buf: &mut Vec<u8>) {
        serialize_cardinality_request(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_cardinality_request(buf)
    }
    fn create_tracker<F>(ctx: &Context, request_id: u64, expected_results: usize, callback: F) -> TrackerEnum
    where
        F: FnOnce(&ThreadSafeContext<BlockedClient>, &[CardinalityResponse]) + Send + 'static
    {
        let cbk = create_response_done_callback(ctx, request_id, callback);

        let tracker: ResultsTracker<CardinalityResponse> = ResultsTracker::new(
            expected_results,
            cbk,
        );

        TrackerEnum::Cardinality(tracker)
    }
    fn exec(&self, ctx: &Context) -> ValkeyResult<CardinalityResponse> {
        // let count = calculate_cardinality(ctx, self.range, &[self.filter])?;
        // Ok(CardinalityResponse {
        //     count
        // })
        unimplemented!("exec not implemented for this request type");
    }
}

#[derive(Clone, Debug, Default)]
pub struct CardinalityResponse {
    pub count: usize,
}

impl Response for CardinalityResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_cardinality_response(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>
    where
        Self: Sized
    {
        deserialize_cardinality_response(buf)
    }
    fn update_tracker(tracker: &TrackerEnum, res: CardinalityResponse) {
        if let TrackerEnum::Cardinality(ref t) = tracker {
            t.update(res);
        }
    }
}


pub fn serialize_cardinality_request(dest: &mut Vec<u8>, request: &CardinalityRequest) {

    let mut bldr = FlatBufferBuilder::with_capacity(128);
    
    let range = serialize_timestamp_range(&mut bldr, request.range);
    let filter = serialize_matchers(&mut bldr, &request.filter);
    
    let root = FBCardinalityRequest::create(&mut bldr, &CardinalityRequestArgs {
        range,
        filter: Some(filter)
    });
    
    bldr.finish(root, None);

    // Copy the serialized FlatBuffers data to our own byte buffer.
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

// todo: FanoutError type
pub fn deserialize_cardinality_request(
    buf: &[u8]
) -> ValkeyResult<CardinalityRequest> {
    // Get access to the root:
    let req = flatbuffers::root::<FBCardinalityRequest>(buf)
        .unwrap();
    
    let range = deserialize_timestamp_range(req.range())?;

    let filter = if let Some(filter) = req.filter() {
        deserialize_matchers(&filter)?
    } else {
        return Err(ValkeyError::Str("TSDB: missing start timestamp"));
    };

    Ok(CardinalityRequest {
        range,
        filter,
    })
}

pub fn serialize_cardinality_response(
    buf: &mut Vec<u8>,
    response: &CardinalityResponse,
) {
    let mut bldr = FlatBufferBuilder::with_capacity(128);
    let resp = FBCardinalityResponse::create(&mut bldr, &CardinalityResponseArgs {
        count: response.count as u64,
    });
    // Serialize the root of the object, without providing a file identifier.
    bldr.finish(resp, None);

    let data = bldr.finished_data(); // Of type `&[u8]`

    buf.extend_from_slice(data);
}

pub(super) fn deserialize_cardinality_response(
    buf: &[u8],
) -> ValkeyResult<CardinalityResponse> {
    let req = flatbuffers::root::<FBCardinalityResponse>(buf)
        .unwrap();
    
    let count = req.count() as usize;

    Ok(CardinalityResponse { count })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::matchers::{Matcher, MatcherSetEnum, PredicateMatch, PredicateValue};

    #[test]
    fn test_cardinality_request_serialization_deserialization() {
        // Create a test request
        let original_request = CardinalityRequest {
            range: Some(TimestampRange::from_timestamps(1000, 2000).unwrap()),
            filter: Matchers {
                name: Some("test_matcher".to_string()),
                matchers: MatcherSetEnum::And(vec![
                    Matcher {
                        label: "label1".to_string(),
                        matcher: PredicateMatch::Equal(PredicateValue::String("value1".to_string())),
                    },
                    Matcher {
                        label: "label2".to_string(),
                        matcher: PredicateMatch::NotEqual(PredicateValue::String("value2".to_string())),
                    },
                ]),
            },
        };

        // Serialize
        let mut buf = Vec::new();
        serialize_cardinality_request(&mut buf, &original_request);

        // Deserialize
        let deserialized_request = deserialize_cardinality_request(&buf).unwrap();

        // Assert equality of fields
        assert_eq!(original_request.range, deserialized_request.range);

        // Compare matchers
        if let Some(original_name) = &original_request.filter.name {
            assert_eq!(Some(original_name), deserialized_request.filter.name.as_ref());
        }

        match (&original_request.filter.matchers, &deserialized_request.filter.matchers) {
            (MatcherSetEnum::And(orig_matchers), MatcherSetEnum::And(des_matchers)) => {
                assert_eq!(orig_matchers.len(), des_matchers.len());
                for (orig, des) in orig_matchers.iter().zip(des_matchers.iter()) {
                    assert_eq!(orig.label, des.label);
                    assert_eq!(orig.matcher, des.matcher);
                }
            },
            _ => panic!("Unexpected matcher type after deserialization"),
        }
    }

    #[test]
    fn test_cardinality_response_serialization_deserialization() {
        // Create a test response
        let original_response = CardinalityResponse {
            count: 42,
        };

        // Serialize
        let mut buf = Vec::new();
        serialize_cardinality_response(&mut buf, &original_response);

        // Deserialize
        let deserialized_response = deserialize_cardinality_response(&buf).unwrap();

        // Assert equality
        assert_eq!(original_response.count, deserialized_response.count);
    }

    #[test]
    fn test_cardinality_request_with_or_matchers() {
        // Create a test request with OR matchers
        let original_request = CardinalityRequest {
            range: TimestampRange::from_timestamps(1000, 2000).ok(),
            filter: Matchers {
                name: None,
                matchers: MatcherSetEnum::Or(vec![
                    vec![
                        Matcher {
                            label: "label1".to_string(),
                            matcher: PredicateMatch::Equal(PredicateValue::String("value1".to_string())),
                        },
                    ],
                    vec![
                        Matcher {
                            label: "label2".to_string(),
                            matcher: PredicateMatch::NotEqual(PredicateValue::String("value2".to_string())),
                        },
                    ],
                ]),
            },
        };

        // Serialize
        let mut buf = Vec::new();
        serialize_cardinality_request(&mut buf, &original_request);

        // Deserialize
        let deserialized_request = deserialize_cardinality_request(&buf).unwrap();

        // Assert equality of basic fields
        assert_eq!(original_request.range, deserialized_request.range);

        // Compare OR matchers
        match (&original_request.filter.matchers, &deserialized_request.filter.matchers) {
            (MatcherSetEnum::Or(orig_lists), MatcherSetEnum::Or(des_lists)) => {
                assert_eq!(orig_lists.len(), des_lists.len());
                for (orig_list, des_list) in orig_lists.iter().zip(des_lists.iter()) {
                    assert_eq!(orig_list.len(), des_list.len());
                    for (orig, des) in orig_list.iter().zip(des_list.iter()) {
                        assert_eq!(orig.label, des.label);
                        assert_eq!(orig.matcher, des.matcher);
                    }
                }
            },
            _ => panic!("Unexpected matcher type after deserialization"),
        }
    }

    #[test]
    fn test_error_cases() {
        // Test with empty buffer
        let empty_buf: &[u8] = &[];
        assert!(deserialize_cardinality_request(empty_buf).is_err());
        assert!(deserialize_cardinality_response(empty_buf).is_err());

        // Test with invalid buffer
        let invalid_buf: &[u8] = &[1, 2, 3, 4];
        assert!(deserialize_cardinality_request(invalid_buf).is_err());
        assert!(deserialize_cardinality_response(invalid_buf).is_err());
    }
}