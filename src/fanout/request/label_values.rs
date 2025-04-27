use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{LabelValuesRequest as FBLabelValuesRequest, LabelValuesRequestArgs};
use super::response_generated::{LabelValuesResponse as FBLabelValuesResponse, LabelValuesResponseArgs};
use crate::fanout::coordinator::create_response_done_callback;
use crate::fanout::request::common::{deserialize_timestamp_range, serialize_timestamp_range};
use crate::fanout::request::{Request, Response};
use crate::fanout::types::{ClusterMessageType, TrackerEnum};
use crate::fanout::ResultsTracker;
use crate::labels::matchers::Matchers;
use crate::series::TimestampRange;
use flatbuffers::FlatBufferBuilder;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct LabelValuesRequest {
    pub label_name: String,
    pub range: Option<TimestampRange>,
    pub filter: Matchers,
}

impl Request<LabelValuesResponse> for LabelValuesRequest {
    fn request_type() -> ClusterMessageType {
        ClusterMessageType::LabelValues
    }
    fn serialize<'a>(&self, buf: &mut Vec<u8>) {
        serialize_label_names_request(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_label_values_request(buf)
    }
    fn create_tracker<F>(ctx: &Context, request_id: u64, expected_results: usize, callback: F) -> TrackerEnum
    where
        F: FnOnce(&ThreadSafeContext<BlockedClient>, &[LabelValuesResponse]) + Send + 'static
    {
        let cbk = create_response_done_callback(ctx, request_id, callback);

        let tracker: ResultsTracker<LabelValuesResponse> = ResultsTracker::new(
            expected_results,
            cbk,
        );

        TrackerEnum::LabelValues(tracker)
    }
}

#[derive(Clone, Debug, Default)]
pub struct LabelValuesResponse {
    pub values: Vec<String>,
}

impl Response for LabelValuesResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_label_values_response(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>
    where
        Self: Sized
    {
        deserialize_label_values_response(buf)
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self) {
        if let TrackerEnum::LabelValues(ref t) = tracker {
            t.update(res);
        }
    }
}


pub fn serialize_label_names_request(dest: &mut Vec<u8>, request: &LabelValuesRequest) {
    let mut bldr = FlatBufferBuilder::with_capacity(128);

    let name = bldr.create_string(&request.label_name);
    let range = serialize_timestamp_range(&mut bldr, request.range);
    let filter = serialize_matchers(&mut bldr, &request.filter);

    let req = FBLabelValuesRequest::create(&mut bldr, &LabelValuesRequestArgs {
        label: Some(name),
        range,
        filter: Some(filter),
    });

    bldr.finish(req, None);
    // Copy the serialized FlatBuffers data to our own byte buffer.
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

// todo: FanoutError type
pub fn deserialize_label_values_request(
    buf: &[u8]
) -> ValkeyResult<LabelValuesRequest> {
    // Get access to the root:
    let req = flatbuffers::root::<FBLabelValuesRequest>(buf)
        .unwrap();

    let range = deserialize_timestamp_range(req.range())?;
    let label_name = if let Some(label_name) = req.label() {
        label_name.to_string()
    } else {
        return Err(ValkeyError::Str("TSDB: missing label name"));
    };

    let filter = if let Some(filter) = req.filter() {
        deserialize_matchers(&filter)?
    } else {
        return Err(ValkeyError::Str("TSDB: missing start timestamp"));
    };

    Ok(LabelValuesRequest {
        label_name,
        range,
        filter,
    })
}

pub fn serialize_label_values_response(
    buf: &mut Vec<u8>,
    response: &LabelValuesResponse,
) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);
    let mut values = Vec::with_capacity(response.values.len());
    for item in response.values.iter() {
        let name_ = bldr.create_string(item);
        values.push(name_);
    }
    let values = bldr.create_vector(&values);

    let obj = FBLabelValuesResponse::create(&mut bldr, &LabelValuesResponseArgs {
        values: Some(values),
    });

    bldr.finish(obj, None);
    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

pub(super) fn deserialize_label_values_response(
    buf: &[u8],
) -> ValkeyResult<LabelValuesResponse> {
    let req = flatbuffers::root::<FBLabelValuesResponse>(buf)
        .unwrap();

    let values = if let Some(resp_rnames) = req.values() {
        resp_rnames
            .iter()
            .map(|x| x.to_string())
            .collect()
    } else {
        vec![]
    };

    Ok(LabelValuesResponse { values })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::matchers::{Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue};

    fn make_sample_matchers() -> Matchers {
        Matchers {
            name: Some("test".to_string()),
            matchers: MatcherSetEnum::And(vec![
                Matcher {
                    label: "foo".to_string(),
                    matcher: PredicateMatch::Equal(PredicateValue::String("bar".to_string())),
                },
                Matcher {
                    label: "baz".to_string(),
                    matcher: PredicateMatch::NotEqual(PredicateValue::String("qux".to_string())),
                },
            ]),
        }
    }

    #[test]
    fn test_label_values_request_serialize_deserialize() {
        let req = LabelValuesRequest {
            label_name: "my_label".to_string(),
            range: TimestampRange::from_timestamps(100, 200).ok(),
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = LabelValuesRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.label_name, req2.label_name);
        assert_eq!(req.range, req2.range);
        assert_eq!(req.filter, req2.filter);
    }

    #[test]
    fn test_label_values_response_serialize_deserialize() {
        let resp = LabelValuesResponse {
            values: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = LabelValuesResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.values, resp2.values);
    }
}