use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{LabelNamesRequest as FBLabelNamesRequest, LabelNamesRequestArgs};
use super::response_generated::{
    LabelNamesResponse as FBLabelNamesResponse, LabelNamesResponseArgs,
};
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
pub struct LabelNamesRequest {
    pub range: Option<TimestampRange>,
    pub filter: Matchers,
}

impl Request<LabelNamesResponse> for LabelNamesRequest {
    fn request_type() -> ClusterMessageType {
        ClusterMessageType::LabelNames
    }
    fn serialize<'a>(&self, buf: &mut Vec<u8>) {
        serialize_label_names_request(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_label_names_request(buf)
    }
    fn create_tracker<F>(
        ctx: &Context,
        request_id: u64,
        expected_results: usize,
        callback: F,
    ) -> TrackerEnum
    where
        F: FnOnce(&ThreadSafeContext<BlockedClient>, &[LabelNamesResponse]) + Send + 'static,
    {
        let cbk = create_response_done_callback(ctx, request_id, callback);

        let tracker: ResultsTracker<LabelNamesResponse> =
            ResultsTracker::new(expected_results, cbk);

        TrackerEnum::LabelNames(tracker)
    }
}

#[derive(Clone, Debug, Default)]
pub struct LabelNamesResponse {
    pub names: Vec<String>,
}

impl Response for LabelNamesResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_label_names_response(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        deserialize_label_names_response(buf)
    }
    fn update_tracker(tracker: &TrackerEnum, res: LabelNamesResponse) {
        if let TrackerEnum::LabelNames(ref t) = tracker {
            t.update(res);
        }
    }
}

pub fn serialize_label_names_request(dest: &mut Vec<u8>, request: &LabelNamesRequest) {
    let mut bldr = FlatBufferBuilder::with_capacity(128);

    let range = serialize_timestamp_range(&mut bldr, request.range);
    let filter = serialize_matchers(&mut bldr, &request.filter);

    // Create a new LabelNamesRequest object
    let obj = FBLabelNamesRequest::create(
        &mut bldr,
        &LabelNamesRequestArgs {
            range,
            filter: Some(filter),
        },
    );
    bldr.finish(obj, None);

    // Copy the serialized FlatBuffers data to our own byte buffer.
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

// todo: FanoutError type
pub fn deserialize_label_names_request(buf: &[u8]) -> ValkeyResult<LabelNamesRequest> {
    // Get access to the root:
    let req = flatbuffers::root::<FBLabelNamesRequest>(buf).unwrap();

    let range = deserialize_timestamp_range(req.range())?;
    let filter = if let Some(filter) = req.filter() {
        deserialize_matchers(&filter)?
    } else {
        return Err(ValkeyError::Str("TSDB: missing start timestamp"));
    };

    Ok(LabelNamesRequest {
        range,
        filter,
    })
}

pub fn serialize_label_names_response(buf: &mut Vec<u8>, response: &LabelNamesResponse) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);
    let mut names = Vec::with_capacity(response.names.len());
    for item in response.names.iter() {
        let name_ = bldr.create_string(item);
        names.push(name_);
    }
    let names = bldr.create_vector(&names);
    let obj =
        FBLabelNamesResponse::create(&mut bldr, &LabelNamesResponseArgs { names: Some(names) });

    bldr.finish(obj, None);

    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

pub(super) fn deserialize_label_names_response(buf: &[u8]) -> ValkeyResult<LabelNamesResponse> {
    let req = flatbuffers::root::<FBLabelNamesResponse>(buf).unwrap();

    let names = if let Some(resp_rnames) = req.names() {
        resp_rnames.iter().map(|x| x.to_string()).collect()
    } else {
        vec![]
    };

    Ok(LabelNamesResponse { names })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::matchers::{
        Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
    };

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
    fn test_label_names_request_serialize_deserialize() {
        let req = LabelNamesRequest {
            range: TimestampRange::from_timestamps(100, 200).ok(),
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = LabelNamesRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.range, req2.range);
        assert_eq!(req.filter, req2.filter);
    }

    #[test]
    fn test_label_names_request_empty_filter_serialize_deserialize() {
        let req = LabelNamesRequest {
            range: TimestampRange::from_timestamps(100, 200).ok(),
            filter: Matchers::default(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        assert!(LabelNamesRequest::deserialize(&buf).is_err());
    }

    #[test]
    fn test_label_names_response_serialize_deserialize() {
        let resp = LabelNamesResponse {
            names: vec![
                "label1".to_string(),
                "label2".to_string(),
                "label3".to_string(),
            ],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = LabelNamesResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.names.len(), resp2.names.len());
        assert_eq!(resp.names, resp2.names);
    }

    #[test]
    fn test_label_names_response_empty_serialize_deserialize() {
        let resp = LabelNamesResponse { names: vec![] };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = LabelNamesResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.names.len(), resp2.names.len());
        assert!(resp2.names.is_empty());
    }

    #[test]
    fn test_label_names_response_large_serialize_deserialize() {
        // Test with a large number of label names
        let mut names = Vec::with_capacity(1000);
        for i in 0..1000 {
            names.push(format!("label{}", i));
        }

        let resp = LabelNamesResponse { names };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = LabelNamesResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.names.len(), resp2.names.len());
        assert_eq!(resp.names[0], resp2.names[0]);
        assert_eq!(resp.names[999], resp2.names[999]);
    }

    #[test]
    fn test_label_names_request_timestamp_edge_cases() {
        // Test with minimum and maximum timestamp values
        let req = LabelNamesRequest {
            range: TimestampRange::from_timestamps(i64::MIN, i64::MAX).ok(),
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = LabelNamesRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.range, req2.range);
    }
}
