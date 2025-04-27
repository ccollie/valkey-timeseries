use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{
    DateRange, DateRangeArgs, IndexQueryRequest as FBIndexQueryRequest, IndexQueryRequestArgs
};
use super::response_generated::IndexQueryResponse as FBIndexQueryResponse;
use super::{Request, Response};
use crate::fanout::coordinator::create_response_done_callback;
use crate::fanout::request::response_generated::IndexQueryResponseArgs;
use crate::fanout::types::{ClusterMessageType, TrackerEnum};
use crate::fanout::ResultsTracker;
use crate::labels::matchers::Matchers;
use crate::series::TimestampRange;
use flatbuffers::FlatBufferBuilder;
use smallvec::SmallVec;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};
use crate::fanout::request::common::deserialize_timestamp_range;

#[derive(Debug, Clone, Default)]
pub struct IndexQueryRequest {
    pub range: Option<TimestampRange>,
    pub filter: Matchers,
}

impl Request<IndexQueryResponse> for IndexQueryRequest {
    fn request_type() -> ClusterMessageType {
        ClusterMessageType::IndexQuery
    }
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_index_query_request(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_index_query_request(buf)
    }
    fn create_tracker<F>(
        ctx: &Context,
        request_id: u64,
        expected_results: usize,
        callback: F,
    ) -> TrackerEnum
    where
        F: FnOnce(&ThreadSafeContext<BlockedClient>, &[IndexQueryResponse]) + Send + 'static,
    {
        let cbk = create_response_done_callback(ctx, request_id, callback);
        let tracker: ResultsTracker<IndexQueryResponse> =
            ResultsTracker::new(expected_results, cbk);
        TrackerEnum::IndexQuery(tracker)
    }
    fn exec(&self, ctx: &Context) -> ValkeyResult<IndexQueryResponse> {
        unimplemented!("exec not implemented for this request type");
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexQueryResponse {
    pub keys: Vec<String>,
}

impl Response for IndexQueryResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_index_query_response(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        deserialize_index_query_response(buf)
    }
    fn update_tracker(tracker: &TrackerEnum, res: Self) {
        if let TrackerEnum::IndexQuery(ref t) = tracker {
            t.add_result(res);
        }
    }
}

pub fn serialize_index_query_request(buf: &mut Vec<u8>, options: &IndexQueryRequest) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);
    let range = options.range.map(|x| {
        let (start, end) = x.get_timestamps(None);
        DateRange::create(&mut bldr, &DateRangeArgs { start, end })
    });
    let filter = Some(serialize_matchers(&mut bldr, &options.filter));

    let root = FBIndexQueryRequest::create(&mut bldr, &IndexQueryRequestArgs { range, filter });
    bldr.finish(root, None);

    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

pub fn deserialize_index_query_request(buf: &[u8]) -> ValkeyResult<IndexQueryRequest> {
    // Get access to the root:
    let req = flatbuffers::root::<FBIndexQueryRequest>(buf).unwrap();

    let range = deserialize_timestamp_range(req.range())?;
    let filter = if let Some(filter) = req.filter() {
        deserialize_matchers(&filter)?
    } else {
        return Err(ValkeyError::Str("TSDB: missing filter"));
    };

    Ok(IndexQueryRequest {
        range,
        filter,
    })
}

pub fn serialize_index_query_response(buf: &mut Vec<u8>, response: &IndexQueryResponse) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);
    let _keys: SmallVec<_, 16> = response
        .keys
        .iter()
        .map(|k| bldr.create_string(k.as_str()))
        .collect();

    let keys = bldr.create_vector(&_keys);
    let obj = FBIndexQueryResponse::create(&mut bldr, &IndexQueryResponseArgs { keys: Some(keys) });
    bldr.finish(obj, None);

    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

pub fn deserialize_index_query_response(buf: &[u8]) -> ValkeyResult<IndexQueryResponse> {
    let req = flatbuffers::root::<FBIndexQueryResponse>(buf).unwrap();
    let keys = req.keys().unwrap_or_default();
    let keys = keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
    Ok(IndexQueryResponse { keys })
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
    fn test_index_query_request_serialize_deserialize() {
        let req = IndexQueryRequest {
            range: Some(TimestampRange::from_timestamps(100, 200).unwrap()),
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = IndexQueryRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.range, req2.range);
        assert_eq!(req.filter, req2.filter);
    }

    #[test]
    fn test_index_query_request_empty_filter_serialize_deserialize() {
        let req = IndexQueryRequest {
            range: Some(TimestampRange::from_timestamps(100, 200).unwrap()),
            filter: Matchers::default(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        assert!(IndexQueryRequest::deserialize(&buf).is_err());
    }

    #[test]
    fn test_index_query_response_serialize_deserialize() {
        let resp = IndexQueryResponse {
            keys: vec![
                "series1".to_string(),
                "series2".to_string(),
                "series3".to_string(),
            ],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = IndexQueryResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.keys.len(), resp2.keys.len());
        assert_eq!(resp.keys, resp2.keys);
    }

    #[test]
    fn test_index_query_response_empty_serialize_deserialize() {
        let resp = IndexQueryResponse { keys: vec![] };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = IndexQueryResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.keys.len(), resp2.keys.len());
        assert!(resp2.keys.is_empty());
    }

    #[test]
    fn test_index_query_response_large_serialize_deserialize() {
        // Test with a large number of keys
        let mut keys = Vec::with_capacity(1000);
        for i in 0..1000 {
            keys.push(format!("series{}", i));
        }

        let resp = IndexQueryResponse { keys };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = IndexQueryResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.keys.len(), resp2.keys.len());
        assert_eq!(resp.keys[0], resp2.keys[0]);
        assert_eq!(resp.keys[999], resp2.keys[999]);
    }

    #[test]
    fn test_index_query_request_timestamp_edge_cases() {
        // Test with minimum and maximum timestamp values
        let req = IndexQueryRequest {
            range: Some(TimestampRange::from_timestamps(i64::MIN, i64::MAX).unwrap()),
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = IndexQueryRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.range, req2.range);
    }

    #[test]
    fn test_index_query_request_complex_matchers() {
        // Test with a more complex matcher structure
        let filter = Matchers::with_or_matchers(
            Some("complex_test".to_string()),
            vec![vec![
                Matcher {
                    label: "region".to_string(),
                    matcher: PredicateMatch::Equal(PredicateValue::String("us-west".to_string())),
                },
                Matcher {
                    label: "region".to_string(),
                    matcher: PredicateMatch::Equal(PredicateValue::String("us-east".to_string())),
                },
                Matcher {
                    label: "status".to_string(),
                    matcher: PredicateMatch::NotEqual(PredicateValue::String("error".to_string())),
                },
            ]],
        );

        let req = IndexQueryRequest {
            range: Some(TimestampRange::from_timestamps(100, 200).unwrap()),
            filter,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = IndexQueryRequest::deserialize(&buf).expect("deserialization failed");

        assert_eq!(req.range, req2.range);
        assert_eq!(req.filter.name, req2.filter.name);
        assert_eq!(req.filter.matchers, req2.filter.matchers);
    }
}
