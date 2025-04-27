use super::common::decode_label;
use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{MultiGetRequest, MultiGetRequestArgs};
use super::response_generated::{
    Label as ResponseLabel, LabelArgs, MGetValue as FBMGetValue, MGetValueBuilder,
    MultiGetResponse as FBMultiGetResponse, MultiGetResponseArgs, Sample as ResponseSample,
};
use crate::common::Sample;
use crate::fanout::coordinator::create_response_done_callback;
use crate::fanout::request::{Request, Response};
use crate::fanout::types::{ClusterMessageType, TrackerEnum};
use crate::fanout::ResultsTracker;
use crate::labels::matchers::Matchers;
use crate::labels::Label;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use smallvec::SmallVec;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};

#[derive(Debug, Clone, Default)]
pub struct MGetRequest {
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub filter: Matchers,
}

impl Request<MultiGetResponse> for MGetRequest {
    fn request_type() -> ClusterMessageType {
        ClusterMessageType::MGetQuery
    }

    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_mget_request(buf, self);
    }

    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_mget_request(buf)
    }

    fn create_tracker<F>(
        ctx: &Context,
        request_id: u64,
        expected_results: usize,
        callback: F,
    ) -> TrackerEnum
    where
        F: FnOnce(&ThreadSafeContext<BlockedClient>, &[MultiGetResponse]) + Send + 'static,
    {
        let cbk = create_response_done_callback(ctx, request_id, callback);

        let tracker: ResultsTracker<MultiGetResponse> = ResultsTracker::new(expected_results, cbk);
        TrackerEnum::MGetQuery(tracker)
    }
}

#[derive(Clone, Debug, Default)]
pub struct MGetValue {
    pub key: String,
    pub value: Option<Sample>,
    pub labels: Vec<Label>,
}

#[derive(Clone, Debug, Default)]
pub struct MultiGetResponse {
    pub series: Vec<MGetValue>,
}

impl Response for MultiGetResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_mget_response(buf, self);
    }

    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>
    where
        Self: Sized,
    {
        deserialize_mget_response(buf)
    }

    fn update_tracker(tracker: &TrackerEnum, res: MultiGetResponse) {
        if let TrackerEnum::MGetQuery(tracker) = tracker {
            tracker.update(res);
        } else {
            panic!("BUG: Invalid tracker type");
        }
    }
}

pub fn serialize_mget_request(dest: &mut Vec<u8>, request: &MGetRequest) {
    let mut bldr = FlatBufferBuilder::with_capacity(1024);
    
    let mut labels: SmallVec<_, 4> = SmallVec::new();
    for label in request.selected_labels.iter() {
        let name = bldr.create_string(label.as_str());
        labels.push(name);
    }
    let selected_labels = bldr.create_vector(&labels);
    let filter = serialize_matchers(&mut bldr, &request.filter);

    let req = MultiGetRequest::create(
        &mut bldr,
        &MultiGetRequestArgs {
            with_labels: request.with_labels,
            filter: Some(filter),
            selected_labels: Some(selected_labels),
        },
    );

    bldr.finish(req, None);

    let data = bldr.finished_data();
    dest.extend_from_slice(data);
}

pub fn deserialize_mget_request(buf: &[u8]) -> ValkeyResult<MGetRequest> {
    // todo: verify the buffer
    // Get access to the root:
    let req = flatbuffers::root::<MultiGetRequest>(buf).unwrap();
    let mut result: MGetRequest = MGetRequest {
        with_labels: req.with_labels(),
        ..MGetRequest::default()
    };
    
    if let Some(selected_labels) = req.selected_labels() {
        for label in selected_labels.iter() {
            let label = label.to_string();
            result.selected_labels.push(label);
        }
    }

    if let Some(filter) = req.filter() {
        result.filter = deserialize_matchers(&filter)?;
    } else {
        return Err(ValkeyError::Str("TSDB: missing filter"));
    }

    Ok(result)
}

pub fn serialize_mget_response(dest: &mut Vec<u8>, response: &MultiGetResponse) {
    let mut bldr = FlatBufferBuilder::with_capacity(1024);

    let mut values = Vec::with_capacity(response.series.len());
    for item in response.series.iter() {
        let value = serialize_mget_value(&mut bldr, item);
        values.push(value);
    }
    let values = bldr.create_vector(&values);

    // Create the response:
    let obj = FBMultiGetResponse::create(
        &mut bldr,
        &MultiGetResponseArgs {
            values: Some(values),
        },
    );
    bldr.finish(obj, None);
    // Serialize the response:
    let data = bldr.finished_data();
    dest.extend_from_slice(data);
}

pub fn deserialize_mget_response(buf: &[u8]) -> ValkeyResult<MultiGetResponse> {
    // Get access to the root:
    let req = flatbuffers::root::<FBMultiGetResponse>(buf).unwrap();
    let mut result: MultiGetResponse = MultiGetResponse::default();

    if let Some(values) = req.values() {
        for item in values.iter() {
            let value = decode_mget_value(&item);
            result.series.push(value);
        }
    }

    Ok(result)
}

fn serialize_mget_value<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    value: &MGetValue,
) -> WIPOffset<FBMGetValue<'a>> {
    let key = bldr.create_string(value.key.as_str());
    let labels = if !value.labels.is_empty() {
        let mut items: SmallVec<_, 8> = SmallVec::new();
        for label in value.labels.iter() {
            let name = Some(bldr.create_string(label.name.as_str()));
            let value = Some(bldr.create_string(label.value.as_str()));
            let args = LabelArgs { name, value };
            let item = ResponseLabel::create(bldr, &args);
            items.push(item);
        }
        Some(bldr.create_vector(&items))
    } else {
        None
    };

    let mut builder = MGetValueBuilder::new(bldr);
    builder.add_key(key);
    if let Some(sample) = &value.value {
        let sample_value = ResponseSample::new(sample.timestamp, sample.value);
        builder.add_value(&sample_value);
    }
    if let Some(labels) = labels {
        builder.add_labels(labels);
    }
    builder.finish()
}
fn decode_mget_value(reader: &FBMGetValue) -> MGetValue {
    let key = reader.key().unwrap_or_default().to_string();
    let value = reader.value().map(|value| Sample::new(value.timestamp(), value.value()));
    let labels = reader
        .labels()
        .unwrap_or_default()
        .iter()
        .map(|label| decode_label(label))
        .collect::<Vec<_>>();

    MGetValue { key, value, labels }
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
    fn test_mget_request_serialize_deserialize() {
        let req = MGetRequest {
            with_labels: true,
            selected_labels: vec!["label1".to_string(), "label2".to_string()],
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = MGetRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.with_labels, req2.with_labels);
        assert_eq!(req.filter, req2.filter);
        // Check that selected labels were preserved
        assert_eq!(req.selected_labels.len(), req.selected_labels.len());
        for label in &req.selected_labels {
            assert!(req2.selected_labels.contains(label));
        }
    }

    #[test]
    fn test_multiget_response_serialize_deserialize() {
        let resp = MultiGetResponse {
            series: vec![
                MGetValue {
                    key: "series1".to_string(),
                    value: Some(Sample::new(1000, 42.5)),
                    labels: vec![
                        Label::new("name", "series1"),
                        Label::new("region", "us-west"),
                    ],
                },
                MGetValue {
                    key: "series2".to_string(),
                    value: Some(Sample::new(1001, 17.8)),
                    labels: vec![
                        Label::new("name", "series2"),
                        Label::new("region", "us-east"),
                    ],
                },
                MGetValue {
                    key: "series3".to_string(),
                    value: None, // Test missing value
                    labels: vec![
                        Label::new("name", "series3"),
                        Label::new("region", "eu-central"),
                    ],
                },
            ],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiGetResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());

        // Check first series
        assert_eq!(resp.series[0].key, resp2.series[0].key);
        assert_eq!(resp.series[0].value, resp2.series[0].value);
        assert_eq!(resp.series[0].labels.len(), resp2.series[0].labels.len());
        assert_eq!(
            resp.series[0].labels[0].name,
            resp2.series[0].labels[0].name
        );
        assert_eq!(
            resp.series[0].labels[0].value,
            resp2.series[0].labels[0].value
        );

        // Check the second series
        assert_eq!(resp.series[1].key, resp2.series[1].key);
        assert_eq!(resp.series[1].value, resp2.series[1].value);

        // Check the third series (with None value)
        assert_eq!(resp.series[2].key, resp2.series[2].key);
        assert_eq!(resp.series[2].value, resp2.series[2].value);
        assert!(resp2.series[2].value.is_none());
    }

    #[test]
    fn test_multiget_response_empty_serialize_deserialize() {
        let resp = MultiGetResponse {
            series: vec![],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiGetResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());
        assert!(resp2.series.is_empty());
    }

    #[test]
    fn test_mget_request_minimal_serialize_deserialize() {
        let req = MGetRequest {
            with_labels: false,
            selected_labels: vec![],
            filter: make_sample_matchers(),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = MGetRequest::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.with_labels, req2.with_labels);
        assert_eq!(req.filter, req2.filter);
        assert!(req2.selected_labels.is_empty());
    }

    #[test]
    fn test_multiget_response_with_empty_labels_serialize_deserialize() {
        let resp = MultiGetResponse {
            series: vec![MGetValue {
                key: "series1".to_string(),
                value: Some(Sample::new(1000, 42.5)),
                labels: vec![], // Empty labels
            }],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiGetResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());
        assert_eq!(resp.series[0].key, resp2.series[0].key);
        assert_eq!(resp.series[0].value, resp2.series[0].value);
        assert!(resp2.series[0].labels.is_empty());
    }
}
