use super::common::{decode_label, load_flatbuffers_object};
use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{MultiGetRequest, MultiGetRequestArgs};
use super::response_generated::{
    Label as FBLabel, LabelArgs, MGetValue as FBMGetValue, MGetValueBuilder,
    MultiGetResponse as FBMultiGetResponse, MultiGetResponseArgs, Sample as ResponseSample,
};
use crate::commands::process_mget_request;
use crate::common::Sample;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{CommandMessageType, MultiShardCommand, TrackerEnum};
use crate::labels::Label;
use crate::series::request_types::MGetRequest;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use smallvec::SmallVec;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyValue};

pub struct MGetShardedCommand;

impl MultiShardCommand for MGetShardedCommand {
    type REQ = MGetRequest;
    type RES = MultiGetResponse;

    fn request_type() -> CommandMessageType {
        CommandMessageType::MGetQuery
    }

    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<MultiGetResponse> {
        let res = process_mget_request(ctx, req)?;
        let values = res
            .into_iter()
            .map(|resp| MGetValue {
                key: resp.series_key.to_string_lossy(),
                value: resp.sample,
                labels: resp.labels,
            })
            .collect::<Vec<_>>();
        Ok(MultiGetResponse { series: values })
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::MGetQuery(t) = tracker {
            t.update(res);
        } else {
            panic!("BUG: Invalid tracker type");
        }
    }
}

impl Serialized for MGetRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(1024);

        let mut labels: SmallVec<_, 4> = SmallVec::new();
        for label in self.selected_labels.iter() {
            let name = bldr.create_string(label.as_str());
            labels.push(name);
        }
        let selected_labels = bldr.create_vector(&labels);
        let filter = serialize_matchers(&mut bldr, &self.filter);

        let req = MultiGetRequest::create(
            &mut bldr,
            &MultiGetRequestArgs {
                with_labels: self.with_labels,
                filter: Some(filter),
                selected_labels: Some(selected_labels),
            },
        );

        bldr.finish(req, None);

        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for MGetRequest {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<MultiGetRequest>(buf, "MGetRequest")?;
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
}

#[derive(Clone, Debug, Default)]
pub struct MGetValue {
    pub key: String,
    pub value: Option<Sample>,
    pub labels: Vec<Option<Label>>,
}

impl From<MGetValue> for ValkeyValue {
    fn from(value: MGetValue) -> ValkeyValue {
        let labels: Vec<_> = value
            .labels
            .into_iter()
            .map(|label| match label {
                Some(label) => label.into(),
                None => ValkeyValue::Null,
            })
            .collect();
        ValkeyValue::Array(vec![
            ValkeyValue::SimpleString(value.key),
            ValkeyValue::Array(labels),
            value.value.map_or(ValkeyValue::Null, |s| s.into()),
        ])
    }
}
#[derive(Clone, Debug, Default)]
pub struct MultiGetResponse {
    pub series: Vec<MGetValue>,
}

impl Serialized for MultiGetResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(1024);

        let mut values = Vec::with_capacity(self.series.len());
        for item in self.series.iter() {
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
        buf.extend_from_slice(data);
    }
}

impl Deserialized for MultiGetResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBMultiGetResponse>(buf, "MultiGetResponse")?;

        let mut result: MultiGetResponse = MultiGetResponse::default();

        if let Some(values) = req.values() {
            for item in values.iter() {
                let value = decode_mget_value(&item);
                result.series.push(value);
            }
        }

        Ok(result)
    }
}

fn serialize_mget_value<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    value: &MGetValue,
) -> WIPOffset<FBMGetValue<'a>> {
    let key = bldr.create_string(value.key.as_str());
    let labels = if !value.labels.is_empty() {
        let mut items: SmallVec<_, 8> = SmallVec::new();
        for label in value.labels.iter() {
            if let Some(label) = label {
                let name = Some(bldr.create_string(label.name.as_str()));
                let value = Some(bldr.create_string(label.value.as_str()));
                let args = LabelArgs { name, value };
                let item = FBLabel::create(bldr, &args);
                items.push(item);
            } else {
                let none_ = FBLabel::create(
                    bldr,
                    &LabelArgs {
                        name: None,
                        value: None,
                    },
                );
                items.push(none_);
            }
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
    let value = reader
        .value()
        .map(|value| Sample::new(value.timestamp(), value.value()));
    let labels = reader
        .labels()
        .unwrap_or_default()
        .iter()
        .map(|label| {
            let lbl = decode_label(label);
            if lbl.name.is_empty() && lbl.value.is_empty() {
                None
            } else {
                Some(lbl)
            }
        })
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
    fn test_mget_response_serialize_deserialize() {
        let resp = MultiGetResponse {
            series: vec![
                MGetValue {
                    key: "series1".to_string(),
                    value: Some(Sample::new(1000, 42.5)),
                    labels: vec![
                        Some(Label::new("name", "series1")),
                        Some(Label::new("region", "us-west")),
                    ],
                },
                MGetValue {
                    key: "series2".to_string(),
                    value: Some(Sample::new(1001, 17.8)),
                    labels: vec![
                        Some(Label::new("name", "series2")),
                        Some(Label::new("region", "us-east")),
                    ],
                },
                MGetValue {
                    key: "series3".to_string(),
                    value: None, // Test missing value
                    labels: vec![
                        Some(Label::new("name", "series3")),
                        Some(Label::new("region", "eu-central")),
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
        assert_eq!(resp.series[0].labels[0], resp2.series[0].labels[0]);

        // Check the second series
        assert_eq!(resp.series[1].key, resp2.series[1].key);
        assert_eq!(resp.series[1].value, resp2.series[1].value);

        // Check the third series (with None value)
        assert_eq!(resp.series[2].key, resp2.series[2].key);
        assert_eq!(resp.series[2].value, resp2.series[2].value);
        assert!(resp2.series[2].value.is_none());
    }

    #[test]
    fn test_mget_response_empty_serialize_deserialize() {
        let resp = MultiGetResponse { series: vec![] };

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
    fn test_mget_response_with_empty_labels_serialize_deserialize() {
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
