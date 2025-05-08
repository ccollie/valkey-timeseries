use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{
    LabelValuesRequest as FBLabelValuesRequest, LabelValuesRequestArgs,
};
use super::response_generated::{
    LabelValuesResponse as FBLabelValuesResponse, LabelValuesResponseArgs,
};
use crate::commands::process_label_values_request;
use crate::fanout::request::common::{
    deserialize_timestamp_range, load_flatbuffers_object, serialize_timestamp_range,
};
use crate::fanout::serialization::{Deserialized, Serialized};
use crate::fanout::{CommandMessageType, MultiShardCommand, TrackerEnum};
use crate::labels::matchers::Matchers;
use crate::series::request_types::MatchFilterOptions;
use crate::series::TimestampRange;
use flatbuffers::FlatBufferBuilder;
use valkey_module::{Context, ValkeyError, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct LabelValuesRequest {
    pub label_name: String,
    pub range: Option<TimestampRange>,
    pub filter: Matchers,
}

pub struct LabelValuesCommand;

impl Serialized for LabelValuesRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(128);

        let name = bldr.create_string(&self.label_name);
        let range = serialize_timestamp_range(&mut bldr, self.range);
        let filter = serialize_matchers(&mut bldr, &self.filter);

        let req = FBLabelValuesRequest::create(
            &mut bldr,
            &LabelValuesRequestArgs {
                label: Some(name),
                range,
                filter: Some(filter),
            },
        );

        bldr.finish(req, None);
        // Copy the serialized FlatBuffers data to our own byte buffer.
        let finished_data = bldr.finished_data();
        buf.extend_from_slice(finished_data);
    }
}

impl Deserialized for LabelValuesRequest {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        // Get access to the root:
        let req = load_flatbuffers_object::<FBLabelValuesRequest>(buf, "LabelValuesRequest")?;

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
}
impl MultiShardCommand for LabelValuesCommand {
    type REQ = LabelValuesRequest;
    type RES = LabelValuesResponse;

    fn request_type() -> CommandMessageType {
        CommandMessageType::LabelValues
    }
    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<LabelValuesResponse> {
        let options = MatchFilterOptions {
            date_range: req.range,
            matchers: vec![req.filter], // todo: workaround clone
            ..Default::default()
        };
        process_label_values_request(ctx, &req.label_name, &options)
            .map(|values| LabelValuesResponse { values })
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::LabelValues(t) = tracker {
            t.update(res);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LabelValuesResponse {
    pub values: Vec<String>,
}

impl Serialized for LabelValuesResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);
        let mut values = Vec::with_capacity(self.values.len());
        for item in self.values.iter() {
            let name_ = bldr.create_string(item);
            values.push(name_);
        }
        let values = bldr.create_vector(&values);

        let obj = FBLabelValuesResponse::create(
            &mut bldr,
            &LabelValuesResponseArgs {
                values: Some(values),
            },
        );

        bldr.finish(obj, None);
        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for LabelValuesResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBLabelValuesResponse>(buf, "LabelValuesResponse")?;

        let values = if let Some(resp_rnames) = req.values() {
            resp_rnames.iter().map(|x| x.to_string()).collect()
        } else {
            vec![]
        };

        Ok(LabelValuesResponse { values })
    }
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
