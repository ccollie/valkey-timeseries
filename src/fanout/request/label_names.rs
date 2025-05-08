use super::response_generated::{
    LabelNamesResponse as FBLabelNamesResponse, LabelNamesResponseArgs,
};
use crate::commands::process_label_names_request;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{ClusterMessageType, MultiShardCommand, TrackerEnum};
use crate::series::request_types::MatchFilterOptions;
use flatbuffers::FlatBufferBuilder;
use valkey_module::{Context, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct LabelNamesCommand;

impl MultiShardCommand for LabelNamesCommand {
    type REQ = MatchFilterOptions;
    type RES = LabelNamesResponse;

    fn request_type() -> ClusterMessageType {
        ClusterMessageType::LabelNames
    }
    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<LabelNamesResponse> {
        process_label_names_request(ctx, &req).map(|names| LabelNamesResponse { names })
    }
    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::LabelNames(t) = tracker {
            t.update(res);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LabelNamesResponse {
    pub names: Vec<String>,
}

impl Serialized for LabelNamesResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);
        let mut names = Vec::with_capacity(self.names.len());
        for item in self.names.iter() {
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
}

impl Deserialized for LabelNamesResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBLabelNamesResponse>(buf, "LabelNamesResponse")?;
        let names = if let Some(resp_rnames) = req.names() {
            resp_rnames.iter().map(|x| x.to_string()).collect()
        } else {
            vec![]
        };

        Ok(LabelNamesResponse { names })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
