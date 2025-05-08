use super::response_generated::IndexQueryResponse as FBIndexQueryResponse;
use crate::commands::process_query_index_request;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::response_generated::IndexQueryResponseArgs;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{ClusterMessageType, MultiShardCommand, TrackerEnum};
use crate::series::request_types::MatchFilterOptions;
use flatbuffers::FlatBufferBuilder;
use smallvec::SmallVec;
use valkey_module::{Context, ValkeyResult};

#[derive(Debug, Clone, Default)]
pub struct IndexQueryCommand;

impl MultiShardCommand for IndexQueryCommand {
    type REQ = MatchFilterOptions;
    type RES = IndexQueryResponse;

    fn request_type() -> ClusterMessageType {
        ClusterMessageType::IndexQuery
    }

    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<IndexQueryResponse> {
        process_query_index_request(ctx, &req.matchers, req.date_range).map(|keys| {
            let keys = keys.into_iter().map(|k| k.to_string()).collect::<Vec<_>>();
            IndexQueryResponse { keys }
        })
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::IndexQuery(ref t) = tracker {
            t.add_result(res);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexQueryResponse {
    pub keys: Vec<String>,
}

impl Serialized for IndexQueryResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);
        let _keys: SmallVec<_, 16> = self
            .keys
            .iter()
            .map(|k| bldr.create_string(k.as_str()))
            .collect();

        let keys = bldr.create_vector(&_keys);
        let obj =
            FBIndexQueryResponse::create(&mut bldr, &IndexQueryResponseArgs { keys: Some(keys) });
        bldr.finish(obj, None);

        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for IndexQueryResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBIndexQueryResponse>(buf, "IndexQueryResponse")?;
        let keys = req.keys().unwrap_or_default();
        let keys = keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
        Ok(IndexQueryResponse { keys })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
