use super::response_generated::{
    RangeResponse as FBRangeResponse, RangeResponseBuilder, Sample as ResponseSample,
    SeriesRangeResponse, SeriesRangeResponseArgs,
};
use crate::commands::process_mrange_query;
use crate::common::Sample;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{CommandMessageType, MultiShardCommand, TrackerEnum};
use crate::series::request_types::{MatchFilterOptions, RangeOptions};
use flatbuffers::FlatBufferBuilder;
use valkey_module::{Context, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct RangeCommand;

impl MultiShardCommand for RangeCommand {
    type REQ = MatchFilterOptions;
    type RES = RangeResponse;
    fn request_type() -> CommandMessageType {
        CommandMessageType::RangeQuery
    }

    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<RangeResponse> {
        let options = RangeOptions {
            date_range: req.date_range.unwrap_or_default(),
            count: req.limit,
            filters: req.matchers,
            ..Default::default()
        };
        process_mrange_query(ctx, options, false).map(|series| {
            let series = series
                .into_iter()
                .map(|s| RangeSeriesResponse {
                    key: s.key,
                    samples: s.samples,
                })
                .collect::<Vec<_>>();
            RangeResponse { series }
        })
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::RangeQuery(t) = tracker {
            t.update(res);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RangeResponse {
    pub series: Vec<RangeSeriesResponse>,
}

impl Serialized for RangeResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_range_response(buf, self);
    }
}

impl Deserialized for RangeResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_range_response(buf)
    }
}

#[derive(Clone, Debug, Default)]
pub struct RangeSeriesResponse {
    pub key: String,
    pub samples: Vec<Sample>,
}

pub fn serialize_range_response(buf: &mut Vec<u8>, response: &RangeResponse) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);
    let mut series = Vec::with_capacity(response.series.len());
    for item in &response.series {
        let key = Some(bldr.create_string(&item.key));
        let samples = {
            let iter = item
                .samples
                .iter()
                .map(|s| ResponseSample::new(s.timestamp, s.value));
            let samples = bldr.create_vector_from_iter(iter);
            Some(samples)
        };
        let args = SeriesRangeResponseArgs { key, samples };
        let bld = SeriesRangeResponse::create(&mut bldr, &args);
        series.push(bld);
    }

    let sample_values = bldr.create_vector(&series);
    let mut builder = RangeResponseBuilder::new(&mut bldr);

    builder.add_series(sample_values);

    builder.finish();
    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

pub(super) fn deserialize_range_response(buf: &[u8]) -> ValkeyResult<RangeResponse> {
    let req = load_flatbuffers_object::<FBRangeResponse>(buf, "RangeResponse")?;

    let mut series = Vec::new();
    if let Some(response_series) = req.series() {
        series = Vec::with_capacity(series.len());
        for item in response_series.iter() {
            let key = item.key().unwrap_or_default().to_string();
            let samples = if let Some(samples) = item.samples() {
                samples
                    .iter()
                    .map(|x| Sample::new(x.timestamp(), x.value()))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            series.push(RangeSeriesResponse { key, samples });
        }
    }

    Ok(RangeResponse { series })
}
