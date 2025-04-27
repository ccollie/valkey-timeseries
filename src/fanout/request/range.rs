use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{
    DateRange,
    DateRangeArgs,
    RangeRequest as FBRangeRequest,
    RangeRequestArgs
};
use super::response_generated::{
    RangeResponse as FBRangeResponse, 
    RangeResponseBuilder,
    Sample as ResponseSample,
    SeriesRangeResponse, 
    SeriesRangeResponseArgs
};
use crate::common::{Sample, Timestamp};
use crate::fanout::coordinator::create_response_done_callback;
use crate::fanout::request::{Request, Response};
use crate::fanout::types::{ClusterMessageType, TrackerEnum};
use crate::fanout::ResultsTracker;
use crate::labels::matchers::Matchers;
use flatbuffers::FlatBufferBuilder;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct RangeRequest {
    pub start_timestamp: Timestamp,
    pub end_timestamp: Timestamp,
    pub filter: Matchers,
}

impl Request<RangeResponse> for RangeRequest {
    fn request_type() -> ClusterMessageType {
        ClusterMessageType::RangeQuery
    }

    fn serialize<'a>(&self, buf: &mut Vec<u8>) {
        serialize_range_request(buf, self);
    }

    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_range_request(buf)
    }

    fn create_tracker<F>(ctx: &Context, request_id: u64, expected_results: usize, callback: F) -> TrackerEnum
    where
        F: FnOnce(&ThreadSafeContext<BlockedClient>, &[RangeResponse]) + Send + 'static
    {
        let cbk = create_response_done_callback(ctx, request_id, callback);

        let tracker: ResultsTracker<RangeResponse> = ResultsTracker::new(
            expected_results,
            cbk,
        );

        TrackerEnum::RangeQuery(tracker)
    }
}

#[derive(Clone, Debug, Default)]
pub struct RangeResponse {
    pub series: Vec<RangeSeriesResponse>,
}

impl Response for RangeResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_range_response(buf, self);
    }
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>
    where
        Self: Sized
    {
        deserialize_range_response(buf)
    }
    fn update_tracker(tracker: &TrackerEnum, res: RangeResponse) {
        if let TrackerEnum::RangeQuery(ref t) = tracker {
            t.update(res);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RangeSeriesResponse {
    pub key: String,
    pub samples: Vec<Sample>,
}

pub fn serialize_range_request(dest: &mut Vec<u8>, request: &RangeRequest) {
    let mut bldr = FlatBufferBuilder::with_capacity(128);

    let range = DateRange::create(&mut bldr, &DateRangeArgs {
        start: request.start_timestamp,
        end: request.end_timestamp,
    });
    let filter = serialize_matchers(&mut bldr, &request.filter);
    let obj = FBRangeRequest::create(&mut bldr, &RangeRequestArgs {
        range: Some(range),
        filters: Some(filter),
    });
    
    bldr.finish(obj, None);
    
    // Copy the serialized FlatBuffers data to our own byte buffer.
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

// todo: FanoutError type
pub fn deserialize_range_request(
    buf: &[u8]
) -> ValkeyResult<RangeRequest> {
    // Get access to the root:
    let req = flatbuffers::root::<FBRangeRequest>(buf)
        .unwrap();

    let (start_timestamp, end_timestamp) = if let Some(range) = req.range() {
        (range.start(), range.end())
    } else {
        return Err(ValkeyError::Str("TSDB: missing range in request"));
    };
    
    let filter = if let Some(filter) = req.filters() {
        deserialize_matchers(&filter)?
    } else {
        return Err(ValkeyError::Str("TSDB: missing start timestamp"));
    };

    Ok(RangeRequest {
        start_timestamp,
        end_timestamp,
        filter,
    })
}

pub fn serialize_range_response(
    buf: &mut Vec<u8>,
    response: &RangeResponse,
) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);
    let mut series = Vec::with_capacity(response.series.len());
    for item in &response.series {
        let key = Some(bldr.create_string(&item.key));
        let samples =  {
            let iter = item.samples.iter()
                .map(|s| ResponseSample::new(s.timestamp, s.value));
            let samples = bldr.create_vector_from_iter(iter);
            Some(samples)
        };
        let args = SeriesRangeResponseArgs {
            key,
            samples,
        };
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

pub(super) fn deserialize_range_response(
    buf: &[u8],
) -> ValkeyResult<RangeResponse> {
    let req = flatbuffers::root::<FBRangeResponse>(buf)
        .unwrap();
    
    let mut series = Vec::new();
    if let Some(response_series) = req.series() {
        series = Vec::with_capacity(series.len());
        for item in response_series.iter() {
            let key = item.key().unwrap_or_default().to_string();
            let samples = if let Some(samples) = item.samples() {
                samples
                    .iter().map(|x| Sample::new(x.timestamp(), x.value()))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            series.push(RangeSeriesResponse {
                key,
                samples,
            });
        }
    }

    Ok(RangeResponse { series })
}