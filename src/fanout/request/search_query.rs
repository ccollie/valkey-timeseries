use super::request_generated::{
    SearchQueryRequest as FBSearchQueryRequest,
    SearchQueryRequestArgs,
};
use super::serialization::{Deserialized, Serialized};
use crate::common::Sample;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::query_matchers::{deserialize_matchers, serialize_matchers};
use crate::fanout::request::response_generated::{
    SearchQueryResponse as FBSearchQueryResponse, SearchQueryResponseArgs, SeriesChunk,
};
use crate::fanout::request::series_chunk::{deserialize_sample_data, serialize_chunk};
use crate::labels::InternedMetricName;
use crate::query::SeriesDataPair;
use crate::series::chunks::utils::samples_to_chunk;
use flatbuffers::FlatBufferBuilder;
use metricsql_parser::label::Matchers;
use metricsql_runtime::{QueryResult, SearchQuery};
use valkey_module::{ValkeyError, ValkeyResult};


#[derive(Debug, Clone, Default)]
pub struct SearchQueryRequest {
    pub start: i64,
    pub end: i64,
    pub matchers: Matchers,
    pub max_metrics: usize,
}

impl From<SearchQuery> for SearchQueryRequest {
    fn from(query: SearchQuery) -> Self {
        SearchQueryRequest {
            start: query.start,
            end: query.end,
            matchers: query.matchers,
            max_metrics: query.max_metrics,
        }
    }
}

/// Type representing the data from a matched series returned by a query.
#[derive(Clone, Debug, Default)]
pub struct QuerySeriesData {
    pub samples: Vec<Sample>,
    pub labels: InternedMetricName,
}

impl<'a> From<SeriesDataPair<'a>> for QuerySeriesData {
    fn from(pair: SeriesDataPair<'a>) -> Self {
        QuerySeriesData {
            samples: pair.samples,
            labels: pair.series.labels.clone(),
        }
    }
}

impl From<QuerySeriesData> for QueryResult {
    fn from(data: QuerySeriesData) -> Self {
        let mut timestamps = Vec::with_capacity(data.samples.len());
        let mut values = Vec::with_capacity(data.samples.len());
        for Sample { timestamp, value } in data.samples.iter() {
            timestamps.push(*timestamp);
            values.push(*value);
        }
        QueryResult {
            metric: data.labels.get_metric_name(),
            timestamps,
            values,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SearchQueryResponse {
    pub series: Vec<QuerySeriesData>,
}

impl Serialized for SearchQueryRequest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);

        // Convert required_tag_filters if present
        let matchers = Some(serialize_matchers(&mut bldr, &self.matchers));

        // Create the SearchQuery object
        let args = SearchQueryRequestArgs {
            start: self.start,
            end: self.end,
            matchers,
            limit: self.max_metrics as u32,
        };

        let obj = FBSearchQueryRequest::create(&mut bldr, &args);

        bldr.finish(obj, None);
        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for SearchQueryRequest {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBSearchQueryRequest>(buf, "SearchQuery")?;

        // Extract simple fields
        let start = req.start();
        let end = req.end();
        let limit = req.limit() as usize;

        // Extract matchers if present
        let matchers = if let Some(ref filters) = req.matchers() {
            deserialize_matchers(filters)?
        } else {
            // todo: user error_consts
            return Err(ValkeyError::String(
                "TSDB: Matchers are required".to_string(),
            ));
        };

        Ok(SearchQueryRequest {
            start,
            end,
            matchers,
            max_metrics: limit,
        })
    }
}

fn deserialize_query_series_data(chunk: SeriesChunk) -> ValkeyResult<QuerySeriesData> {
    let samples = chunk
        .data()
        .ok_or_else(|| ValkeyError::String("TSDB: SeriesChunk data is missing".to_string()))?;

    let labels = chunk
        .labels()
        .ok_or_else(|| ValkeyError::String("TSDB: SeriesChunk labels are missing".to_string()))?;

    let mut resolved_labels: InternedMetricName = InternedMetricName::with_capacity(labels.len());
    for label in labels.iter() {
        match (label.name(), label.value()) {
            (Some(name), Some(value)) => {
                resolved_labels.add_label(name, value);
            }
            _ => {
                // todo: log an error
            }
        }
    }

    let sample_chunk = deserialize_sample_data(&samples)?;
    let samples = sample_chunk
        .iter()
        .map(|s| Sample {
            timestamp: s.timestamp,
            value: s.value,
        })
        .collect();

    Ok(QuerySeriesData {
        samples,
        labels: resolved_labels,
    })
}

impl Serialized for SearchQueryResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(1024);

        let mut series_offsets = Vec::with_capacity(self.series.len());
        for data in self.series.iter() {
            let chunk = samples_to_chunk(&data.samples)
                .expect("TSDB: Failed to serialize search query response");
            let series_chunk = serialize_chunk(&mut bldr, chunk, None, &data.labels)
                .expect("TSDB: Failed to serialize series chunk");
            series_offsets.push(series_chunk);
        }

        let series_offsets = bldr.create_vector(&series_offsets);
        let obj = FBSearchQueryResponse::create(
            &mut bldr,
            &SearchQueryResponseArgs {
                series: Some(series_offsets),
            },
        );

        bldr.finish(obj, None);

        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for SearchQueryResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBSearchQueryResponse>(buf, "SearchQueryResponse")?;
        let mut series = Vec::with_capacity(req.series().unwrap_or_default().len());

        for chunk in req.series().unwrap_or_default() {
            let data = deserialize_query_series_data(chunk)?;
            series.push(data);
        }

        Ok(SearchQueryResponse { series })
    }
}
