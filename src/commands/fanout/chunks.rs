use super::generated::{CompressionType, Label as FanoutLabel, SampleData, SeriesRangeResponse};
use crate::common::logging::log_warning;
use crate::error_consts;
use crate::labels::Label;
use crate::series::TimeSeries;
use crate::series::chunks::{Chunk, ChunkEncoding, TimeSeriesChunk};
use crate::series::request_types::MRangeSeriesResult;
use valkey_module::{ValkeyError, ValkeyResult};

// for future compatibility
const VERSION: u32 = 1;

fn get_compression_type(chunk: &TimeSeriesChunk) -> CompressionType {
    chunk.get_encoding().into()
}

/// Serializes a TimeSeriesChunk for fanout.
pub fn serialize_chunk(chunk: TimeSeriesChunk) -> ValkeyResult<SampleData> {
    let compression = get_compression_type(&chunk);
    // estimate of the size needed
    let capacity = chunk.len() * chunk.bytes_per_sample();
    let mut data: Vec<u8> = Vec::with_capacity(capacity);
    chunk.serialize(&mut data);

    Ok(SampleData {
        version: VERSION,
        compression: compression.into(),
        data,
    })
}

/// Deserializes TimeSeriesChunk coming from a cluster node.
pub fn deserialize_chunk(chunk: &SampleData) -> ValkeyResult<TimeSeriesChunk> {
    let data = &chunk.data;

    let encoding = CompressionType::try_from(chunk.compression)
        .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?;

    let deserialized = TimeSeriesChunk::deserialize(data.as_ref())
        .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?; // ?? better error

    let encoding: ChunkEncoding = encoding.into();
    if encoding != deserialized.get_encoding() {
        log_warning("Invalid encoding type for deserialized time series chunk");
        return Err(ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION));
    }
    Ok(deserialized)
}

impl TryFrom<SampleData> for TimeSeriesChunk {
    type Error = ValkeyError;

    fn try_from(value: SampleData) -> Result<Self, Self::Error> {
        deserialize_chunk(&value)
    }
}

impl TryFrom<&SampleData> for TimeSeriesChunk {
    type Error = ValkeyError;

    fn try_from(value: &SampleData) -> Result<Self, Self::Error> {
        deserialize_chunk(value)
    }
}

impl TryFrom<&SampleData> for TimeSeries {
    type Error = ValkeyError;

    fn try_from(value: &SampleData) -> Result<Self, Self::Error> {
        let chunk = deserialize_chunk(value)?;
        TimeSeries::from_chunk(chunk).map_err(|e| {
            ValkeyError::String(format!("Failed to create time series from chunk: {e}"))
        })
    }
}

impl TryFrom<SampleData> for TimeSeries {
    type Error = ValkeyError;

    fn try_from(value: SampleData) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<MRangeSeriesResult> for SeriesRangeResponse {
    type Error = ValkeyError;

    fn try_from(value: MRangeSeriesResult) -> Result<Self, Self::Error> {
        let group_label_value = value.group_label_value.unwrap_or_default();

        let labels = convert_labels(value.labels);
        let data = serialize_chunk(value.data)
            .map_err(|e| ValkeyError::String(format!("Failed to serialize sample data: {e}")))?;

        Ok(SeriesRangeResponse {
            key: value.key,
            group_label_value,
            labels,
            samples: Some(data),
        })
    }
}

fn convert_labels(labels: Vec<Label>) -> Vec<FanoutLabel> {
    labels
        .into_iter()
        .map(|label| FanoutLabel {
            name: label.name,
            value: label.value,
        })
        .collect()
}

impl TryFrom<SeriesRangeResponse> for MRangeSeriesResult {
    type Error = ValkeyError;

    fn try_from(value: SeriesRangeResponse) -> Result<Self, Self::Error> {
        let key = value.key;
        let group_label_value = Some(value.group_label_value);
        let data = if let Some(data) = &value.samples {
            deserialize_chunk(data)?
        } else {
            TimeSeriesChunk::default()
        };

        let labels: Vec<Label> = value
            .labels
            .into_iter()
            .map(|label| Label {
                name: label.name,
                value: label.value,
            })
            .collect::<Vec<_>>();

        Ok(MRangeSeriesResult {
            key,
            group_label_value,
            labels,
            data,
        })
    }
}
