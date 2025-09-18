use super::generated::{CompressionType, Label as FanoutLabel, SampleData, SeriesResponse};
use crate::error_consts;
use crate::labels::Label;
use crate::series::chunks::samples_to_chunk;
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
        .map_err(|_| ValkeyError::Str("Invalid compression type in SampleData"))?;

    let deserialized = TimeSeriesChunk::deserialize(data.as_ref())
        .map_err(|_| ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION))?; // ?? better error

    let encoding: ChunkEncoding = encoding.into();
    if encoding != deserialized.get_encoding() {
        // todo: log
        return Err(ValkeyError::Str(error_consts::CHUNK_DECOMPRESSION));
    }
    Ok(deserialized)
}

impl TryFrom<MRangeSeriesResult> for SeriesResponse {
    type Error = ValkeyError;

    fn try_from(value: MRangeSeriesResult) -> Result<Self, Self::Error> {
        let group_label_value = value.group_label_value.unwrap_or_default();

        let labels = convert_labels(value.labels);
        let sample_chunk = samples_to_chunk(&value.samples)?;
        let sample_data = serialize_chunk(sample_chunk)
            .map_err(|e| ValkeyError::String(format!("Failed to serialize sample data: {e}")))?;

        Ok(SeriesResponse {
            key: value.key,
            group_label_value,
            labels,
            samples: Some(sample_data),
        })
    }
}

fn convert_labels(labels: Vec<Option<Label>>) -> Vec<FanoutLabel> {
    labels
        .into_iter()
        .map(|label| {
            label.map_or_else(
                || FanoutLabel {
                    name: String::new(),
                    value: String::new(),
                },
                |label| FanoutLabel {
                    name: label.name,
                    value: label.value,
                },
            )
        })
        .collect()
}

impl TryFrom<SeriesResponse> for MRangeSeriesResult {
    type Error = ValkeyError;

    fn try_from(value: SeriesResponse) -> Result<Self, Self::Error> {
        let key = value.key;
        let group_label_value = Some(value.group_label_value);
        let samples = match value.samples.as_ref() {
            Some(sample_data) => {
                let chunk = deserialize_chunk(sample_data)?;
                chunk.iter().collect()
            }
            None => Vec::new(),
        };

        let labels = value
            .labels
            .into_iter()
            .map(|label| {
                if label.name.is_empty() {
                    None
                } else {
                    Some(Label {
                        name: label.name,
                        value: label.value,
                    })
                }
            })
            .collect::<Vec<_>>();

        Ok(MRangeSeriesResult {
            key,
            group_label_value,
            samples,
            labels,
        })
    }
}
