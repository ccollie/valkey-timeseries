use crate::common::Sample;
use crate::series::chunks::{GorillaChunk, PcoChunk, TimeSeriesChunk, UncompressedChunk};
use valkey_module::{ValkeyError, ValkeyResult};

// for future compatibility
const VERSION: u32 = 1;

pub fn samples_to_chunk(samples: &[Sample]) -> ValkeyResult<TimeSeriesChunk> {
    let mut chunk = if samples.len() >= 1000 {
        TimeSeriesChunk::Pco(PcoChunk::default())
    } else if samples.len() >= 5 {
        TimeSeriesChunk::Gorilla(GorillaChunk::default())
    } else {
        TimeSeriesChunk::Uncompressed(UncompressedChunk::default())
    };
    chunk
        .set_data(samples)
        .map_err(|e| ValkeyError::String(format!("Failed to set chunk data: {e}")))?;
    Ok(chunk)
}
