use crate::common::Timestamp;
use crate::error::{TsdbError, TsdbResult};
use pco::data_types::Number;
use pco::standalone::{simple_compress, simple_decompress};
use pco::DEFAULT_COMPRESSION_LEVEL;
use pco::{ChunkConfig, DeltaSpec};
use std::error::Error;

// mirror ChunkConfig here so downstream users don't need to import pco
#[derive(Clone, Debug)]
pub struct CompressorConfig {
    pub compression_level: usize,
    pub delta_encoding_order: usize,
}

impl Default for CompressorConfig {
    fn default() -> Self {
        Self {
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            delta_encoding_order: 0,
        }
    }
}

pub fn pco_encode<T: Number>(src: &[T], dst: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
    let config = ChunkConfig::default();
    if src.is_empty() {
        return Ok(());
    }

    let compressed = simple_compress(src, &config)?;
    dst.extend_from_slice(&compressed);
    Ok(())
}

pub fn encode_with_options<T: Number>(
    src: &[T],
    dst: &mut Vec<u8>,
    options: CompressorConfig,
) -> Result<(), Box<dyn Error>> {
    let mut config = ChunkConfig::default();
    config.compression_level = options.compression_level;
    if options.delta_encoding_order != 0 {
        config.delta_spec = DeltaSpec::TryConsecutive(options.delta_encoding_order);
    }

    let compressed = simple_compress(src, &config)?;
    dst.extend_from_slice(&compressed);
    Ok(())
}

pub fn pco_decode<T: Number>(src: &[u8], dst: &mut Vec<T>) -> Result<(), Box<dyn Error>> {
    if src.is_empty() {
        return Ok(());
    }
    match simple_decompress(src) {
        Ok(values) => dst.extend_from_slice(&values),
        Err(e) => return Err(Box::new(e)),
    }
    Ok(())
}

pub(super) fn compress_values(compressed: &mut Vec<u8>, values: &[f64]) -> TsdbResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    pco_encode(values, compressed).map_err(|e| TsdbError::CannotSerialize(format!("values: {}", e)))
}

pub(super) fn decompress_values(compressed: &[u8], dst: &mut Vec<f64>) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }
    pco_decode(compressed, dst).map_err(|e| TsdbError::CannotDeserialize(format!("values: {}", e)))
}

pub(super) fn compress_timestamps(
    compressed: &mut Vec<u8>,
    timestamps: &[Timestamp],
) -> TsdbResult<()> {
    if timestamps.is_empty() {
        return Ok(());
    }
    let config = CompressorConfig {
        compression_level: DEFAULT_COMPRESSION_LEVEL,
        delta_encoding_order: 2,
    };
    encode_with_options(timestamps, compressed, config)
        .map_err(|e| TsdbError::CannotSerialize(format!("timestamps: {}", e)))
}

pub(super) fn decompress_timestamps(compressed: &[u8], dst: &mut Vec<i64>) -> TsdbResult<()> {
    if compressed.is_empty() {
        return Ok(());
    }
    pco_decode(compressed, dst)
        .map_err(|e| TsdbError::CannotDeserialize(format!("timestamps: {}", e)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn encode_no_values() {
        let src: Vec<f64> = vec![];
        let mut dst = vec![];

        // check for error
        super::pco_encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        let exp: Vec<u8> = Vec::new();
        assert_eq!(dst.to_vec(), exp);
    }
}
