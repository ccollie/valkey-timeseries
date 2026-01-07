use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::{DuplicatePolicy, SampleAddResult};
use get_size2::GetSize;
use std::fmt::Display;
use valkey_module::digest::Digest;
use valkey_module::{ValkeyError, ValkeyResult, raw};

pub const MIN_CHUNK_SIZE: usize = 48;
pub const MAX_CHUNK_SIZE: usize = 1048576;

#[derive(Copy, Clone, Debug, Default, PartialEq, GetSize, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub enum ChunkEncoding {
    Uncompressed = 1,
    #[default]
    Gorilla = 2,
    Pco = 4,
}

impl ChunkEncoding {
    pub const fn name(&self) -> &'static str {
        match self {
            ChunkEncoding::Uncompressed => "uncompressed",
            ChunkEncoding::Gorilla => "gorilla",
            ChunkEncoding::Pco => "pco",
        }
    }

    pub fn is_compressed(&self) -> bool {
        *self != ChunkEncoding::Uncompressed
    }
}

impl Display for ChunkEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl TryFrom<u8> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ChunkEncoding::Uncompressed),
            2 => Ok(ChunkEncoding::Gorilla),
            4 => Ok(ChunkEncoding::Pco),
            _ => Err(ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING)),
        }
    }
}

impl TryFrom<&str> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if let Some(compression) = parse_encoding(s) {
            return Ok(compression);
        }
        Err(ValkeyError::Str(error_consts::INVALID_CHUNK_ENCODING))
    }
}

impl TryFrom<String> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        ChunkEncoding::try_from(&s[..])
    }
}

fn parse_encoding(encoding: &str) -> Option<ChunkEncoding> {
    hashify::tiny_map_ignore_case! {
        encoding.as_bytes(),
        "compressed" => ChunkEncoding::default(),
        "uncompressed" => ChunkEncoding::Uncompressed,
        "gorilla" => ChunkEncoding::Gorilla,
        "pco" => ChunkEncoding::Pco,
    }
}

pub trait Chunk: Sized {
    fn first_timestamp(&self) -> Timestamp;
    fn last_timestamp(&self) -> Timestamp;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn last_value(&self) -> f64;
    fn size(&self) -> usize;
    fn max_size(&self) -> usize;
    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize>;
    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()>;
    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>>;

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize>;

    /// Efficiently merge a slice of Sample objects into the chunk.
    ///
    /// * `samples` - A slice of Sample objects to be merged into the chunk.
    /// * `dp_policy` - An optional DuplicatePolicy that specifies how to handle duplicate samples.
    ///
    /// ### Returns
    /// The method returns a TsdbResult containing a vector of SampleAddResult objects, which indicate
    /// the result of adding each sample.
    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>>;

    fn split(&mut self) -> TsdbResult<Self>;

    fn optimize(&mut self) -> TsdbResult<()> {
        Ok(())
    }

    fn save_rdb(&self, rdb: *mut raw::RedisModuleIO);
    fn load_rdb(rdb: *mut raw::RedisModuleIO, enc_ver: i32) -> ValkeyResult<Self>;

    fn serialize(&self, dest: &mut Vec<u8>);

    fn deserialize(buf: &[u8]) -> TsdbResult<Self>;

    fn debug_digest(&self, dig: &mut Digest);
}

pub(crate) fn validate_chunk_size(chunk_size_bytes: usize) -> TsdbResult<()> {
    fn get_error_result() -> TsdbResult<()> {
        let msg = format!(
            "TSDB: CHUNK_SIZE value must be a multiple of 8 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]"
        );
        Err(TsdbError::InvalidConfiguration(msg))
    }

    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size_bytes) {
        return get_error_result();
    }

    if !chunk_size_bytes.is_multiple_of(8) {
        return get_error_result();
    }

    Ok(())
}

#[cfg(test)]
mod tests {}
