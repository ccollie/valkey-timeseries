use crate::common::{Sample, Timestamp};
use crate::error::{TsdbError, TsdbResult};
use crate::error_consts;
use crate::series::chunks::TimeSeriesChunk;
use crate::series::types::ValueFilter;
use crate::series::{DuplicatePolicy, SampleAddResult};
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::vec;
use valkey_module::{raw, ValkeyError, ValkeyResult};

pub const MIN_CHUNK_SIZE: usize = 48;
pub const MAX_CHUNK_SIZE: usize = 1048576;

#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize, GetSize)]
#[non_exhaustive]
pub enum ChunkEncoding {
    Uncompressed = 1,
    #[default]
    Gorilla = 2,
    Pco = 4,
}

impl ChunkEncoding {
    pub fn name(&self) -> &'static str {
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

impl TryFrom<&str> for ChunkEncoding {
    type Error = ValkeyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if let Some(compression) = parse_encoding(s) {
            return Ok(compression);
        }
        Err(ValkeyError::Str(error_consts::INVALID_CHUNK_COMPRESSION))
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
}

pub struct ChunkSampleIterator<'a> {
    inner: vec::IntoIter<Sample>,
    chunk: &'a TimeSeriesChunk,
    value_filter: &'a Option<ValueFilter>,
    ts_filter: &'a Option<Vec<Timestamp>>,
    start: Timestamp,
    end: Timestamp,
    is_overlap: bool,
    is_init: bool,
}

impl<'a> ChunkSampleIterator<'a> {
    pub fn new(
        chunk: &'a TimeSeriesChunk,
        start: Timestamp,
        end: Timestamp,
        value_filter: &'a Option<ValueFilter>,
        ts_filter: &'a Option<Vec<Timestamp>>,
    ) -> Self {
        Self {
            inner: Default::default(),
            start,
            end,
            chunk,
            value_filter,
            ts_filter,
            is_overlap: chunk.overlaps(start, end),
            is_init: false,
        }
    }

    fn handle_init(&mut self) {
        self.is_init = true;
        self.inner = if !self.is_overlap {
            Default::default()
        } else {
            self.chunk
                .get_range_filtered(self.start, self.end, self.ts_filter, self.value_filter)
                .into_iter()
        }
    }
}

// todo: implement next_chunk
impl Iterator for ChunkSampleIterator<'_> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_init {
            self.handle_init();
        }
        self.inner.next()
    }
}

pub(crate) fn validate_chunk_size(chunk_size_bytes: usize) -> TsdbResult<()> {
    fn get_error_result() -> TsdbResult<()> {
        let msg = format!("ERR: CHUNK_SIZE value must be a multiple of 2 in the range [{MIN_CHUNK_SIZE} .. {MAX_CHUNK_SIZE}]");
        Err(TsdbError::InvalidConfiguration(msg))
    }

    if chunk_size_bytes < MIN_CHUNK_SIZE {
        return get_error_result();
    }

    if chunk_size_bytes > MAX_CHUNK_SIZE {
        return get_error_result();
    }

    if chunk_size_bytes % 2 != 0 {
        return get_error_result();
    }

    Ok(())
}

#[cfg(test)]
mod tests {}
