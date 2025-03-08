use crate::common::Sample;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Enum for various errors in Tsdb.
pub enum TsdbError {
    #[error("Chunk at full capacity. Max capacity {0}.")]
    CapacityFull(usize),

    #[error("Invalid configuration. {0}")]
    InvalidConfiguration(String),

    #[error("Decoding error. {0}")]
    DecodingError(String),

    #[error("Serialization error. {0}")]
    CannotSerialize(String),

    #[error("Cannot deserialize. {0}")]
    CannotDeserialize(String),

    #[error("Compression error. {0}")]
    CannotCompress(String),

    #[error("Decompression error. {0}")]
    CannotDecompress(String),

    #[error("Duplicate sample. {0}")] // need better error
    DuplicateSample(String),

    #[error("Invalid metric: {0}")]
    InvalidMetric(String),

    #[error("Invalid compressed method. {0}")]
    InvalidCompression(String),

    #[error("Error removing range")]
    RemoveRangeError,

    #[error("Invalid number. {0}")]
    InvalidNumber(String),

    #[error("Invalid series selector. {0}")]
    InvalidSeriesSelector(String),

    #[error("Sample timestamp exceeds retention period")]
    SampleTooOld,

    #[error("Error adding sample. {0:?}")]
    CannotAddSample(Sample),

    #[error("{0}")]
    General(String),

    #[error("End of stream")]
    EndOfStream,
}

pub type TsdbResult<T = ()> = Result<T, TsdbError>;

impl From<&str> for TsdbError {
    fn from(s: &str) -> Self {
        TsdbError::General(s.to_string())
    }
}

impl From<String> for TsdbError {
    fn from(s: String) -> Self {
        TsdbError::General(s)
    }
}

// impl Into<ValkeyError> for TsdbError {
//   fn into(self) -> ValkeyError {
//     let msg = format!("TSDB: {}", self.to_string());
//     ValkeyError::String(msg)
//   }
// }
