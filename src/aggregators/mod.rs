use crate::common::Timestamp;
use crate::error_consts;
use crate::parser::timestamp::parse_timestamp;
use valkey_module::{ValkeyError, ValkeyString};

mod handlers;
mod iterator;

pub use handlers::*;
pub use iterator::*;

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketTimestamp {
    #[default]
    Start,
    End,
    Mid,
}

impl BucketTimestamp {
    pub fn calculate(&self, ts: Timestamp, time_delta: u64) -> Timestamp {
        match self {
            Self::Start => ts,
            Self::Mid => ts.saturating_add_unsigned(time_delta / 2),
            Self::End => ts.saturating_add_unsigned(time_delta),
        }
    }
}
impl TryFrom<&str> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() == 1 {
            let c = value.chars().next().unwrap();
            match c {
                '-' => return Ok(BucketTimestamp::Start),
                '+' => return Ok(BucketTimestamp::End),
                '~' => return Ok(BucketTimestamp::Mid),
                _ => {}
            }
        }
        match value {
            value if value.eq_ignore_ascii_case("start") => return Ok(BucketTimestamp::Start),
            value if value.eq_ignore_ascii_case("end") => return Ok(BucketTimestamp::End),
            value if value.eq_ignore_ascii_case("mid") => return Ok(BucketTimestamp::Mid),
            _ => {}
        }
        Err(ValkeyError::Str("TSDB: invalid BUCKETTIMESTAMP parameter"))
    }
}

impl TryFrom<&ValkeyString> for BucketTimestamp {
    type Error = ValkeyError;
    fn try_from(value: &ValkeyString) -> Result<Self, Self::Error> {
        value.to_string_lossy().as_str().try_into()
    }
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BucketAlignment {
    #[default]
    Default,
    Start,
    End,
    Timestamp(Timestamp),
}

impl BucketAlignment {
    pub fn get_aligned_timestamp(&self, start: Timestamp, end: Timestamp) -> Timestamp {
        match self {
            BucketAlignment::Default => 0,
            BucketAlignment::Start => start,
            BucketAlignment::End => end,
            BucketAlignment::Timestamp(ts) => *ts,
        }
    }
}

impl TryFrom<&str> for BucketAlignment {
    type Error = ValkeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let alignment = match value {
            arg if arg.eq_ignore_ascii_case("start") => BucketAlignment::Start,
            arg if arg.eq_ignore_ascii_case("end") => BucketAlignment::End,
            arg if arg.len() == 1 => {
                let c = arg.chars().next().unwrap();
                match c {
                    '-' => BucketAlignment::Start,
                    '+' => BucketAlignment::End,
                    _ => return Err(ValkeyError::Str(error_consts::INVALID_ALIGN)),
                }
            }
            _ => {
                let timestamp = parse_timestamp(value)
                    .map_err(|_| ValkeyError::Str(error_consts::INVALID_ALIGN))?;
                BucketAlignment::Timestamp(timestamp)
            }
        };
        Ok(alignment)
    }
}
