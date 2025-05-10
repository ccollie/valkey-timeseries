use crate::error_consts;
use crate::fanout::request::common::load_flatbuffers_object;
use crate::fanout::request::response_generated::{
    ErrorKind as FBErrorKind, ErrorResponse as FBErrorResponse, ErrorResponseArgs,
};
use crate::fanout::serialization::{Deserialized, Serialized};
use flatbuffers::FlatBufferBuilder;
use valkey_module::{ValkeyError, ValkeyResult};

/// Multi-shard error. Designed mostly for compactness since it's sent over the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,

    /// Extra context about error
    pub extra: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum ErrorKind {
    /// Something went wrong
    Failed = 0,

    /// A cluster node was unreachable.
    NodeUnreachable = 1,

    /// One or more of the nodes in the cluster did not respond to the request in time.
    Timeout = 2,

    /// Unknown message type.
    UnknownMessageType = 3,

    Permissions = 4,

    /// The user does not have access to one or more keys required to fulfill the request.
    KeyPermissions = 5,

    /// Error during serialization or deserialization of the request or response.
    Serialization = 6,

    BadRequestId = 7,
}

impl Error {
    pub fn failed(description: String) -> Self {
        Self {
            extra: description,
            kind: ErrorKind::Failed,
        }
    }

    pub fn serialization(description: String) -> Self {
        Self {
            extra: description,
            kind: ErrorKind::Serialization,
        }
    }

    pub fn key_permissions(description: String) -> Self {
        Self {
            extra: description,
            kind: ErrorKind::KeyPermissions,
        }
    }
}

impl TryFrom<u8> for ErrorKind {
    type Error = ValkeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ErrorKind::Failed),
            1 => Ok(ErrorKind::NodeUnreachable),
            2 => Ok(ErrorKind::Timeout),
            3 => Ok(ErrorKind::UnknownMessageType),
            4 => Ok(ErrorKind::Permissions),
            5 => Ok(ErrorKind::KeyPermissions),
            6 => Ok(ErrorKind::Serialization),
            7 => Ok(ErrorKind::BadRequestId),
            _ => {
                let msg = format!("Invalid error kind: {value}");
                Err(ValkeyError::String(msg))
            }
        }
    }
}

impl core::fmt::Display for ErrorKind {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        match self {
            Self::Failed => write!(fmt, "Failed"),
            Self::Permissions => write!(fmt, "Permission denied"),
            Self::KeyPermissions => {
                write!(fmt, "User does not have access to one or more keys")
            }
            Self::UnknownMessageType => write!(fmt, "Unknown message type."),
            Self::Serialization => write!(fmt, "Serialization error"),
            Self::BadRequestId => write!(fmt, "Bad request id"),
            Self::Timeout => write!(fmt, "Multi-shard command timed out"),
            Self::NodeUnreachable => write!(fmt, "Cluster node unreachable"),
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> Result<(), core::fmt::Error> {
        if self.extra.is_empty() {
            write!(fmt, "{}", self.kind)
        } else {
            write!(fmt, "{}: {}", self.kind, self.extra)
        }
    }
}

impl core::error::Error for Error {
    fn description(&self) -> &str {
        &self.extra
    }
    fn cause(&self) -> Option<&dyn (core::error::Error)> {
        None
    }
}

/// Converts an error string into a structured Error object.
/// Maps known error strings to specific ErrorKind variants,
/// or falls back to a general Failed error with the original message.
fn convert_from_string(err: &str) -> Error {
    if err.is_empty() {
        return Error {
            kind: ErrorKind::Failed,
            extra: String::new(),
        };
    }

    match err {
        error_consts::COMMAND_DESERIALIZATION_ERROR => Error::serialization(String::new()),
        error_consts::NO_CLUSTER_NODES_AVAILABLE => Error {
            kind: ErrorKind::NodeUnreachable,
            extra: String::new(),
        },
        error_consts::KEY_READ_PERMISSION_ERROR => Error::key_permissions(String::new()),
        _ if err.contains("permission") => Error {
            kind: ErrorKind::Permissions,
            extra: String::new(),
        },
        _ => Error::failed(err.to_string()),
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        convert_from_string(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        convert_from_string(&value)
    }
}

impl From<ValkeyError> for Error {
    fn from(value: ValkeyError) -> Self {
        match value {
            ValkeyError::Str(msg) => convert_from_string(msg),
            ValkeyError::String(err) => convert_from_string(&err),
            _ => Error::failed(value.to_string()),
        }
    }
}

impl Serialized for Error {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(128);
        let kind: FBErrorKind = match self.kind {
            ErrorKind::Failed => FBErrorKind::Failed,
            ErrorKind::NodeUnreachable => FBErrorKind::NodeUnreachable,
            ErrorKind::Timeout => FBErrorKind::Timeout,
            ErrorKind::UnknownMessageType => FBErrorKind::UnknownMessageType,
            ErrorKind::Permissions => FBErrorKind::Permissions,
            ErrorKind::KeyPermissions => FBErrorKind::KeyPermissions,
            ErrorKind::Serialization => FBErrorKind::Serialization,
            ErrorKind::BadRequestId => FBErrorKind::BadRequestId,
        };
        let extra = bldr.create_string(&self.extra);
        let args = ErrorResponseArgs {
            kind,
            extra: Some(extra),
        };
        let obj = FBErrorResponse::create(&mut bldr, &args);
        bldr.finish(obj, None);
        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for Error {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        let req = load_flatbuffers_object::<FBErrorResponse>(buf, "Error")?;
        let kind = match req.kind() {
            FBErrorKind::Failed => ErrorKind::Failed,
            FBErrorKind::NodeUnreachable => ErrorKind::NodeUnreachable,
            FBErrorKind::Timeout => ErrorKind::Timeout,
            FBErrorKind::UnknownMessageType => ErrorKind::UnknownMessageType,
            FBErrorKind::Permissions => ErrorKind::Permissions,
            FBErrorKind::KeyPermissions => ErrorKind::KeyPermissions,
            FBErrorKind::Serialization => ErrorKind::Serialization,
            FBErrorKind::BadRequestId => ErrorKind::BadRequestId,
            _ => {
                let msg = format!("Invalid error kind: {:?}", req.kind());
                return Err(ValkeyError::String(msg));
            }
        };
        let extra = req.extra().map_or_else(String::new, |x| x.to_string());
        Ok(Error { kind, extra })
    }
}
