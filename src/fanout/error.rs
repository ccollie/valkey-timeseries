// Copyright (c) 2013-2015 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

/// An enum value or union discriminant that was not found among those defined in a schema.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct NotInSchema(pub u16);

impl core::fmt::Display for NotInSchema {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(
            fmt,
            "Enum value or union discriminant {} was not present in the schema.",
            self.0
        )
    }
}

impl core::error::Error for NotInSchema {
    fn description(&self) -> &str {
        "Enum value or union discriminant was not present in schema."
    }
}

/// Because messages are lazily validated, the return type of any method that reads a pointer field
/// must be wrapped in a Result.
pub type Result<T> = core::result::Result<T, Error>;

/// Describes an arbitrary error that prevented an operation from completing.
#[derive(Debug, Clone)]
pub struct Error {
    /// The general kind of the error. Code that decides how to respond to an error
    /// should read only this field in making its decision.
    pub kind: ErrorKind,

    /// Extra context about error
    pub extra: String,
}

/// The general nature of an error. The purpose of this enum is not to describe the error itself,
/// but rather to describe how the client might want to respond to the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum ErrorKind {
    /// Something went wrong
    Failed,

    /// The call failed because of a temporary lack of resources. This could be space resources
    /// (out of memory, out of disk space) or time resources (request queue overflow, operation
    /// timed out).
    ///
    /// The operation might work if tried again, but it should NOT be repeated immediately as this
    /// may simply exacerbate the problem.
    Overloaded,

    /// The call required communication over a connection that has been lost. The caller will need
    /// to re-establish connections and try again.
    Disconnected,

    /// Empty buffer
    EmptyBuffer,

    /// Message is too large
    MessageTooLarge(usize),

    /// Premature end of stream
    PrematureEndOfStream,

    /// type mismatch
    TypeMismatch,

    /// Unknown message type.
    UnknownMessageType,

    PermissionDenied,

    KeyPermissionDenied,

    SerializationError,

    BadRequestId,
}

impl Error {
    /// Writes to the `extra` field. Does nothing if the "alloc" feature is not enabled.
    /// This is intended to be used with the `write!()` macro from core.
    pub fn write_fmt(&mut self, fmt: core::fmt::Arguments<'_>) {
        use core::fmt::Write;
        let _ = self.extra.write_fmt(fmt);
    }

    pub fn failed(description: String) -> Self {
        Self {
            extra: description,
            kind: ErrorKind::Failed,
        }
    }

    pub fn from_kind(kind: ErrorKind) -> Self {
        Self {
            kind,
            extra: String::new(),
        }
    }

    pub fn overloaded(description: String) -> Self {
        Self {
            extra: description,
            kind: ErrorKind::Overloaded,
        }
    }
    pub fn disconnected(description: String) -> Self {
        Self {
            extra: description,
            kind: ErrorKind::Disconnected,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        use std::io;
        let kind = match err.kind() {
            io::ErrorKind::TimedOut => ErrorKind::Overloaded,
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected => ErrorKind::Disconnected,
            io::ErrorKind::UnexpectedEof => ErrorKind::PrematureEndOfStream,
            _ => ErrorKind::Failed,
        };

        Self {
            kind,
            extra: format!("{err}"),
        }
    }
}

impl core::fmt::Display for ErrorKind {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        match self {
            Self::Failed => write!(fmt, "Failed"),
            Self::Overloaded => write!(fmt, "Overloaded"),
            Self::Disconnected => write!(fmt, "Disconnected"),
            Self::EmptyBuffer => write!(fmt, "empty buffer"),
            Self::MessageTooLarge(val) => write!(fmt, "Message is too large: {val}"),
            Self::PrematureEndOfStream => write!(fmt, "Premature end of stream"),
            Self::TypeMismatch => write!(fmt, "type mismatch"),
            Self::PermissionDenied => write!(fmt, "Permission denied"),
            Self::KeyPermissionDenied => {
                write!(fmt, "User does not have access to one or more keys")
            }
            Self::UnknownMessageType => write!(fmt, "Unknown message type."),
            Self::SerializationError => write!(fmt, "Serialization error"),
            Self::BadRequestId => write!(fmt, "Bad request id"),
        }
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
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
