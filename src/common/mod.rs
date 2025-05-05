use std::io::Write;
use valkey_module::ValkeyResult;

pub mod binary_search;
pub mod binop;
pub mod constants;
pub mod db;
pub mod hash;
pub mod humanize;
pub mod parallel;
pub mod pool;
pub mod rounding;
pub mod rdb;
pub mod time;
mod types;
pub mod unit_vec;
mod utils;
pub mod ids;

pub use types::*;

/// A trait for types that can be serialized to and deserialized from a byte stream.
///
/// The generic parameter `T` represents the type that will be produced when deserializing.
/// This is typically the implementing type itself, but allows for flexibility when needed.
pub trait Serialized<T> {
    /// Serializes the implementing type to the provided writer.
    fn serialize<W: Write>(&self, writer: &mut W);

    /// Deserializes an instance of type `T` from the provided reader.
    fn deserialize(buf: &[u8]) -> ValkeyResult<T>;
}