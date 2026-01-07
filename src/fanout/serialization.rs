use crate::error_consts;
use prost::Message;
use valkey_module::{ValkeyError, ValkeyResult};

/// A trait for types that can be serialized to and deserialized from a byte stream.
///
/// The generic parameter `T` represents the type that will be produced when deserializing.
/// This is typically the implementing type itself but allows for flexibility when needed.
pub trait Serialized {
    /// Serializes the implementing type to the provided writer.
    fn serialize(&self, dest: &mut Vec<u8>);
}

pub trait Deserialized: Sized {
    /// Deserializes an instance of type `T` from the provided buffer.
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self>;
}

/// Trait that must be implemented for Request and Response types to be used as
/// messages in the fanout system.
pub trait Serializable: Serialized + Deserialized {}

impl<T> Serialized for T
where
    T: Message + Default,
{
    fn serialize(&self, dest: &mut Vec<u8>) {
        let size = self.encoded_len();
        dest.reserve(size);
        self.encode_raw(dest)
    }
}

impl<T> Deserialized for T
where
    T: Message + Default,
{
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        T::decode(buf).map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))
    }
}

impl<T> Serializable for T where T: Serialized + Deserialized {}
