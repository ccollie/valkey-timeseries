use flatbuffers::{FlatBufferBuilder, WIPOffset};
use super::response_generated::{CompressionType, SeriesChunk, SeriesChunkArgs};
use crate::series::chunks::TimeSeriesChunk;
use bincode::serde::{
    encode_to_vec,
    decode_from_slice,
    encode_into_slice,
    encode_into_std_write,
};
use bincode::config;

// for future compatibility
const VERSION: u32 = 1;

pub(super) fn serialize_chunk<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    chunk: &TimeSeriesChunk,
) -> WIPOffset<SeriesChunk<'a>> {
    let count = chunk.len() as u64;
    let compression =  match chunk {
        TimeSeriesChunk::Uncompressed(_) => {
            CompressionType::None
        },
        TimeSeriesChunk::Gorilla(_) => {
            CompressionType::Gorilla
        },
        TimeSeriesChunk::Pco(_) => {
            CompressionType::Pco
        },
    };
    SeriesChunk::create(
        bldr,
        &SeriesChunkArgs {
            version: VERSION,
            count,
            compression,
            data: None,
        },
    )
}


fn serialize_internal<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    chunk: &TimeSeriesChunk,
    dest: &mut Vec<u8>,
) -> WIPOffset<Vec<u8>> {
    match chunk {
        TimeSeriesChunk::Uncompressed(data) => {
            let serialized = bincode::(data, dest, config::standard() ).unwrap();
            bldr.create_vector(data)
        },
        TimeSeriesChunk::Gorilla(data) => {
            bldr.create_vector(data)
        },
        TimeSeriesChunk::Pco(data) => {
            bldr.create_vector(data)
        },
    }
}