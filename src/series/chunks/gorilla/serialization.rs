use crate::series::chunks::stream::bitstream::BitStream;
use valkey_module::{ValkeyError, raw};

pub(crate) fn save_bitwriter_to_rdb(rdb: *mut raw::RedisModuleIO, writer: &BitStream) {
    let bytes = writer.get_ref();
    raw::save_slice(rdb, bytes);

    raw::save_unsigned(rdb, writer.position() as u64);
}

pub(crate) fn load_bitwriter_from_rdb(
    rdb: *mut raw::RedisModuleIO,
) -> Result<BitStream, ValkeyError> {
    // the load_string_buffer does not return an Err, so we can unwrap
    let bytes = raw::load_string_buffer(rdb)?.as_ref().to_vec();
    let pos = raw::load_unsigned(rdb)? as u32;

    let writer = BitStream::hydrate(bytes, pos as u8);

    Ok(writer)
}
