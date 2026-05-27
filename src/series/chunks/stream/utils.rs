use crate::common::logging::log_warning;
use crate::series::chunks::stream::traits::BitRead;

#[inline]
pub(crate) fn read_bool<R: BitRead>(reader: &mut R) -> std::io::Result<bool> {
    match reader.read_bit() {
        Ok(v) => Ok(v),
        Err(e) => {
            log_warning(format!("bitstream read_bool error: {e}"));
            Err(e)
        }
    }
}

pub(crate) fn read_bits<R: BitRead>(reader: &mut R, bits: u32) -> std::io::Result<u64> {
    match reader.read_bits(bits) {
        Ok(v) => Ok(v),
        Err(e) => {
            log_warning(format!("bitstream read_bits error: {e}"));
            Err(e)
        }
    }
}
