use crate::common::rounding::RoundingStrategy;
use crate::common::Timestamp;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use valkey_module::{raw, RedisModuleIO, ValkeyError, ValkeyResult};

const OPTIONAL_MARKER_PRESENT: u64 = 0xfe;
const OPTIONAL_MARKER_ABSENT: u64 = 0xff;

pub(crate) fn load_optional_marker(rdb: *mut RedisModuleIO) -> ValkeyResult<bool> {
    let marker = raw::load_unsigned(rdb)?;
    match marker {
        OPTIONAL_MARKER_PRESENT => Ok(true),
        OPTIONAL_MARKER_ABSENT => Ok(false),
        _ => Err(ValkeyError::String(format!("Invalid marker: {marker}"))),
    }
}

pub(crate) fn rdb_save_optional_marker(rdb: *mut RedisModuleIO, is_some: bool) {
    if is_some {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_PRESENT);
    } else {
        raw::save_unsigned(rdb, OPTIONAL_MARKER_ABSENT);
    }
}

/// WARNING!: internal and *ONLY* for ints < 64bits! We used a signed integer with value.abs() if a value
/// is present, and -1 otherwise
pub(crate) fn save_optional_unsigned(rdb: *mut RedisModuleIO, value: Option<u64>) {
    if let Some(value) = value {
        raw::save_signed(rdb, value as i64);
    } else {
        raw::save_signed(rdb, -1);
    }
}

pub fn rdb_save_duration(rdb: *mut RedisModuleIO, duration: &Duration) {
    let millis = duration.as_millis() as i64;
    raw::save_signed(rdb, millis);
}

/// NOTE: represents optional duration as i64, meaning that durations above i64::MAX milliseconds
/// will be truncated
pub fn rdb_load_optional_duration(rdb: *mut RedisModuleIO) -> ValkeyResult<Option<Duration>> {
    let val = raw::load_signed(rdb)?;
    if val == -1 {
        return Ok(None);
    }
    Ok(Some(Duration::from_millis(val as u64)))
}

pub fn rdb_save_optional_duration(rdb: *mut RedisModuleIO, duration: &Option<Duration>) {
    if let Some(duration) = duration {
        rdb_save_optional_marker(rdb, true);
        raw::save_signed(rdb, duration.as_millis() as i64);
    } else {
        raw::save_signed(rdb, -1);
    }
}

pub fn rdb_load_duration(rdb: *mut RedisModuleIO) -> ValkeyResult<Duration> {
    let millis = raw::load_unsigned(rdb)?;
    Ok(Duration::from_millis(millis))
}

#[inline]
pub fn rdb_save_usize(rdb: *mut RedisModuleIO, value: usize) {
    raw::save_unsigned(rdb, value as u64)
}

pub fn rdb_load_usize(rdb: *mut RedisModuleIO) -> ValkeyResult<usize> {
    let value = raw::load_unsigned(rdb)?;
    Ok(value as usize)
}

pub fn rdb_save_optional_usize(rdb: *mut RedisModuleIO, value: Option<usize>) {
    save_optional_unsigned(rdb, value.map(|x| x as u64));
}

#[inline]
pub(crate) fn rdb_save_timestamp(rdb: *mut RedisModuleIO, value: Timestamp) {
    raw::save_signed(rdb, value)
}

pub(crate) fn rdb_load_timestamp(rdb: *mut RedisModuleIO) -> ValkeyResult<Timestamp> {
    let value = raw::load_signed(rdb)?;
    Ok(value as Timestamp)
}

pub fn rdb_save_u8(rdb: *mut RedisModuleIO, value: u8) {
    raw::save_unsigned(rdb, value as u64)
}

pub fn rdb_load_u8(rdb: *mut RedisModuleIO) -> ValkeyResult<u8> {
    let value = raw::load_unsigned(rdb)?;
    // todo: validate that value is in range
    Ok(value as u8)
}

#[inline]
pub fn rdb_save_i32(rdb: *mut RedisModuleIO, value: i32) {
    raw::save_signed(rdb, value as i64)
}

#[inline]
pub fn rdb_load_i32(rdb: *mut RedisModuleIO) -> ValkeyResult<i32> {
    let value = raw::load_signed(rdb)?;
    // todo: validate that value is in range
    Ok(value as i32)
}

#[inline]
pub fn rdb_save_string(rdb: *mut RedisModuleIO, value: &str) {
    raw::save_string(rdb, value);
}

#[inline]
pub fn rdb_load_string(rdb: *mut RedisModuleIO) -> ValkeyResult<String> {
    Ok(String::from(raw::load_string(rdb)?))
}

pub fn rdb_save_bool(rdb: *mut RedisModuleIO, val: bool) {
    let bool_val = if val { 1 } else { 0 };
    rdb_save_u8(rdb, bool_val)
}

pub fn rdb_load_bool(rdb: *mut RedisModuleIO) -> ValkeyResult<bool> {
    let bool_val = rdb_load_u8(rdb)?;
    Ok(bool_val != 0)
}

pub fn save_optional_bool(rdb: *mut RedisModuleIO, value: Option<bool>) {
    if let Some(value) = value {
        rdb_save_bool(rdb, value);
    } else {
        rdb_save_u8(rdb, 2)
    }
}

pub fn load_optional_bool(rdb: *mut RedisModuleIO) -> ValkeyResult<Option<bool>> {
    let marker = rdb_load_u8(rdb)?;
    match marker {
        0 => Ok(Some(false)),
        1 => Ok(Some(true)),
        2 => Ok(None),
        _ => Err(ValkeyError::String(format!(
            "Invalid bool marker: {marker}"
        ))),
    }
}

pub fn rdb_save_string_hashmap(rdb: *mut RedisModuleIO, map: &HashMap<String, String>) {
    rdb_save_usize(rdb, map.len());
    for (key, val) in map.iter() {
        rdb_save_string(rdb, key);
        rdb_save_string(rdb, val);
    }
}

pub fn rdb_load_string_hashmap(rdb: *mut RedisModuleIO) -> ValkeyResult<HashMap<String, String>> {
    let len = rdb_load_usize(rdb)?;
    // todo: check available mem first
    let mut map = HashMap::with_capacity(len);
    for _ in 0..len {
        let key = rdb_load_string(rdb)?;
        let val = rdb_load_string(rdb)?;
        map.insert(key, val);
    }
    Ok(map)
}

pub fn save_atomic_u64(rdb: *mut RedisModuleIO, value: &AtomicU64) {
    raw::save_unsigned(rdb, value.load(std::sync::atomic::Ordering::Relaxed))
}

pub fn load_atomic_u64(rdb: *mut RedisModuleIO) -> ValkeyResult<AtomicU64> {
    let value = raw::load_unsigned(rdb)?;
    Ok(AtomicU64::new(value))
}

pub(crate) fn rdb_save_rounding(rdb: *mut RedisModuleIO, rounding: &RoundingStrategy) {
    match rounding {
        RoundingStrategy::SignificantDigits(sig_figs) => {
            rdb_save_u8(rdb, 1);
            rdb_save_i32(rdb, *sig_figs)
        }
        RoundingStrategy::DecimalDigits(digits) => {
            rdb_save_u8(rdb, 2);
            rdb_save_i32(rdb, *digits)
        }
    }
}

pub(crate) fn rdb_load_rounding(rdb: *mut RedisModuleIO) -> ValkeyResult<RoundingStrategy> {
    let marker = rdb_load_u8(rdb)?;
    match marker {
        1 => {
            let sig_figs = rdb_load_i32(rdb)?;
            Ok(RoundingStrategy::SignificantDigits(sig_figs))
        }
        2 => {
            let digits = rdb_load_i32(rdb)?;
            Ok(RoundingStrategy::DecimalDigits(digits))
        }
        _ => Err(ValkeyError::String(format!(
            "Invalid rounding marker: {marker}"
        ))),
    }
}

pub(crate) fn rdb_save_optional_rounding(
    rdb: *mut RedisModuleIO,
    rounding: &Option<RoundingStrategy>,
) {
    if let Some(rounding) = rounding {
        rdb_save_optional_marker(rdb, true);
        rdb_save_rounding(rdb, rounding)
    } else {
        rdb_save_optional_marker(rdb, false);
    }
}

pub(crate) fn rdb_load_optional_rounding(
    rdb: *mut RedisModuleIO,
) -> ValkeyResult<Option<RoundingStrategy>> {
    if load_optional_marker(rdb)? {
        let rounding = rdb_load_rounding(rdb)?;
        Ok(Some(rounding))
    } else {
        Ok(None)
    }
}
