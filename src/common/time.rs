use crate::common::Timestamp;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use valkey_module::{RedisModule_CachedMicroseconds, RedisModule_Milliseconds};

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn system_time_millis() -> i64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as i64
}

pub fn valkey_current_time_millis() -> i64 {
    unsafe {
        if let Some(func) = RedisModule_Milliseconds {
            func()
        } else {
            // Fallback to system time if the Valkey function pointer is not available.
            system_time_millis()
        }
    }
}

pub fn valkey_cached_time_micros() -> i64 {
    unsafe {
        if let Some(func) = RedisModule_CachedMicroseconds {
            func()
        } else {
            // Fallback: use system time in milliseconds and convert to microseconds.
            system_time_millis() * 1000
        }
    }
}

pub fn valkey_cached_time_millis() -> i64 {
    valkey_cached_time_micros() / 1000
}

pub fn current_time_millis() -> i64 {
    cfg_select! {
        test => { system_time_millis() }
        _ => { valkey_current_time_millis() }
    }
}

/// `t` in milliseconds since the epoch: negative before it (PromQL's `@ -100`),
/// rounded down to a whole millisecond as Prometheus does, saturating at the
/// `i64` range.
pub fn system_time_to_millis(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_millis()).unwrap_or(i64::MAX),
        Err(before) => {
            let before = before.duration();
            let partial = u128::from(before.subsec_nanos() % 1_000_000 != 0);
            u64::try_from(before.as_millis() + partial)
                .ok()
                .and_then(|ms| 0i64.checked_sub_unsigned(ms))
                .unwrap_or(i64::MIN)
        }
    }
}

pub fn system_time_to_secs(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

/// The inverse of [`system_time_to_millis`]: a negative timestamp is before the
/// epoch. (`as u64` made it a time ~584 million years out, which only came back
/// right because `system_time_to_millis` wrapped it.)
pub fn timestamp_to_system_time(timestamp: Timestamp) -> SystemTime {
    let magnitude = Duration::from_millis(timestamp.unsigned_abs());
    if timestamp < 0 {
        UNIX_EPOCH - magnitude
    } else {
        UNIX_EPOCH + magnitude
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_time_before_the_epoch_is_negative_millis() {
        let t = UNIX_EPOCH - Duration::from_millis(100_500);
        assert_eq!(system_time_to_millis(t), -100_500);
    }

    #[test]
    fn system_time_rounds_down_to_whole_millis() {
        assert_eq!(
            system_time_to_millis(UNIX_EPOCH - Duration::from_micros(1)),
            -1
        );
        assert_eq!(
            system_time_to_millis(UNIX_EPOCH - Duration::from_micros(1_000)),
            -1
        );
        assert_eq!(
            system_time_to_millis(UNIX_EPOCH + Duration::from_micros(1_999)),
            1
        );
    }

    #[test]
    fn negative_timestamp_is_before_the_epoch() {
        assert_eq!(
            timestamp_to_system_time(-299_000),
            UNIX_EPOCH - Duration::from_secs(299)
        );
    }

    #[test]
    fn millis_round_trip_through_system_time() {
        for ms in [i64::MIN, -299_000, -1, 0, 1, 1_700_000_000_000, i64::MAX] {
            assert_eq!(
                system_time_to_millis(timestamp_to_system_time(ms)),
                ms,
                "{ms}"
            );
        }
    }

    #[test]
    fn system_time_past_i64_millis_saturates() {
        // `range_bounds_to_system_time` makes this for an unbounded end;
        // `as i64` wrapped it to -1000.
        let far = UNIX_EPOCH + Duration::from_secs(i64::MAX as u64);
        assert_eq!(system_time_to_millis(far), i64::MAX);
    }
}
