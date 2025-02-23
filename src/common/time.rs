use valkey_module::RedisModule_Milliseconds;

/// Returns the time duration since UNIX_EPOCH in milliseconds.
pub fn system_time_millis() -> i64 {
    // TODO: use a more efficient way to get current time
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as i64
}

pub fn valkey_current_time_millis() -> i64 {
    unsafe { RedisModule_Milliseconds.unwrap()() }
}

pub fn current_time_millis() -> i64 {
    cfg_if::cfg_if! {
        if #[cfg(test)] {
            system_time_millis()
        } else {
            valkey_current_time_millis()
        }
    }
}
