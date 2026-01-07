#[cfg(test)]
use crate::is_module_initialized;
use valkey_module::logging::ValkeyLogLevel;

fn log(level: ValkeyLogLevel, message: &str) {
    // if the module is not initialized, log to the console directly instead of possibly causing a panic
    #[cfg(test)]
    if !is_module_initialized() {
        match level {
            ValkeyLogLevel::Warning => eprintln!("{message}"),
            _ => println!("{message}"),
        }
        return;
    }
    valkey_module::logging::log(level, message);
}

pub fn log_notice<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Notice, message.as_ref());
}

pub fn log_debug<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Debug, message.as_ref());
}

pub fn log_warning<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Warning, message.as_ref());
}

pub fn log_error<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Warning, message.as_ref());
}
