#[cfg(test)]
use crate::is_module_initialized;
use std::borrow::Cow;
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{Context, DetachedContext, RedisModuleIO};

/// The server logs a module's message by passing it to `printf` as the *format*
/// string, after converting it to a C string with a panic on interior NUL. Text
/// that came from a peer or a client (a key, a handler name, an error) could
/// therefore read stray arguments through a `%` directive, or abort the server
/// through a NUL. Escape both.
fn printf_safe(message: &str) -> Cow<'_, str> {
    if !message.contains(['%', '\0']) {
        return Cow::Borrowed(message);
    }
    Cow::Owned(message.replace('%', "%%").replace('\0', "\\0"))
}

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
    #[allow(clippy::disallowed_methods)]
    valkey_module::logging::log(level, printf_safe(message));
}

pub fn log_verbose<T: AsRef<str>>(message: T) {
    log(ValkeyLogLevel::Verbose, message.as_ref());
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

/// Logs a failure to load or save data through the RDB `io` handle, escaped
/// like every other message here.
pub fn log_io_error(io: *mut RedisModuleIO, level: ValkeyLogLevel, message: &str) {
    #[allow(clippy::disallowed_methods)]
    valkey_module::logging::log_io_error(io, level, &printf_safe(message));
}

/// Logging through a context, so the server tags the line with this module's
/// name. Use these instead of the SDK's own `log_*` methods on [`Context`] and
/// [`DetachedContext`], which hand the text to `printf` unescaped (see
/// [`printf_safe`]); `clippy.toml` rejects those.
pub trait ContextLog {
    fn log_at(&self, level: ValkeyLogLevel, message: &str);

    fn warning<T: AsRef<str>>(&self, message: T) {
        self.log_at(ValkeyLogLevel::Warning, message.as_ref());
    }

    fn notice<T: AsRef<str>>(&self, message: T) {
        self.log_at(ValkeyLogLevel::Notice, message.as_ref());
    }

    fn verbose<T: AsRef<str>>(&self, message: T) {
        self.log_at(ValkeyLogLevel::Verbose, message.as_ref());
    }

    fn debug<T: AsRef<str>>(&self, message: T) {
        self.log_at(ValkeyLogLevel::Debug, message.as_ref());
    }
}

impl ContextLog for Context {
    fn log_at(&self, level: ValkeyLogLevel, message: &str) {
        #[allow(clippy::disallowed_methods)]
        self.log(level, &printf_safe(message));
    }
}

impl ContextLog for DetachedContext {
    fn log_at(&self, level: ValkeyLogLevel, message: &str) {
        #[allow(clippy::disallowed_methods)]
        self.log(level, &printf_safe(message));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_printf_safe_escapes_directives_and_nul() {
        assert!(matches!(printf_safe("plain text"), Cow::Borrowed(_)));
        assert_eq!(printf_safe("handler %s%n"), "handler %%s%%n");
        assert_eq!(printf_safe("unknown\0handler"), "unknown\\0handler");
    }
}
