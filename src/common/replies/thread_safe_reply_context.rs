use crate::common::replies::{IntoRawCtx, ReplyContext};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::Deref;
use std::os::raw::{c_int, c_longlong};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use valkey_module::logging::ValkeyLogLevel;
use valkey_module::{Context, ValkeyError, ValkeyResult, raw};

/// A lightweight "fork" of the BlockedClient in `valkey_module` to allow raw client replies from background threads
/// without needing to lock the context. This is safe, since the Valkey modules API does not require locking for
/// `Reply` functions,
pub struct BlockedClient {
    pub(crate) inner: *mut raw::RedisModuleBlockedClient,
    /// Set by the server-side timeout callback (see [`block_client_with_timeout`]).
    /// `None` when the client was blocked without a timeout.
    timed_out: Option<Arc<AtomicBool>>,
}

/// Blocked clients that were given a timeout, keyed by their handle, so the timeout
/// callback — a plain `extern "C"` fn with no captured state — can find the flag to raise
/// and the error text to answer with. Entries live from `block_client_with_timeout` until
/// the [`BlockedClient`] is dropped.
static TIMEOUT_WATCHERS: LazyLock<Mutex<HashMap<usize, TimeoutWatcher>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct TimeoutWatcher {
    timed_out: Arc<AtomicBool>,
    error: &'static str,
}

/// Runs on the main thread when a blocked client's timeout elapses. The server has already
/// detached the real client from the handle by the time the worker finishes, so whatever the
/// worker replies later is discarded; this reply is the one the client sees.
extern "C" fn blocked_client_timeout(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    let handle = unsafe { raw::RedisModule_GetBlockedClientHandle.unwrap()(ctx) };
    let error = TIMEOUT_WATCHERS
        .lock()
        .ok()
        .and_then(|watchers| {
            watchers.get(&(handle as usize)).map(|w| {
                w.timed_out.store(true, Ordering::SeqCst);
                w.error
            })
        })
        .unwrap_or(BLOCKED_CLIENT_TIMEOUT_ERROR);
    Context::new(ctx).reply_error_string(error);
    raw::REDISMODULE_OK as c_int
}

const BLOCKED_CLIENT_TIMEOUT_ERROR: &str = "TSDB: command timed out before the result was ready";

// We need to be able to send the inner pointer to another thread
unsafe impl Send for BlockedClient {}

impl BlockedClient {
    pub(crate) fn new(inner: *mut raw::RedisModuleBlockedClient) -> Self {
        Self {
            inner,
            timed_out: None,
        }
    }

    /// Whether the server has already answered this client with a timeout error.
    /// A worker should skip side effects (such as a `STORE` write) once this is set,
    /// since the client has been told the command failed.
    pub fn is_timed_out(&self) -> bool {
        self.timed_out
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }

    /// Aborts the blocked client operation
    ///
    /// # Returns
    /// * `Ok(())` - If the blocked client was successfully aborted
    /// * `Err(ValkeyError)` - If the abort operation failed
    pub fn abort(mut self) -> Result<(), ValkeyError> {
        unsafe {
            if raw::RedisModule_AbortBlock.unwrap()(self.inner) == raw::REDISMODULE_OK as c_int {
                // Prevent the normal Drop from running
                self.inner = ptr::null_mut();
                Ok(())
            } else {
                Err(ValkeyError::Str("Failed to abort blocked client"))
            }
        }
    }
}

impl Drop for BlockedClient {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            if self.timed_out.is_some()
                && let Ok(mut watchers) = TIMEOUT_WATCHERS.lock()
            {
                watchers.remove(&(self.inner as usize));
            }
            unsafe {
                raw::RedisModule_UnblockClient.unwrap()(self.inner, ptr::null_mut());
            }
        }
    }
}

pub(crate) fn block_client(ctx: &Context) -> BlockedClient {
    let blocked_client = unsafe {
        raw::RedisModule_BlockClient.unwrap()(
            ctx.ctx, // ctx
            None,    // reply_func
            None,    // timeout_func
            None, 0,
        )
    };

    BlockedClient::new(blocked_client)
}

/// Block the calling client with a server-enforced deadline.
///
/// `timeout_ms == 0` means no deadline, exactly like [`block_client`]. Otherwise, once
/// `timeout_ms` elapses the server replies to the client with `error` and marks the
/// returned handle as timed out ([`BlockedClient::is_timed_out`]). The worker still owns
/// the handle and must let it drop as usual; its own reply is then discarded by the server.
///
/// The deadline starts now, so time spent queued behind other jobs counts against it.
pub(crate) fn block_client_with_timeout(
    ctx: &Context,
    timeout_ms: u64,
    error: &'static str,
) -> BlockedClient {
    if timeout_ms == 0 {
        return block_client(ctx);
    }
    let blocked_client = unsafe {
        raw::RedisModule_BlockClient.unwrap()(
            ctx.ctx,
            None,
            Some(blocked_client_timeout),
            None,
            timeout_ms as c_longlong,
        )
    };
    let timed_out = Arc::new(AtomicBool::new(false));
    // Timeouts are delivered on the main thread after this command handler returns, so
    // the watcher is always registered before the callback can look for it.
    if let Ok(mut watchers) = TIMEOUT_WATCHERS.lock() {
        watchers.insert(
            blocked_client as usize,
            TimeoutWatcher {
                timed_out: Arc::clone(&timed_out),
                error,
            },
        );
    }
    BlockedClient {
        inner: blocked_client,
        timed_out: Some(timed_out),
    }
}

pub struct ThreadSafeReplyContext {
    pub(crate) ctx: *mut raw::RedisModuleCtx,

    /// 'Drop' only uses this field implicitly, so avoid a compiler warning
    #[allow(dead_code)]
    blocked_client: BlockedClient,
}

pub struct ContextGuard {
    ctx: Context,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        unsafe {
            raw::RedisModule_ThreadSafeContextUnlock.unwrap()(self.ctx.ctx);
            raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx.ctx);
        };
    }
}

impl Deref for ContextGuard {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Borrow<Context> for ContextGuard {
    fn borrow(&self) -> &Context {
        &self.ctx
    }
}

/// SAFETY:
/// This is copied from the implementation of `ThreadSafeContext` in `thread_safe.rs`, with the same safety guarantees.
/// The Valkey modules API does not require locking for `Reply` functions, and the `ReplyContext` constructed has its context
/// as private.
unsafe impl Send for ThreadSafeReplyContext {}
unsafe impl Sync for ThreadSafeReplyContext {}

impl ThreadSafeReplyContext {
    #[must_use]
    pub fn with_blocked_client(blocked_client: BlockedClient) -> Self {
        let ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(blocked_client.inner) };
        Self {
            ctx,
            blocked_client,
        }
    }

    /// The Valkey modules API does not require locking for `Reply` functions,
    /// so we pass through its functionality directly.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, r: ValkeyResult) -> raw::Status {
        let ctx = Context::new(self.ctx);
        ctx.reply(r)
    }

    /// See [`BlockedClient::is_timed_out`].
    pub fn is_timed_out(&self) -> bool {
        self.blocked_client.is_timed_out()
    }

    pub fn get_reply_context(&self) -> ReplyContext {
        ReplyContext::new(self.ctx)
    }

    /// All non-reply APIs require locking, so we mirror
    /// `valkey_module::ThreadSafeContext::lock` semantics.
    pub fn lock(&self) -> ContextGuard {
        unsafe { raw::RedisModule_ThreadSafeContextLock.unwrap()(self.ctx) };
        let ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(ptr::null_mut()) };
        let ctx = Context::new(ctx);
        ContextGuard { ctx }
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        Context::new(self.ctx).log(level, message);
    }

    /// Convenience logging helpers.
    pub fn log_debug(&self, message: &str) {
        self.log(ValkeyLogLevel::Debug, message);
    }

    pub fn log_notice(&self, message: &str) {
        self.log(ValkeyLogLevel::Notice, message);
    }

    pub fn log_verbose(&self, message: &str) {
        self.log(ValkeyLogLevel::Verbose, message);
    }

    pub fn log_warning(&self, message: &str) {
        self.log(ValkeyLogLevel::Warning, message);
    }
}

impl Drop for ThreadSafeReplyContext {
    fn drop(&mut self) {
        unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx) };
    }
}

impl IntoRawCtx for &ThreadSafeReplyContext {
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.ctx
    }
}
