use crate::common::replies::{IntoRawCtx, ReplyContext};
use crate::common::threads::GilToken;
use crate::error_consts;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::marker::PhantomData;
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
///
/// A blocked client must be answered before it is unblocked: the server delivers only what was
/// written to the client's thread-safe context, and unblocking with nothing written leaves the
/// client waiting forever (or, if it pipelines, reading the next command's reply as this one's).
/// So a handle dropped before anyone took responsibility for the reply (a job that panicked, or
/// one the executor dropped without running) answers with an error itself. After a timeout
/// the server has already answered and discards that error, as it discards any late reply.
pub struct BlockedClient {
    pub(crate) inner: *mut raw::RedisModuleBlockedClient,
    /// Set by the server-side timeout callback (see [`block_client_with_timeout`]).
    /// `None` when the client was blocked without a timeout.
    timed_out: Option<Arc<AtomicBool>>,
    /// Whether a reply has been written or handed to a writer.
    answered: bool,
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
            answered: false,
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
}

impl Drop for BlockedClient {
    fn drop(&mut self) {
        if self.inner.is_null() {
            return;
        }
        if self.timed_out.is_some()
            && let Ok(mut watchers) = TIMEOUT_WATCHERS.lock()
        {
            watchers.remove(&(self.inner as usize));
        }
        unsafe {
            if !self.answered {
                let ctx = raw::RedisModule_GetThreadSafeContext.unwrap()(self.inner);
                reply_no_reply_written(ctx);
                raw::RedisModule_FreeThreadSafeContext.unwrap()(ctx);
            }
            raw::RedisModule_UnblockClient.unwrap()(self.inner, ptr::null_mut());
        }
    }
}

/// Answers a blocked client whose worker never wrote a reply.
fn reply_no_reply_written(ctx: *mut raw::RedisModuleCtx) {
    let _ = Context::new(ctx).reply(Err(ValkeyError::Str(error_consts::NO_REPLY_WRITTEN)));
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
        answered: false,
    }
}

pub struct ThreadSafeReplyContext {
    pub(crate) ctx: *mut raw::RedisModuleCtx,
    blocked_client: BlockedClient,
    /// Set once a reply is written or a [`ReplyContext`] is handed out. If it is still unset
    /// when this is dropped, the client is answered with an error (see [`BlockedClient`]).
    ///
    /// A writer that panics after starting its reply leaves a partial reply that nothing
    /// here can repair; this covers the far likelier failure, before any reply.
    answered: AtomicBool,
}

/// Holds the GIL for a [`ThreadSafeReplyContext`] and derefs to its context. Borrows the
/// context rather than owning it, so dropping the guard only releases the lock.
///
/// Taken through a [`GilToken`], so it is checked like every other GIL acquisition (no pool
/// worker, no re-entry: R1 in `common::threads`).
pub struct ContextGuard<'a> {
    ctx: Context,
    /// Dropped, clearing the thread's GIL mark, just before the lock is released.
    token: Option<GilToken>,
    _owner: PhantomData<&'a ThreadSafeReplyContext>,
}

impl Drop for ContextGuard<'_> {
    fn drop(&mut self) {
        drop(self.token.take());
        unsafe {
            raw::RedisModule_ThreadSafeContextUnlock.unwrap()(self.ctx.ctx);
        };
    }
}

impl Deref for ContextGuard<'_> {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl Borrow<Context> for ContextGuard<'_> {
    fn borrow(&self) -> &Context {
        &self.ctx
    }
}

/// SAFETY: a thread-safe context belongs to one blocked client and may be used from any one
/// thread at a time; the Valkey modules API does not require locking for `Reply` functions.
/// Moving it to the thread that writes the reply is sound. Sharing it is not: two threads
/// writing replies through one `&ThreadSafeReplyContext` would interleave them. So `Send`
/// only; nothing needs `Sync`.
unsafe impl Send for ThreadSafeReplyContext {}

impl ThreadSafeReplyContext {
    #[must_use]
    pub fn with_blocked_client(blocked_client: BlockedClient) -> Self {
        let ctx = unsafe { raw::RedisModule_GetThreadSafeContext.unwrap()(blocked_client.inner) };
        Self {
            ctx,
            blocked_client,
            answered: AtomicBool::new(false),
        }
    }

    /// The Valkey modules API does not require locking for `Reply` functions,
    /// so we pass through its functionality directly.
    #[allow(clippy::must_use_candidate)]
    pub fn reply(&self, r: ValkeyResult) -> raw::Status {
        self.answered.store(true, Ordering::Relaxed);
        let ctx = Context::new(self.ctx);
        ctx.reply(r)
    }

    /// See [`BlockedClient::is_timed_out`].
    pub fn is_timed_out(&self) -> bool {
        self.blocked_client.is_timed_out()
    }

    /// A context to write the reply through. The caller takes responsibility for writing it.
    pub fn get_reply_context(&self) -> ReplyContext {
        self.answered.store(true, Ordering::Relaxed);
        ReplyContext::new(self.ctx)
    }

    /// All non-reply APIs require locking, so we mirror
    /// `valkey_module::ThreadSafeContext::lock` semantics.
    ///
    /// The guard runs calls against this blocked client's own context, not a detached one:
    /// only this context has the client's selected db, so key writes land in the right db
    /// and `RM_Replicate` propagates them with the right `SELECT`.
    #[track_caller]
    pub fn lock(&self) -> ContextGuard<'_> {
        let (token, ()) = GilToken::take(|| unsafe {
            raw::RedisModule_ThreadSafeContextLock.unwrap()(self.ctx);
        });
        ContextGuard {
            ctx: Context::new(self.ctx),
            token: Some(token),
            _owner: PhantomData,
        }
    }

    /// Log a message at the specified `level` using the underlying context.
    pub fn log(&self, level: ValkeyLogLevel, message: &str) {
        Context::new(self.ctx).log(level, message);
    }

    pub fn log_warning(&self, message: &str) {
        self.log(ValkeyLogLevel::Warning, message);
    }
}

impl Drop for ThreadSafeReplyContext {
    fn drop(&mut self) {
        if !self.answered.load(Ordering::Relaxed) {
            reply_no_reply_written(self.ctx);
        }
        unsafe { raw::RedisModule_FreeThreadSafeContext.unwrap()(self.ctx) };
        // Answered above if it was not already; the handle must not answer again.
        self.blocked_client.answered = true;
    }
}

impl IntoRawCtx for &ThreadSafeReplyContext {
    /// Writing through the raw context is answering too: the caller takes responsibility for
    /// the reply, so the drop must not add an error after it.
    fn into_raw(self) -> *mut raw::RedisModuleCtx {
        self.answered.store(true, Ordering::Relaxed);
        self.ctx
    }
}
