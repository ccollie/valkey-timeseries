//! Telling a blocked client's background work that the client has gone.
//!
//! A command that blocks its client and answers from another thread (`TS.QUERY`,
//! `TS.QUERYRANGE`, a deferred `TS.MRANGE`) would otherwise run to completion after its
//! client disconnected, holding a worker for a reply the server discards.
//!
//! [`block_client`](super::block_client) installs a disconnect callback on every handle it
//! blocks. The callback runs on the main thread and only sets a flag here; the work reads it
//! through a [`CancelToken`] ([`BlockedClient::cancel_token`](super::BlockedClient::cancel_token))
//! wherever it already checks its deadline, and stops there.
//!
//! The flag is keyed by the handle's address, which the server may reuse for a later blocked
//! client once this one is unblocked. So the callback only ever *sets* a registered flag,
//! never inserts one, and the registration is removed before the client is unblocked: an
//! address is registered for at most one live handle at a time.
//!
//! Work that a flag cannot reach — a cluster fan-out the work is waiting on, whose shards would
//! otherwise read on to their time budget — registers a [`DisconnectAction`] with the token
//! instead ([`CancelToken::on_disconnect`]); the callback runs it.

use crate::common::sync::lock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use valkey_module::Context;

/// Something to stop when a blocked client disconnects, beyond what its work learns from the
/// flag: a cluster fan-out it waits on, say.
pub(crate) trait DisconnectAction: Send {
    /// Whether there is still something to stop: one that finished is dropped unrun rather
    /// than kept for the life of the query.
    fn is_pending(&self) -> bool;
    /// Stop it. On a thread holding the GIL: the main thread, from the disconnect callback, or
    /// the thread registering it, if the client had already gone.
    fn run(self: Box<Self>, ctx: &Context);
}

/// One blocked client's disconnect flag, and what to stop when it is set.
#[derive(Default)]
struct Entry {
    disconnected: AtomicBool,
    actions: Mutex<Vec<Box<dyn DisconnectAction>>>,
}

type Registry = papaya::HashMap<u64, Entry, ahash::RandomState>;

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::default);

/// What a background job checks to learn that its client disconnected. `Copy`, so it can
/// travel inside options that are passed by value; the default token is never cancelled.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CancelToken(u64);

impl CancelToken {
    /// Whether the client this token was issued for has disconnected.
    ///
    /// A lookup in a lock-free map, cheap enough for every deadline check. A token outliving
    /// its registration (work abandoned after its query ended) reads as not cancelled, or as
    /// a later registration at the same address: either way it only affects work whose
    /// result nobody is waiting for.
    pub fn is_cancelled(self) -> bool {
        self.0 != 0
            && REGISTRY
                .pin()
                .get(&self.0)
                .is_some_and(|entry| entry.disconnected.load(Ordering::Acquire))
    }

    /// Run `action` should this token's client disconnect while it is still registered: at
    /// once, if it already has. Dropped unrun for the default token, or once the client was
    /// answered (its registration is gone).
    ///
    /// Only with the GIL held, as `ctx` attests: the disconnect callback runs under it too, so
    /// the client cannot disconnect between the check and the registration.
    pub(crate) fn on_disconnect(self, ctx: &Context, action: Box<dyn DisconnectAction>) {
        if self.0 == 0 {
            return;
        }
        let run_now = {
            let registry = REGISTRY.pin();
            match registry.get(&self.0) {
                Some(entry) if entry.disconnected.load(Ordering::Acquire) => Some(action),
                Some(entry) => {
                    let mut actions = lock(&entry.actions);
                    actions.retain(|action| action.is_pending());
                    actions.push(action);
                    None
                }
                None => None,
            }
        };
        if let Some(action) = run_now {
            action.run(ctx);
        }
    }
}

/// A live registration for one blocked client; removed when dropped.
#[derive(Debug)]
pub(super) struct Registration(u64);

impl Registration {
    /// Register `key` (the blocked-client handle's address). The key must not be registered
    /// already, which holds as long as each registration is dropped before its handle is
    /// released to the server.
    pub(super) fn new(key: u64) -> Self {
        debug_assert_ne!(key, 0, "a null handle cannot be registered");
        REGISTRY.pin().insert(key, Entry::default());
        Self(key)
    }

    pub(super) fn token(&self) -> CancelToken {
        CancelToken(self.0)
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        REGISTRY.pin().remove(&self.0);
    }
}

/// Record that the client registered under `key` disconnected, and hand back what it asked to
/// have stopped, for the caller to run with the GIL ([`DisconnectAction::run`]). A key with no
/// registration (its work already finished) is ignored rather than inserted: see the module
/// docs.
#[must_use]
pub(super) fn mark_disconnected(key: u64) -> Vec<Box<dyn DisconnectAction>> {
    let registry = REGISTRY.pin();
    let Some(entry) = registry.get(&key) else {
        return Vec::new();
    };
    entry.disconnected.store(true, Ordering::Release);
    std::mem::take(&mut *lock(&entry.actions))
}

/// Run what [`mark_disconnected`] handed back, outside any lock.
pub(super) fn stop_all(actions: Vec<Box<dyn DisconnectAction>>, ctx: &Context) {
    for action in actions {
        action.run(ctx);
    }
}

/// A registered client for tests elsewhere in the crate: no server, no handle.
#[cfg(test)]
pub(crate) struct TestClient(Registration);

#[cfg(test)]
impl TestClient {
    pub(crate) fn new() -> Self {
        use std::sync::atomic::AtomicU64;
        // Odd, so never a real (aligned) handle address, and distinct per client.
        static NEXT: AtomicU64 = AtomicU64::new(0x7e57_0000_0001);
        Self(Registration::new(NEXT.fetch_add(2, Ordering::Relaxed)))
    }

    pub(crate) fn token(&self) -> CancelToken {
        self.0.token()
    }

    pub(crate) fn disconnect(&self) {
        stop_all(mark_disconnected(self.0.0), &test_context());
    }
}

#[cfg(test)]
impl CancelToken {
    /// Disconnect the [`TestClient`] this token was issued for, from wherever
    /// the token travelled; a token with no live registration is left alone.
    pub(crate) fn disconnect_for_test(self) {
        stop_all(mark_disconnected(self.0), &test_context());
    }
}

/// The context test-run actions get: no server behind it.
#[cfg(test)]
fn test_context() -> Context {
    Context::new(std::ptr::null_mut())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Keys well away from any real address, distinct per test so they can run in parallel.

    #[test]
    fn a_registered_client_is_cancelled_once_it_disconnects() {
        let registration = Registration::new(0xdead_0001);
        let token = registration.token();
        assert!(!token.is_cancelled());
        let _ = mark_disconnected(0xdead_0001);
        assert!(token.is_cancelled());
    }

    #[test]
    fn a_disconnect_after_the_work_finished_is_ignored() {
        let token = Registration::new(0xdead_0002).token();
        // The registration is gone: the callback must not resurrect it, or a later client
        // blocked at the same address would start out cancelled.
        let _ = mark_disconnected(0xdead_0002);
        assert!(!token.is_cancelled());
        let reused = Registration::new(0xdead_0002);
        assert!(!reused.token().is_cancelled());
    }

    /// An action that counts its runs, pending until told otherwise.
    struct Probe {
        pending: std::sync::Arc<AtomicBool>,
        ran: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DisconnectAction for Probe {
        fn is_pending(&self) -> bool {
            self.pending.load(Ordering::Relaxed)
        }
        fn run(self: Box<Self>, _ctx: &Context) {
            self.ran.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn probe() -> (
        Box<Probe>,
        std::sync::Arc<AtomicBool>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let pending = std::sync::Arc::new(AtomicBool::new(true));
        let ran = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let action = Box::new(Probe {
            pending: pending.clone(),
            ran: ran.clone(),
        });
        (action, pending, ran)
    }

    #[test]
    fn an_action_runs_once_when_its_client_disconnects() {
        let registration = Registration::new(0xdead_0003);
        let (action, _, ran) = probe();
        registration.token().on_disconnect(&test_context(), action);
        assert_eq!(ran.load(Ordering::Relaxed), 0, "not before the disconnect");
        stop_all(mark_disconnected(0xdead_0003), &test_context());
        assert_eq!(ran.load(Ordering::Relaxed), 1);
        stop_all(mark_disconnected(0xdead_0003), &test_context());
        assert_eq!(ran.load(Ordering::Relaxed), 1, "and only once");
    }

    #[test]
    fn an_action_registered_after_the_disconnect_runs_at_once() {
        let registration = Registration::new(0xdead_0004);
        let _ = mark_disconnected(0xdead_0004);
        let (action, _, ran) = probe();
        registration.token().on_disconnect(&test_context(), action);
        assert_eq!(ran.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn finished_actions_are_dropped_unrun() {
        let registration = Registration::new(0xdead_0005);
        let token = registration.token();
        let (finished, done, finished_ran) = probe();
        token.on_disconnect(&test_context(), finished);
        done.store(false, Ordering::Relaxed);
        // Registering the next one prunes it.
        let (live, _, live_ran) = probe();
        token.on_disconnect(&test_context(), live);
        stop_all(mark_disconnected(0xdead_0005), &test_context());
        assert_eq!(finished_ran.load(Ordering::Relaxed), 0);
        assert_eq!(live_ran.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn an_action_for_a_client_already_answered_is_dropped() {
        let token = Registration::new(0xdead_0006).token();
        let (action, _, ran) = probe();
        token.on_disconnect(&test_context(), action);
        let (action, _, default_ran) = probe();
        CancelToken::default().on_disconnect(&test_context(), action);
        let _ = mark_disconnected(0xdead_0006);
        assert_eq!(ran.load(Ordering::Relaxed), 0);
        assert_eq!(default_ran.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn the_default_token_is_never_cancelled() {
        let _ = mark_disconnected(0);
        assert!(!CancelToken::default().is_cancelled());
    }
}
