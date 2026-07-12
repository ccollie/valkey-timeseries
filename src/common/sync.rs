use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Acquires a read lock, recovering the guard if the lock is poisoned.
///
/// Poisoning only happens when some thread panicked while holding the lock. This
/// codebase's locking discipline keeps critical sections short with no user code inside,
/// so the protected data is never left torn by a panic there — treating poison as
/// recoverable avoids turning one unrelated panic into a permanent crash loop on every
/// later access.
pub fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|e| e.into_inner())
}

/// Acquires a write lock, recovering the guard if the lock is poisoned. See [`read_lock`].
pub fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|e| e.into_inner())
}

/// Acquires a mutex, recovering the guard if the lock is poisoned. See [`read_lock`].
pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
