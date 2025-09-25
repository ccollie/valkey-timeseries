/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2025 Davide Di Carlo
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

//! *A safe place for your strings.*
//!
//! **asylum** is a fast, lightweight string interner with automatic cleanup to prevent memory bloat.
//!
//! It stores each unique string once, supports fast equality checks,
//! and automatically removes unused strings to keep memory usage low.

use hashbrown::HashSet;

use get_size::GetSize;
use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Deref;
use std::ptr::NonNull;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};

static POOL: LazyLock<[Mutex<HashSet<Holder>>; 64]> = LazyLock::new(|| {
    [
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
    ]
});

static MEMORY_USAGE: AtomicUsize = AtomicUsize::new(0);

fn get_shard(s: &str) -> &Mutex<HashSet<Holder>> {
    let bytes = s.as_bytes();

    let len = bytes.len() as u8;
    let last = bytes.last().copied().unwrap_or(0);
    let middle = bytes.get(bytes.len() / 2).copied().unwrap_or(0);
    let first = bytes.first().copied().unwrap_or(0);

    let mut hash = u32::from_be_bytes([first, middle, last, len]);
    hash ^= hash >> 19;
    hash ^= hash >> 13;
    hash ^= hash >> 5;

    let index = hash as usize % POOL.len();
    unsafe { POOL.get_unchecked(index) }
}

static LOCK_ERROR: &str = "string interner shard lock poisoned";

/// Interns the given string slice and returns a [`Symbol`] representing it.
///
/// If the string was already interned, returns the existing [`Symbol`].
/// Otherwise, stores the string and returns a new [`Symbol`] pointing to it.
///
/// # Example
/// ```rust
/// use crate::common::string_interner::*;
///
/// let sym = string_interner::intern("hello");
/// ```
pub fn intern(key: &str) -> Symbol {
    let mut shard = get_shard(key)
        .lock()
        .expect("intern(): shard lock poisoned");
    let holder = shard.get_or_insert_with(key, Holder::new);
    if holder.count() == 0 {
        MEMORY_USAGE.fetch_add(holder.get_size(), Ordering::Relaxed);
    }
    holder.symbol()
}

/// Returns the number of currently interned strings.
///
/// # Example
/// ```rust
/// use crate::common::string_interner;
///
/// assert_eq!(string_interner::size(), 0);
///
/// let sym = string_interner::intern("hello");
/// assert_eq!(string_interner::size(), 1);
///
/// drop(sym);
/// assert_eq!(string_interner::size(), 0);
/// ```
pub fn size() -> usize {
    POOL.iter()
        .map(|shard| shard.lock().expect(LOCK_ERROR).len())
        .sum()
}

/// Returns the total number of slots currently allocated in the interner.
///
/// This may be larger than [size] due to internal capacity growth.
///
/// # Example
/// ```rust
/// use crate::common::string_interner;
///
/// let cap = string_interner::capacity();
/// ```
pub fn capacity() -> usize {
    POOL.iter()
        .map(|shard| shard.lock().expect(LOCK_ERROR).capacity())
        .sum()
}

/// Reduces the memory usage by shrinking the interner's capacity
/// to fit exactly the number of currently interned strings.
///
/// This operation may reallocate internal storage, it locks the global pool to collect space,
/// so use it with caution since it may decrease the performance of your application.
///
/// # Example
/// ```rust
/// use crate::common::string_interner;
///
/// string_interner::shrink_to_fit();
/// ```
pub fn shrink_to_fit() {
    POOL.iter()
        .for_each(|shard| shard.lock().expect(LOCK_ERROR).shrink_to_fit());
}

/// Returns the total memory usage in bytes of interned strings.
/// # Example
/// ```rust
/// use crate::common::string_interner;
///
/// string_interner::memory_usage();
/// ```
pub fn memory_usage() -> usize {
    MEMORY_USAGE.load(Ordering::Relaxed)
}

/// A lightweight handle to an interned string.
///
/// [`Symbol`] is a clonable, comparable, and hashable reference
/// to a string stored inside the interner.
///
/// This struct is not copyable since it has to update the reference count
/// atomically, but it's (relatively) inexpensive to clone.
///
/// You can efficiently compare [`Symbol`]s by value and resolve them
/// back to string slices when needed.
///
/// # Example
/// ```rust
/// use crate::common::string_interner;
///
/// let sym = string_interner::intern("hello");
/// let string: &str = sym.as_str();
/// ```
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Symbol {
    ptr: NonNull<Atom>,
}

const _: [(); size_of::<usize>()] = [(); size_of::<Symbol>()];

impl Symbol {
    /// Creates a new [`Symbol`] for the given string slice.
    ///
    /// This method is actually the same as calling `asylum::intern` directly.
    ///
    /// # Arguments
    /// - `key`: The string slice to intern.
    ///
    /// # Example
    /// ```rust
    /// use crate::common:string_interner::Symbol;
    ///
    /// let sym = string_interner::new("hello");
    /// ```
    pub fn new(key: &str) -> Self {
        intern(key)
    }

    /// Returns the interned string slice associated with this [`Symbol`].
    ///
    /// # Example
    /// ```rust
    /// use crate::common::string_interner;
    ///
    /// let sym = string_interner::intern("hello");
    /// assert_eq!(sym.as_str(), "hello");
    /// ```
    pub fn as_str(&self) -> &str {
        self.atom().as_str()
    }

    /// Returns the current reference count for this [`Symbol`].
    ///
    /// Useful for debugging or advanced memory management scenarios.
    ///
    /// # Example
    /// ```rust
    /// use crate::common::string_interner;
    ///
    /// let sym = string_interner::intern("hello");
    /// let count = sym.count();
    /// assert_eq!(count, 1);
    /// ```
    pub fn count(&self) -> usize {
        self.atom().count()
    }

    fn atom(&self) -> &Atom {
        unsafe { self.ptr.as_ref() }
    }
}

impl AsRef<str> for Symbol {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Clone for Symbol {
    fn clone(&self) -> Self {
        unsafe { self.atom().incr_count() };
        Self { ptr: self.ptr }
    }
}

impl Debug for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Symbol({:?})", self.as_str())
    }
}

unsafe impl Send for Symbol {}

unsafe impl Sync for Symbol {}

impl Drop for Symbol {
    fn drop(&mut self) {
        if unsafe { self.atom().decr_count() } == 1 {
            std::sync::atomic::fence(Ordering::Acquire);

            let key = self.as_str();
            let mut shard = get_shard(key)
                .lock()
                .expect("intern(): shard lock poisoned in Symbol::drop()");
            let holder = shard
                .take(key)
                .expect("string interner: dropping symbol without holder in map");
            MEMORY_USAGE.fetch_sub(holder.get_size(), Ordering::Relaxed);
            drop(shard);
            drop(holder);
        }
    }
}

struct Holder {
    ptr: NonNull<Atom>,
}

impl Holder {
    fn new(s: &str) -> Self {
        let atom = Atom::new(0, s);
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(atom))) };
        Self { ptr }
    }

    fn atom(&self) -> &Atom {
        unsafe { self.ptr.as_ref() }
    }

    fn as_str(&self) -> &str {
        self.atom().as_str()
    }

    fn count(&self) -> usize {
        self.atom().count()
    }

    fn symbol(&self) -> Symbol {
        unsafe { self.atom().incr_count() };
        Symbol { ptr: self.ptr }
    }
}

impl Borrow<str> for Holder {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Eq for Holder {}

impl Hash for Holder {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Ord for Holder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialEq for Holder {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialOrd for Holder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

unsafe impl Send for Holder {}

unsafe impl Sync for Holder {}

impl Drop for Holder {
    fn drop(&mut self) {
        debug_assert_eq!(self.count(), 0);
        unsafe { drop(Box::from_raw(self.ptr.as_ptr())) }
    }
}

impl GetSize for Holder {
    fn get_size(&self) -> usize {
        size_of::<Self>() + self.atom().get_size()
    }
}

struct Atom {
    count: AtomicUsize,
    buf: Box<str>,
}

impl Atom {
    fn new(count: usize, buf: &str) -> Self {
        Self {
            count: AtomicUsize::new(count),
            buf: buf.into(),
        }
    }

    fn as_str(&self) -> &str {
        &self.buf
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    unsafe fn incr_count(&self) -> usize {
        self.count.fetch_add(1, Ordering::Relaxed)
    }

    unsafe fn decr_count(&self) -> usize {
        self.count.fetch_sub(1, Ordering::Release)
    }
}

impl GetSize for Atom {
    fn get_size(&self) -> usize {
        size_of::<Self>() + self.buf.len()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct InternedString(Symbol);
impl InternedString {
    pub fn intern(s: impl AsRef<str>) -> Self {
        Self(intern(s.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn count(&self) -> usize {
        self.0.count()
    }
}

impl GetSize for InternedString {
    fn get_size(&self) -> usize {
        size_of::<Self>()
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Borrow<str> for InternedString {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Clone for InternedString {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Debug for InternedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0.as_str(), f)
    }
}

impl Default for InternedString {
    fn default() -> Self {
        Self::intern(String::default())
    }
}

impl std::fmt::Display for InternedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0.as_str(), f)
    }
}

impl Ord for InternedString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left = self.0.as_str();
        let right = other.0.as_str();
        left.cmp(right)
    }
}

impl PartialOrd for InternedString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Deref for InternedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl FromStr for InternedString {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::intern(s))
    }
}

impl From<String> for InternedString {
    fn from(s: String) -> Self {
        Self::intern(s)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Mutex;
    use std::thread;

    // prevent tests from running in threads
    static LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn no_contention() {
        LOCK.lock()
            .map(|_| {
                let k1 = intern("foo");
                let k2 = intern("foo");
                let k3 = k1.clone();

                assert_eq!(k1.count(), 3);
                assert_eq!(k1.as_str(), "foo");

                assert_eq!(k2.count(), 3);
                assert_eq!(k2.as_str(), "foo");

                assert_eq!(k3.count(), 3);
                assert_eq!(k3.as_str(), "foo");

                assert_eq!(k1, k2);
                assert_eq!(k2, k3);
                assert_eq!(k3, k1);

                drop(k1);

                assert_eq!(k2.count(), 2);
                assert_eq!(k2.as_str(), "foo");

                assert_eq!(k3.count(), 2);
                assert_eq!(k3.as_str(), "foo");

                assert_eq!(k2, k3);
                assert_eq!(k3, k2);

                drop(k2);

                assert_eq!(k3.count(), 1);
                assert_eq!(k3.as_str(), "foo");

                drop(k3);

                let k4 = intern("bar");
                let k5 = intern("spam");

                assert_ne!(k4, k5);
            })
            .unwrap();
    }

    #[test]
    fn contention() {
        LOCK.lock()
            .map(|_| {
                let seeds = ["foo", "bar", "spam", "lorem", "ipsum", "dolor"];
                let t1 =
                    thread::spawn(move || seeds.iter().copied().map(intern).collect::<Vec<_>>());
                let t2 =
                    thread::spawn(move || seeds.iter().copied().map(intern).collect::<Vec<_>>());

                let s3 = seeds.iter().copied().map(intern).collect::<Vec<_>>();
                let s2 = t2.join().unwrap();
                let s1 = t1.join().unwrap();

                seeds
                    .iter()
                    .zip(s1)
                    .zip(s2)
                    .zip(s3)
                    .for_each(|(((&seed, s1), s2), s3)| {
                        assert_eq!(s1.count(), 3);
                        assert_eq!(s2.count(), 3);
                        assert_eq!(s3.count(), 3);
                        assert_eq!(s1, s2);
                        assert_eq!(s2, s3);
                        assert_eq!(s3, s1);
                        assert_eq!(seed, s1.as_str());
                    });
            })
            .unwrap();
    }
}
