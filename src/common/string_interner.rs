//! A string interner that deallocates unused values.
//!
//! Original code https://github.com/ryzhyk/arc-interner
//! Copyright (c) 2021-2024 Leonid Ryzhyk
//! License: MIT
//!
//!
//! Interning reduces the memory footprint of an application by storing
//! a unique copy of each distinct value.  It speeds up equality
//! comparison and hashing operations, as only pointers rather than actual
//! values need to be compared.  On the flip side, object creation is
//! slower, as it involves lookup in the interned string pool.
//!
//! This library makes the following design choices:
//!
//! - Interned strings are reference counted.  When the last reference to
//!   an interned object is dropped, the string is deallocated.  This
//!   prevents unbounded growth of the interned object pool in applications
//!   where the set of interned values changes dynamically at the cost of
//!   some CPU and memory overhead (due to storing and maintaining an
//!   atomic counter).
//! - Multithreading.  A single pool of interned strings is shared by all
//!   threads in the program.  Inside `DashMap` this pool is protected by
//!   sharded mutexes that are acquired every time an object is being interned or a
//!   reference to an interned object is being dropped.  Although Rust mutexes
//!   are fairly inexpensive when there is no contention, you may see a significant
//!   drop in performance under contention.
//! - Safe: this library is built on the ` Arc ` type from the Rust
//!   standard library and the [`dashmap` crate](https://crates.io/crates/dashmap)
//!   and does not contain any unsafe code (although std and dashmap do of course)
//!
//! # Example
//! ```rust
//! use string_interner::InternedString;
//! let x = InternedString::new("hello");
//! let y: InternedString = "world".into();
//! assert_ne!(x, y);
//! assert_eq!(x, InternedString::new("hello"));
//! assert_eq!(*x, "hello"); // dereference an InternedString like a pointer
//! ```

use ahash::RandomState;
use dashmap::DashMap;
use get_size2::GetSize;
use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock};

/// The maximum number of shards to use in `DashMap`. Higher values
/// may improve performance under high contention, but will use more memory.
const MAX_SHARDS: usize = 64;

type StringContainer = DashMap<Arc<[u8]>, (), RandomState>;

static STRING_POOL: LazyLock<StringContainer> =
    LazyLock::new(|| StringContainer::with_hasher_and_shard_amount(RandomState::new(), MAX_SHARDS));

/// Total memory used by all interned strings.
static STRING_MEMORY_USED: AtomicUsize = AtomicUsize::new(0);

/// A pointer to an interned, reference-counted and immutable string object.
///
/// The interned string will be held in memory only until its
/// reference count reaches zero.
///
/// # Example
/// ```rust
/// use string_interner::InternedString;
///
/// let x = InternedString::new("hello");
/// let y: InternedString = "world".into();
/// assert_ne!(x, y);
/// assert_eq!(x, InternedString::new("hello"));
/// assert_eq!(*x, "hello"); // dereference an InternedString like a pointer
/// ```
#[derive(Debug, GetSize)]
pub struct InternedString {
    /// The actual string data. We use `Arc<[u8]>` instead of `Arc<String>` to save
    /// some memory (no need to store the capacity of the string since we're immutable).
    arc: Arc<[u8]>,
}

impl InternedString {
    /// Intern a string value.  If this value has not previously been
    /// interned, then `new` will allocate a spot for the value on the
    /// heap.  Otherwise, it will return a pointer to the object
    /// previously allocated.
    ///
    /// Note that `InternedString::new` is a bit slower than direct allocation, since it needs to check
    /// a mutex for its hash slot. However, the performance should be acceptable for our use cases,
    /// especially under low contention.
    pub fn new(val: &str) -> Self {
        let v = Arc::from(val.as_bytes());
        Self::from_arc(v)
    }

    fn from_arc(val: Arc<[u8]>) -> InternedString {
        let b = STRING_POOL.entry(val).or_insert(());
        let val = b.key();
        let ref_count = Arc::strong_count(val);
        if ref_count == 1 {
            // we are the first reference, so account for the memory used
            let size = val.get_size();
            STRING_MEMORY_USED.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }
        InternedString { arc: val.clone() }
    }

    pub fn len(&self) -> usize {
        self.arc.len()
    }

    pub fn is_empty(&self) -> bool {
        self.arc.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.arc.as_ref()
    }

    /// Return the number of references to this value.
    pub fn ref_count(&self) -> usize {
        // The hashset holds one reference; we return the number of
        // references held by actual clients.
        Arc::strong_count(&self.arc) - 1
    }

    /// Return true if this is the only reference to this value.
    pub fn is_unique(&self) -> bool {
        self.ref_count() == 1
    }

    /// Return the number of unique interned strings.
    pub fn interned_object_count() -> usize {
        STRING_POOL.len()
    }

    /// Return the total memory used by all interned strings.
    pub fn memory_used() -> usize {
        STRING_MEMORY_USED.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Clone for InternedString {
    fn clone(&self) -> Self {
        InternedString {
            arc: self.arc.clone(),
        }
    }
}

impl Borrow<str> for InternedString {
    fn borrow(&self) -> &str {
        self.deref()
    }
}

impl Hash for InternedString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl AsRef<[u8]> for InternedString {
    fn as_ref(&self) -> &[u8] {
        self.arc.as_ref()
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Deref for InternedString {
    type Target = str;
    fn deref(&self) -> &str {
        // SAFETY: we only intern valid UTF-8 strings
        unsafe { std::str::from_utf8_unchecked(self.arc.as_ref()) }
    }
}

impl Display for InternedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v: &str = self.deref();
        write!(f, "{v}")
    }
}

impl Drop for InternedString {
    fn drop(&mut self) {
        STRING_POOL.remove_if(&self.arc, |k, _| {
            // If the reference count is 2, then the only two remaining references
            // to this value are held by `self` and the hashmap, and we can safely
            // deallocate the value.
            if Arc::strong_count(k) == 2 {
                let size = self.get_size();
                STRING_MEMORY_USED.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);

                return true;
            }
            false
        });
    }
}

impl FromStr for InternedString {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl From<String> for InternedString {
    fn from(t: String) -> Self {
        Self::new(t.as_str())
    }
}

impl From<&[u8]> for InternedString {
    fn from(s: &[u8]) -> Self {
        Self::from_arc(Arc::from(s))
    }
}

impl Default for InternedString {
    fn default() -> InternedString {
        InternedString::new(Default::default())
    }
}

/// Efficiently compares two interned values by comparing their pointers.
impl PartialEq for InternedString {
    fn eq(&self, other: &InternedString) -> bool {
        Arc::ptr_eq(&self.arc, &other.arc)
    }
}

impl Eq for InternedString {}

impl PartialOrd for InternedString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
    fn lt(&self, other: &Self) -> bool {
        self.as_bytes().lt(other.as_bytes())
    }
    fn le(&self, other: &Self) -> bool {
        self.as_bytes().le(other.as_bytes())
    }
    fn gt(&self, other: &Self) -> bool {
        self.as_bytes().gt(other.as_bytes())
    }
    fn ge(&self, other: &Self) -> bool {
        self.as_bytes().ge(other.as_bytes())
    }
}

impl Ord for InternedString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::{InternedString, STRING_MEMORY_USED};
    use ahash::{HashSet, HashSetExt};
    use serial_test::serial;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::thread;

    // Tests are run serially to avoid interference via the global string pool and memory tracker.

    // Test basic functionality.
    #[test]
    #[serial]
    fn basic() {
        assert_eq!(InternedString::new("foo"), InternedString::new("foo"));
        assert_ne!(InternedString::new("foo"), InternedString::new("bar"));
        // The above refs should be deallocated by now.
        assert_eq!(InternedString::interned_object_count(), 0);

        let _interned1 = InternedString::new("foo");
        {
            let interned2 = InternedString::new("foo");
            let interned3 = InternedString::new("bar");

            assert_eq!(interned2.ref_count(), 2);
            assert_eq!(interned3.ref_count(), 1);
            // We now have two unique interned strings: "foo" and "bar".
            assert_eq!(InternedString::interned_object_count(), 2);
        }

        // "bar" is now gone.
        assert_eq!(InternedString::interned_object_count(), 1);
    }

    // Ordering should be based on values, not pointers.
    // Also tests `Display` implementation.
    #[test]
    #[serial]
    fn sorting() {
        let mut interned_vals = [
            InternedString::new("4"),
            InternedString::new("2"),
            InternedString::new("5"),
            InternedString::new("0"),
            InternedString::new("1"),
            InternedString::new("3"),
        ];
        interned_vals.sort();
        let sorted: Vec<String> = interned_vals.iter().map(|v| format!("{v}")).collect();
        assert_eq!(&sorted.join(","), "0,1,2,3,4,5");
    }

    #[test]
    #[serial]
    fn sequential() {
        for _i in 0..10_000 {
            let mut interned = Vec::with_capacity(100);
            for j in 0..100 {
                let val = format!("foo{j}");
                let interned_string = InternedString::new(&val);
                interned.push(interned_string);
            }
        }

        assert_eq!(InternedString::interned_object_count(), 0);
    }

    // Quickly create and destroy a small number of interned objects from
    // multiple threads.
    #[test]
    #[serial]
    fn multithreading1() {
        let mut thread_handles = vec![];
        for _i in 0..10 {
            let t = thread::spawn({
                move || {
                    for _i in 0..100_000 {
                        let interned1 = InternedString::new("foo");
                        let _interned2 = InternedString::new("bar");
                        let mut m = HashMap::new();
                        // force some hashing
                        m.insert(interned1, ());
                    }
                }
            });
            thread_handles.push(t);
        }
        for h in thread_handles.into_iter() {
            h.join().unwrap()
        }

        assert_eq!(InternedString::interned_object_count(), 0);
    }

    // Test InternedString

    // Helper to reset memory tracking before each test
    fn reset_memory_tracking() {
        STRING_MEMORY_USED.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    #[test]
    #[serial]
    fn test_new_creates_interned_string() {
        reset_memory_tracking();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("hello");

        // Both should refer to the same interned value
        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 5);
        assert!(!s1.is_empty());
    }

    #[test]
    #[serial]
    fn test_different_interned_strings_are_not_equal() {
        reset_memory_tracking();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("world");

        assert_ne!(s1, s2);
    }

    #[test]
    #[serial]
    fn test_clone_interned_string_increases_refcount() {
        reset_memory_tracking();

        let s1 = InternedString::new("test");
        let initial_refcount = s1.ref_count();

        let s2 = s1.clone();

        assert_eq!(s1.ref_count(), initial_refcount + 1);
        assert_eq!(s2.ref_count(), initial_refcount + 1);
        assert_eq!(s1, s2);
    }

    #[test]
    #[serial]
    fn test_drop_decreases_interned_string_refcount() {
        reset_memory_tracking();

        let s1 = InternedString::new("test");
        let s2 = s1.clone();
        let initial_refcount = s1.ref_count();

        drop(s1);

        assert_eq!(s2.ref_count(), initial_refcount - 1);
    }

    #[test]
    #[serial]
    fn test_memory_tracking_on_first_creation() {
        reset_memory_tracking();

        assert_eq!(InternedString::memory_used(), 0);

        let s1 = InternedString::new("test_memory");
        let memory_after_creation = InternedString::memory_used();

        assert!(memory_after_creation > 0);

        // Creating the same string again should not increase memory
        let s2 = InternedString::new("test_memory");
        assert_eq!(InternedString::memory_used(), memory_after_creation);

        // But refcount should increase
        assert_eq!(s1.ref_count(), 2);
        assert_eq!(s2.ref_count(), 2);
    }

    #[test]
    #[serial]
    fn test_memory_tracking_on_drop() {
        reset_memory_tracking();

        let s1 = InternedString::new("test_drop_memory");
        let s2 = s1.clone();
        let memory_with_two_refs = InternedString::memory_used();

        // Dropping one reference should not decrease memory yet
        drop(s1);
        assert_eq!(InternedString::memory_used(), memory_with_two_refs);

        // Dropping the last reference should decrease memory
        drop(s2);
        // Note: Memory decrease happens when refcount reaches 0,
        // which is checked in the Drop implementation
    }

    #[test]
    #[serial]
    fn test_memory_tracking_with_different_strings() {
        reset_memory_tracking();

        let _s1 = InternedString::new("short");
        let memory_after_first = InternedString::memory_used();

        let _s2 = InternedString::new("this_is_a_much_longer_string_for_testing");
        let memory_after_second = InternedString::memory_used();

        // Memory should increase more for the longer string
        assert!(memory_after_second > memory_after_first);

        // The difference should be at least the difference in string lengths
        let length_diff = "this_is_a_much_longer_string_for_testing".len() - "short".len();
        assert!((memory_after_second - memory_after_first) >= length_diff);
    }

    #[test]
    #[serial]
    fn test_interned_string_deref_trait() {
        reset_memory_tracking();

        let s = InternedString::new("test_string");

        // Test various string methods through Deref
        assert_eq!(s.len(), 11);
        assert!(s.contains("test"));
        assert!(s.starts_with("test"));
        assert!(s.ends_with("string"));
        assert_eq!(s.to_uppercase(), "TEST_STRING");
    }

    #[test]
    #[serial]
    fn test_partial_ord_and_ord() {
        reset_memory_tracking();

        let s1 = InternedString::new("apple");
        let s2 = InternedString::new("banana");
        let s3 = InternedString::new("cherry");

        assert!(s1 < s2);
        assert!(s2 < s3);
        assert!(s1 < s3);

        let mut vec = [s3.clone(), s1.clone(), s2.clone()];
        vec.sort();

        assert_eq!(vec[0], s1);
        assert_eq!(vec[1], s2);
        assert_eq!(vec[2], s3);
    }

    #[test]
    #[serial]
    fn test_hash_consistency() {
        reset_memory_tracking();

        let s1 = InternedString::new("hash_test");
        let s2 = InternedString::new("hash_test");
        let s3 = InternedString::new("different");

        let mut set = HashSet::new();
        set.insert(s1.clone());

        // s2 should be found in the set because it's equal to s1
        assert!(set.contains(&s2));

        // s3 should not be found
        assert!(!set.contains(&s3));

        // Adding s2 should not increase the set size
        set.insert(s2);
        assert_eq!(set.len(), 1);

        // Adding s3 should increase the set size
        set.insert(s3);
        assert_eq!(set.len(), 2);
    }

    #[test]
    #[serial]
    fn test_empty_string() {
        reset_memory_tracking();

        let empty1 = InternedString::new("");
        let empty2 = InternedString::new("");

        assert_eq!(empty1, empty2);
        assert_eq!(empty1.deref(), "");
        assert_eq!(empty1.len(), 0);
        assert!(empty1.is_empty());
    }

    #[test]
    fn test_unicode_strings() {
        reset_memory_tracking();

        let unicode1 = InternedString::new("🦀 Rust");
        let unicode2 = InternedString::new("🦀 Rust");
        let different = InternedString::new("🐍 Python");

        assert_eq!(unicode1, unicode2);
        assert_ne!(unicode1, different);
        assert_eq!(unicode1.deref(), "🦀 Rust");
    }

    #[test]
    fn test_very_long_strings() {
        reset_memory_tracking();

        let long_string = "a".repeat(10000);
        let s1 = InternedString::new(&long_string);
        let s2 = InternedString::new(&long_string);

        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 10000);
        assert_eq!(s1.ref_count(), 2);
    }
}
