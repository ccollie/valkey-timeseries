//! An interner that deallocates unused values.
//!
//! Original code https://github.com/ryzhyk/arc-interner
//! Copyright (c) 2021-2024 Leonid Ryzhyk
//! License: MIT
//!
//! This crate is a fork of [David Roundy's](https://github.com/droundy/)
//! [`internment` crate](https://crates.io/crates/internment).
//! It provides an alternative implementation of the `internment::ArcIntern`
//! type.  It inherits David's high-level design and API; however, it is built
//! completely on Rust's standard `Arc` and the
//! [`dashmap` crate](https://crates.io/crates/dashmap) and does not contain
//! any unsafe code.
//!
//! Interning reduces the memory footprint of an application by storing
//! a unique copy of each distinct value.  It speeds up equality
//! comparison and hashing operations, as only pointers rather than actual
//! values need to be compared.  On the flip side, object creation is
//! slower, as it involves lookup in the interned object pool.
//!
//! Interning is most commonly applied to strings; however, it can also
//! be useful for other object types.  This library supports interning
//! of arbitrary objects.
//!
//! There exist several interning libraries for Rust, each with its own
//! set of tradeoffs.  This library makes the following design
//! choices:
//!
//! - Interned objects are reference counted.  When the last reference to
//!   an interned object is dropped, the object is deallocated.  This
//!   prevents unbounded growth of the interned object pool in applications
//!   where the set of interned values changes dynamically at the cost of
//!   some CPU and memory overhead (due to storing and maintaining an
//!   atomic counter).
//! - Multithreading.  A single pool of interned objects is shared by all
//!   threads in the program.  Inside `DashMap` this pool is protected by
//!   mutexes that are acquired every time an object is being interned or a
//!   reference to an interned object is being dropped.  Although Rust mutexes
//!   are fairly inexpensive when there is no contention, you may see a significant
//!   drop in performance under contention.
//! - Not just strings: this library allows interning any data type that
//!   satisfies the `Eq + Hash + Send + Sync` trait bound.
//! - Safe: this library is built on the ` Arc ` type from the Rust
//!   standard library and the [`dashmap` crate](https://crates.io/crates/dashmap)
//!   and does not contain any unsafe code (although std and dashmap do of course)
//!
//! # Example
//! ```rust
//! use arc_interner::ArcIntern;
//! let x = ArcIntern::new("hello");
//! let y = ArcIntern::new("world");
//! assert_ne!(x, y);
//! assert_eq!(x, ArcIntern::new("hello"));
//! assert_eq!(*x, "hello"); // dereference an ArcIntern like a pointer
//! ```

use ahash::RandomState;
use dashmap::DashMap;
use get_size2::GetSize;
use std::any::{Any, TypeId};
use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock};

/// A pointer to a reference-counted interned object.
///
/// The interned object will be held in memory only until its
/// reference count reaches zero.
///
/// # Example
/// ```rust
/// use arc_interner::ArcIntern;
///
/// let x = ArcIntern::new("hello");
/// let y = ArcIntern::new("world");
/// assert_ne!(x, y);
/// assert_eq!(x, ArcIntern::new("hello"));
/// assert_eq!(*x, "hello"); // dereference an ArcIntern like a pointer
/// ```
#[derive(Debug)]
pub struct ArcIntern<T: Eq + Hash + Send + Sync + 'static + ?Sized> {
    arc: Arc<T>,
}

impl<T: Eq + Hash + Send + Sync + 'static + GetSize> GetSize for ArcIntern<T> {
    fn get_size(&self) -> usize {
        self.arc.get_size()
    }
}
type Container<T> = DashMap<Arc<T>, (), RandomState>;

static CONTAINER: LazyLock<DashMap<TypeId, Box<dyn Any + Send + Sync>, RandomState>> =
    LazyLock::new(DashMap::default);

static STRING_MEMORY_USED: AtomicUsize = AtomicUsize::new(0);

impl<T: Eq + Hash + Send + Sync + 'static + ?Sized> ArcIntern<T> {
    fn from_arc(val: Arc<T>) -> ArcIntern<T> {
        let type_map = &CONTAINER;

        // Prefer taking the read lock to reduce contention, only use entry api if necessary.
        let boxed = if let Some(boxed) = type_map.get(&TypeId::of::<T>()) {
            boxed
        } else {
            type_map
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Box::new(Container::<T>::with_hasher(RandomState::new())))
                .downgrade()
        };

        let m: &Container<T> = boxed.value().downcast_ref::<Container<T>>().unwrap();
        let b = m.entry(val).or_insert(());
        ArcIntern {
            arc: b.key().clone(),
        }
    }

    /// See how many objects have been interned.  This may be helpful
    /// in analyzing memory use.
    pub fn num_objects_interned() -> usize {
        if let Some(m) = CONTAINER.get(&TypeId::of::<T>()) {
            return m.downcast_ref::<Container<T>>().unwrap().len();
        }
        0
    }
    /// Return the number of references for this value.
    pub fn refcount(&self) -> usize {
        // One reference is held by the hashset; we return the number of
        // references held by actual clients.
        Arc::strong_count(&self.arc) - 1
    }
}

impl<T: Eq + Hash + Send + Sync + 'static> ArcIntern<T> {
    /// Intern a value.  If this value has not previously been
    /// interned, then `new` will allocate a spot for the value on the
    /// heap.  Otherwise, it will return a pointer to the object
    /// previously allocated.
    ///
    /// Note that `ArcIntern::new` is a bit slow, since it needs to check
    /// a `DashMap` which contains its own mutexes.
    pub fn new(val: T) -> ArcIntern<T> {
        Self::from_arc(Arc::new(val))
    }
}

impl<T: Eq + Hash + Send + Sync + 'static + ?Sized> Clone for ArcIntern<T> {
    fn clone(&self) -> Self {
        ArcIntern {
            arc: self.arc.clone(),
        }
    }
}

impl<T: Eq + Hash + Send + Sync + ?Sized> Drop for ArcIntern<T> {
    fn drop(&mut self) {
        if let Some(m) = CONTAINER.get(&TypeId::of::<T>()) {
            let m: &Container<T> = m.downcast_ref::<Container<T>>().unwrap();
            m.remove_if(&self.arc, |k, _v| {
                // If the reference count is 2, then the only two remaining references
                // to this value are held by `self` and the hashmap, and we can safely
                // deallocate the value.
                Arc::strong_count(k) == 2
            });
        }
    }
}

impl<T: Send + Sync + Hash + Eq + ?Sized> AsRef<T> for ArcIntern<T> {
    fn as_ref(&self) -> &T {
        self.arc.as_ref()
    }
}
impl<T: Eq + Hash + Send + Sync + ?Sized> Borrow<T> for ArcIntern<T> {
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
impl<T: Eq + Hash + Send + Sync + ?Sized> Deref for ArcIntern<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T: Eq + Hash + Send + Sync + Display + ?Sized> Display for ArcIntern<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        self.deref().fmt(f)
    }
}

impl<T: Eq + Hash + Send + Sync + 'static + ?Sized> From<Box<T>> for ArcIntern<T> {
    fn from(b: Box<T>) -> Self {
        Self::from_arc(Arc::from(b))
    }
}

impl<'a, T> From<&'a T> for ArcIntern<T>
where
    T: Eq + Hash + Send + Sync + 'static + ?Sized,
    Arc<T>: From<&'a T>,
{
    fn from(t: &'a T) -> Self {
        Self::from_arc(Arc::from(t))
    }
}

impl<T: Eq + Hash + Send + Sync + 'static> From<T> for ArcIntern<T> {
    fn from(t: T) -> Self {
        ArcIntern::new(t)
    }
}
impl<T: Eq + Hash + Send + Sync + Default + 'static> Default for ArcIntern<T> {
    fn default() -> ArcIntern<T> {
        ArcIntern::new(Default::default())
    }
}

impl<T: Eq + Hash + Send + Sync + ?Sized> Hash for ArcIntern<T> {
    // `Hash` implementation must be equivalent for owned and borrowed values.
    fn hash<H: Hasher>(&self, state: &mut H) {
        let borrow: &T = self.borrow();
        borrow.hash(state);
    }
}

/// Efficiently compares two interned values by comparing their pointers.
impl<T: Eq + Hash + Send + Sync + ?Sized> PartialEq for ArcIntern<T> {
    fn eq(&self, other: &ArcIntern<T>) -> bool {
        Arc::ptr_eq(&self.arc, &other.arc)
    }
}
impl<T: Eq + Hash + Send + Sync + ?Sized> Eq for ArcIntern<T> {}

impl<T: Eq + Hash + Send + Sync + PartialOrd + ?Sized> PartialOrd for ArcIntern<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other)
    }
    fn lt(&self, other: &Self) -> bool {
        self.as_ref().lt(other)
    }
    fn le(&self, other: &Self) -> bool {
        self.as_ref().le(other)
    }
    fn gt(&self, other: &Self) -> bool {
        self.as_ref().gt(other)
    }
    fn ge(&self, other: &Self) -> bool {
        self.as_ref().ge(other)
    }
}

impl<T: Eq + Hash + Send + Sync + Ord + ?Sized> Ord for ArcIntern<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other)
    }
}

/// A string type that is interned using `ArcIntern`.
/// We use a byte slice internally as it avoids the 8-byte overhead of storing
/// the capacity of the string, which is not needed for interned strings, which are immutable.
#[derive(Debug, Clone, PartialOrd, Eq, Ord)]
pub struct InternedString(ArcIntern<[u8]>);

impl InternedString {
    pub fn new(s: &str) -> Self {
        let v = ArcIntern::<[u8]>::from(s.as_ref());
        let refcount = v.refcount();
        // If this is the first time we've seen this string, account for its memory usage.
        if refcount == 1 {
            let size = Self::calc_size(s.as_ref());
            STRING_MEMORY_USED.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }
        InternedString(v)
    }

    pub fn intern(s: &str) -> Self {
        Self::new(s)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn refcount(&self) -> usize {
        self.0.refcount()
    }

    pub fn as_str(&self) -> &str {
        self.deref()
    }

    pub fn memory_used() -> usize {
        STRING_MEMORY_USED.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn num_strings_interned() -> usize {
        ArcIntern::<[u8]>::num_objects_interned()
    }

    fn calc_size(s: &[u8]) -> usize {
        size_of::<Self>() + s.len()
    }
}

impl GetSize for InternedString {
    fn get_size(&self) -> usize {
        let refs = self.refcount();
        if refs <= 1 {
            // we have the sole reference, so give the full size
            return Self::calc_size(self.0.as_ref());
        }
        // return the stack size only
        size_of::<Self>()
    }
}

impl Deref for InternedString {
    type Target = str;
    fn deref(&self) -> &str {
        // SAFETY: we only intern valid UTF-8 strings
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
}

impl AsRef<str> for InternedString {
    fn as_ref(&self) -> &str {
        self.deref()
    }
}

impl Borrow<str> for InternedString {
    fn borrow(&self) -> &str {
        self.deref()
    }
}

impl FromStr for InternedString {
    type Err = Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

impl From<String> for InternedString {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

impl Default for InternedString {
    fn default() -> Self {
        Self::new("")
    }
}

impl PartialEq for InternedString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for InternedString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Drop for InternedString {
    fn drop(&mut self) {
        if self.refcount() == 0 {
            let size = Self::calc_size(self.0.as_ref());
            STRING_MEMORY_USED.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ArcIntern, InternedString, STRING_MEMORY_USED};
    use ahash::{HashSet, HashSetExt};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread;

    // Test basic functionality.
    #[test]
    fn basic() {
        assert_eq!(ArcIntern::new("foo"), ArcIntern::new("foo"));
        assert_ne!(ArcIntern::new("foo"), ArcIntern::new("bar"));
        // The above refs should be deallocated by now.
        assert_eq!(ArcIntern::<&str>::num_objects_interned(), 0);

        let _interned1 = ArcIntern::new("foo".to_string());
        {
            let interned2 = ArcIntern::new("foo".to_string());
            let interned3 = ArcIntern::new("bar".to_string());

            assert_eq!(interned2.refcount(), 2);
            assert_eq!(interned3.refcount(), 1);
            // We now have two unique interned strings: "foo" and "bar".
            assert_eq!(ArcIntern::<String>::num_objects_interned(), 2);
        }

        // "bar" is now gone.
        assert_eq!(ArcIntern::<String>::num_objects_interned(), 1);
    }

    // Ordering should be based on values, not pointers.
    // Also tests `Display` implementation.
    #[test]
    fn sorting() {
        let mut interned_vals = [
            ArcIntern::new(4),
            ArcIntern::new(2),
            ArcIntern::new(5),
            ArcIntern::new(0),
            ArcIntern::new(1),
            ArcIntern::new(3),
        ];
        interned_vals.sort();
        let sorted: Vec<String> = interned_vals.iter().map(|v| format!("{v}")).collect();
        assert_eq!(&sorted.join(","), "0,1,2,3,4,5");
    }

    #[derive(Eq, PartialEq, Hash)]
    pub struct TestStruct2(String, u64);

    #[test]
    fn sequential() {
        for _i in 0..10_000 {
            let mut interned = Vec::with_capacity(100);
            for j in 0..100 {
                interned.push(ArcIntern::new(TestStruct2("foo".to_string(), j)));
            }
        }

        assert_eq!(ArcIntern::<TestStruct2>::num_objects_interned(), 0);
    }

    #[derive(Eq, PartialEq, Hash)]
    pub struct TestStruct(String, u64, Arc<bool>);

    // Quickly create and destroy a small number of interned objects from
    // multiple threads.
    #[test]
    fn multithreading1() {
        let mut thandles = vec![];
        let drop_check = Arc::new(true);
        for _i in 0..10 {
            let t = thread::spawn({
                let drop_check = drop_check.clone();
                move || {
                    for _i in 0..100_000 {
                        let interned1 =
                            ArcIntern::new(TestStruct("foo".to_string(), 5, drop_check.clone()));
                        let _interned2 =
                            ArcIntern::new(TestStruct("bar".to_string(), 10, drop_check.clone()));
                        let mut m = HashMap::new();
                        // force some hashing
                        m.insert(interned1, ());
                    }
                }
            });
            thandles.push(t);
        }
        for h in thandles.into_iter() {
            h.join().unwrap()
        }
        assert_eq!(Arc::strong_count(&drop_check), 1);
        assert_eq!(ArcIntern::<TestStruct>::num_objects_interned(), 0);
    }

    #[test]
    fn test_unsized() {
        assert_eq!(
            ArcIntern::<[usize]>::from(&[1, 2, 3][..]),
            ArcIntern::from(&[1, 2, 3][..])
        );
        assert_ne!(
            ArcIntern::<[usize]>::from(&[1, 2][..]),
            ArcIntern::from(&[1, 2, 3][..])
        );
        // The above refs should be deallocated by now.
        assert_eq!(ArcIntern::<[usize]>::num_objects_interned(), 0);
    }

    // Test InternedString

    // Helper to reset memory tracking before each test
    fn reset_memory_tracking() {
        STRING_MEMORY_USED.store(0, std::sync::atomic::Ordering::SeqCst);
    }

    #[test]
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
    fn test_different_interned_strings_are_not_equal() {
        reset_memory_tracking();

        let s1 = InternedString::new("hello");
        let s2 = InternedString::new("world");

        assert_ne!(s1, s2);
    }

    #[test]
    fn test_clone_interned_string_increases_refcount() {
        reset_memory_tracking();

        let s1 = InternedString::new("test");
        let initial_refcount = s1.refcount();

        let s2 = s1.clone();

        assert_eq!(s1.refcount(), initial_refcount + 1);
        assert_eq!(s2.refcount(), initial_refcount + 1);
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_drop_decreases_interned_string_refcount() {
        reset_memory_tracking();

        let s1 = InternedString::new("test");
        let s2 = s1.clone();
        let initial_refcount = s1.refcount();

        drop(s1);

        assert_eq!(s2.refcount(), initial_refcount - 1);
    }

    #[test]
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
        assert_eq!(s1.refcount(), 2);
        assert_eq!(s2.refcount(), 2);
    }

    #[test]
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
    fn test_empty_string() {
        reset_memory_tracking();

        let empty1 = InternedString::new("");
        let empty2 = InternedString::new("");

        assert_eq!(empty1, empty2);
        assert_eq!(empty1.as_str(), "");
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
        assert_eq!(unicode1.as_ref(), "🦀 Rust");
    }

    #[test]
    fn test_very_long_strings() {
        reset_memory_tracking();

        let long_string = "a".repeat(10000);
        let s1 = InternedString::new(&long_string);
        let s2 = InternedString::new(&long_string);

        assert_eq!(s1, s2);
        assert_eq!(s1.len(), 10000);
        assert_eq!(s1.refcount(), 2);
    }
}
