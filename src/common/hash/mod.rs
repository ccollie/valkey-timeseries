mod deterministic_hasher;
mod no_hash;

pub use deterministic_hasher::*;
pub use no_hash::*;

/// Hash a 64-bit float value using bitwise representation
///
/// The only reason this function exists is to provide a consistent way to hash f64 values
/// for use in the module type's `digest()` function. F64s in rust do not implement
/// the `std::hash::Hash` trait, so we need our own implementation. The concern is not
/// uniqueness or collision resistance, but consistency.
pub(crate) fn hash_f64<H: std::hash::Hasher>(value: f64, hasher: &mut H) {
    if value.is_nan() {
        // Handle NaN as a special case. In rust there are multiple representations of NaN,
        // so we use a specific representation for consistency.
        hasher.write_u64(0x7FF8000000000000); // IEEE 754 representation of NaN
        return;
    }
    let bits = value.to_bits();
    hasher.write_u64(bits);
}
