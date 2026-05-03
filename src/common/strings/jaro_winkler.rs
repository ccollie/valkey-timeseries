// Original Source: https://github.com/joshuaclayton/jaro_winkler
// Copyright (c) 2022 Josh Clayton
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
// and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use std::collections::HashMap;

/// Calculates the Jaro-Winkler distance of two strings.
///
/// Comparison is performed on Unicode scalar values (chars), not raw UTF-8
/// bytes. This ensures that multi-byte characters are treated as single units
/// and non-ASCII strings are scored correctly.
///
/// When both inputs are pure ASCII the implementation uses a zero-allocation
/// byte-slice fast path, so the common case of ASCII label names incurs no
/// heap overhead.
///
/// The return value is between 0.0 and 1.0, where 1.0 means the strings are equal.
///
/// # Examples
///
/// ```
/// use valkey_timeseries::common::strings::jaro_winkler;
///
/// assert_eq!(jaro_winkler("martha", "marhta"), 0.96111107);
/// assert_eq!(jaro_winkler("", "words"), 0.0);
/// assert_eq!(jaro_winkler("same", "same"), 1.0);
/// ```
pub fn jaro_winkler(left_: &str, right_: &str) -> f32 {
    // Fast path: both strings are pure ASCII — bytes and chars are identical, so
    // we can use the byte slices directly without any allocation.
    if left_.is_ascii() && right_.is_ascii() {
        return jaro_winkler_bytes(left_.as_bytes(), right_.as_bytes());
    }

    // Unicode path: collect char vectors so every codepoint is treated as a
    // single unit regardless of its UTF-8 byte width.
    let left_chars: Vec<char> = left_.chars().collect();
    let right_chars: Vec<char> = right_.chars().collect();
    let llen = left_chars.len();
    let rlen = right_chars.len();

    let (left, right, s1_len, s2_len) = if llen < rlen {
        (right_chars.as_slice(), left_chars.as_slice(), rlen, llen)
    } else {
        (left_chars.as_slice(), right_chars.as_slice(), llen, rlen)
    };

    match (s1_len, s2_len) {
        (0, 0) => return 1.0,
        (0, _) | (_, 0) => return 0.0,
        (_, _) => (),
    }

    if s1_len == s2_len && left == right {
        return 1.0;
    }

    let range = matching_distance(s1_len, s2_len);

    if s1_len > 128 {
        return jaro_winkler_long(left, right, s1_len, s2_len, range);
    }

    // Both strings fit in 128 chars — use u128 bitmasks directly.
    let mut s1m: u128 = 0;
    let mut s2m: u128 = 0;
    let mut matching: f32 = 0.0;
    let mut transpositions: f32 = 0.0;

    #[allow(clippy::needless_range_loop)]
    for i in 0..s2_len {
        let mut j = i.saturating_sub(range);
        let l = (i + range + 1).min(s1_len);
        while j < l {
            if right[i] == left[j] && (s1m >> j) & 1 != 1 {
                s1m |= 1 << j;
                s2m |= 1 << i;
                matching += 1.0;
                break;
            }

            j += 1;
        }
    }

    if matching == 0.0 {
        return 0.0;
    }

    let mut l = 0;

    #[allow(clippy::needless_range_loop)]
    for i in 0..s2_len - 1 {
        if (s2m >> i) & 1 == 1 {
            let mut j = l;

            while j < s1_len {
                if (s1m >> j) & 1 == 1 {
                    l = j + 1;
                    break;
                }

                j += 1;
            }

            if right[i] != left[j] {
                transpositions += 1.0;
            }
        }
    }
    transpositions = (transpositions / 2.0).ceil();

    winkler_chars(matching, transpositions, s1_len, s2_len, left, right)
}

/// Zero-allocation Jaro-Winkler over pure-ASCII byte slices.
///
/// Both inputs MUST be valid ASCII; calling this with non-ASCII bytes produces
/// incorrect results.
fn jaro_winkler_bytes(left_: &[u8], right_: &[u8]) -> f32 {
    let llen = left_.len();
    let rlen = right_.len();

    let (left, right, s1_len, s2_len) = if llen < rlen {
        (right_, left_, rlen, llen)
    } else {
        (left_, right_, llen, rlen)
    };

    match (s1_len, s2_len) {
        (0, 0) => return 1.0,
        (0, _) | (_, 0) => return 0.0,
        (_, _) => (),
    }

    if s1_len == s2_len && left == right {
        return 1.0;
    }

    let range = matching_distance(s1_len, s2_len);

    if s1_len > 128 {
        return jaro_winkler_long_bytes(left, right, s1_len, s2_len, range);
    }

    let mut s1m: u128 = 0;
    let mut s2m: u128 = 0;
    let mut matching: f32 = 0.0;
    let mut transpositions: f32 = 0.0;

    #[allow(clippy::needless_range_loop)]
    for i in 0..s2_len {
        let mut j = i.saturating_sub(range);
        let l = (i + range + 1).min(s1_len);
        while j < l {
            if right[i] == left[j] && (s1m >> j) & 1 != 1 {
                s1m |= 1 << j;
                s2m |= 1 << i;
                matching += 1.0;
                break;
            }
            j += 1;
        }
    }

    if matching == 0.0 {
        return 0.0;
    }

    let mut l = 0;

    #[allow(clippy::needless_range_loop)]
    for i in 0..s2_len - 1 {
        if (s2m >> i) & 1 == 1 {
            let mut j = l;
            while j < s1_len {
                if (s1m >> j) & 1 == 1 {
                    l = j + 1;
                    break;
                }
                j += 1;
            }
            if right[i] != left[j] {
                transpositions += 1.0;
            }
        }
    }
    transpositions = (transpositions / 2.0).ceil();

    winkler_bytes(matching, transpositions, s1_len, s2_len, left, right)
}

/// Bit-parallel Jaro-Winkler for strings where s1_len > 128 chars.
///
/// Instead of scanning the match window char-by-char, we precompute a bitmask
/// for each distinct char value, recording all positions where it occurs in the
/// longer string. Matching then reduces to bitwise AND/NOT operations per 64-bit
/// word, replacing ~range char comparisons with ~(range/64) word operations.
fn jaro_winkler_long(
    left: &[char],
    right: &[char],
    s1_len: usize,
    s2_len: usize,
    range: usize,
) -> f32 {
    let s1_words = (s1_len + 63) >> 6;
    let s2_words = (s2_len + 63) >> 6;

    // Precompute: for each char, a bitmask of positions in left where it occurs.
    // Using a HashMap because chars have a large domain (unlike the 256-byte case).
    // Capacity is capped at 256 since that's the maximum number of distinct ASCII
    // byte values; for real Unicode the actual distinct char count is usually small.
    let mut char_masks: HashMap<char, Vec<u64>> = HashMap::with_capacity(s1_len.min(256));
    for (j, &c) in left.iter().enumerate() {
        char_masks
            .entry(c)
            .or_insert_with(|| vec![0u64; s1_words])[j >> 6] |= 1u64 << (j & 63);
    }

    let mut s1m = vec![0u64; s1_words];
    let mut s2m = vec![0u64; s2_words];
    let mut matching: f32 = 0.0;

    for i in 0..s2_len {
        let target = right[i];
        let j_start = i.saturating_sub(range);
        let j_end = (i + range + 1).min(s1_len);

        let start_word = j_start >> 6;
        let end_word = ((j_end - 1) >> 6) + 1;

        if let Some(masks) = char_masks.get(&target) {
            for w in start_word..end_word.min(s1_words) {
                let mut candidates = masks[w] & !s1m[w];

                // Mask out bits outside the match window in boundary words.
                let lo = if w == start_word { j_start & 63 } else { 0 };
                let hi = if w == end_word - 1 {
                    let b = j_end & 63;
                    if b == 0 { 64 } else { b }
                } else {
                    64
                };

                if lo > 0 {
                    candidates &= !((1u64 << lo) - 1);
                }
                if hi < 64 {
                    candidates &= (1u64 << hi) - 1;
                }

                if candidates != 0 {
                    let bit = candidates.trailing_zeros();
                    s1m[w] |= 1u64 << bit;
                    s2m[i >> 6] |= 1u64 << (i & 63);
                    matching += 1.0;
                    break;
                }
            }
        }
    }

    if matching == 0.0 {
        return 0.0;
    }

    // Count transpositions
    let mut transpositions: f32 = 0.0;
    let mut l = 0usize;

    for i in 0..s2_len - 1 {
        if (s2m[i >> 6] >> (i & 63)) & 1 == 1 {
            let mut j = l;
            while j < s1_len {
                if (s1m[j >> 6] >> (j & 63)) & 1 == 1 {
                    l = j + 1;
                    break;
                }
                j += 1;
            }
            if right[i] != left[j] {
                transpositions += 1.0;
            }
        }
    }
    transpositions = (transpositions / 2.0).ceil();

    winkler_chars(matching, transpositions, s1_len, s2_len, left, right)
}

fn winkler_chars(
    matching: f32,
    transpositions: f32,
    s1_len: usize,
    s2_len: usize,
    left: &[char],
    right: &[char],
) -> f32 {
    let jaro = (matching / (s1_len as f32)
        + matching / (s2_len as f32)
        + (matching - transpositions) / matching)
        / 3.0;

    let prefix_length = left
        .iter()
        .zip(right)
        .take(4)
        .take_while(|(l, r)| l == r)
        .count() as f32;

    jaro + prefix_length * 0.1 * (1.0 - jaro)
}

fn winkler_bytes(
    matching: f32,
    transpositions: f32,
    s1_len: usize,
    s2_len: usize,
    left: &[u8],
    right: &[u8],
) -> f32 {
    let jaro = (matching / (s1_len as f32)
        + matching / (s2_len as f32)
        + (matching - transpositions) / matching)
        / 3.0;

    let prefix_length = left
        .iter()
        .zip(right)
        .take(4)
        .take_while(|(l, r)| l == r)
        .count() as f32;

    jaro + prefix_length * 0.1 * (1.0 - jaro)
}

/// Bit-parallel Jaro-Winkler for pure-ASCII strings where s1_len > 128 bytes.
fn jaro_winkler_long_bytes(
    left: &[u8],
    right: &[u8],
    s1_len: usize,
    s2_len: usize,
    range: usize,
) -> f32 {
    let s1_words = (s1_len + 63) >> 6;
    let s2_words = (s2_len + 63) >> 6;

    // Precompute: for each byte value, a bitmask of positions in left where it occurs.
    let mut char_masks = vec![0u64; 256 * s1_words];
    for (j, &b) in left.iter().enumerate() {
        char_masks[(b as usize) * s1_words + (j >> 6)] |= 1u64 << (j & 63);
    }

    let mut s1m = vec![0u64; s1_words];
    let mut s2m = vec![0u64; s2_words];
    let mut matching: f32 = 0.0;

    for i in 0..s2_len {
        let target = right[i] as usize;
        let j_start = i.saturating_sub(range);
        let j_end = (i + range + 1).min(s1_len);

        let start_word = j_start >> 6;
        let end_word = ((j_end - 1) >> 6) + 1;

        for w in start_word..end_word.min(s1_words) {
            let mut candidates = char_masks[target * s1_words + w] & !s1m[w];

            let lo = if w == start_word { j_start & 63 } else { 0 };
            let hi = if w == end_word - 1 {
                let b = j_end & 63;
                if b == 0 { 64 } else { b }
            } else {
                64
            };

            if lo > 0 {
                candidates &= !((1u64 << lo) - 1);
            }
            if hi < 64 {
                candidates &= (1u64 << hi) - 1;
            }

            if candidates != 0 {
                let bit = candidates.trailing_zeros();
                s1m[w] |= 1u64 << bit;
                s2m[i >> 6] |= 1u64 << (i & 63);
                matching += 1.0;
                break;
            }
        }
    }

    if matching == 0.0 {
        return 0.0;
    }

    let mut transpositions: f32 = 0.0;
    let mut l = 0usize;

    for i in 0..s2_len - 1 {
        if (s2m[i >> 6] >> (i & 63)) & 1 == 1 {
            let mut j = l;
            while j < s1_len {
                if (s1m[j >> 6] >> (j & 63)) & 1 == 1 {
                    l = j + 1;
                    break;
                }
                j += 1;
            }
            if right[i] != left[j] {
                transpositions += 1.0;
            }
        }
    }
    transpositions = (transpositions / 2.0).ceil();

    winkler_bytes(matching, transpositions, s1_len, s2_len, left, right)
}

fn matching_distance(s1_len: usize, s2_len: usize) -> usize {
    (s1_len.max(s2_len) / 2).saturating_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn different_is_zero() {
        assert_eq!(jaro_winkler("foo", "bar"), 0.0);
    }

    #[test]
    fn same_is_one() {
        assert_eq!(jaro_winkler("foo", "foo"), 1.0);
        assert_eq!(jaro_winkler("", ""), 1.0);
    }

    #[test]
    fn test_hello() {
        assert_eq!(jaro_winkler("hell", "hello"), 0.96_f32);
    }

    /// Reference implementation using the standard (non-bit-parallel) algorithm
    /// operating on Unicode scalar values. Used to validate that the optimised
    /// bit-parallel path produces identical results.
    fn jaro_winkler_reference(left_: &str, right_: &str) -> f64 {
        let left_chars: Vec<char> = left_.chars().collect();
        let right_chars: Vec<char> = right_.chars().collect();
        let llen = left_chars.len();
        let rlen = right_chars.len();
        let (left, right, s1_len, s2_len) = if llen < rlen {
            (right_chars.as_slice(), left_chars.as_slice(), rlen, llen)
        } else {
            (left_chars.as_slice(), right_chars.as_slice(), llen, rlen)
        };
        match (s1_len, s2_len) {
            (0, 0) => return 1.0,
            (0, _) | (_, 0) => return 0.0,
            _ => (),
        }
        if s1_len == s2_len && left == right {
            return 1.0;
        }
        let range = (s1_len.max(s2_len) / 2).saturating_sub(1);
        let mut s1m = vec![false; s1_len];
        let mut s2m = vec![false; s2_len];
        let mut matching: f64 = 0.0;
        let mut transpositions: f64 = 0.0;
        for i in 0..s2_len {
            let j_start = (i as isize - range as isize).max(0) as usize;
            let j_end = (i + range + 1).min(s1_len);
            for j in j_start..j_end {
                if right[i] == left[j] && !s1m[j] {
                    s1m[j] = true;
                    s2m[i] = true;
                    matching += 1.0;
                    break;
                }
            }
        }
        if matching == 0.0 {
            return 0.0;
        }
        let mut l = 0;
        for i in 0..s2_len - 1 {
            if s2m[i] {
                let mut j = l;
                while j < s1_len {
                    if s1m[j] {
                        l = j + 1;
                        break;
                    }
                    j += 1;
                }
                if right[i] != left[j] {
                    transpositions += 1.0;
                }
            }
        }
        transpositions = (transpositions / 2.0).ceil();
        let jaro = (matching / (s1_len as f64)
            + matching / (s2_len as f64)
            + (matching - transpositions) / matching)
            / 3.0;
        let prefix_length = left
            .iter()
            .zip(right)
            .take(4)
            .take_while(|(l, r)| l == r)
            .count() as f64;
        jaro + prefix_length * 0.1 * (1.0 - jaro)
    }

    /// Validate the optimized jaro_winkler (including bit-parallel path for
    /// long strings) produces identical results to the reference implementation
    /// across a wide range of inputs. Also verifies order independence.
    #[test]
    fn cross_validate_optimized_vs_reference() {
        let pairs: Vec<(&str, &str)> = vec![
            ("", ""),
            ("", "a"),
            ("a", ""),
            ("a", "a"),
            ("a", "b"),
            ("foo", "bar"),
            ("foo", "foo"),
            ("hell", "hello"),
            ("martha", "marhta"),
            ("wonderful", "wonderment"),
            ("hello hi what is going on", "hell"),
            // Long: exercises jaro_winkler_long (223 chars)
            (
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
                "wonderment double double",
            ),
            // Both long: both > 128
            (
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
                "; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
            ),
            // Boundary: exactly 129 chars
            (
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test",
                "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured;test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s Doc-tests jaro running 0 tests test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s",
            ),
            // Short left, long right (forces swap)
            (
                "abc",
                "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz0123456789",
            ),
            // Both very long, mostly different
            (
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
            // Non-ASCII: identical Unicode strings score 1.0
            ("café", "café"),
            // Non-ASCII: completely different multibyte strings score < 1.0
            ("日本語", "中文"),
            // Non-ASCII: partial overlap of multibyte chars
            ("naïve", "naive"),
        ];

        for (a, b) in &pairs {
            let optimized = jaro_winkler(a, b);
            let reference = jaro_winkler_reference(a, b);
            assert!(
                (optimized as f64 - reference).abs() < 1e-6,
                "Mismatch for ({:?}, {:?}): optimized={}, reference={}",
                a,
                b,
                optimized,
                reference
            );
            // Verify order independence
            let optimized_flipped = jaro_winkler(b, a);
            assert!(
                (optimized_flipped as f64 - reference).abs() < 1e-6,
                "Mismatch (flipped) for ({:?}, {:?}): optimized={}, reference={}",
                b,
                a,
                optimized_flipped,
                reference
            );
        }
    }

    /// Identical non-ASCII strings must score exactly 1.0.
    #[test]
    fn non_ascii_identical_scores_one() {
        assert_eq!(jaro_winkler("café", "café"), 1.0);
        assert_eq!(jaro_winkler("日本語", "日本語"), 1.0);
        assert_eq!(jaro_winkler("Ünïcödé", "Ünïcödé"), 1.0);
    }

    /// Completely different non-ASCII strings must score < 1.0 (previously
    /// raw-byte comparison could produce false matches due to shared encoding bytes).
    #[test]
    fn non_ascii_different_scores_below_one() {
        // These Japanese and Chinese strings share no Unicode scalar values.
        let score = jaro_winkler("日本語", "中文字");
        assert!(
            score < 1.0,
            "expected score < 1.0 for unrelated non-ASCII strings, got {score}"
        );
    }
}
