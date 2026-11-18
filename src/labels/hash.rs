use crate::labels::Label;
use std::hash::Hasher;
use std::sync::LazyLock;
use xxhash_rust::xxh3::{Xxh3, Xxh3Default, xxh3_128};

/// Series fingerprint (hash of a label set)
pub(crate) type SeriesFingerprint = u128;

pub(crate) trait HasFingerprint {
    fn fingerprint(&self) -> SeriesFingerprint;
}

impl HasFingerprint for Vec<Label> {
    fn fingerprint(&self) -> SeriesFingerprint {
        self.as_slice().fingerprint()
    }
}

/// The streaming XXH3-128 hasher label fingerprints use, on XXH3's default secret.
///
/// Fingerprints key in-process maps (grouping, partial aggregation, dedup, the selector
/// caches); nothing sends or persists them, so the default secret, the cheapest, is enough:
/// `Xxh3Default` reads the built-in secret instead of carrying its own copy, so a fresh
/// hasher is a 384-byte init with nothing to copy. The one hash that reaches query results
/// is [`create_selection_hasher`]'s. Feed `Xxh3*` with `update`: they implement both `Hasher`
/// and `io::Write`, so `write` is ambiguous wherever both traits are in scope.
pub(crate) type LabelHasher = Xxh3Default;

/// A fresh label hasher.
#[inline]
pub(crate) fn create_hasher() -> LabelHasher {
    Xxh3Default::new()
}

/// Seed of [`create_selection_hasher`]: part of query results, so it must not change.
const SELECTION_SEED: u64 = 0xa4d3f1c2b7e98d5f;

/// `Xxh3::with_seed` derives a 192-byte secret on every call; derive it once and clone.
static SELECTION_HASHER: LazyLock<Xxh3> = LazyLock::new(|| Xxh3::with_seed(SELECTION_SEED));

/// The hasher `limitk` and `limit_ratio` select and order series by.
///
/// Unlike a fingerprint, its values are visible: they decide which series those operators
/// return and in what order. They must also agree across builds, because a shard selects
/// under its build's hash and the coordinator re-applies the selection under its own, so a
/// rolling upgrade that changed them would return the wrong series. Pinned by the tests below.
#[inline]
pub(crate) fn create_selection_hasher() -> Xxh3 {
    SELECTION_HASHER.clone()
}

/// Written after every label name and every label value.
///
/// The byte 0xFF never occurs in UTF-8, so a sequence of framed labels decodes
/// one way only: `{a="x", b="y"}`, `{a="xb", ""="y"}` and `{a="", xb="y"}`
/// all hash differently. (The same framing as Prometheus' label hash.)
pub(crate) const LABEL_SEP: u8 = 0xff;

/// A framed label up to this many bytes is hashed with a single write.
const INLINE_LABEL_LEN: usize = 128;

/// Feed one `name=value` pair into a label-set hash, framed by [`LABEL_SEP`].
///
/// The streaming hash is the same however its input is split, so a label that
/// fits is framed in a stack buffer and written once: four writes per label, two
/// of them a single byte, cost a join's match keys about 15 %.
#[inline]
pub(crate) fn hash_key_value<H: Hasher>(hasher: &mut H, key: &str, value: &str) {
    let len = key.len() + value.len() + 2;
    if len <= INLINE_LABEL_LEN {
        let mut buf = [0u8; INLINE_LABEL_LEN];
        buf[..key.len()].copy_from_slice(key.as_bytes());
        buf[key.len()] = LABEL_SEP;
        buf[key.len() + 1..len - 1].copy_from_slice(value.as_bytes());
        buf[len - 1] = LABEL_SEP;
        hasher.write(&buf[..len]);
    } else {
        hasher.write(key.as_bytes());
        hasher.write(&[LABEL_SEP]);
        hasher.write(value.as_bytes());
        hasher.write(&[LABEL_SEP]);
    }
}

impl HasFingerprint for &str {
    fn fingerprint(&self) -> SeriesFingerprint {
        xxh3_128(self.as_bytes())
    }
}

impl HasFingerprint for String {
    fn fingerprint(&self) -> SeriesFingerprint {
        self.as_str().fingerprint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A label framed in the stack buffer hashes exactly as its four separate
    /// writes do, on both sides of the inline limit and across a long sequence
    /// that crosses the hash's internal block boundaries.
    #[test]
    fn inline_label_write_matches_separate_writes() {
        let separate = |hasher: &mut LabelHasher, key: &str, value: &str| {
            hasher.update(key.as_bytes());
            hasher.update(&[LABEL_SEP]);
            hasher.update(value.as_bytes());
            hasher.update(&[LABEL_SEP]);
        };
        let fits = "x".repeat(INLINE_LABEL_LEN - 3);
        let over = "x".repeat(INLINE_LABEL_LEN - 2);
        let long = "y".repeat(1000);
        let labels: Vec<(String, String)> = [
            ("", ""),
            ("job", "api"),
            ("a", &fits),
            ("a", &over),
            ("b", &long),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .chain((0..40).map(|i| (format!("label_{i}"), format!("value-{}", i * 7))))
        .collect();

        for (key, value) in &labels {
            let (mut one, mut four) = (create_hasher(), create_hasher());
            hash_key_value(&mut one, key, value);
            separate(&mut four, key, value);
            assert_eq!(one.digest128(), four.digest128(), "{key}={value}");
        }

        let (mut one, mut four) = (create_hasher(), create_hasher());
        for (key, value) in &labels {
            hash_key_value(&mut one, key, value);
            separate(&mut four, key, value);
        }
        assert_eq!(one.digest128(), four.digest128());
    }

    /// Label boundaries must survive hashing: the separator used to be the
    /// four ASCII bytes `0xfe`, with nothing after a value, so a value
    /// containing that text (or an empty value) could stand in for a label
    /// boundary and two different label sets shared a fingerprint.
    #[test]
    fn label_boundaries_are_part_of_the_fingerprint() {
        use crate::labels::{Label, fingerprint_labels};
        let set = |pairs: &[(&str, &str)]| -> Vec<Label> {
            pairs.iter().map(|(n, v)| Label::new(*n, *v)).collect()
        };
        let fp = |pairs: &[(&str, &str)]| fingerprint_labels(set(pairs).iter());
        let two = fp(&[("a", "x"), ("b", "y")]);
        assert_ne!(two, fp(&[("a", "xb0xfey")]));
        assert_ne!(two, fp(&[("a", ""), ("xb", "y")]));
        assert_ne!(two, fp(&[("a", "xb"), ("", "y")]));
        assert_ne!(fp(&[("a", "0xfeb")]), fp(&[("a0xfe", "b")]));

        // `impl Hash for Label` frames the same way.
        let std_hash = |pairs: &[(&str, &str)]| {
            use std::hash::{BuildHasher, RandomState};
            thread_local!(static STATE: RandomState = RandomState::new());
            STATE.with(|s| s.hash_one(set(pairs)))
        };
        let two = std_hash(&[("a", "x"), ("b", "y")]);
        assert_ne!(two, std_hash(&[("a", ""), ("xb", "y")]));
        assert_ne!(two, std_hash(&[("a", "xb"), ("", "y")]));
    }

    /// XXH3-128 reference values (computed with `twox-hash`, an independent implementation),
    /// on both sides of the 240-byte short-input path, so a dependency bump that changed the
    /// output, or a framing change in `fingerprint_labels`, shows up here.
    #[test]
    fn hasher_produces_pinned_xxh3_values() {
        let cases: [(&[u8], u128); 4] = [
            (b"", 0x99aa06d3014798d86001c324468d497f),
            (b"host", 0x539fc971e1a39d162f1d49ce83ae3d88),
            (
                b"__name__=cpu,host=h1,region=us",
                0xb555043a1c6c512a8f24acd9f77353b5,
            ),
            (&[7u8; 1024], 0x30c217e3c4356cdbc8e98c406ca2eda1),
        ];
        for (input, expected) in cases {
            let mut hasher = create_hasher();
            hasher.update(input);
            assert_eq!(hasher.digest128(), expected, "{} bytes", input.len());
        }
        // The one-shot `&str` fingerprint and the streaming hasher agree.
        assert_eq!("host".fingerprint(), 0x539fc971e1a39d162f1d49ce83ae3d88);

        use crate::labels::fingerprint_labels;
        let labels = [
            Label::new("__name__", "cpu"),
            Label::new("host", "h1"),
            Label::new("region", "us"),
        ];
        assert_eq!(
            fingerprint_labels(labels.iter()),
            0x1784afd961a790018d98651b6a423b71
        );
    }

    /// `limitk`/`limit_ratio` results depend on these, and shards and the coordinator must
    /// agree on them: seeded XXH3-128 reference values (computed with `twox-hash`).
    #[test]
    fn selection_hasher_produces_pinned_values() {
        let cases: [(&[u8], u128); 4] = [
            (b"", 0x89fe23895a7226ff7114d8e46b672ae8),
            (b"host", 0x7e3788019bbdca50a7e21df72495c688),
            (
                b"__name__=cpu,host=h1,region=us",
                0x90eb47891523d9df7bb7ca9bfbad315b,
            ),
            (&[7u8; 1024], 0xd9898fe9e538af29fd44bdc54262f2f6),
        ];
        for (input, expected) in cases {
            let mut hasher = create_selection_hasher();
            hasher.update(input);
            assert_eq!(hasher.digest128(), expected, "{} bytes", input.len());
        }

        use crate::labels::selection_hash_labels;
        let labels = [
            Label::new("__name__", "cpu"),
            Label::new("host", "h1"),
            Label::new("region", "us"),
        ];
        assert_eq!(
            selection_hash_labels(labels.iter()),
            0x970c84ff2032b3b16e8fc8fef3ffdf47
        );
    }
}
