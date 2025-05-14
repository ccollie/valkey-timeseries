use super::index_key::{format_key_for_label_value, get_key_for_label_prefix, IndexKey};
use blart::map::Entry as ARTEntry;
use blart::TreeMap;
use std::borrow::Cow;
use std::sync::LazyLock;

use crate::common::hash::IntMap;
use crate::labels::matchers::{Matcher, PredicateMatch, PredicateValue};
use crate::labels::SeriesLabel;
use crate::series::index::init_croaring_allocator;
use crate::series::{SeriesRef, TimeSeries};
use croaring::Bitmap64;

pub(super) const ALL_POSTINGS_KEY_NAME: &str = "$_ALL_P0STINGS_";
pub(super) static EMPTY_BITMAP: LazyLock<PostingsBitmap> = LazyLock::new(PostingsBitmap::new);
pub(super) static ALL_POSTINGS_KEY: LazyLock<IndexKey> =
    LazyLock::new(|| IndexKey::from(ALL_POSTINGS_KEY_NAME));

pub type PostingsBitmap = Bitmap64;
// label
// label=value
pub type PostingsIndex = TreeMap<IndexKey, PostingsBitmap>;

/// Type for the key of the index.
pub type KeyType = Box<[u8]>;

#[derive(Clone)]
pub struct MemoryPostings {
    /// Map from label name and (label name, label value) to a set of timeseries ids.
    pub(super) label_index: PostingsIndex,
    /// Map from timeseries id to the key of the timeseries.
    pub(super) id_to_key: IntMap<SeriesRef, KeyType>, // todo: use an interned string
    /// Map valkey key to timeseries id. An ART is used for prefix compression.
    pub(super) key_to_id: TreeMap<IndexKey, SeriesRef>,
}

impl Default for MemoryPostings {
    fn default() -> Self {
        init_croaring_allocator();
        MemoryPostings {
            label_index: PostingsIndex::new(),
            id_to_key: IntMap::default(),
            key_to_id: Default::default(),
        }
    }
}

impl MemoryPostings {
    pub(super) fn clear(&mut self) {
        self.label_index.clear();
        self.id_to_key.clear();
        self.key_to_id.clear();
    }

    // swap the inner value with some other value
    // this is specifically to handle the `swapdb` event callback
    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.label_index, &mut other.label_index);
        std::mem::swap(&mut self.id_to_key, &mut other.id_to_key);
        std::mem::swap(&mut self.key_to_id, &mut other.key_to_id);
    }

    pub(super) fn remove_posting_for_label_value(
        &mut self,
        label: &str,
        value: &str,
        ts_id: SeriesRef,
    ) {
        let key = IndexKey::for_label_value(label, value);
        if let Some(bmp) = self.label_index.get_mut(&key) {
            bmp.remove(ts_id);
            if bmp.is_empty() {
                self.label_index.remove(&key);
            }
        }
    }

    pub(super) fn remove_posting_by_id_and_labels<T: SeriesLabel>(
        &mut self,
        id: SeriesRef,
        labels: &[T],
    ) {
        self.remove_id_from_all_postings(id);

        // should never happen, but just in case
        if labels.is_empty() {
            return;
        }

        for label in labels.iter() {
            self.remove_posting_for_label_value(label.name(), label.value(), id);
        }
    }

    pub(crate) fn add_posting_for_label_value(
        &mut self,
        ts_id: SeriesRef,
        label: &str,
        value: &str,
    ) -> bool {
        let key = IndexKey::for_label_value(label, value);
        match self.label_index.entry(key) {
            ARTEntry::Occupied(mut entry) => {
                entry.get_mut().add(ts_id);
                false
            }
            ARTEntry::Vacant(entry) => {
                let mut bmp = PostingsBitmap::new();
                bmp.add(ts_id);
                entry.insert(bmp);
                true
            }
        }
    }

    pub(super) fn add_id_to_all_postings(&mut self, id: SeriesRef) {
        let key = &*ALL_POSTINGS_KEY;
        if let Some(bitmap) = self.label_index.get_mut(key) {
            bitmap.add(id);
        } else {
            let mut bmp = PostingsBitmap::new();
            bmp.add(id);
            self.label_index.insert(key.clone(), bmp);
        }
    }

    fn remove_id_from_all_postings(&mut self, id: SeriesRef) {
        if let Some(bmp) = self.label_index.get_mut(&*ALL_POSTINGS_KEY) {
            bmp.remove(id);
        }
    }

    pub fn set_timeseries_key(&mut self, id: SeriesRef, new_key: &[u8]) {
        if let Some(existing) = self.id_to_key.get(&id) {
            if existing.as_ref() == new_key {
                return;
            }
        }
        let key = new_key.to_vec().into_boxed_slice();
        self.id_to_key.insert(id, key);
        self.key_to_id.insert(new_key.into(), id);
    }

    pub fn remove_timeseries(&mut self, series: &TimeSeries) {
        let id = series.id;
        if let Some(key) = self.id_to_key.remove(&id) {
            let key = IndexKey::from(key.as_ref());
            if let Some(existing) = self.key_to_id.remove(&key) {
                debug_assert_eq!(existing, id);
            }
        }
        let labels = series.labels.iter().collect::<Vec<_>>();
        self.remove_posting_by_id_and_labels(id, &labels);
        self.remove_id_from_all_postings(id);
    }
    
    pub fn rename_series_key(
        &mut self,
        old_key: &[u8],
        new_key: &[u8],
    ) -> Option<SeriesRef> {
        // TODO: figure out how to handle this allocation
        let old_key = IndexKey::from(old_key);
        if let Some(id) = self.key_to_id.remove(&old_key) {
            let key = new_key.to_vec().into_boxed_slice();
            self.id_to_key.remove(&id);
            self.id_to_key.insert(id, key);
            
            let new_key = IndexKey::from(new_key);
            self.key_to_id.insert(new_key, id);
            Some(id)
        } else {
            None
        }
    }

    pub fn count(&self) -> usize {
        self.id_to_key.len()
    }

    pub fn all_postings(&self) -> &PostingsBitmap {
        self.label_index
            .get(&*ALL_POSTINGS_KEY)
            .unwrap_or(&*EMPTY_BITMAP)
    }

    pub fn max_id(&self) -> SeriesRef {
        self.all_postings().maximum().unwrap_or_default()
    }

    pub fn get_id_for_key(&self, key: &[u8]) -> Option<SeriesRef> {
        let key = IndexKey::from(key);
        self.key_to_id.get(&key).map(|v| *v)
    }

    pub(super) fn has_id(&self, id: SeriesRef) -> bool {
        self.id_to_key.contains_key(&id)
    }

    pub fn postings_for_label_value<'a>(
        &'a self,
        name: &str,
        value: &str,
    ) -> Cow<'a, PostingsBitmap> {
        let key = IndexKey::for_label_value(name, value);
        if let Some(bmp) = self.label_index.get(&key) {
            Cow::Borrowed(bmp)
        } else {
            Cow::Borrowed(&*EMPTY_BITMAP)
        }
    }

    pub fn postings_for_all_label_values(&self, label_name: &str) -> PostingsBitmap {
        let prefix = get_key_for_label_prefix(label_name);
        let mut result = PostingsBitmap::new();
        for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
            result |= map;
        }
        result
    }

    /// `postings` returns the postings list iterator for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    pub fn postings(&self, name: &str, values: &[String]) -> PostingsBitmap {
        let mut result = PostingsBitmap::new();

        for value in values {
            let key = IndexKey::for_label_value(name, value);
            if let Some(bmp) = self.label_index.get(&key) {
                result |= bmp;
            }
        }
        result
    }

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which match returns true. If no postings are found having at least one matching label,
    /// an empty bitmap is returned.
    pub fn postings_for_label_matching<F, STATE>(
        &self,
        name: &str,
        state: &mut STATE,
        match_fn: F,
    ) -> PostingsBitmap
    where
        F: Fn(&str, &mut STATE) -> bool,
    {
        let prefix = get_key_for_label_prefix(name);
        let start_pos = prefix.len();
        let mut result = PostingsBitmap::new();

        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if match_fn(value, state) {
                result |= map;
            }
        }
        result
    }

    /// Return all series ids corresponding to the given labels
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let mut key: String = String::new();
        let mut first = true;
        let mut acc = PostingsBitmap::new();

        for label in labels.iter() {
            format_key_for_label_value(&mut key, label.name(), label.value());
            if let Some(bmp) = self.label_index.get(key.as_bytes()) {
                if bmp.is_empty() {
                    break;
                }
                if first {
                    acc |= bmp;
                    first = false;
                } else {
                    acc &= bmp;
                }
            }
        }
        acc
    }

    pub fn postings_without_label(&self, label: &str) -> Cow<PostingsBitmap> {
        let all = self.all_postings();
        let to_remove = self.postings_for_all_label_values(label);
        if to_remove.is_empty() {
            Cow::Borrowed(all)
        } else {
            Cow::Owned(all.andnot(&to_remove))
        }
    }

    #[allow(dead_code)]
    pub fn postings_without_labels<'a>(&'a self, labels: &[&str]) -> Cow<'a, PostingsBitmap> {
        match labels.len() {
            0 => Cow::Borrowed(self.all_postings()),     // bad boy !!
            1 => self.postings_without_label(labels[0]), // slightly more efficient (1 less allocation)
            _ => {
                let all = self.all_postings();
                let mut to_remove = PostingsBitmap::new();
                for label in labels {
                    let prefix = get_key_for_label_prefix(label);
                    for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
                        to_remove.or_inplace(map);
                    }
                }
                if to_remove.is_empty() {
                    Cow::Borrowed(all)
                } else {
                    Cow::Owned(all.andnot(&to_remove))
                }
            }
        }
    }

    pub fn postings_for_matcher(&self, matcher: &Matcher) -> Cow<PostingsBitmap> {
        match matcher.matcher {
            PredicateMatch::Equal(ref value) => handle_equal_match(self, &matcher.label, value),
            PredicateMatch::NotEqual(ref value) => {
                handle_not_equal_match(self, &matcher.label, value)
            }
            PredicateMatch::RegexEqual(_) => handle_regex_equal_match(self, matcher),
            PredicateMatch::RegexNotEqual(_) => handle_regex_not_equal_match(self, matcher),
        }
    }

    pub(super) fn get_key_range(&self) -> Option<(&IndexKey, &IndexKey)> {
        if let Some((start, _)) = self.label_index.first_key_value() {
            if let Some((end, _)) = self.label_index.last_key_value() {
                return Some((start, end));
            }
        }
        None
    }

    pub(crate) fn get_key_by_id(&self, id: SeriesRef) -> Option<&KeyType> {
        self.id_to_key.get(&id)
    }
}

pub(super) fn handle_equal_match<'a>(
    ix: &'a MemoryPostings,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    match value {
        PredicateValue::String(ref s) => {
            if s.is_empty() {
                return ix.postings_without_label(label);
            }
            ix.postings_for_label_value(label, s)
        }
        PredicateValue::List(ref val) => match val.len() {
            0 => ix.postings_without_label(label),
            1 => ix.postings_for_label_value(label, &val[0]),
            _ => Cow::Owned(ix.postings(label, val)),
        },
        PredicateValue::Empty => ix.postings_without_label(label),
    }
}

// return postings for series which has the label `label
fn with_label<'a>(ix: &'a MemoryPostings, label: &str) -> Cow<'a, PostingsBitmap> {
    let mut state = ();
    let postings = ix.postings_for_label_matching(label, &mut state, |_value, _| true);
    Cow::Owned(postings)
}

pub(super) fn handle_not_equal_match<'a>(
    ix: &'a MemoryPostings,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    // the time series has a label named label
    match value {
        PredicateValue::String(ref s) => {
            if s.is_empty() {
                return with_label(ix, label);
            }
            let all = ix.all_postings();
            let postings = ix.postings_for_label_value(label, s);
            if postings.is_empty() {
                Cow::Borrowed(all)
            } else {
                let result = all.andnot(&postings);
                Cow::Owned(result)
            }
        }
        PredicateValue::List(ref values) => {
            match values.len() {
                0 => with_label(ix, label), // TODO !!
                _ => {
                    // get postings for label m.label without values in values
                    let to_remove = ix.postings(label, values);
                    let all_postings = ix.all_postings();
                    if to_remove.is_empty() {
                        Cow::Borrowed(all_postings)
                    } else {
                        let result = all_postings.andnot(&to_remove);
                        Cow::Owned(result)
                    }
                }
            }
        }
        PredicateValue::Empty => with_label(ix, label),
    }
}

pub(super) fn handle_regex_equal_match<'a>(
    postings: &'a MemoryPostings,
    matcher: &Matcher,
) -> Cow<'a, PostingsBitmap> {
    if matcher.is_empty_matcher() {
        return postings.postings_without_label(&matcher.label);
    }
    let mut state = matcher;
    let postings =
        postings.postings_for_label_matching(&matcher.label, &mut state, |value, matcher| {
            matcher.matches(value)
        });
    Cow::Owned(postings)
}

pub(super) fn handle_regex_not_equal_match<'a>(
    postings: &'a MemoryPostings,
    matcher: &Matcher,
) -> Cow<'a, PostingsBitmap> {
    let matches_empty = matcher.is_empty_matcher();
    if matches_empty {
        return with_label(postings, &matcher.label);
    }
    let mut state = matcher;
    let postings =
        postings.postings_for_label_matching(&matcher.label, &mut state, |value, matcher| {
            matcher.matches(value)
        });
    Cow::Owned(postings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{InternedMetricName, Label};
    use crate::series::time_series::TimeSeries;

    #[test]
    fn test_memory_postings_add_and_remove() {
        let mut postings = MemoryPostings::default();

        // Add postings
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");

        // Check postings
        assert_eq!(
            postings
                .postings_for_label_value("label1", "value1")
                .cardinality(),
            2
        );
        assert_eq!(
            postings
                .postings_for_label_value("label2", "value2")
                .cardinality(),
            1
        );

        // Remove posting
        postings.remove_posting_for_label_value("label1", "value1", 1);
        assert_eq!(
            postings
                .postings_for_label_value("label1", "value1")
                .cardinality(),
            1
        );

        // Remove non-existent posting (should not panic)
        postings.remove_posting_for_label_value("label3", "value3", 3);
    }

    #[test]
    fn test_memory_postings_all_postings() {
        let mut postings = MemoryPostings::default();

        postings.add_id_to_all_postings(1);
        postings.add_id_to_all_postings(2);
        postings.add_id_to_all_postings(3);

        assert_eq!(postings.all_postings().cardinality(), 3);

        postings.remove_id_from_all_postings(2);
        assert_eq!(postings.all_postings().cardinality(), 2);
    }

    #[test]
    fn test_postings_multiple_values_same_label() {
        let mut postings = MemoryPostings::default();

        // Add postings for multiple values of the same label
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label1", "value2");
        postings.add_posting_for_label_value(3, "label1", "value3");
        postings.add_posting_for_label_value(4, "label1", "value1");

        // Query for multiple values of the same label
        let values = vec!["value1".to_string(), "value3".to_string()];
        let result = postings.postings("label1", &values);

        // Check that the result contains the correct series IDs
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(3));
        assert!(result.contains(4));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_postings_with_duplicate_values() {
        let mut postings = MemoryPostings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label", "value1");
        postings.add_posting_for_label_value(2, "label", "value2");
        postings.add_posting_for_label_value(3, "label", "value1");

        // Create an array with duplicate values
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value1".to_string(),
        ];

        // Call the postings method
        let result = postings.postings("label", &values);

        // Check the result
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_postings_all_values_match() {
        let mut postings = MemoryPostings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label", "value1");
        postings.add_posting_for_label_value(2, "label", "value2");
        postings.add_posting_for_label_value(3, "label", "value3");
        postings.add_posting_for_label_value(4, "label", "value1");

        // Create values to search for
        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value3".to_string(),
        ];

        // Get the postings
        let result = postings.postings("label", &values);

        // Check if the result contains all the expected series IDs
        assert_eq!(result.cardinality(), 4);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(result.contains(4));
    }

    #[test]
    fn test_postings_with_large_number_of_values() {
        let mut postings = MemoryPostings::default();
        let label_name = "large_label";
        let num_values = 10_000;

        // Add postings for a large number of values
        for i in 0..num_values {
            postings.add_posting_for_label_value(
                i as SeriesRef,
                label_name,
                &format!("value_{}", i),
            );
        }

        // Create a large array of values to search for
        let values: Vec<String> = (0..num_values).map(|i| format!("value_{}", i)).collect();

        // Measure the time taken to execute the postings function
        let start_time = std::time::Instant::now();
        let result = postings.postings(label_name, &values);
        let duration = start_time.elapsed();

        // Assert that all series IDs are present in the result
        assert_eq!(result.cardinality() as usize, num_values);
        for i in 0..num_values {
            assert!(result.contains(i as SeriesRef));
        }

        // Check that the execution time is reasonable (adjust the threshold as needed)
        assert!(
            duration < std::time::Duration::from_secs(1),
            "Postings retrieval took too long: {:?}",
            duration
        );
    }

    #[test]
    fn test_postings_with_unicode_characters() {
        let mut postings = MemoryPostings::default();

        // Add postings with Unicode characters
        postings.add_posting_for_label_value(1, "标签", "值1");
        postings.add_posting_for_label_value(2, "标签", "値2");
        postings.add_posting_for_label_value(3, "标签", "🌟");

        // Test postings method with Unicode characters
        let values = vec!["值1".to_string(), "値2".to_string(), "🌟".to_string()];
        let result = postings.postings("标签", &values);

        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    // postings_without_labels
    #[test]
    fn test_postings_without_labels_all_series_have_label() {
        let mut postings = MemoryPostings::default();

        // Add postings for three series, all having "common_label"
        postings.add_posting_for_label_value(1, "common_label", "value1");
        postings.add_posting_for_label_value(2, "common_label", "value2");
        postings.add_posting_for_label_value(3, "common_label", "value3");

        // Add some other labels
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value2");
        postings.add_posting_for_label_value(3, "label3", "value3");

        // Add all series to ALL_POSTINGS
        postings.add_id_to_all_postings(1);
        postings.add_id_to_all_postings(2);
        postings.add_id_to_all_postings(3);

        // Get postings without the common label
        let result = postings.postings_without_labels(&["common_label"]);

        // The result should be empty as all series have the common label
        assert!(result.is_empty());
        assert_eq!(result.cardinality(), 0);
    }

    #[test]
    fn test_postings_without_labels_empty_array() {
        let mut postings = MemoryPostings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value2");
        postings.add_posting_for_label_value(3, "label3", "value3");

        // Add all postings to the ALL_POSTINGS_KEY
        postings.add_id_to_all_postings(1);
        postings.add_id_to_all_postings(2);
        postings.add_id_to_all_postings(3);

        // Call postings_without_labels with an empty array
        let result = postings.postings_without_labels(&[]);

        // The result should be equal to all_postings
        assert_eq!(result.as_ref(), postings.all_postings());
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }

    #[test]
    fn test_postings_without_labels_mixed_existing_and_non_existing() {
        let mut postings = MemoryPostings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value2");
        postings.add_posting_for_label_value(3, "label3", "value3");
        postings.add_posting_for_label_value(4, "label4", "value4");

        // Add all IDs to all_postings
        for id in 1..=4 {
            postings.add_id_to_all_postings(id);
        }

        // Test with a mix of existing and non-existing labels
        let result = postings.postings_without_labels(&["label1", "label3", "non_existing_label"]);

        // Expected result: IDs 2 and 4 (not associated with label1 or label3)
        assert_eq!(result.cardinality(), 2);
        assert!(!result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));
        assert!(result.contains(4));
    }

    #[test]
    fn test_postings_without_labels_with_nonexistent_labels() {
        let mut postings = MemoryPostings::default();

        // Add some postings
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value2");
        postings.add_posting_for_label_value(3, "label3", "value3");
        postings.add_posting_for_label_value(4, "label4", "value4");

        // Add all postings to the ALL_POSTINGS_KEY
        for id in 1..=4 {
            postings.add_id_to_all_postings(id);
        }

        // Test with a mix of existing and non-existing labels
        let labels = &[
            "label1",
            "label3",
            "nonexistent_label1",
            "nonexistent_label2",
        ];
        let result = postings.postings_without_labels(labels);

        // Expected result: series without label1 and label3
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(2));
        assert!(result.contains(4));
        assert!(!result.contains(1));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_postings_without_labels_multiple_labels() {
        let mut postings = MemoryPostings::default();

        // Add postings for multiple series with different label combinations
        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");
        postings.add_posting_for_label_value(2, "label3", "value3");
        postings.add_posting_for_label_value(3, "label2", "value2");
        postings.add_posting_for_label_value(3, "label3", "value3");
        postings.add_posting_for_label_value(4, "label4", "value4");

        // Add all series to ALL_POSTINGS
        for id in 1..=4 {
            postings.add_id_to_all_postings(id);
        }

        // Test postings_without_labels for multiple labels
        let result = postings.postings_without_labels(&["label1", "label2"]);

        // Verify the result
        assert_eq!(result.cardinality(), 1);
        assert!(!result.contains(1)); // Has both label1 and label2
        assert!(!result.contains(2)); // Has label1
        assert!(!result.contains(3)); // Has label2
        assert!(result.contains(4)); // Has neither label1 nor label2
    }

    #[test]
    fn test_memory_postings_set_timeseries_key() {
        let mut postings = MemoryPostings::default();

        postings.set_timeseries_key(1, b"key1");
        postings.set_timeseries_key(2, b"key2");

        assert_eq!(
            postings.get_key_by_id(1),
            Some(&b"key1".to_vec().into_boxed_slice())
        );
        assert_eq!(
            postings.get_key_by_id(2),
            Some(&b"key2".to_vec().into_boxed_slice())
        );
        assert_eq!(postings.get_key_by_id(3), None);
    }

    #[test]
    fn test_memory_postings_remove_timeseries() {
        let mut postings = MemoryPostings::default();
        let mut series = TimeSeries::new();
        series.id = 1;
        series.labels = InternedMetricName::new(&[
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ]);

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_id_to_all_postings(1);

        postings.remove_timeseries(&series);

        assert!(postings
            .postings_for_label_value("label1", "value1")
            .is_empty());
        assert!(postings
            .postings_for_label_value("label2", "value2")
            .is_empty());
        assert!(postings.all_postings().is_empty());
    }

    #[test]
    fn test_memory_postings_postings_by_labels() {
        let mut postings = MemoryPostings::default();

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_posting_for_label_value(2, "label1", "value1");
        postings.add_posting_for_label_value(2, "label2", "value3");

        let labels = vec![
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ];
        let result = postings.postings_by_labels(&labels);

        assert_eq!(result.cardinality(), 1);
        assert!(result.contains(1));
    }
}
