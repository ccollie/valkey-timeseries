use super::index_key::IndexKey;
use super::key_buffer::KeyBuffer;
use crate::common::hash::IntMap;
use crate::labels::matchers::{LabelFilter, PredicateMatch, PredicateValue};
use crate::labels::{InternedLabel, SeriesLabel};
use crate::series::index::init_croaring_allocator;
use crate::series::{SeriesRef, TimeSeries};
use blart::map::Entry as ARTEntry;
use blart::{AsBytes, TreeMap};
use croaring::Bitmap64;
use std::borrow::Cow;
use std::sync::LazyLock;

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

/// `Postings` is the core in-memory inverted index for time series data. It is designed for efficient
/// querying and retrieving of time series based on their labels.
#[derive(Clone)]
pub struct Postings {
    /// Map from label name and (label name, label value) to a set of timeseries ids.
    pub(super) label_index: PostingsIndex,
    /// Map from timeseries id to the key of the timeseries.
    pub(super) id_to_key: IntMap<SeriesRef, KeyType>, // todo: use an interned string
    /// Set of timeseries ids of series that should be removed from the index. This really only
    /// happens when the index is inconsistent (value does not exist in the db but exists in the index)
    /// Keep track and cleanup from the index during a gc pass.
    pub(super) stale_ids: PostingsBitmap,
}

impl Default for Postings {
    fn default() -> Self {
        init_croaring_allocator();
        Postings {
            label_index: PostingsIndex::new(),
            id_to_key: IntMap::default(),
            stale_ids: PostingsBitmap::default(),
        }
    }
}

impl Postings {
    #[allow(dead_code)]
    pub(super) fn clear(&mut self) {
        self.label_index.clear();
        self.id_to_key.clear();
        self.stale_ids.clear();
    }

    /// `swap` the inner value with some other value
    /// this is specifically to handle the `swapdb` event callback
    pub fn swap(&mut self, other: &mut Self) {
        std::mem::swap(&mut self.label_index, &mut other.label_index);
        std::mem::swap(&mut self.id_to_key, &mut other.id_to_key);
        std::mem::swap(&mut self.stale_ids, &mut other.stale_ids);
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
    }

    pub fn index_timeseries(&mut self, ts: &TimeSeries, key: &[u8]) {
        debug_assert!(ts.id != 0);
        let id = ts.id;
        let measurement = ts.labels.get_measurement();
        if !measurement.is_empty() {
            // todo: error !
        }

        for InternedLabel { name, value } in ts.labels.iter() {
            self.add_posting_for_label_value(id, name, value);
        }

        self.add_id_to_all_postings(id);
        self.set_timeseries_key(id, key);
    }

    pub fn remove_timeseries(&mut self, series: &TimeSeries) {
        let id = series.id;
        if self.id_to_key.remove(&id).is_none() {
            log::warn!("Tried to remove non-existing series id {id} from index");
        };
        let labels = series.labels.iter().collect::<Vec<_>>();
        self.remove_posting_by_id_and_labels(id, &labels);
    }

    pub fn count(&self) -> usize {
        self.id_to_key.len()
    }

    pub fn all_postings(&self) -> &PostingsBitmap {
        self.label_index
            .get(&*ALL_POSTINGS_KEY)
            .unwrap_or(&*EMPTY_BITMAP)
    }

    pub(super) fn has_id(&self, id: SeriesRef) -> bool {
        self.id_to_key.contains_key(&id)
    }

    pub fn postings_for_label_value<'a>(
        &'a self,
        name: &str,
        value: &str,
    ) -> Cow<'a, PostingsBitmap> {
        let key = KeyBuffer::for_label_value(name, value);
        if let Some(bmp) = self.label_index.get(key.as_bytes()) {
            if self.stale_ids.is_empty() {
                Cow::Borrowed(bmp)
            } else {
                let result = bmp.andnot(&self.stale_ids);
                Cow::Owned(result)
            }
        } else {
            Cow::Borrowed(&*EMPTY_BITMAP)
        }
    }

    #[inline]
    fn remove_stale_if_needed(&self, postings: &mut PostingsBitmap) {
        if !self.stale_ids.is_empty() {
            postings.andnot_inplace(&self.stale_ids);
        }
    }

    pub fn postings_for_all_label_values(&self, label_name: &str) -> PostingsBitmap {
        let prefix = KeyBuffer::for_prefix(label_name);
        let mut result = PostingsBitmap::new();
        for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
            result |= map;
        }
        self.remove_stale_if_needed(&mut result);
        result
    }

    /// `postings` returns the postings list iterator for the label pairs.
    /// The postings here contain the ids to the series inside the index.
    pub fn postings(&self, name: &str, values: &[String]) -> PostingsBitmap {
        let mut result = PostingsBitmap::new();

        for value in values {
            let key = KeyBuffer::for_label_value(name, value);
            if let Some(bmp) = self.label_index.get(key.as_bytes()) {
                result |= bmp;
            }
        }

        self.remove_stale_if_needed(&mut result);
        result
    }

    /// `postings_for_label_matching` returns postings having a label with the given name and a value
    /// for which `match_fn` returns true. If no postings are found having at least one matching label,
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
        let prefix = KeyBuffer::for_prefix(name);
        let start_pos = prefix.len();
        let mut result = PostingsBitmap::new();

        for (key, map) in self.label_index.prefix(prefix.as_bytes()) {
            let value = key.sub_string(start_pos);
            if match_fn(value, state) {
                result |= map;
            }
        }

        self.remove_stale_if_needed(&mut result);
        result
    }

    /// Return all series ids corresponding to the given labels
    pub fn postings_by_labels<T: SeriesLabel>(&self, labels: &[T]) -> PostingsBitmap {
        let mut first = true;
        let mut acc = PostingsBitmap::new();

        for label in labels.iter() {
            let key = KeyBuffer::for_label_value(label.name(), label.value());
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
        if !self.stale_ids.is_empty() {
            acc.andnot_inplace(&self.stale_ids);
        }

        acc
    }

    pub fn postings_without_label(&'_ self, label: &str) -> Cow<'_, PostingsBitmap> {
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
                    let prefix = KeyBuffer::for_prefix(label);
                    for (_, map) in self.label_index.prefix(prefix.as_bytes()) {
                        to_remove.or_inplace(map);
                    }
                }
                if !self.stale_ids.is_empty() {
                    to_remove.or_inplace(&self.stale_ids);
                }
                if to_remove.is_empty() {
                    Cow::Borrowed(all)
                } else {
                    Cow::Owned(all.andnot(&to_remove))
                }
            }
        }
    }

    pub fn postings_for_matcher(&'_ self, matcher: &LabelFilter) -> Cow<'_, PostingsBitmap> {
        match matcher.matcher {
            PredicateMatch::Equal(ref value) => handle_equal_match(self, &matcher.label, value),
            PredicateMatch::NotEqual(ref value) => {
                handle_not_equal_match(self, &matcher.label, value)
            }
            PredicateMatch::RegexEqual(_) => handle_regex_equal_match(self, matcher),
            PredicateMatch::RegexNotEqual(_) => handle_regex_not_equal_match(self, matcher),
        }
    }

    pub(crate) fn get_key_by_id(&self, id: SeriesRef) -> Option<&KeyType> {
        self.id_to_key.get(&id)
    }

    /// Marks an id as stale by adding its ID to the stale IDs set.
    /// Context: used in the case of possible index sync issues. When the index is queried and an id is returned
    /// with no corresponding series, we have no access to the series data to do a proper
    /// cleanup. We remove the key from the index and mark the ID as stale, which will be cleaned up later.
    /// The stale IDs are stored in a bitmap for efficient removal and are checked to ensure that no stale IDs are
    /// returned in queries until they are removed.
    pub(crate) fn mark_id_as_stale(&mut self, id: SeriesRef) {
        let _ = self.id_to_key.remove(&id);
        self.stale_ids.add(id);
        self.remove_id_from_all_postings(id);
    }

    #[cfg(test)]
    pub(super) fn has_stale_ids(&self) -> bool {
        !self.stale_ids.is_empty()
    }

    /// Removes stale series IDs from a subset of the index structures.
    ///
    /// This method processes at most `count` keys starting from `start_prefix`,
    /// removing stale IDs from their bitmaps and cleaning up empty entries.
    ///
    /// ## Arguments
    /// * `start_prefix` - The key to start processing from (inclusive)
    /// * `count` - Maximum number of keys to process in this batch
    ///
    /// ## Returns
    /// * `Option<IndexKey>` - The next key to continue processing from, or None if processing is complete
    ///
    pub(crate) fn remove_stale_ids(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        // Skip if there are no stale IDs to process
        if self.stale_ids.is_empty() {
            return None;
        }

        let mut keys_processed = 0;
        let mut keys_to_remove = Vec::new();
        let mut next_key = None;

        // Determine the prefix to use for iteration
        let prefix_bytes = start_prefix.map_or_else(Vec::new, |k| k.as_bytes().to_vec());

        for (key, bitmap) in self.label_index.prefix_mut(&prefix_bytes) {
            // Remove stale IDs from the bitmap
            let should_remove = if !bitmap.is_empty() {
                bitmap.andnot_inplace(&self.stale_ids);
                bitmap.is_empty()
            } else {
                true
            };

            if should_remove && key != &*ALL_POSTINGS_KEY {
                keys_to_remove.push(key.clone());
            }

            if keys_processed == count {
                // Save the key we stopped at as the next starting point
                next_key = Some(key.clone());
                break;
            }

            keys_processed += 1;
        }

        // Process empty keys
        for key in keys_to_remove {
            self.label_index.remove(&key);
        }

        // Clean up id_to_key map for all stale IDs
        // This is done in every batch since we need to ensure consistency
        if keys_processed > 0 {
            self.stale_ids.iter().for_each(|id| {
                let _ = self.id_to_key.remove(&id);
            });

            // Clear stale_ids if we've processed all keys
            if next_key.is_none() {
                self.stale_ids.clear();
            }
        }

        next_key
    }

    /// Incrementally optimizes posting bitmaps for better memory usage and performance.
    ///
    /// This method processes at most `count` keys starting from `start_prefix`,
    /// performing the following optimizations on each bitmap:
    /// 1. Remove the bitmap if it is empty
    /// 2. Call run_optimize() to optimize the bitmap's internal structure
    /// 3. Call shrink_to_fit() to reduce memory overhead
    ///
    /// ### Arguments
    /// * `start_prefix` - The key to start processing from (inclusive)
    /// * `count` - Maximum number of keys to process in this batch
    ///
    /// ### Returns
    /// * `Option<IndexKey>` - The next key to continue processing from, or None if processing is complete
    ///
    pub(crate) fn optimize_postings(
        &mut self,
        start_prefix: Option<IndexKey>,
        count: usize,
    ) -> Option<IndexKey> {
        let mut next_key = None;

        if start_prefix.is_none() {
            let key = &*ALL_POSTINGS_KEY;
            if let Some(all_postings) = self.label_index.get_mut(key) {
                optimize_bitmap(all_postings);
            }
        }

        let mut keys_to_delete = Vec::new();
        let mut keys_processed: usize = 0;
        // Determine the prefix to use for iteration
        let prefix_bytes = start_prefix.map_or_else(Vec::new, |k| k.as_bytes().to_vec());

        // Collect keys to process
        for (key, bitmap) in self.label_index.prefix_mut(&prefix_bytes) {
            if bitmap.is_empty() && key != &*ALL_POSTINGS_KEY {
                keys_to_delete.push(key.clone());
                continue;
            }
            if keys_processed == count {
                // Save the key we stopped at as the next starting point
                next_key = Some(key.clone());
                break;
            }

            optimize_bitmap(bitmap);

            keys_processed += 1;
        }

        // Remove empty bitmaps collected earlier
        for key in keys_to_delete {
            self.label_index.remove(&key);
        }

        next_key
    }
}

/// Optimizes a bitmap in place for better memory usage and performance.
/// This applies run_optimize() and shrink_to_fit() operations to the bitmap
/// if it exists in the index.
fn optimize_bitmap(bitmap: &mut PostingsBitmap) {
    // Optimize the bitmap's internal structure
    bitmap.run_optimize();

    // Shrink to fit to reduce memory overhead
    bitmap.shrink_to_fit();
}

pub(super) fn handle_equal_match<'a>(
    ix: &'a Postings,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    match value {
        PredicateValue::String(s) => {
            if s.is_empty() {
                return ix.postings_without_label(label);
            }
            ix.postings_for_label_value(label, s)
        }
        PredicateValue::List(val) => match val.len() {
            0 => ix.postings_without_label(label),
            1 => ix.postings_for_label_value(label, &val[0]),
            _ => Cow::Owned(ix.postings(label, val)),
        },
        PredicateValue::Empty => ix.postings_without_label(label),
    }
}

// return postings for series which has the label `label
fn with_label<'a>(ix: &'a Postings, label: &str) -> Cow<'a, PostingsBitmap> {
    let mut state = ();
    let postings = ix.postings_for_label_matching(label, &mut state, |_value, _| true);
    Cow::Owned(postings)
}

pub(super) fn handle_not_equal_match<'a>(
    ix: &'a Postings,
    label: &str,
    value: &PredicateValue,
) -> Cow<'a, PostingsBitmap> {
    // the time series has a label named label
    match value {
        PredicateValue::String(s) => {
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
        PredicateValue::List(values) => {
            match values.len() {
                0 => with_label(ix, label),
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
    postings: &'a Postings,
    matcher: &LabelFilter,
) -> Cow<'a, PostingsBitmap> {
    if matcher.matches_empty() {
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
    postings: &'a Postings,
    matcher: &LabelFilter,
) -> Cow<'a, PostingsBitmap> {
    let matches_empty = matcher.matches_empty();
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
    use crate::labels::{Label, MetricName};
    use crate::series::time_series::TimeSeries;

    #[test]
    fn test_memory_postings_add_and_remove() {
        let mut postings = Postings::default();

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
    fn test_postings_all_postings() {
        let mut postings = Postings::default();

        postings.add_id_to_all_postings(1);
        postings.add_id_to_all_postings(2);
        postings.add_id_to_all_postings(3);

        assert_eq!(postings.all_postings().cardinality(), 3);

        postings.remove_id_from_all_postings(2);
        assert_eq!(postings.all_postings().cardinality(), 2);
    }

    #[test]
    fn test_postings_multiple_values_same_label() {
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();
        let label_name = "large_label";
        let num_values = 10_000;

        // Add postings for a large number of values
        for i in 0..num_values {
            postings.add_posting_for_label_value(i as SeriesRef, label_name, &format!("value_{i}"));
        }

        // Create a large array of values to search for
        let values: Vec<String> = (0..num_values).map(|i| format!("value_{i}")).collect();

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
            "Postings retrieval took too long: {duration:?}"
        );
    }

    #[test]
    fn test_postings_with_unicode_characters() {
        let mut postings = Postings::default();

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

    #[test]
    fn test_postings_for_label_value_exceeds_stack_size() {
        let mut postings = Postings::default();

        // The STACK_SIZE in KeyBuffer is 64 bytes
        // We need label_name + "=" + value + "\0" to exceed 64 bytes
        // Let's create a label name and value that together exceed this

        // Create a label name of 30 characters
        let label_name = "very_long_label_name_here_1234";

        // Create a value of 40 characters, so total length is:
        // 30 (label) + 1 (=) + 40 (value) + 1 (\0) = 72 bytes > 64
        let value = "this_is_a_very_long_value_string_12345";

        // Verify our assumption about the length
        let total_len = label_name.len() + 1 + value.len() + 1; // +1 for '=', +1 for '\0'
        assert!(
            total_len > 64,
            "Test setup error: combined length should exceed STACK_SIZE"
        );

        // Add a posting with this long label-value pair
        postings.add_posting_for_label_value(1, label_name, value);
        postings.add_posting_for_label_value(2, label_name, value);
        postings.add_posting_for_label_value(3, label_name, "short");

        // Test that we can retrieve postings for the long label-value pair
        let result = postings.postings_for_label_value(label_name, value);

        // Verify the result contains the correct series IDs
        assert_eq!(result.cardinality(), 2);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(!result.contains(3));

        // Test that we can also retrieve the short value
        let result_short = postings.postings_for_label_value(label_name, "short");
        assert_eq!(result_short.cardinality(), 1);
        assert!(result_short.contains(3));
    }

    #[test]
    fn test_postings_for_all_label_values_exceeds_stack_size() {
        let mut postings = Postings::default();

        // The STACK_SIZE in KeyBuffer is 64 bytes
        // KeyBuffer::for_prefix creates: label_name + "="
        // We need label_name + "=" to exceed 64 bytes

        // Create a label name of 70 characters to exceed the stack size
        let label_name = "very_long_label_name_here_that_definitely_exceeds_the_stack_buffer_size";

        // Verify our assumption about the length
        let prefix_len = label_name.len() + 1; // +1 for '='
        assert!(
            prefix_len > 64,
            "Test setup error: prefix length should exceed STACK_SIZE"
        );

        // Add multiple postings with different values for the same long label name
        postings.add_posting_for_label_value(1, label_name, "value1");
        postings.add_posting_for_label_value(2, label_name, "value2");
        postings.add_posting_for_label_value(3, label_name, "value3");
        postings.add_posting_for_label_value(4, "short_label", "value1");

        // Test that we can retrieve all postings for the long label name
        let result = postings.postings_for_all_label_values(label_name);

        // Verify the result contains all series IDs with the long label name
        assert_eq!(result.cardinality(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
        assert!(!result.contains(4)); // This has a different label

        // Also test that we can retrieve postings for the short label
        let result_short = postings.postings_for_all_label_values("short_label");
        assert_eq!(result_short.cardinality(), 1);
        assert!(result_short.contains(4));
    }

    // postings_without_labels
    #[test]
    fn test_postings_without_labels_all_series_have_label() {
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();

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
        let mut postings = Postings::default();
        let mut series = TimeSeries::new();
        series.id = 1;
        series.labels = MetricName::new(&[
            Label::new("label1", "value1"),
            Label::new("label2", "value2"),
        ]);

        postings.add_posting_for_label_value(1, "label1", "value1");
        postings.add_posting_for_label_value(1, "label2", "value2");
        postings.add_id_to_all_postings(1);

        postings.remove_timeseries(&series);

        assert!(
            postings
                .postings_for_label_value("label1", "value1")
                .is_empty()
        );
        assert!(
            postings
                .postings_for_label_value("label2", "value2")
                .is_empty()
        );
        assert!(postings.all_postings().is_empty());
    }

    #[test]
    fn test_memory_postings_postings_by_labels() {
        let mut postings = Postings::default();

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
