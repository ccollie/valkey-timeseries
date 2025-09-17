use super::label::{Label, SeriesLabel};
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::rdb::{rdb_load_string, rdb_load_usize, rdb_save_usize};
use crate::common::string_interner::InternedString;
use enquote::enquote;
use get_size::GetSize;
use std::collections::HashMap;
use std::fmt::Display;
use valkey_module::{ValkeyResult, ValkeyValue, raw};

const VALUE_SEPARATOR: &str = "=";
const EMPTY_LABEL: &str = "";

pub struct InternedLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl From<InternedLabel<'_>> for ValkeyValue {
    fn from(label: InternedLabel) -> Self {
        let row = vec![
            ValkeyValue::from(label.name),
            ValkeyValue::from(label.value),
        ];
        ValkeyValue::from(row)
    }
}

impl SeriesLabel for InternedLabel<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn value(&self) -> &str {
        self.value
    }
}

/// A time series is optionally identified by a series of label-value pairs used to retrieve the
/// series in queries. Given that these labels are used to group semantically similar time series,
/// they are necessarily duplicated. We take advantage of this fact to intern label-value pairs,
/// meaning that only a single allocation is made per unique pair, irrespective of the number of
/// series it occurs in.
///
/// We choose to store the label/value pair as a single interned string in the format "key=value". This reduces
/// the memory overhead associated with storing separate strings for keys and values (8 bytes vs 16 bytes on 64-bit systems).
///
/// The labels are stored in a sorted order to allow for efficient comparison and retrieval.
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct InternedMetricName(Vec<InternedString>);

impl GetSize for InternedMetricName {
    fn get_size(&self) -> usize {
        // all actual content is shared by interner, so we only need to count the stack size of the vector
        self.0.capacity() * size_of::<InternedString>()
    }
}

impl InternedMetricName {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn new(labels: &[Label]) -> Self {
        let mut metric_name = Self::with_capacity(labels.len());
        for label in labels {
            metric_name.add_label(label.name(), label.value());
        }
        metric_name.shrink_to_fit();
        metric_name
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// adds a new label to mn with the given key and value.
    pub fn add_label(&mut self, key: &str, value: &str) {
        let full_label = format!("{key}{VALUE_SEPARATOR}{value}");
        let interned_value = InternedString::intern(full_label);
        match self.0.binary_search_by_key(&key, |tag| {
            if let Some((k, _)) = tag.split_once(VALUE_SEPARATOR) {
                k
            } else {
                EMPTY_LABEL
            }
        }) {
            Ok(idx) => {
                self.0[idx] = interned_value;
            }
            Err(idx) => {
                self.0.insert(idx, interned_value);
            }
        }
    }

    pub fn get_tag(&'_ self, key: &str) -> Option<InternedLabel<'_>> {
        if let Some(label) = self.0.iter().find(|x| {
            if let Some((k, _)) = x.split_once(VALUE_SEPARATOR) {
                k == key
            } else {
                false
            }
        }) {
            if let Some((name, value)) = label.split_once(VALUE_SEPARATOR) {
                return Some(InternedLabel { name, value });
            }
        }
        None
    }

    pub fn get_value(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .filter_map(|interned| interned.split_once(VALUE_SEPARATOR))
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v)
    }

    pub fn remove_label(&mut self, key: &str) {
        if let Some(idx) = self.0.iter().position(|x| {
            if let Some((k, _)) = x.split_once(VALUE_SEPARATOR) {
                k == key
            } else {
                false
            }
        }) {
            self.0.remove(idx);
        }
    }

    pub fn set_label(&mut self, key: &str, value: &str) {
        self.add_label(key, value);
    }

    pub fn get_measurement(&self) -> &str {
        if let Some(measurement) = self.get_value(METRIC_NAME_LABEL) {
            measurement
        } else {
            EMPTY_LABEL
        }
    }

    pub fn iter(&'_ self) -> impl Iterator<Item = InternedLabel<'_>> {
        self.0.iter().filter_map(|x| {
            if let Some((name, value)) = x.split_once(VALUE_SEPARATOR) {
                Some(InternedLabel { name, value })
            } else {
                None
            }
        })
    }

    pub fn to_label_vec(&self) -> Vec<Label> {
        self.iter()
            .map(|label| Label::new(label.name, label.value))
            .collect()
    }

    pub fn sort(&mut self) {
        self.0.sort();
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_rdb(&self, rdb: *mut raw::RedisModuleIO) {
        rdb_save_usize(rdb, self.0.len());
        for label in self.iter() {
            raw::save_string(rdb, label.name);
            raw::save_string(rdb, label.value);
        }
    }

    pub fn from_rdb(rdb: *mut raw::RedisModuleIO) -> ValkeyResult<Self> {
        let count = rdb_load_usize(rdb)?;
        let mut result = Self::with_capacity(count);
        for _ in 0..count {
            let name = rdb_load_string(rdb)?;
            let value = rdb_load_string(rdb)?;
            result.add_label(&name, &value);
        }
        Ok(result)
    }

    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

impl From<HashMap<String, String>> for InternedMetricName {
    fn from(map: HashMap<String, String>) -> Self {
        let mut metric_name = InternedMetricName::with_capacity(map.len());
        for (key, value) in map {
            metric_name.add_label(&key, &value);
        }
        metric_name
    }
}

impl From<&[Label]> for InternedMetricName {
    fn from(labels: &[Label]) -> Self {
        InternedMetricName::new(labels)
    }
}

impl From<Vec<Label>> for InternedMetricName {
    fn from(labels: Vec<Label>) -> Self {
        InternedMetricName::new(&labels)
    }
}

impl Display for InternedMetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{{", self.get_measurement())?;
        let len = self.0.len();
        for (i, label) in self.iter().enumerate() {
            if !label.name.is_empty() && label.name != METRIC_NAME_LABEL {
                write!(f, "{}={}", label.name, enquote('"', label.value))?;
                if i < len - 1 {
                    write!(f, ",")?;
                }
            }
        }
        write!(f, "}}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_label() {
        let mut metric_name = InternedMetricName::with_capacity(2);
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.get_value("key1"), Some("value1"));
        assert_eq!(metric_name.get_value("key2"), Some("value2"));
    }

    #[test]
    fn test_get_measurement() {
        let mut metric_name = InternedMetricName::default();
        metric_name.add_label("key1", "value1");

        assert_eq!(metric_name.get_measurement(), "");

        metric_name.add_label(METRIC_NAME_LABEL, "metric_name");
        assert_eq!(metric_name.get_measurement(), "metric_name");
    }

    #[test]
    fn test_add_labels() {
        let mut metric_name = InternedMetricName::with_capacity(2);

        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.get_value("key1"), Some("value1"));
        assert_eq!(metric_name.get_value("key2"), Some("value2"));
    }

    #[test]
    fn test_sort() {
        let mut metric_name = InternedMetricName::default();
        metric_name.add_label("key2", "value2");
        metric_name.add_label("key1", "value1");
        metric_name.sort();

        let sorted_labels: Vec<_> = metric_name.iter().collect();
        assert_eq!(sorted_labels[0].name, "key1");
        assert_eq!(sorted_labels[1].name, "key2");
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut metric_name = InternedMetricName::default();
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        assert_eq!(metric_name.len(), 2);
        assert!(!metric_name.is_empty());
    }

    #[test]
    fn test_display() {
        let mut metric_name = InternedMetricName::with_capacity(3);
        metric_name.add_label(METRIC_NAME_LABEL, "metric");
        metric_name.add_label("key1", "value1");
        metric_name.add_label("key2", "value2");

        let display = format!("{metric_name}");

        assert_eq!(display, "metric{key1=\"value1\",key2=\"value2\"}");
    }
}
