use super::label::{Label, SeriesLabel};
use crate::common::arc_interner::ArcIntern;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::serialization::{rdb_load_string, rdb_load_usize, rdb_save_usize};
use enquote::enquote;
use get_size::GetSize;
use std::collections::HashMap;
use std::fmt::Display;
use valkey_module::{raw, ValkeyResult};

const VALUE_SEPARATOR: &str = "=";
const EMPTY_LABEL: &str = "";

pub struct InternedLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl SeriesLabel for InternedLabel<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn value(&self) -> &str {
        self.value
    }
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct InternedMetricName(Vec<ArcIntern<String>>);

impl GetSize for InternedMetricName {
    fn get_size(&self) -> usize {
        // all actual content is shared by interner, so we only need to count the stack size of the vector
        self.0.capacity() * size_of::<ArcIntern<String>>()
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
        metric_name
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// adds new label to mn with the given key and value.
    pub fn add_label(&mut self, key: &str, value: &str) {
        let full_label = format!("{}{}{}", key, VALUE_SEPARATOR, value);
        let interned_value = ArcIntern::new(full_label);
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

    pub fn get_value(&self, key: &str) -> Option<&str> {
        for interned in &self.0 {
            if let Some((k, v)) = &interned.split_once(VALUE_SEPARATOR) {
                if *k == key {
                    return Some(*v);
                }
            }
        }
        None
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

    pub fn iter(&self) -> impl Iterator<Item = InternedLabel> {
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

        let display = format!("{}", metric_name);

        assert_eq!(display, "metric{key1=\"value1\",key2=\"value2\"}");
    }
}
