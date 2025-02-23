use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::InternedLabel;
use enquote::enquote;
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

pub trait SeriesLabel: Sized {
    fn name(&self) -> &str;
    fn value(&self) -> &str;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new<S: Into<String>>(key: S, value: S) -> Self {
        Self {
            name: key.into(),
            value: value.into(),
        }
    }
}

impl PartialOrd for Label {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Label {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.name.cmp(&other.name);
        if cmp != Ordering::Equal {
            cmp
        } else {
            self.value.cmp(&other.value)
        }
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{name}={value}", name = self.name, value = self.value)
    }
}

impl From<InternedLabel<'_>> for Label {
    fn from(label: InternedLabel) -> Self {
        Self {
            name: label.name.to_string(),
            value: label.value.to_string(),
        }
    }
}

const SEP: u8 = 0xfe;

impl Hash for Label {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_label(state, &self.name, &self.value);
    }
}

impl SeriesLabel for Label {
    fn name(&self) -> &str {
        &self.name
    }

    fn value(&self) -> &str {
        &self.value
    }
}

pub struct BorrowedLabel<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl SeriesLabel for BorrowedLabel<'_> {
    fn name(&self) -> &str {
        self.name
    }

    fn value(&self) -> &str {
        self.value
    }
}

impl Display for BorrowedLabel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{name}={value}", name = self.name, value = self.value)
    }
}

fn hash_label<H: Hasher>(state: &mut H, name: &str, value: &str) {
    state.write(name.as_bytes());
    state.write_u8(SEP);
    state.write(value.as_bytes());
}

// Note - assumes that labels is sorted
pub fn format_labels<T: SeriesLabel>(labels: &[T]) -> String {
    let name = if let Some(label) = labels.iter().find(|l| l.name() == METRIC_NAME_LABEL) {
        label.value()
    } else {
        ""
    };

    let mut full_name = String::with_capacity(name.len() + labels.len() * 16);
    full_name.push_str(name);
    if !labels.is_empty() {
        full_name.push('{');
        for (i, label) in labels.iter().enumerate() {
            let name = label.name();
            if label.name() == METRIC_NAME_LABEL {
                continue;
            }
            let value = label.value();
            full_name.push_str(name);
            full_name.push_str("=\"");
            // avoid allocation if possible
            if value.contains('"') {
                let quoted_value = enquote('\"', value);
                full_name.push_str(&quoted_value);
            } else {
                full_name.push_str(value);
            }
            full_name.push('"');
            if i < labels.len() - 1 {
                full_name.push(',');
            }
        }
        full_name.push('}');
    }
    full_name
}
