use crate::error_consts;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::labels::{HasFingerprint, InternedLabel, SeriesFingerprint};
use std::cmp::Ordering;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

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

    /// Creates a metric name label (`__name__`).
    ///
    /// This is a convenience method for creating the special label that
    /// identifies the metric name.
    pub fn metric_name(name: impl Into<String>) -> Self {
        Self {
            name: METRIC_NAME_LABEL.to_string(),
            value: name.into(),
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

impl From<Label> for ValkeyValue {
    fn from(label: Label) -> Self {
        let label_value = if label.value.is_empty() {
            ValkeyValue::Null
        } else {
            ValkeyValue::from(label.value)
        };
        let res = vec![ValkeyValue::from(label.name), label_value];
        ValkeyValue::from(res)
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

fn hash_label<H: Hasher>(state: &mut H, name: &str, value: &str) {
    state.write(name.as_bytes());
    state.write_u8(SEP);
    state.write(value.as_bytes());
}

/// Rejects a label name containing `=` (DIV-0063). No filter can select such a label (`a=b=2`
/// is a parse error here and matches nothing on RTS), and the label index keys `name=value`
/// split at the first `=`, so it would file `a=b 2` under the name `a`. Values may contain `=`.
///
/// Every way a label enters the keyspace checks this: the `LABELS` parser and RDB load
/// (`DUMP`/`RESTORE`, `TS._RESTORE`, replica sync), so no stored series has such a name.
pub fn check_label_name(name: &[u8]) -> ValkeyResult<()> {
    if name.contains(&b'=') {
        return Err(ValkeyError::Str(
            error_consts::LABEL_NAME_CONTAINS_SEPARATOR,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_names_must_not_contain_the_separator() {
        for name in ["a=b", "=", "=a", "a=", "a==b"] {
            assert!(
                matches!(
                    check_label_name(name.as_bytes()),
                    Err(ValkeyError::Str(
                        error_consts::LABEL_NAME_CONTAINS_SEPARATOR
                    ))
                ),
                "{name:?} was accepted"
            );
        }
        for name in ["a", "host", "__name__", "a:b.c-d", "région"] {
            assert!(
                check_label_name(name.as_bytes()).is_ok(),
                "{name:?} was rejected"
            );
        }
    }
}

impl HasFingerprint for [Label] {
    fn fingerprint(&self) -> SeriesFingerprint {
        let mut hasher = super::hash::create_hasher();
        for label in self {
            super::hash::hash_key_value(&mut hasher, &label.name, &label.value);
        }
        hasher.finish_128()
    }
}
