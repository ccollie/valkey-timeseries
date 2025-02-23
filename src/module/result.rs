use crate::common::Sample;
use crate::labels::{InternedLabel, Label};
use crate::series::TimeSeries;
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{ValkeyString, ValkeyValue};

pub static META_KEY_LABEL: &str = "__meta:key__";

pub(crate) fn metric_name_to_valkey_value(labels: &[Label], key: Option<&str>) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(labels.len() + 1);

    if let Some(key) = key {
        map.insert(ValkeyValueKey::from(META_KEY_LABEL), ValkeyValue::from(key));
    }

    for Label { name, value } in labels.iter() {
        map.insert(ValkeyValueKey::from(name), value.into());
    }

    ValkeyValue::Map(map)
}

pub(super) fn get_ts_metric_selector(ts: &TimeSeries, key: Option<&ValkeyString>) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::with_capacity(ts.labels.len() + 1);
    if let Some(key) = key {
        map.insert(
            ValkeyValueKey::String(META_KEY_LABEL.into()),
            ValkeyValue::from(key),
        );
    }
    for InternedLabel { name, value } in ts.labels.iter() {
        map.insert(
            ValkeyValueKey::String(name.into()),
            ValkeyValue::from(value),
        );
    }
    ValkeyValue::Map(map)
}

pub(crate) fn sample_to_value(sample: Sample) -> ValkeyValue {
    let row = vec![
        ValkeyValue::from(sample.timestamp),
        ValkeyValue::from(sample.value),
    ];
    ValkeyValue::from(row)
}
