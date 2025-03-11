use crate::common::Sample;
use valkey_module::ValkeyValue;

pub static META_KEY_LABEL: &str = "__meta:key__";

pub(crate) fn sample_to_value(sample: Sample) -> ValkeyValue {
    let row = vec![
        ValkeyValue::from(sample.timestamp),
        ValkeyValue::from(sample.value),
    ];
    ValkeyValue::from(row)
}
