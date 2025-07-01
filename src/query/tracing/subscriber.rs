use std::collections::BTreeMap;
use valkey_module::ValkeyValue;

#[derive(Debug)]
struct CustomFieldStorage(BTreeMap<String, ValkeyValue>);
