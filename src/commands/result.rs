use crate::common::Sample;
use crate::labels::InternedLabel;
use crate::query::{InstantQueryResult, RangeQueryResult};
use crate::series::TimeSeries;
use metricsql_runtime::types::{MetricName, METRIC_NAME_LABEL};
use std::collections::HashMap;
use valkey_module::redisvalue::ValkeyValueKey;
use valkey_module::{ValkeyString, ValkeyValue};

pub static META_KEY_LABEL: &str = "__meta:key__";
pub(crate) fn metric_name_to_valkey_value(
    metric_name: &MetricName,
    key: Option<&str>,
) -> ValkeyValue {
    let mut map: HashMap<ValkeyValueKey, ValkeyValue> =
        HashMap::with_capacity(metric_name.labels.len() + 1);

    if !metric_name.measurement.is_empty() {
        map.insert(
            ValkeyValueKey::from(METRIC_NAME_LABEL),
            metric_name.measurement.clone().into(),
        );
    }

    if let Some(key) = key {
        map.insert(ValkeyValueKey::from(META_KEY_LABEL), ValkeyValue::from(key));
    }

    for label in metric_name.labels.iter() {
        let value: ValkeyValue = ValkeyValue::from(label.value.clone());
        map.insert(ValkeyValueKey::from(label.name.clone()), value);
    }

    ValkeyValue::Map(map)
}

/// `To Prometheus Range Vector output
/// https://prometheus.io/docs/prometheus/latest/querying/api/#range-vectors
/// ``` json
/// {
//     "resultType" : "matrix",
//     "data" : [
//         {
//             "metric" : {
//                 "__name__" : "up",
//                 "job" : "prometheus",
//                 "instance" : "localhost:9090"
//             },
//             "values" : [
//                 [ 1435781430.781, "1" ],
//                 [ 1435781445.781, "1" ],
//                 [ 1435781460.781, "1" ]
//             ]
//         },
//         {
//             "metric" : {
//                 "__name__" : "up",
//                 "job" : "node",
//                 "instance" : "localhost:9091"
//             },
//             "values" : [
//                 [ 1435781430.781, "0" ],
//                 [ 1435781445.781, "0" ],
//                 [ 1435781460.781, "1" ]
//             ]
//         }
//     ]
/// }
/// ```
pub(super) fn to_matrix_result(vals: Vec<RangeQueryResult>) -> ValkeyValue {
    let data: Vec<ValkeyValue> = vals
        .into_iter()
        .map(|val| {
            let metric_name = metric_name_to_valkey_value(&val.metric, None);
            let samples = val
                .samples
                .into_iter()
                .map(sample_to_value)
                .collect::<Vec<_>>();

            let map: HashMap<ValkeyValueKey, ValkeyValue> = vec![
                (ValkeyValueKey::from("metric"), metric_name),
                (ValkeyValueKey::from("values"), samples.into()),
            ]
            .into_iter()
            .collect();

            ValkeyValue::Map(map)
        })
        .collect();

    let map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::from([
        (ValkeyValueKey::from("resultType"), "vector".into()),
        (ValkeyValueKey::from("data"), data.into()),
    ]);

    ValkeyValue::Map(map)
}

/// Convert to Prometheus Instant Vector output format
/// https://prometheus.io/docs/prometheus/latest/querying/api/#instant-vectors
/// ``` json
/// {
///    "resultType" : "vector",
//     "data" : [
//         {
//             "metric" : {
//                 "__name__" : "up",
//                 "job" : "prometheus",
//                 "instance" : "localhost:9090"
//             },
//             "value": [ 1435781451.781, "1" ]
//         },
//         {
//             "metric" : {
//                 "__name__" : "up",
//                 "job" : "node",
//                 "instance" : "localhost:9100"
//             },
//             "value" : [ 1435781451.781, "0" ]
//         }
//     ]
/// }
/// ```
pub(super) fn to_instant_vector_result(results: Vec<InstantQueryResult>) -> ValkeyValue {
    let data: Vec<_> = results
        .iter()
        .map(|x| {
            let metric_name = metric_name_to_valkey_value(&x.metric, None);
            let map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::from([
                (ValkeyValueKey::from("metric"), metric_name),
                (ValkeyValueKey::from("value"), sample_to_value(x.sample)),
            ]);
            map
        })
        .collect();

    let map: HashMap<ValkeyValueKey, ValkeyValue> = HashMap::from([
        (ValkeyValueKey::from("resultType"), "matrix".into()),
        (ValkeyValueKey::from("data"), data.into()),
    ]);

    ValkeyValue::Map(map)
}

pub fn format_array_result(arr: Vec<ValkeyValue>) -> ValkeyValue {
    let map: HashMap<ValkeyValueKey, ValkeyValue> = [
        status_element(true),
        (ValkeyValueKey::from("data"), ValkeyValue::Array(arr)),
    ]
    .into_iter()
    .collect();

    ValkeyValue::Map(map)
}

fn status_element(success: bool) -> (ValkeyValueKey, ValkeyValue) {
    let status = if success { "success" } else { "error" };
    (
        ValkeyValueKey::from("status"),
        ValkeyValue::SimpleStringStatic(status),
    )
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
