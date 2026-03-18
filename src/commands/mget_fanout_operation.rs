use super::fanout::generated::{Label, MGetValue, MultiGetRequest, MultiGetResponse, Sample};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_mget_request;
use crate::error_consts;
use crate::fanout::{NodeInfo, SimpleFanoutOperation};
use crate::series::request_types::MGetRequest;
use valkey_module::{
    BlockedClient, Context, Status, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyValue,
};

#[derive(Debug, Default)]
pub struct MGetFanoutOperation {
    options: MGetRequest,
    series: Vec<MGetValue>,
}

impl MGetFanoutOperation {
    pub fn new(options: MGetRequest) -> Self {
        Self {
            options,
            series: Vec::new(),
        }
    }
}

impl SimpleFanoutOperation for MGetFanoutOperation {
    type Request = MultiGetRequest;
    type Response = MultiGetResponse;

    fn name() -> &'static str {
        "mget"
    }

    fn get_local_response(ctx: &Context, req: MultiGetRequest) -> ValkeyResult<MultiGetResponse> {
        let filters = deserialize_matchers_list(Some(req.filters))
            .map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))?;

        let mreq = MGetRequest {
            with_labels: req.with_labels,
            filters,
            selected_labels: req.selected_labels,
            latest: req.latest,
        };

        let results = process_mget_request(ctx, mreq)?;

        let values: Vec<MGetValue> = results.into_iter().map(|resp| resp.into()).collect();

        Ok(MultiGetResponse { values })
    }

    fn generate_request(&self) -> MultiGetRequest {
        let filters =
            serialize_matchers_list(&self.options.filters).expect("serialize matchers list");
        MultiGetRequest {
            with_labels: self.options.with_labels,
            filters,
            selected_labels: self.options.selected_labels.clone(),
            latest: self.options.latest,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        self.series.extend(resp.values);
    }

    fn reply(&mut self, thread_ctx: &ThreadSafeContext<BlockedClient>) -> Status {
        let response = std::mem::take(&mut self.series)
            .into_iter()
            .map(mget_value_to_reply)
            .collect::<Vec<_>>();
        thread_ctx.reply(Ok(ValkeyValue::Array(response)))
    }
}

fn mget_value_to_reply(value: MGetValue) -> ValkeyValue {
    ValkeyValue::Array(vec![
        ValkeyValue::BulkString(value.key),
        fanout_labels_to_reply(value.labels),
        fanout_sample_to_reply(value.sample),
    ])
}

fn fanout_labels_to_reply(labels: Vec<Label>) -> ValkeyValue {
    ValkeyValue::Array(labels.into_iter().map(fanout_label_to_reply).collect())
}

fn fanout_label_to_reply(label: Label) -> ValkeyValue {
    if label.name.is_empty() {
        ValkeyValue::Null
    } else {
        ValkeyValue::Array(vec![
            ValkeyValue::BulkString(label.name),
            ValkeyValue::BulkString(label.value),
        ])
    }
}

fn fanout_sample_to_reply(sample: Option<Sample>) -> ValkeyValue {
    match sample {
        Some(sample) => ValkeyValue::Array(vec![
            ValkeyValue::Integer(sample.timestamp),
            ValkeyValue::Float(sample.value),
        ]),
        None => ValkeyValue::Null,
    }
}
