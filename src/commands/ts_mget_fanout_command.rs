use super::fanout::generated::{Label, MGetValue, MultiGetRequest, MultiGetResponse, Sample};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_mget_request;
use crate::commands::utils::{reply_with_fanout_labels, reply_with_fanout_sample};
use crate::error_consts;
use crate::fanout::FanoutContext;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::series::request_types::MGetRequest;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult, ValkeyValue};

#[derive(Debug, Default)]
pub struct MGetFanoutCommand {
    options: MGetRequest,
    series: Vec<MGetValue>,
}

impl MGetFanoutCommand {
    pub fn new(options: MGetRequest) -> Self {
        Self {
            options,
            series: Vec::new(),
        }
    }
}

impl FanoutClientCommand for MGetFanoutCommand {
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

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let count = self.series.len();
        let status = ctx.reply_with_array(count);
        if status != Status::Ok {
            return status;
        }
        for response in self.series.iter() {
            let status = reply_with_mget_value(ctx, response);
            if status != Status::Ok {
                return status;
            }
        }
        Status::Ok
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

pub(super) fn reply_with_mget_value(ctx: &FanoutContext, value: &MGetValue) -> Status {
    ctx.reply_with_array(3);
    ctx.reply_with_bulk_string(value.key.as_str());
    reply_with_fanout_labels(ctx, &value.labels);
    reply_with_fanout_sample(ctx, &value.sample);
    Status::Ok
}
