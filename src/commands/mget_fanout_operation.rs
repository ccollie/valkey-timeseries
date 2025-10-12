use super::fanout::generated::{
    Label as FanoutLabel, MGetValue, MultiGetRequest, MultiGetResponse, Sample as FanoutSample,
};
use super::utils::{reply_with_bulk_string, reply_with_fanout_labels, reply_with_fanout_sample};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_mget_request;
use crate::error_consts;
use crate::fanout::FanoutOperation;
use crate::fanout::{FanoutTarget, exec_fanout_request_base};
use crate::series::request_types::MGetRequest;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyValue, raw};

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

impl FanoutOperation for MGetFanoutOperation {
    type Request = MultiGetRequest;
    type Response = MultiGetResponse;

    fn name() -> &'static str {
        "mget"
    }

    fn get_local_response(ctx: &Context, req: MultiGetRequest) -> ValkeyResult<MultiGetResponse> {
        let MultiGetRequest {
            with_labels,
            filters,
            selected_labels,
            latest,
        } = req;
        let filters = deserialize_matchers_list(Some(filters))
            .map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))?;
        let mreq = MGetRequest {
            with_labels,
            filters,
            selected_labels,
            latest,
        };
        let res = process_mget_request(ctx, mreq)?;
        let values = res
            .into_iter()
            .map(|resp| {
                let labels = resp
                    .labels
                    .into_iter()
                    .map(|l| {
                        if let Some(l) = l {
                            FanoutLabel {
                                name: l.name,
                                value: l.value,
                            }
                        } else {
                            // Represent missing label with empty label name
                            FanoutLabel {
                                name: String::new(),
                                value: String::new(),
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                let sample = resp.sample.map(|s| FanoutSample {
                    timestamp: s.timestamp,
                    value: s.value,
                });
                MGetValue {
                    key: resp.series_key.to_string_lossy(),
                    labels,
                    sample,
                }
            })
            .collect::<Vec<_>>();
        Ok(MultiGetResponse { values })
    }

    fn generate_request(&mut self) -> MultiGetRequest {
        let filters =
            serialize_matchers_list(&self.options.filters).expect("serialize matchers list");
        MultiGetRequest {
            with_labels: self.options.with_labels,
            filters,
            selected_labels: std::mem::take(&mut self.options.selected_labels),
            latest: self.options.latest,
        }
    }

    fn on_response(&mut self, resp: MultiGetResponse, _target: FanoutTarget) {
        self.series.extend(resp.values);
    }

    fn generate_reply(&mut self, ctx: &Context) {
        let count = self.series.len();
        raw::reply_with_array(ctx.ctx, count as i64);
        for response in self.series.iter() {
            reply_with_mget_value(ctx, response);
        }
    }
}

fn reply_with_mget_value(ctx: &Context, value: &MGetValue) -> raw::Status {
    let mut status = raw::reply_with_array(ctx.ctx, 3);
    if status != raw::Status::Ok {
        return status;
    }
    status = reply_with_bulk_string(ctx, value.key.as_str());
    if status != raw::Status::Ok {
        return status;
    }
    status = reply_with_fanout_labels(ctx, &value.labels);
    if status != raw::Status::Ok {
        return status;
    }
    reply_with_fanout_sample(ctx, &value.sample)
}

pub(super) fn exec_mget_fanout_request(
    ctx: &Context,
    options: MGetRequest,
) -> ValkeyResult<ValkeyValue> {
    let operation = MGetFanoutOperation::new(options);
    exec_fanout_request_base(ctx, operation)
}
