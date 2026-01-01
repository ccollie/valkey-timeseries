use super::fanout::generated::{
    Label as FanoutLabel, MGetValue, MultiGetRequest, MultiGetResponse, Sample as FanoutSample,
};
use super::utils::{reply_with_bulk_string, reply_with_fanout_labels, reply_with_fanout_sample};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_mget_request;
use crate::error_consts;
use crate::fanout::{FanoutOperation, NodeInfo};
use crate::series::request_types::MGetRequest;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult, reply_with_array};

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
        let filters = deserialize_matchers_list(Some(req.filters))
            .map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))?;

        let mreq = MGetRequest {
            with_labels: req.with_labels,
            filters,
            selected_labels: req.selected_labels,
            latest: req.latest,
        };

        let results = process_mget_request(ctx, mreq)?;

        let values = results
            .into_iter()
            .map(|resp| {
                let labels = resp
                    .labels
                    .into_iter()
                    .map(|l| l.map_or_else(|| FanoutLabel::default(), |l| l.into()))
                    .collect();

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
            .collect();

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

    fn generate_reply(&mut self, ctx: &Context) -> Status {
        let count = self.series.len();
        let status = reply_with_array(ctx.ctx, count as i64);
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

fn reply_with_mget_value(ctx: &Context, value: &MGetValue) -> Status {
    reply_with_array(ctx.ctx, 3);
    reply_with_bulk_string(ctx, value.key.as_str());
    reply_with_fanout_labels(ctx, &value.labels);
    reply_with_fanout_sample(ctx, &value.sample);
    Status::Ok
}
