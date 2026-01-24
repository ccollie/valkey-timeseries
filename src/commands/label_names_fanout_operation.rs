use super::fanout::deserialize_match_filter_options;
use super::fanout::generated::{LabelNamesRequest, LabelNamesResponse};
use crate::commands::fanout::filters::serialize_matchers_list;
use crate::commands::process_label_names_request;
use crate::fanout::{FanoutOperation, NodeInfo};
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult, ValkeyValue};

#[derive(Debug, Default)]
pub struct LabelNamesFanoutOperation {
    pub options: MatchFilterOptions,
    names: BTreeSet<String>,
}

impl LabelNamesFanoutOperation {
    pub fn new(options: MatchFilterOptions) -> Self {
        Self {
            options,
            names: BTreeSet::new(),
        }
    }
}

impl FanoutOperation for LabelNamesFanoutOperation {
    type Request = LabelNamesRequest;
    type Response = LabelNamesResponse;

    fn name() -> &'static str {
        "label_names"
    }

    fn get_local_response(
        ctx: &Context,
        req: LabelNamesRequest,
    ) -> ValkeyResult<LabelNamesResponse> {
        let mut options = deserialize_match_filter_options(req.range, Some(req.filters))?;
        options.limit = None; // limit is applied in the sender node
        process_label_names_request(ctx, &options).map(|names| LabelNamesResponse { names })
    }

    fn generate_request(&self) -> LabelNamesRequest {
        let filters =
            serialize_matchers_list(&self.options.matchers).expect("serialize matchers list");
        LabelNamesRequest {
            range: self.options.date_range.map(|x| x.into()),
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        for name in resp.names {
            self.names.insert(name);
        }
    }

    fn generate_reply(&mut self, ctx: &Context) -> Status {
        let count = self.options.limit.unwrap_or(self.names.len());
        let results = std::mem::take(&mut self.names);
        let arr = results
            .into_iter()
            .take(count)
            .map(ValkeyValue::BulkString)
            .collect::<Vec<ValkeyValue>>();
        ctx.reply(Ok(ValkeyValue::Array(arr)))
    }
}
