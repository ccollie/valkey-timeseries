use super::fanout::deserialize_match_filter_options;
use super::fanout::generated::{LabelNamesRequest, LabelNamesResponse};
use crate::commands::fanout::filters::serialize_matchers_list;
use crate::commands::process_label_names_request;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Debug, Default)]
pub struct LabelNamesFanoutCommand {
    pub options: MatchFilterOptions,
    names: BTreeSet<String>,
}

impl LabelNamesFanoutCommand {
    pub fn new(options: MatchFilterOptions) -> Self {
        Self {
            options,
            names: BTreeSet::new(),
        }
    }
}

impl FanoutClientCommand for LabelNamesFanoutCommand {
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

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        for name in resp.names {
            self.names.insert(name);
        }
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let names_len = self.names.len();
        let limit = self.options.limit.unwrap_or(names_len).min(names_len);
        ctx.reply_with_array(limit);
        for name in self.names.iter().take(limit) {
            ctx.reply_with_bulk_string(name);
        }
        Status::Ok
    }
}
