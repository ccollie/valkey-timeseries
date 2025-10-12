use super::fanout::deserialize_match_filter_options;
use super::fanout::generated::{LabelNamesRequest, LabelNamesResponse};
use crate::commands::fanout::filters::serialize_matchers_list;
use crate::commands::process_label_names_request;
use crate::commands::utils::reply_with_btree_set;
use crate::fanout::FanoutOperation;
use crate::fanout::FanoutTarget;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, ValkeyResult, ValkeyValue};

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
        let options = deserialize_match_filter_options(req.range, Some(req.filters))?;
        process_label_names_request(ctx, &options).map(|names| LabelNamesResponse { names })
    }

    fn generate_request(&mut self) -> LabelNamesRequest {
        let filters =
            serialize_matchers_list(&self.options.matchers).expect("serialize matchers list");
        let range = self.options.date_range.map(|x| x.into());
        LabelNamesRequest { range, filters }
    }

    fn on_response(&mut self, resp: LabelNamesResponse, _target: FanoutTarget) {
        for name in resp.names {
            self.names.insert(name);
        }
    }

    fn generate_reply(&mut self, ctx: &Context) {
        reply_with_btree_set(ctx, &self.names);
    }
}

pub(super) fn exec_label_names_fanout_request(
    ctx: &Context,
    options: MatchFilterOptions,
) -> ValkeyResult<ValkeyValue> {
    let operation = LabelNamesFanoutOperation::new(options);
    super::exec_fanout_request_base(ctx, operation)
}
