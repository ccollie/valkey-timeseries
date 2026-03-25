use super::fanout::filters::serialize_matchers_list;
use super::fanout::{IndexQueryRequest, IndexQueryResponse, deserialize_match_filter_options};
use crate::fanout::FanoutContext;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::series::index::series_keys_by_selectors;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Clone, Debug, Default)]
pub struct QueryIndexFanoutCommand {
    options: MatchFilterOptions,
    keys: BTreeSet<String>,
}

impl QueryIndexFanoutCommand {
    pub fn new(options: MatchFilterOptions) -> Self {
        Self {
            options,
            keys: BTreeSet::new(),
        }
    }
}

impl FanoutClientCommand for QueryIndexFanoutCommand {
    type Request = IndexQueryRequest;
    type Response = IndexQueryResponse;

    fn name() -> &'static str {
        "index_query"
    }

    fn get_local_response(
        ctx: &Context,
        req: IndexQueryRequest,
    ) -> ValkeyResult<IndexQueryResponse> {
        let options = deserialize_match_filter_options(req.range, Some(req.filters))?;
        let keys = series_keys_by_selectors(ctx, &options.matchers, options.date_range)?;
        let keys = keys.into_iter().map(|k| k.to_string()).collect::<Vec<_>>();
        Ok(IndexQueryResponse { keys })
    }

    fn generate_request(&self) -> IndexQueryRequest {
        let filters =
            serialize_matchers_list(&self.options.matchers).expect("serialize matchers list");
        let range = self.options.date_range.map(|r| r.into());
        IndexQueryRequest { range, filters }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        for key in resp.keys {
            self.keys.insert(key);
        }
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        ctx.reply_with_array(self.keys.len());
        for key in self.keys.iter() {
            ctx.reply_with_bulk_string(key);
        }
        Status::Ok
    }
}
