use super::fanout::generated::{LabelValuesRequest, LabelValuesResponse};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_label_values_request;
use crate::fanout::FanoutContext;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::series::request_types::{MatchFilterOptions, MetaDateRangeFilter};
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
pub struct LabelValuesFanoutCommand {
    pub label: String,
    pub options: MatchFilterOptions,
    results: BTreeSet<String>,
}

impl LabelValuesFanoutCommand {
    pub fn new(label: String, options: MatchFilterOptions) -> Self {
        Self {
            label,
            options,
            results: BTreeSet::new(),
        }
    }
}

impl FanoutClientCommand for LabelValuesFanoutCommand {
    type Request = LabelValuesRequest;
    type Response = LabelValuesResponse;

    fn name() -> &'static str {
        "label_values"
    }

    fn get_local_response(
        ctx: &Context,
        req: LabelValuesRequest,
    ) -> ValkeyResult<LabelValuesResponse> {
        let date_range: Option<MetaDateRangeFilter> = req.range.map(|r| r.into());
        let matchers: Vec<SeriesSelector> = deserialize_matchers_list(Some(req.filters))?;
        let options = MatchFilterOptions {
            date_range,
            matchers,
            // send all values to requester. Limit is applied in the sender node.
            limit: None,
        };
        process_label_values_request(ctx, &req.label, &options)
            .map(|values| LabelValuesResponse { values })
    }

    fn generate_request(&self) -> LabelValuesRequest {
        let filters = serialize_matchers_list(self.options.matchers.as_ref())
            .expect("serialize matchers list");
        LabelValuesRequest {
            label: self.label.clone(),
            range: self.options.date_range.map(|x| x.into()),
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        for value in resp.values {
            self.results.insert(value);
        }
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let limit = self
            .options
            .limit
            .unwrap_or(self.results.len())
            .min(self.results.len());

        ctx.reply_with_array(limit);
        for value in self.results.iter().take(limit) {
            ctx.reply_with_bulk_string(value);
        }

        Status::Ok
    }
}
