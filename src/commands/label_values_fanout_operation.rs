use super::fanout::generated::{DateRange, LabelValuesRequest, LabelValuesResponse};
use super::utils::reply_with_btree_set;
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::process_label_values_request;
use crate::fanout::{FanoutOperation, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::series::TimestampRange;
use crate::series::request_types::MatchFilterOptions;
use std::collections::BTreeSet;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
pub struct LabelValuesFanoutOperation {
    pub label: String,
    pub options: MatchFilterOptions,
    results: BTreeSet<String>,
}

impl LabelValuesFanoutOperation {
    pub fn new(label: String, options: MatchFilterOptions) -> Self {
        Self {
            label,
            options,
            results: BTreeSet::new(),
        }
    }
}

impl FanoutOperation for LabelValuesFanoutOperation {
    type Request = LabelValuesRequest;
    type Response = LabelValuesResponse;

    fn name() -> &'static str {
        "label_values"
    }

    fn get_local_response(
        ctx: &Context,
        req: LabelValuesRequest,
    ) -> ValkeyResult<LabelValuesResponse> {
        let date_range: Option<TimestampRange> = req.range.map(|x| x.into());
        let matchers: Vec<SeriesSelector> = deserialize_matchers_list(Some(req.filters))?;
        let options = MatchFilterOptions {
            date_range,
            matchers,
            ..Default::default()
        };
        process_label_values_request(ctx, &req.label, &options)
            .map(|values| LabelValuesResponse { values })
    }

    fn generate_request(&self) -> LabelValuesRequest {
        let range: Option<DateRange> = self.options.date_range.map(|r| r.into());
        let filters = serialize_matchers_list(self.options.matchers.as_ref())
            .expect("serialize matchers list");
        LabelValuesRequest {
            label: self.label.clone(),
            range,
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        for value in resp.values {
            self.results.insert(value);
        }
    }

    fn generate_reply(&mut self, ctx: &Context) -> Status {
        reply_with_btree_set(ctx, &self.results)
    }
}
