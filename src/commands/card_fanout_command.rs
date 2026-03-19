use super::fanout::generated::{CardinalityRequest, CardinalityResponse};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::fanout::FanoutContext;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::series::index::count_matched_series;
use crate::series::request_types::{MatchFilterOptions, MetaDateRangeFilter};
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
pub struct CardFanoutCommand {
    options: MatchFilterOptions,
    result: usize,
}

impl CardFanoutCommand {
    pub fn new(options: MatchFilterOptions) -> Self {
        Self { options, result: 0 }
    }
}

impl FanoutClientCommand for CardFanoutCommand {
    type Request = CardinalityRequest;
    type Response = CardinalityResponse;

    fn name() -> &'static str {
        "card"
    }

    fn get_local_response(
        ctx: &Context,
        req: CardinalityRequest,
    ) -> ValkeyResult<CardinalityResponse> {
        let date_range: Option<MetaDateRangeFilter> = req.range.map(|r| r.into());
        let matchers = deserialize_matchers_list(Some(req.filters))?;
        let count = count_matched_series(ctx, date_range, &matchers)? as u64;
        Ok(CardinalityResponse { cardinality: count })
    }

    fn generate_request(&self) -> CardinalityRequest {
        let filters = serialize_matchers_list(&self.options.matchers).expect("serialize matchers");
        CardinalityRequest {
            range: self.options.date_range.map(|r| r.into()),
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        self.result += resp.cardinality as usize;
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        ctx.reply_with_i64(self.result as i64)
    }
}
