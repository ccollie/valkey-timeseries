use super::fanout::generated::{CardinalityRequest, CardinalityResponse, DateRange};
use super::utils::reply_with_usize;
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::fanout::{FanoutOperation, NodeInfo};
use crate::series::TimestampRange;
use crate::series::index::count_matched_series;
use crate::series::request_types::MatchFilterOptions;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
pub struct CardFanoutOperation {
    options: MatchFilterOptions,
    result: usize,
}

impl CardFanoutOperation {
    pub fn new(options: MatchFilterOptions) -> Self {
        Self { options, result: 0 }
    }
}

impl FanoutOperation for CardFanoutOperation {
    type Request = CardinalityRequest;
    type Response = CardinalityResponse;

    fn name() -> &'static str {
        "cardinality"
    }

    fn get_local_response(
        ctx: &Context,
        req: CardinalityRequest,
    ) -> ValkeyResult<CardinalityResponse> {
        let date_range: Option<TimestampRange> = req.range.map(|r| r.into());
        let matchers = deserialize_matchers_list(Some(req.filters))?;
        let count = count_matched_series(ctx, date_range, &matchers)? as u64;
        Ok(CardinalityResponse { cardinality: count })
    }

    fn generate_request(&self) -> CardinalityRequest {
        let filters = serialize_matchers_list(&self.options.matchers).expect("serialize matchers");
        let range: Option<DateRange> = self.options.date_range.map(|r| r.into());
        CardinalityRequest { range, filters }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        self.result += resp.cardinality as usize;
    }

    fn generate_reply(&mut self, ctx: &Context) -> Status {
        reply_with_usize(ctx, self.result)
    }
}
