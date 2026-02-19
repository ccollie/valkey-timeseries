use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::fanout::{DateRange, MDelRequest, MDelResponse};
use crate::error_consts;
use crate::fanout::{FanoutCommand, FanoutOperation, NodeInfo};
use crate::labels::filters::SeriesSelector;
use crate::series::{TimestampRange, delete_series_by_selectors};
use valkey_module::{
    BlockedClient, Context, Status, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyValue,
};

#[derive(Default)]
pub struct MDelFanoutOperation {
    selectors: Vec<SeriesSelector>,
    date_range: Option<DateRange>,
    total_deleted: usize,
}

impl MDelFanoutOperation {
    pub fn new(selectors: Vec<SeriesSelector>, date_range: Option<TimestampRange>) -> Self {
        let date_range = date_range.map(|dr| {
            let (start, end) = dr.get_timestamps(None);
            DateRange { start, end }
        });
        MDelFanoutOperation {
            selectors,
            date_range,
            total_deleted: 0,
        }
    }
}

impl FanoutCommand for MDelFanoutOperation {
    type Request = MDelRequest;
    type Response = MDelResponse;

    fn name() -> &'static str {
        "mdel"
    }

    fn get_local_response(ctx: &Context, req: Self::Request) -> ValkeyResult<Self::Response> {
        let filters = deserialize_matchers_list(Some(req.filters))
            .map_err(|_e| ValkeyError::Str(error_consts::COMMAND_DESERIALIZATION_ERROR))?;

        let range = if let Some(date_range) = req.range {
            let ts = TimestampRange::from_timestamps(date_range.start, date_range.end)?;
            Some(ts)
        } else {
            None
        };

        let deleted_count = delete_series_by_selectors(ctx, &filters, range)?;
        Ok(MDelResponse {
            deleted_count: deleted_count as u64,
        })
    }

    fn generate_request(&self) -> Self::Request {
        let filters = serialize_matchers_list(&self.selectors)
            .expect("Failed to serialize selectors for MDelRequest");

        MDelRequest {
            range: self.date_range,
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        self.total_deleted += resp.deleted_count as usize;
    }
}

impl FanoutOperation for MDelFanoutOperation {
    fn reply(&mut self, thread_ctx: &ThreadSafeContext<BlockedClient>) -> Status {
        thread_ctx.reply(Ok(ValkeyValue::Integer(self.total_deleted as i64)))
    }
}
