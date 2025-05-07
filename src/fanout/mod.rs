pub mod cluster;
mod coordinator;
mod error;
pub mod request;
mod results_tracker;

use crate::common::time::current_time_millis;
use crate::fanout::coordinator::send_request;
use crate::series::index::PostingsStats;
use crate::series::request_types::{MGetRequest, MatchFilterOptions};
pub use coordinator::register_cluster_message_handlers;
pub use request::*;
pub(super) use results_tracker::*;
use std::sync::atomic::AtomicBool;
use valkey_module::{BlockedClient, Context, RedisModuleTimerID, ThreadSafeContext, ValkeyResult};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterMessageType {
    IndexQuery = 0,
    RangeQuery = 1,
    MultiRangeQuery = 2,
    MGetQuery = 3,
    LabelNames = 4,
    LabelValues = 5,
    Cardinality = 6,
    Stats = 7,
    /// Response types
    /// These are the same as the request but with a different value.
    IndexQueryResponse = 100,
    RangeQueryResponse = 101,
    MultiRangeQueryResponse = 102,
    MultiGetResponse = 103,
    LabelNamesResponse = 104,
    LabelValuesResponse = 105,
    CardinalityResponse = 106,
    StatsResponse = 107,
    Error = 255,
}

impl From<u8> for ClusterMessageType {
    fn from(value: u8) -> Self {
        match value {
            0 => ClusterMessageType::IndexQuery,
            1 => ClusterMessageType::RangeQuery,
            2 => ClusterMessageType::MultiRangeQuery,
            3 => ClusterMessageType::MGetQuery,
            4 => ClusterMessageType::LabelNames,
            5 => ClusterMessageType::LabelValues,
            6 => ClusterMessageType::Cardinality,
            7 => ClusterMessageType::Stats,
            100 => ClusterMessageType::IndexQueryResponse,
            101 => ClusterMessageType::RangeQueryResponse,
            102 => ClusterMessageType::MultiRangeQueryResponse,
            103 => ClusterMessageType::MultiGetResponse,
            104 => ClusterMessageType::LabelNamesResponse,
            105 => ClusterMessageType::LabelValuesResponse,
            106 => ClusterMessageType::CardinalityResponse,
            107 => ClusterMessageType::StatsResponse,
            _ => ClusterMessageType::Error,
        }
    }
}

impl std::fmt::Display for ClusterMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterMessageType::IndexQuery => write!(f, "queryindex"),
            ClusterMessageType::RangeQuery => write!(f, "range"),
            ClusterMessageType::MultiRangeQuery => write!(f, "mrange"),
            ClusterMessageType::MGetQuery => write!(f, "mget"),
            ClusterMessageType::LabelNames => write!(f, "labelnames"),
            ClusterMessageType::LabelValues => write!(f, "labelvalues"),
            ClusterMessageType::Cardinality => write!(f, "card"),
            ClusterMessageType::IndexQueryResponse => write!(f, "IndexQueryResponse"),
            ClusterMessageType::RangeQueryResponse => write!(f, "RangeQueryResponse"),
            ClusterMessageType::MultiRangeQueryResponse => write!(f, "MultiRangeQueryResponse"),
            ClusterMessageType::MultiGetResponse => write!(f, "MultiGetResponse"),
            ClusterMessageType::LabelNamesResponse => write!(f, "LabelNamesResponse"),
            ClusterMessageType::LabelValuesResponse => write!(f, "LabelValuesResponse"),
            ClusterMessageType::CardinalityResponse => write!(f, "CardinalityResponse"),
            ClusterMessageType::Stats => write!(f, "stats"),
            ClusterMessageType::StatsResponse => write!(f, "StatsResponse"),
            ClusterMessageType::Error => write!(f, "Error"),
        }
    }
}

pub enum TrackerEnum {
    IndexQuery(ResultsTracker<IndexQueryResponse>),
    RangeQuery(ResultsTracker<RangeResponse>),
    MultiRangeQuery(ResultsTracker<MultiRangeResponse>),
    MGetQuery(ResultsTracker<MultiGetResponse>),
    LabelNames(ResultsTracker<LabelNamesResponse>),
    LabelValues(ResultsTracker<LabelValuesResponse>),
    Cardinality(ResultsTracker<CardinalityResponse>),
    Stats(ResultsTracker<PostingsStats>),
}

impl From<ResultsTracker<IndexQueryResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<IndexQueryResponse>) -> Self {
        TrackerEnum::IndexQuery(tracker)
    }
}

impl From<ResultsTracker<RangeResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<RangeResponse>) -> Self {
        TrackerEnum::RangeQuery(tracker)
    }
}

impl From<ResultsTracker<MultiRangeResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<MultiRangeResponse>) -> Self {
        TrackerEnum::MultiRangeQuery(tracker)
    }
}

impl From<ResultsTracker<MultiGetResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<MultiGetResponse>) -> Self {
        TrackerEnum::MGetQuery(tracker)
    }
}

impl From<ResultsTracker<LabelNamesResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<LabelNamesResponse>) -> Self {
        TrackerEnum::LabelNames(tracker)
    }
}

impl From<ResultsTracker<LabelValuesResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<LabelValuesResponse>) -> Self {
        TrackerEnum::LabelValues(tracker)
    }
}

impl From<ResultsTracker<CardinalityResponse>> for TrackerEnum {
    fn from(tracker: ResultsTracker<CardinalityResponse>) -> Self {
        TrackerEnum::Cardinality(tracker)
    }
}

impl From<ResultsTracker<PostingsStats>> for TrackerEnum {
    fn from(tracker: ResultsTracker<PostingsStats>) -> Self {
        TrackerEnum::Stats(tracker)
    }
}

pub(super) struct InFlightRequest {
    pub db: i32,
    pub request_start: i64,
    pub responses: TrackerEnum,
    pub timer_id: RedisModuleTimerID,
    pub timed_out: AtomicBool,
}

impl InFlightRequest {
    pub fn new(db: i32, tracker: TrackerEnum) -> Self {
        let request_start = current_time_millis();
        Self {
            db,
            request_start,
            responses: tracker,
            timer_id: 0,
            timed_out: AtomicBool::new(false),
        }
    }

    pub fn request_type(&self) -> ClusterMessageType {
        match self.responses {
            TrackerEnum::IndexQuery(_) => ClusterMessageType::IndexQuery,
            TrackerEnum::RangeQuery(_) => ClusterMessageType::RangeQuery,
            TrackerEnum::MultiRangeQuery(_) => ClusterMessageType::MultiRangeQuery,
            TrackerEnum::MGetQuery(_) => ClusterMessageType::MGetQuery,
            TrackerEnum::LabelNames(_) => ClusterMessageType::LabelNames,
            TrackerEnum::LabelValues(_) => ClusterMessageType::LabelValues,
            TrackerEnum::Cardinality(_) => ClusterMessageType::Cardinality,
            TrackerEnum::Stats(_) => ClusterMessageType::Stats,
        }
    }
    pub fn response_type(&self) -> ClusterMessageType {
        match self.responses {
            TrackerEnum::IndexQuery(_) => ClusterMessageType::IndexQueryResponse,
            TrackerEnum::RangeQuery(_) => ClusterMessageType::RangeQueryResponse,
            TrackerEnum::MultiRangeQuery(_) => ClusterMessageType::MultiRangeQueryResponse,
            TrackerEnum::MGetQuery(_) => ClusterMessageType::MultiGetResponse,
            TrackerEnum::LabelNames(_) => ClusterMessageType::LabelNamesResponse,
            TrackerEnum::LabelValues(_) => ClusterMessageType::LabelValuesResponse,
            TrackerEnum::Cardinality(_) => ClusterMessageType::CardinalityResponse,
            TrackerEnum::Stats(_) => ClusterMessageType::StatsResponse,
        }
    }

    pub fn raise_error(&self, error: &str) {
        match self.responses {
            TrackerEnum::IndexQuery(ref t) => t.raise_error(error),
            TrackerEnum::RangeQuery(ref t) => t.raise_error(error),
            TrackerEnum::MultiRangeQuery(ref t) => t.raise_error(error),
            TrackerEnum::MGetQuery(ref t) => t.raise_error(error),
            TrackerEnum::LabelNames(ref t) => t.raise_error(error),
            TrackerEnum::LabelValues(ref t) => t.raise_error(error),
            TrackerEnum::Cardinality(ref t) => t.raise_error(error),
            TrackerEnum::Stats(ref t) => t.raise_error(error),
        }
    }

    pub fn call_done(&self) {
        match self.responses {
            TrackerEnum::IndexQuery(ref t) => t.call_done(),
            TrackerEnum::RangeQuery(ref t) => t.call_done(),
            TrackerEnum::MultiRangeQuery(ref t) => t.call_done(),
            TrackerEnum::MGetQuery(ref t) => t.call_done(),
            TrackerEnum::LabelNames(ref t) => t.call_done(),
            TrackerEnum::LabelValues(ref t) => t.call_done(),
            TrackerEnum::Cardinality(ref t) => t.call_done(),
            TrackerEnum::Stats(ref t) => t.call_done(),
        }
    }

    pub fn is_completed(&self) -> bool {
        match self.responses {
            TrackerEnum::IndexQuery(ref t) => t.completed(),
            TrackerEnum::RangeQuery(ref t) => t.completed(),
            TrackerEnum::MultiRangeQuery(ref t) => t.completed(),
            TrackerEnum::MGetQuery(ref t) => t.completed(),
            TrackerEnum::LabelNames(ref t) => t.completed(),
            TrackerEnum::LabelValues(ref t) => t.completed(),
            TrackerEnum::Cardinality(ref t) => t.completed(),
            TrackerEnum::Stats(ref t) => t.completed(),
        }
    }

    pub fn is_timed_out(&self) -> bool {
        self.timed_out.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn time_out(&self) {
        self.timed_out
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if !self.is_completed() {
            self.call_done();
        }
    }
}

pub fn perform_remote_mget_request<F>(
    ctx: &Context,
    request: &MGetRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<MultiGetResponse>) + Send + 'static,
{
    send_request::<MGetShardedCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_card_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<CardinalityResponse>) + Send + 'static,
{
    send_request::<CardinalityCommand, _>(ctx, request, on_done)
}

pub fn perform_remote_label_names_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<LabelNamesResponse>) + Send + 'static,
{
    send_request::<LabelNamesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_label_values_request<F>(
    ctx: &Context,
    request: &LabelValuesRequest,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<LabelValuesResponse>) + Send + 'static,
{
    send_request::<LabelValuesCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_index_query_request<F>(
    ctx: &Context,
    request: &MatchFilterOptions,
    on_done: F,
) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<IndexQueryResponse>) + Send + 'static,
{
    send_request::<IndexQueryCommand, F>(ctx, request, on_done)
}

pub fn perform_remote_stats_request<F>(ctx: &Context, limit: usize, on_done: F) -> ValkeyResult<u64>
where
    F: FnOnce(&ThreadSafeContext<BlockedClient>, Vec<PostingsStats>) + Send + 'static,
{
    let request = StatsRequest { limit };
    send_request::<StatsCommand, F>(ctx, &request, on_done)
}
