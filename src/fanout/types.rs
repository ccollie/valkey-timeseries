use super::request::CardinalityResponse;
use crate::common::time::current_time_millis;
use crate::fanout::request::{IndexQueryResponse, LabelNamesResponse, LabelValuesResponse, MultiGetResponse, MultiRangeResponse, RangeResponse};
use crate::fanout::ResultsTracker;
use std::sync::Mutex;

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
    /// Response types
    /// These are the same as the request, but with a different value.

    IndexQueryResponse = 100,
    RangeQueryResponse = 101,
    MultiRangeQueryResponse = 102,
    MultiGetResponse = 103,
    LabelNamesResponse = 104,
    LabelValuesResponse = 105,
    CardinalityResponse = 106,
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
            100 => ClusterMessageType::IndexQueryResponse,
            101 => ClusterMessageType::RangeQueryResponse,
            102 => ClusterMessageType::MultiRangeQueryResponse,
            103 => ClusterMessageType::MultiGetResponse,
            104 => ClusterMessageType::LabelNamesResponse,
            105 => ClusterMessageType::LabelValuesResponse,
            106 => ClusterMessageType::CardinalityResponse,
            _ => ClusterMessageType::Error,
        }
    }
}

impl std::fmt::Display for ClusterMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterMessageType::IndexQuery => write!(f, "IndexQuery"),
            ClusterMessageType::RangeQuery => write!(f, "RangeQuery"),
            ClusterMessageType::MultiRangeQuery => write!(f, "MultiRangeQuery"),
            ClusterMessageType::MGetQuery => write!(f, "MGetQuery"),
            ClusterMessageType::LabelNames => write!(f, "LabelNames"),
            ClusterMessageType::LabelValues => write!(f, "LabelValues"),
            ClusterMessageType::Cardinality => write!(f, "Cardinality"),
            ClusterMessageType::IndexQueryResponse => write!(f, "IndexQueryResponse"),
            ClusterMessageType::RangeQueryResponse => write!(f, "RangeQueryResponse"),
            ClusterMessageType::MultiRangeQueryResponse => write!(f, "MultiRangeQueryResponse"),
            ClusterMessageType::MultiGetResponse => write!(f, "MultiGetResponse"),
            ClusterMessageType::LabelNamesResponse => write!(f, "LabelNamesResponse"),
            ClusterMessageType::LabelValuesResponse => write!(f, "LabelValuesResponse"),
            ClusterMessageType::CardinalityResponse => write!(f, "CardinalityResponse"),
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
}

impl TrackerEnum {
    pub fn request_type(&self) -> ClusterMessageType {
        match self {
            TrackerEnum::IndexQuery(_) => ClusterMessageType::IndexQuery,
            TrackerEnum::RangeQuery(_) => ClusterMessageType::RangeQuery,
            TrackerEnum::MultiRangeQuery(_) => ClusterMessageType::MultiRangeQuery,
            TrackerEnum::MGetQuery(_) => ClusterMessageType::MGetQuery,
            TrackerEnum::LabelNames(_) => ClusterMessageType::LabelNames,
            TrackerEnum::LabelValues(_) => ClusterMessageType::LabelValues,
            TrackerEnum::Cardinality(_) => ClusterMessageType::Cardinality,
        }
    }

    pub fn decrement(&self) -> bool {
        match self {
            TrackerEnum::IndexQuery(ref t) => t.decrement(),
            TrackerEnum::RangeQuery(ref t) => t.decrement(),
            TrackerEnum::MultiRangeQuery(ref t) => t.decrement(),
            TrackerEnum::MGetQuery(ref t) => t.decrement(),
            TrackerEnum::LabelNames(ref t) => t.decrement(),
            TrackerEnum::LabelValues(ref t) => t.decrement(),
            TrackerEnum::Cardinality(ref t) => t.decrement(),
        }
    }
    
    pub fn raise_error(&self, error: &str) {
        match self {
            TrackerEnum::IndexQuery(ref t) => t.raise_error(error),
            TrackerEnum::RangeQuery(ref t) => t.raise_error(error),
            TrackerEnum::MultiRangeQuery(ref t) => t.raise_error(error),
            TrackerEnum::MGetQuery(ref t) => t.raise_error(error),
            TrackerEnum::LabelNames(ref t) => t.raise_error(error),
            TrackerEnum::LabelValues(ref t) => t.raise_error(error),
            TrackerEnum::Cardinality(ref t) => t.raise_error(error),
        }
    }
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

struct InFlightRequestInner {
    outstanding_responses: u32,
    responses: TrackerEnum,
}

pub(super) struct InFlightRequest {
    pub db: i32,
    pub request_start: i64,
    pub responses: TrackerEnum,
    _error_msg: Mutex<Option<String>>,
    _errors: Mutex<Vec<String>>,
}

impl InFlightRequest {
    pub fn new(
        db: i32,
        tracker: TrackerEnum
    ) -> Self {
        let request_start = current_time_millis();
        Self {
            db,
            request_start,
            responses: tracker,
            _error_msg: Mutex::new(None),
            _errors: Mutex::new(vec![]),
        }
    }

    pub fn request_type(&self) -> ClusterMessageType {
        self.responses.request_type()
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
        }
    }
    
    pub fn get_error_msg(&self) -> Option<String> {
        let guard = self._error_msg.lock().unwrap();
        guard.clone()
    }
    
    pub(crate) fn raise_error(&self, msg: String) {
        self.responses.raise_error(&msg);
        self.responses.decrement();
        let mut errors = self._errors.lock().unwrap();
        errors.push(msg.clone());
        let mut guard = self._error_msg.lock().unwrap();
        *guard = Some(msg);
    }
}