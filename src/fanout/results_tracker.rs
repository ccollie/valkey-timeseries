use super::{
    CardinalityResponse, IndexQueryResponse, LabelNamesResponse, LabelValuesResponse,
    MultiGetResponse, MultiRangeResponse, RangeResponse,
};
use crate::fanout::error::Error;
use crate::series::index::PostingsStats;
use crate::series::request_types::RangeGroupingOptions;
use std::sync::{Mutex, MutexGuard};

pub type ResponseCallback<T, S> = Box<dyn FnOnce(Vec<T>, Vec<Error>, S) + Send>;


struct ResultsTrackerInner<T, S: Default = ()> {
    results: Vec<T>,
    errors: Vec<Error>,
    callback: Option<ResponseCallback<T, S>>,
    state: S,
    outstanding_requests: u32,
}

pub struct ResultsTracker<T, S: Default = ()> {
    inner: Mutex<ResultsTrackerInner<T, S>>,
}

impl<T, S: Default> ResultsTracker<T, S> {
    pub fn new(outstanding_requests: usize, state: S, callback: ResponseCallback<T, S>) -> Self {
        let inner = ResultsTrackerInner {
            results: Vec::new(),
            errors: Vec::new(),
            callback: Some(callback),
            state,
            outstanding_requests: outstanding_requests as u32,
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub fn add_result(&self, result: T) {
        let mut inner = self.inner.lock().unwrap();
        inner.results.push(result)
    }

    pub fn update(&self, result: T) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.results.push(result);
        self.decrement_internal(&mut inner)
    }

    fn callback_if_needed(inner: &mut MutexGuard<ResultsTrackerInner<T, S>>) -> bool {
        if inner.outstanding_requests != 0 {
            return false;
        }
        if let Some(callback) = inner.callback.take() {
            let final_results: Vec<T> = std::mem::take(&mut inner.results);
            let errors: Vec<Error> = std::mem::take(&mut inner.errors);
            let state = std::mem::take(&mut inner.state);
            callback(final_results, errors, state);
        }
        true
    }

    fn decrement_internal(&self, inner: &mut MutexGuard<ResultsTrackerInner<T, S>>) -> bool {
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        // If no outstanding request, execute the callback
        Self::callback_if_needed(inner)
    }

    pub fn completed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.outstanding_requests == 0
    }

    pub fn raise_error(&self, error: Error) {
        let mut inner = self.inner.lock().unwrap();
        inner.errors.push(error);
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        Self::callback_if_needed(&mut inner);
    }

    pub fn call_done(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.outstanding_requests = 0;
        Self::callback_if_needed(&mut inner);
    }
}

pub enum TrackerEnum {
    IndexQuery(ResultsTracker<IndexQueryResponse>),
    RangeQuery(ResultsTracker<RangeResponse>),
    MultiRangeQuery(ResultsTracker<MultiRangeResponse, Option<RangeGroupingOptions>>),
    MGetQuery(ResultsTracker<MultiGetResponse>),
    LabelNames(ResultsTracker<LabelNamesResponse>),
    LabelValues(ResultsTracker<LabelValuesResponse>),
    Cardinality(ResultsTracker<CardinalityResponse>),
    Stats(ResultsTracker<PostingsStats, u64>),
}

impl TrackerEnum {
    pub fn raise_error(&self, error: Error) {
        match self {
            TrackerEnum::IndexQuery(t) => t.raise_error(error),
            TrackerEnum::RangeQuery(t) => t.raise_error(error),
            TrackerEnum::MultiRangeQuery(t) => t.raise_error(error),
            TrackerEnum::MGetQuery(t) => t.raise_error(error),
            TrackerEnum::LabelNames(t) => t.raise_error(error),
            TrackerEnum::LabelValues(t) => t.raise_error(error),
            TrackerEnum::Cardinality(t) => t.raise_error(error),
            TrackerEnum::Stats(t) => t.raise_error(error),
        }
    }

    pub fn call_done(&self) {
        match self {
            TrackerEnum::IndexQuery(t) => t.call_done(),
            TrackerEnum::RangeQuery(t) => t.call_done(),
            TrackerEnum::MultiRangeQuery(t) => t.call_done(),
            TrackerEnum::MGetQuery(t) => t.call_done(),
            TrackerEnum::LabelNames(t) => t.call_done(),
            TrackerEnum::LabelValues(t) => t.call_done(),
            TrackerEnum::Cardinality(t) => t.call_done(),
            TrackerEnum::Stats(t) => t.call_done(),
        }
    }

    pub fn is_completed(&self) -> bool {
        match self {
            TrackerEnum::IndexQuery(t) => t.completed(),
            TrackerEnum::RangeQuery(t) => t.completed(),
            TrackerEnum::MultiRangeQuery(t) => t.completed(),
            TrackerEnum::MGetQuery(t) => t.completed(),
            TrackerEnum::LabelNames(t) => t.completed(),
            TrackerEnum::LabelValues(t) => t.completed(),
            TrackerEnum::Cardinality(t) => t.completed(),
            TrackerEnum::Stats(t) => t.completed(),
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

impl From<ResultsTracker<MultiRangeResponse, Option<RangeGroupingOptions>>> for TrackerEnum {
    fn from(tracker: ResultsTracker<MultiRangeResponse, Option<RangeGroupingOptions>>) -> Self {
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

impl From<ResultsTracker<PostingsStats, u64>> for TrackerEnum {
    fn from(tracker: ResultsTracker<PostingsStats, u64>) -> Self {
        TrackerEnum::Stats(tracker)
    }
}