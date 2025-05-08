use super::{
    CardinalityResponse, IndexQueryResponse, LabelNamesResponse, LabelValuesResponse,
    MultiGetResponse, MultiRangeResponse, RangeResponse,
};
use crate::fanout::error::Error;
use crate::series::index::PostingsStats;
use dtype_variant::{build_dtype_tokens, DType};
use std::sync::{Mutex, MutexGuard};

pub type ResponseCallback<T> = Box<dyn FnOnce(Vec<T>, Vec<Error>) + Send>;

struct ResultsTrackerInner<T> {
    results: Vec<T>,
    errors: Vec<Error>,
    callback: Option<ResponseCallback<T>>,
    outstanding_requests: u32,
}

pub struct ResultsTracker<T> {
    inner: Mutex<ResultsTrackerInner<T>>,
}

impl<T> ResultsTracker<T> {
    pub fn new(outstanding_requests: usize, callback: ResponseCallback<T>) -> Self {
        let inner = ResultsTrackerInner {
            results: Vec::new(),
            errors: Vec::new(),
            callback: Some(callback),
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

    fn callback_if_needed(inner: &mut MutexGuard<ResultsTrackerInner<T>>) -> bool {
        if inner.outstanding_requests != 0 {
            return false;
        }
        if let Some(callback) = inner.callback.take() {
            let final_results: Vec<T> = std::mem::take(&mut inner.results);
            let errors: Vec<Error> = std::mem::take(&mut inner.errors);
            callback(final_results, errors);
        }
        true
    }

    fn decrement_internal(&self, inner: &mut MutexGuard<ResultsTrackerInner<T>>) -> bool {
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

// Define types that your system can handle
build_dtype_tokens!([
    IndexQuery,
    RangeQuery,
    MultiRangeQuery,
    MGetQuery,
    LabelNames,
    LabelValues,
    Cardinality,
    Stats,
]);

#[derive(DType)]
#[dtype(tokens_path = self, container = ResultsTracker)]
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
