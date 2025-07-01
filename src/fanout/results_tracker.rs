use super::{CardinalityCommand, IndexQueryCommand, LabelNamesCommand, LabelValuesCommand, MGetShardedCommand, MultiRangeCommand, MultiShardCommand, StatsCommand};
use crate::fanout::error::Error;
use enum_dispatch::enum_dispatch;
use std::sync::{Mutex, MutexGuard};

pub type ResponseCallback<REQ, RES> = Box<dyn FnOnce(REQ, Vec<RES>, Vec<Error>) + Send>;

#[enum_dispatch]
pub trait Tracker {
    fn raise_error(&self, error: Error);
    fn call_done(&self);
    fn is_completed(&self) -> bool;
}

struct ResultsTrackerInner<T: MultiShardCommand> {
    request: T::REQ,
    results: Vec<T::RES>,
    errors: Vec<Error>,
    callback: Option<ResponseCallback<T::REQ, T::RES>>,
    outstanding_requests: u32,
}

pub struct ResultsTracker<T: MultiShardCommand> {
    inner: Mutex<ResultsTrackerInner<T>>,
}

impl<T: MultiShardCommand> ResultsTracker<T> {
    pub fn new(
        outstanding_requests: usize,
        request: T::REQ,
        callback: ResponseCallback<T::REQ, T::RES>,
    ) -> Self {
        let inner = ResultsTrackerInner {
            results: Vec::new(),
            errors: Vec::new(),
            callback: Some(callback),
            request,
            outstanding_requests: outstanding_requests as u32,
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub fn add_result(&self, result: T::RES) {
        let mut inner = self.inner.lock().unwrap();
        inner.results.push(result)
    }

    pub fn update(&self, result: T::RES) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.results.push(result);
        self.decrement_internal(&mut inner)
    }

    fn callback_if_needed(inner: &mut MutexGuard<ResultsTrackerInner<T>>) -> bool {
        if inner.outstanding_requests != 0 {
            return false;
        }
        if let Some(callback) = inner.callback.take() {
            let final_results: Vec<T::RES> = std::mem::take(&mut inner.results);
            let errors: Vec<Error> = std::mem::take(&mut inner.errors);
            let request = std::mem::take(&mut inner.request);
            callback(request, final_results, errors);
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

impl<T: MultiShardCommand> Tracker for ResultsTracker<T> {
    fn raise_error(&self, error: Error) {
        self.raise_error(error);
    }

    fn call_done(&self) {
        self.call_done();
    }

    fn is_completed(&self) -> bool {
        self.completed()
    }
}

#[enum_dispatch(Tracker)]
pub enum TrackerEnum {
    IndexQuery(ResultsTracker<IndexQueryCommand>),
    MultiRangeQuery(ResultsTracker<MultiRangeCommand>),
    MGetQuery(ResultsTracker<MGetShardedCommand>),
    LabelNames(ResultsTracker<LabelNamesCommand>),
    LabelValues(ResultsTracker<LabelValuesCommand>),
    Cardinality(ResultsTracker<CardinalityCommand>),
    Stats(ResultsTracker<StatsCommand>),
}
