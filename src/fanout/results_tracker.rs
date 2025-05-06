use enum_dispatch::enum_dispatch;
use std::any::Any;
use std::collections::BinaryHeap;
use std::sync::{Mutex, MutexGuard};
use valkey_module::{ValkeyError, ValkeyResult};

pub type ResponseCallback<T> = Box<dyn FnOnce(ValkeyResult<Vec<T>>) + Send>;

#[enum_dispatch]
pub trait ResponseTracker<T>: Any {
    fn update(&self, result: T) -> bool;
    fn decrement(&self) -> bool;
    fn completed(&self) -> bool;
    fn raise_error(&self, error: &str);
}

struct ResultsTrackerInner<T> {
    results: Vec<T>,
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

    /// This is used only in the case of a node failure, so that the callback is still called
    /// when the last request is received from the remaining nodes.
    pub fn decrement(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        self.decrement_internal(&mut inner)
    }

    fn decrement_internal(&self, inner: &mut MutexGuard<ResultsTrackerInner<T>>) -> bool {
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        // If no outstanding request, execute the callback
        if inner.outstanding_requests == 0 {
            if let Some(callback) = inner.callback.take() {
                let final_results: Vec<T> = std::mem::take(&mut inner.results);
                callback(Ok(final_results));
                return true;
            }
        }
        false
    }

    pub fn completed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.outstanding_requests == 0
    }

    pub fn raise_error(&self, error: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        if let Some(callback) = inner.callback.take() {
            callback(Err(ValkeyError::String(error.to_string())));
        }
    }
}

impl<T: 'static> ResponseTracker<T> for ResultsTracker<T> {
    fn update(&self, result: T) -> bool {
        self.update(result)
    }

    fn decrement(&self) -> bool {
        self.decrement()
    }

    fn completed(&self) -> bool {
        self.completed()
    }

    fn raise_error(&self, error: &str) {
        self.raise_error(error)
    }
}

struct TopKResultsTrackerInner<T> {
    data: BinaryHeap<T>,
    callback: Option<ResponseCallback<T>>,
    outstanding_requests: u32,
}

pub struct TopKResultsTracker<T: PartialOrd> {
    inner: Mutex<TopKResultsTrackerInner<T>>,
    max_results: usize, // Maximum number of results to keep
}

impl<T: PartialOrd + Ord> TopKResultsTracker<T> {
    pub fn new(
        outstanding_requests: u32,
        max_results: usize,
        callback: ResponseCallback<T>,
    ) -> Self {
        let data = BinaryHeap::with_capacity(max_results);
        let inner = TopKResultsTrackerInner {
            data,
            callback: Some(callback),
            outstanding_requests,
        };
        Self {
            inner: Mutex::new(inner),
            max_results,
        }
    }

    pub fn add_internal(&self, result: T, data: &mut BinaryHeap<T>) {
        if data.len() < self.max_results {
            data.push(result);
        } else if let Some(top) = data.peek() {
            if result < *top {
                data.pop();
                data.push(result);
            }
        }
    }

    pub fn update(&self, result: T) -> bool {
        let mut inner = self.inner.lock().unwrap();
        self.add_internal(result, &mut inner.data);
        self.decrement_internal(&mut inner)
    }

    /// This is used only in the case of a node failure, so that the callback is still called
    /// when the last request is received from the remaining nodes.
    pub fn decrement(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        self.decrement_internal(&mut inner)
    }

    fn decrement_internal(&self, inner: &mut MutexGuard<TopKResultsTrackerInner<T>>) -> bool {
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        // If no outstanding request, execute the callback
        if inner.outstanding_requests == 0 {
            if let Some(callback) = inner.callback.take() {
                let final_results: Vec<T> = inner.data.drain().collect();
                callback(Ok(final_results));
                return true;
            }
        }
        false
    }

    pub fn raise_error(&self, error: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(callback) = inner.callback.take() {
            callback(Err(ValkeyError::String(error.to_string())));
        }
    }

    pub fn completed(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.outstanding_requests == 0
    }
}

impl<T: PartialOrd + 'static + Ord> ResponseTracker<T> for TopKResultsTracker<T> {
    fn update(&self, result: T) -> bool {
        TopKResultsTracker::update(self, result)
    }

    fn decrement(&self) -> bool {
        TopKResultsTracker::decrement(self)
    }

    fn completed(&self) -> bool {
        TopKResultsTracker::completed(self)
    }

    fn raise_error(&self, error: &str) {
        TopKResultsTracker::raise_error(self, error)
    }
}
