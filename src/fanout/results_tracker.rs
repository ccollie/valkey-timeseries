use enum_dispatch::enum_dispatch;
use std::any::Any;
use std::sync::{Mutex, MutexGuard};

pub type ResponseCallback<T> = Box<dyn FnOnce(Vec<T>, Vec<String>) + Send>;

#[enum_dispatch]
pub trait ResponseTracker<T>: Any {
    fn update(&self, result: T) -> bool;
    fn decrement(&self) -> bool;
    fn completed(&self) -> bool;
    fn raise_error(&self, error: &str);
}

struct ResultsTrackerInner<T> {
    results: Vec<T>,
    errors: Vec<String>,
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

    /// This is used only in the case of a node failure, so that the callback is still called
    /// when the last request is received from the remaining nodes.
    pub fn decrement(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        self.decrement_internal(&mut inner)
    }

    fn callback_if_needed(inner: &mut MutexGuard<ResultsTrackerInner<T>>) -> bool {
        if inner.outstanding_requests != 0 {
            return false;
        }
        if let Some(callback) = inner.callback.take() {
            let final_results: Vec<T> = std::mem::take(&mut inner.results);
            let errors: Vec<String> = std::mem::take(&mut inner.errors);
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

    pub fn raise_error(&self, error: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.errors.push(error.to_string());
        inner.outstanding_requests = inner.outstanding_requests.saturating_sub(1);
        Self::callback_if_needed(&mut inner);
    }

    pub fn call_done(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.outstanding_requests = 0;
        Self::callback_if_needed(&mut inner);
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
