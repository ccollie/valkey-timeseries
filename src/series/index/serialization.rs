use super::{TimeSeriesIndex, TIMESERIES_INDEX};
use std::sync::{LazyLock, Mutex};

pub(super) static STAGED_TIMESERIES_INDEX: LazyLock<
    Mutex<std::collections::HashMap<i32, TimeSeriesIndex>>,
> = LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));


pub fn series_on_async_load_done(completed: bool) {
    let staged = std::mem::take(&mut *STAGED_TIMESERIES_INDEX.lock().unwrap());
    if completed {
        let ts_index = TIMESERIES_INDEX.pin();
        // todo: it's much faster to do a swap, but LazyLock doesn't support it and using
        // a Mutex would be a performance hit.
        for (k, v) in staged.into_iter() {
            ts_index.insert(k, v);
        }
    }
}
