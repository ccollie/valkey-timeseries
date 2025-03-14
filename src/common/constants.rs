pub(crate) const VEC_BASE_SIZE: usize = 24;
pub const MAX_TIMESTAMP: i64 = 253402300799;
pub const METRIC_NAME_LABEL: &str = "__name__";

pub const REDUCER_KEY: &str = "__reducer__";
pub const SOURCE_KEY: &str = "__source__";

pub const MILLIS_PER_SEC: u64 = 1000;
pub const MILLIS_PER_MIN: u64 = 60 * MILLIS_PER_SEC;
pub const MILLIS_PER_HOUR: u64 = 60 * MILLIS_PER_MIN;
pub const MILLIS_PER_DAY: u64 = 24 * MILLIS_PER_HOUR;
pub const MILLIS_PER_WEEK: u64 = 7 * MILLIS_PER_DAY;
pub const MILLIS_PER_YEAR: u64 = 365 * MILLIS_PER_DAY;