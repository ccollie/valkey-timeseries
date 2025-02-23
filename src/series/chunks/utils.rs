use crate::common::binary_search::*;
use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::series::types::ValueFilter;
use ahash::AHasher;
use std::hash::{Hash, Hasher};

#[inline]
pub(crate) fn filter_samples_by_date_range(
    samples: &mut Vec<Sample>,
    start: Timestamp,
    end: Timestamp,
) {
    samples.retain(|s| s.timestamp >= start && s.timestamp <= end)
}

#[inline]
pub(crate) fn filter_samples_by_value(samples: &mut Vec<Sample>, value_filter: &ValueFilter) {
    samples.retain(|s| s.value >= value_filter.min && s.value <= value_filter.max)
}

/// Finds the start and end indices of timestamps within a specified range.
///
/// This function searches for the indices of timestamps that fall within the given
/// start and end timestamps (inclusive).
///
/// ## Parameters
///
/// * `timestamps`: A slice of i64 values representing timestamps, expected to be sorted.
/// * `start_ts`: The lower bound of the timestamp range to search for (inclusive).
/// * `end_ts`: The upper bound of the timestamp range to search for (inclusive).
///
/// ## Returns
///
/// Returns `Option<(usize, usize)>`:
/// * `Some((start_index, end_index))` if valid indices are found within the range.
/// * `None` if the input `timestamps` slice is empty.
///
/// The returned indices can be used to slice the original `timestamps` array
/// to get the subset of timestamps within the specified range.
pub(crate) fn get_timestamp_index_bounds(
    timestamps: &[i64],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> Option<(usize, usize)> {
    get_index_bounds(timestamps, &start_ts, &end_ts)
}

pub(crate) fn get_sample_index_bounds(
    samples: &[Sample],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> Option<(usize, usize)> {
    let start_sample = Sample {
        timestamp: start_ts,
        value: 0.0,
    };
    let end_sample = Sample {
        timestamp: end_ts,
        value: 0.0,
    };

    get_index_bounds(samples, &start_sample, &end_sample)
}

pub fn trim_to_range_inclusive(
    timestamps: &mut Vec<i64>,
    values: &mut Vec<f64>,
    start_ts: Timestamp,
    end_ts: Timestamp,
) {
    if let Some((start_idx, end_idx)) = get_timestamp_index_bounds(timestamps, start_ts, end_ts) {
        if start_idx == end_idx {
            // todo: get_unchecked
            let ts = timestamps[start_idx];
            let value = values[start_idx];
            timestamps.clear();
            values.clear();
            timestamps.push(ts);
            values.push(value);
        }
        if start_idx > 0 {
            timestamps.drain(..start_idx);
            values.drain(..start_idx);
        }
        let new_len = end_idx - start_idx + 1;
        timestamps.truncate(new_len);
        values.truncate(new_len);
    } else {
        timestamps.clear();
        values.clear();
    }
}

// Generate a unique key for a series based on its labels. Assumes that labels are sorted,
pub(crate) fn make_series_key(labels: &[Label]) -> String {
    let mut hasher = AHasher::default();
    let mut measurement: String = "".to_string();
    for Label { name, value } in labels {
        if name == METRIC_NAME_LABEL {
            measurement.push('{');
            measurement.push_str(value);
            measurement.push_str("}:");
            value.hash(&mut hasher);
        } else {
            name.hash(&mut hasher);
            hasher.write_u8(0xfe);
            value.hash(&mut hasher);
        }
    }
    let prefix = crate::config::KEY_PREFIX.as_str();
    format!("{prefix}:{measurement}{:x}", hasher.finish())
}
// todo: maybe x-vm:{alert_for_name}::name=joe::foo=bar::bar=baz

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_to_range_inclusive_all_before_start_ts() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 10;
        let end_ts = 20;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert!(timestamps.is_empty());
        assert!(values.is_empty());
    }

    #[test]
    fn trim_to_range_inclusive_within_range() {
        let mut timestamps = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let start_ts = 3;
        let end_ts = 8;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![3, 4, 5, 6, 7, 8]);
        assert_eq!(values, vec![3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);
    }

    #[test]
    fn trim_to_range_inclusive_all_within_range() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 0;
        let end_ts = 6;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![1, 2, 3, 4, 5]);
        assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[test]
    fn trim_to_range_inclusive_start_ts_equals_end_ts() {
        let mut timestamps = vec![1, 2, 3, 4, 5];
        let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let start_ts = 3;
        let end_ts = 3;

        trim_to_range_inclusive(&mut timestamps, &mut values, start_ts, end_ts);

        assert_eq!(timestamps, vec![3]);
        assert_eq!(values, vec![3.0]);
    }
}
