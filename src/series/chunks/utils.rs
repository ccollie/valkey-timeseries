use crate::common::binary_search::*;
use crate::common::{Sample, Timestamp};
use crate::series::types::ValueFilter;

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

#[cfg(test)]
mod tests {}
