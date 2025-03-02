use std::cmp::Ordering;

/// Find the index of the first element of `arr` that is greater
/// or equal to `val`.
/// Assumes that `arr` is sorted.
pub fn find_first_ge_index<T>(arr: &[T], val: &T) -> usize
where
    T: Ord,
{
    if arr.len() <= 16 {
        // If the vectors are small, perform a linear search.
        return arr.iter().position(|x| x >= val).unwrap_or(arr.len());
    }
    arr.binary_search(val).unwrap_or_else(|x| x)
}

/// Find the index of the first element of `arr` that is greater
/// than `val`.
/// Assumes that `arr` is sorted.
pub fn find_first_gt_index<T>(arr: &[T], val: T) -> usize
where
    T: Ord,
{
    match arr.binary_search(&val) {
        Ok(x) => x + 1,
        Err(x) => x,
    }
}

pub fn find_last_ge_index<T: Ord>(arr: &[T], val: &T) -> usize {
    if arr.len() <= 16 {
        return arr.iter().rposition(|x| val >= x).map_or(0, |idx| {
            if arr[idx] > *val {
                idx.saturating_sub(1)
            } else {
                idx
            }
        });
    }
    arr.binary_search(val)
        .unwrap_or_else(|x| x.saturating_sub(1))
}

/// Finds the start and end indices (inclusive) of a range within a sorted slice.
///
/// #### Parameters
///
/// * `values`: A slice of ordered elements to search within.
/// * `start`: The lower bound of the range to search for.
/// * `end`: The upper bound of the range to search for.
///
/// #### Returns
///
/// Returns `Option<(usize, usize)>`:
/// * `Some((start_idx, end_idx))` if valid indices are found within the range.
/// * `None` if the `values` slice is empty, if all samples are less than `start`,
///   or if `start` and `end` are equal and greater than the sample at the found index.
///
/// Used to get an inclusive bounds for the slice (all elements in slice[start_index...=end_index]
/// satisfy the condition x >= start &&  <= end).
pub(crate) fn get_index_bounds<T: Ord>(values: &[T], start: &T, end: &T) -> Option<(usize, usize)> {
    if values.is_empty() {
        return None;
    }

    let len = values.len();

    let start_idx = find_first_ge_index(values, start);
    if start_idx >= len {
        return None;
    }

    let right = &values[start_idx..];
    let idx = find_last_ge_index(right, end);
    let end_idx = start_idx + idx;

    // imagine this scenario:
    // samples = &[10, 20, 30, 40]
    // start = 25, end = 25
    // we have a situation where start_index == end_index (2), yet samples[2] is greater than end,
    if start_idx == end_idx {
        // todo: get_unchecked
        if values[start_idx] > *end {
            return None;
        }
    }

    Some((start_idx, end_idx))
}

// https://en.wikipedia.org/wiki/Exponential_search
// Use if you expect matches to be close by. Otherwise, use binary search.
pub trait ExponentialSearch<T> {
    fn exponential_search_by<F>(&self, f: F) -> Result<usize, usize>
    where
        F: FnMut(&T) -> Ordering;

    fn partition_point_exponential<P>(&self, mut pred: P) -> usize
    where
        P: FnMut(&T) -> bool,
    {
        self.exponential_search_by(|x| {
            if pred(x) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
        .unwrap_or_else(|i| i)
    }
}

impl<T: std::fmt::Debug> ExponentialSearch<T> for &[T] {
    fn exponential_search_by<F>(&self, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(&T) -> Ordering,
    {
        if self.is_empty() {
            return Err(0);
        }

        let mut bound = 1;

        while bound < self.len() {
            // SAFETY
            // Bound is always >=0 and < len.
            let cmp = f(unsafe { self.get_unchecked(bound) });

            if cmp == Ordering::Greater {
                break;
            }
            bound *= 2
        }
        let end_bound = std::cmp::min(self.len(), bound);
        // SAFETY:
        // We checked the end bound and previous bound was within slice as per the `while` condition.
        let prev_bound = bound / 2;

        let slice = unsafe { self.get_unchecked(prev_bound..end_bound) };

        match slice.binary_search_by(f) {
            Ok(i) => Ok(i + prev_bound),
            Err(i) => Err(i + prev_bound),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_partition_point() {
        let v = [1, 2, 3, 3, 5, 6, 7];
        let i = v.as_slice().partition_point_exponential(|&x| x < 5);
        assert_eq!(i, 4);
    }
}
