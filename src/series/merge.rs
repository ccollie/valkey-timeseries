use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::iterators::{SampleIter, SampleMergeIterator};
use crate::series::chunks::{Chunk, TimeSeriesChunk};
use crate::series::DuplicatePolicy;

#[allow(dead_code)]
pub fn merge_samples<'a, F, STATE>(
    left: SampleIter<'a>,
    right: SampleIter<'a>,
    dp_policy: Option<DuplicatePolicy>,
    state: &mut STATE,
    mut f: F,
) -> TsdbResult<()>
where
    F: FnMut(&mut STATE, Sample, bool) -> TsdbResult<()>,
{
    let dp_policy = dp_policy.unwrap_or(DuplicatePolicy::KeepLast);

    let mut merge_iterator = SampleMergeIterator::new(left, right, dp_policy);

    while let Some((sample, blocked)) = merge_iterator.next_internal() {
        f(state, sample, blocked)?;
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn merge_by_capacity(
    dest: &mut TimeSeriesChunk,
    src: &mut TimeSeriesChunk,
    min_timestamp: Timestamp,
    duplicate_policy: Option<DuplicatePolicy>,
) -> TsdbResult<Option<usize>> {
    if src.is_empty() {
        return Ok(None);
    }

    // check if previous block has capacity, and if so merge into it
    let count = src.len();
    let remaining_capacity = dest.estimate_remaining_sample_capacity();
    let first_ts = src.first_timestamp().max(min_timestamp);
    // if there is enough capacity in the previous block, merge the last block into it
    if remaining_capacity >= count {
        // copy all from last_chunk
        let iter = src.iter();
        let res = dest.merge_range(iter, duplicate_policy)?;
        // reuse last block
        src.clear();
        return Ok(Some(res));
    } else if remaining_capacity > count / 4 {
        // do a partial merge
        let samples = src.get_range(first_ts, src.last_timestamp())?;
        let (left, right) = samples.split_at(remaining_capacity);
        let res = dest.merge_samples(left, duplicate_policy)?;
        src.set_data(right)?;
        let count = res.iter().filter(|s| s.is_ok()).count();
        return Ok(Some(count));
    }
    Ok(None)
}
