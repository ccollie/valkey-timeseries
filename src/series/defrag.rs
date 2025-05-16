use super::chunks::{merge_by_capacity, Chunk};
use super::time_series::TimeSeries;
use crate::error::TsdbResult;

pub fn defrag_series(series: &mut TimeSeries) -> TsdbResult {
    series.trim()?;

    if series.chunks.len() < 2 {
        return Ok(());
    }

    let min_timestamp = series.get_min_timestamp();
    let duplicate_policy = series.sample_duplicates.policy;
    let mut deleted_count = 0;

    let mut chunks_to_remove = Vec::new();
    let mut i = 0;

    let mut iter = series.chunks.iter_mut();
    // we ensure above that we have at least 2 chunks
    let mut prev_chunk = iter.next().unwrap();
    while let Some(mut chunk) = iter.next() {
        let count = chunk.len();
        let is_empty = count == 0;
        if is_empty {
            deleted_count += count;
            chunks_to_remove.push(i);
            i += 1;
            continue;
        }

        // while previous block has capacity merge into it
        while let Some(deleted) =
            merge_by_capacity(prev_chunk, chunk, min_timestamp, duplicate_policy)?
        {
            deleted_count -= deleted;
            if chunk.is_empty() {
                chunks_to_remove.push(i);
            }
            i += 1;
            if let Some(next_chunk) = iter.next() {
                chunk = next_chunk;
            } else {
                break;
            }
        }

        i += 1;
        prev_chunk = chunk;
    }

    // todo: don't delete last chunk if it's empty
    // chunks_to_remove.remove(series.chunks.len() - 1);

    for chunk in chunks_to_remove {
        series.chunks.remove(chunk);
    }

    series.total_samples -= deleted_count;

    Ok(())
}
