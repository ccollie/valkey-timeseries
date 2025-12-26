use crate::common::{Sample, Timestamp};
use crate::iterators::SampleIter;
use crate::series::TimeSeries;
use crate::series::chunks::{Chunk, TimeSeriesChunk};
use smallvec::SmallVec;
use std::borrow::Cow;

/// Iterator over a provided list of timestamps. For each timestamp it
/// returns `Some(sample)` if an exact timestamp match exists in the series,
/// otherwise `None`. Timestamps are iterated in the order determined by `is_reverse`.
///
/// This iterator is optimized for sparse timestamp lookups over a time series.
/// It reuses chunk lookup state to avoid redundant searches across chunks.
pub struct TimestampFilterIterator<'a> {
    series: &'a TimeSeries,
    chunk: Option<&'a TimeSeriesChunk>,
    sample_iter: SampleIter<'a>,
    timestamps: SmallVec<Timestamp, 16>,
    is_reverse: bool,
    /// reusable sample buffer for reverse iteration
    buffer: Cow<'a, [Sample]>,
    /// index into timestamps vector for next output
    pos: usize,
    /// chunk search hint/current index
    chunk_idx: isize,
    random_access: bool,
}

impl<'a> TimestampFilterIterator<'a> {
    /// Create a new iterator.
    /// - `chunks` should be the series chunks in chronological order (earliest -> latest).
    /// - `timestamps` is the list of timestamps to look up.
    /// - `is_reverse` controls whether iteration goes from the end of `timestamps` to the start.
    pub fn new(series: &'a TimeSeries, timestamps: &[Timestamp], is_reverse: bool) -> Self {
        let timestamps: SmallVec<Timestamp, 16> = SmallVec::from(timestamps);
        // timestamps.reverse();

        let chunk_idx = if series.is_empty() {
            -1
        } else if is_reverse {
            (series.chunks.len() - 1) as isize
        } else {
            0
        };

        Self {
            series,
            chunk: None,
            sample_iter: Default::default(),
            timestamps,
            is_reverse,
            pos: 0,
            chunk_idx,
            buffer: Cow::Owned(Vec::new()),
            random_access: false,
        }
    }

    // fill the buffer with samples from the given chunk for reverse iteration
    fn fill_buffer(&mut self, chunk: &'a TimeSeriesChunk) {
        // determine the current range
        let current_ts = self.timestamps[self.pos];

        if let TimeSeriesChunk::Uncompressed(chunk_) = chunk {
            // Borrow the underlying samples
            self.buffer = Cow::Borrowed(&chunk_.samples);
        } else {
            // Create an iterator over the chunk's samples filtered by the calculated range
            let it = chunk.range_iter(chunk.first_timestamp(), current_ts);

            // compressed chunk: use owned buffer
            if let Cow::Owned(ref mut buf) = self.buffer {
                buf.clear();
                buf.extend(it);
            } else {
                // Transition Cow from Borrowed (from a previous uncompressed chunk) to Owned
                let mut buf = Vec::with_capacity(chunk.len());
                buf.extend(it);
                self.buffer = Cow::Owned(buf);
            }
        }
    }

    fn ensure_chunk_for_timestamp(&mut self, ts: Timestamp) -> Option<&'a TimeSeriesChunk> {
        if let Some(chunk) = self.chunk {
            if chunk.is_timestamp_in_range(ts) {
                return Some(chunk);
            }
        }

        let idx = self.find_chunk_idx_for_ts(ts)?;
        let chunk = &self.series.chunks[idx];

        if self.is_reverse || !chunk.is_compressed() {
            self.random_access = true;
            self.sample_iter = SampleIter::Empty;
            self.fill_buffer(chunk);
        } else {
            self.random_access = false;
            // When switching chunks, we need to refresh the sample iterator for this specific chunk
            self.sample_iter = chunk.range_iter(ts, chunk.last_timestamp());
        }

        self.chunk = Some(chunk);
        Some(chunk)
    }

    fn find_ts_sample(&mut self, ts: Timestamp) -> Option<Sample> {
        self.ensure_chunk_for_timestamp(ts)?;
        if self.random_access {
            if let Ok(idx) = self.buffer.binary_search_by_key(&ts, |s| s.timestamp) {
                // Found at idx (relative to the slice which starts at 0).
                let sample = unsafe {
                    // SAFETY: safe because binary_search_by_key returned a valid index
                    *self.buffer.get_unchecked(idx)
                };
                if idx == 0 && self.is_reverse {
                    // no more elements to the left — drop chunk
                    self.chunk = None;
                }
                return Some(sample);
            }

            return None;
        }

        for sample in self.sample_iter.by_ref() {
            if sample.timestamp == ts {
                return Some(sample);
            } else if sample.timestamp > ts {
                // The current sample is ahead of target; the current chunk might still contain future target ts.
                break;
            }
        }
        self.chunk = None;
        None
    }

    /// Try to locate a chunk index that might contain `ts` starting from `chunk_idx`.
    /// Adjusts `chunk_idx` forward/backward depending on `is_reverse` and returns
    /// the resolved chunk index or `None` if no chunk can contain `ts`.
    fn find_chunk_idx_for_ts(&mut self, ts: Timestamp) -> Option<usize> {
        let chunks = &self.series.chunks;
        if chunks.is_empty() {
            self.chunk_idx = -1;
            return None;
        }
        if self.is_reverse {
            let mut idx = self.chunk_idx;
            while idx >= 0 {
                let chunk = &chunks[idx as usize];
                if chunk.is_timestamp_in_range(ts) {
                    self.chunk_idx = idx;
                    return Some(idx as usize);
                } else if ts < chunk.first_timestamp() {
                    idx -= 1;
                } else {
                    break; // ts > chunk.last_timestamp()
                }
            }
        } else {
            let mut idx = self.chunk_idx as usize;
            while idx < chunks.len() {
                let chunk = &chunks[idx];
                if chunk.is_timestamp_in_range(ts) {
                    self.chunk_idx = idx as isize;
                    return Some(idx);
                } else if ts > chunk.last_timestamp() {
                    idx += 1;
                } else {
                    break; // ts < chunk.first_timestamp()
                }
            }
        }
        None
    }
}

impl<'a> Iterator for TimestampFilterIterator<'a> {
    type Item = Sample;

    fn next(&mut self) -> Option<Self::Item> {
        // current timestamp to resolve
        let len = self.timestamps.len();
        while self.pos < len {
            let ts = self.timestamps[self.pos];
            let sample = self.find_ts_sample(ts);
            self.pos += 1;
            if sample.is_some() {
                return sample;
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.timestamps.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Sample;
    use crate::series::TimeSeries;
    use crate::series::chunks::{GorillaChunk, TimeSeriesChunk, UncompressedChunk};

    // helper to create a sample
    fn s(ts: Timestamp, v: f64) -> Sample {
        Sample {
            timestamp: ts,
            value: v,
        }
    }

    // Build a series with two uncompressed chunks:
    // chunk1: timestamps 1..=3, chunk2: timestamps 5..=7
    fn build_series() -> TimeSeries {
        let c1_samples = vec![s(1, 10.0), s(2, 20.0), s(3, 30.0)];
        let c2_samples = vec![s(5, 50.0), s(6, 60.0), s(7, 70.0)];

        let c1 = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(c1_samples));
        let c2 = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(c2_samples));

        TimeSeries::from_chunks(vec![c1, c2]).unwrap()
    }

    #[test]
    fn finds_exact_matches_forward() {
        let series = build_series();
        // timestamps include matches and a missing entry (4)
        let timestamps = &[1, 4, 6];
        let it = TimestampFilterIterator::new(&series, timestamps, false);

        let out: Vec<Sample> = it.collect();
        // Expect samples for 1 and 6 only
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].timestamp, 1);
        assert_eq!(out[1].timestamp, 6);
    }

    #[test]
    fn finds_exact_matches_reverse() {
        let series = build_series();
        // iterate in reverse over timestamps: [7, 5, 2]
        let timestamps = &[7, 5, 2];
        let it = TimestampFilterIterator::new(&series, timestamps, true);

        let out: Vec<Sample> = it.collect();
        // Expect samples for 7,5,2 in that order
        let ts: Vec<Timestamp> = out.into_iter().map(|s| s.timestamp).collect();
        assert_eq!(ts, vec![7, 5, 2]);
    }

    #[test]
    fn missing_timestamps_are_skipped() {
        let series = build_series();
        let timestamps = &[0, 4, 8]; // none exist
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);
        assert!(it.next().is_none());
    }

    #[test]
    fn reuses_chunk_lookup_hint_across_timestamps() {
        let series = build_series();
        // timestamps mostly increasing, ensures iterator moves forward across chunks
        let timestamps = &[1, 2, 3, 5, 6, 7];
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);

        // consume partially to exercise chunk switching
        assert_eq!(it.next().map(|s| s.timestamp), Some(1));
        assert_eq!(it.next().map(|s| s.timestamp), Some(2));
        assert_eq!(it.next().map(|s| s.timestamp), Some(3));
        // chunk hint should advance to the second chunk and continue
        assert_eq!(it.next().map(|s| s.timestamp), Some(5));
        assert_eq!(it.next().map(|s| s.timestamp), Some(6));
        assert_eq!(it.next().map(|s| s.timestamp), Some(7));
        assert!(it.next().is_none());
    }

    // Existing helpers...

    // New helper to create a series with compressed chunks
    fn build_compressed_series() -> TimeSeries {
        let c1_samples = vec![s(1, 10.0), s(2, 20.0), s(3, 30.0)];
        let c2_samples = vec![s(5, 50.0), s(6, 60.0), s(7, 70.0)];

        // Compress chunks using Gorilla encoding
        let mut c1 = TimeSeriesChunk::Gorilla(GorillaChunk::default());
        let mut c2 = TimeSeriesChunk::Gorilla(GorillaChunk::default());

        c1.set_data(&c1_samples).unwrap();
        c2.set_data(&c2_samples).unwrap();

        TimeSeries::from_chunks(vec![c1, c2]).unwrap()
    }

    #[test]
    fn finds_exact_matches_forward_compressed() {
        let series = build_compressed_series();
        let timestamps = &[1, 4, 6];
        let it = TimestampFilterIterator::new(&series, timestamps, false);

        let out: Vec<Sample> = it.collect();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].timestamp, 1);
        assert_eq!(out[1].timestamp, 6);
    }

    #[test]
    fn finds_exact_matches_reverse_compressed() {
        let series = build_compressed_series();
        let timestamps = &[7, 5, 2];
        let it = TimestampFilterIterator::new(&series, timestamps, true);

        let out: Vec<Sample> = it.collect();
        let ts: Vec<Timestamp> = out.into_iter().map(|s| s.timestamp).collect();
        assert_eq!(ts, vec![7, 5, 2]);
    }

    #[test]
    fn missing_timestamps_are_skipped_compressed() {
        let series = build_compressed_series();
        let timestamps = &[0, 4, 8];
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);
        assert!(it.next().is_none());
    }

    #[test]
    fn reuses_chunk_lookup_hint_across_timestamps_compressed() {
        let series = build_compressed_series();
        let timestamps = &[1, 2, 3, 5, 6, 7];
        let mut it = TimestampFilterIterator::new(&series, timestamps, false);

        assert_eq!(it.next().map(|s| s.timestamp), Some(1));
        assert_eq!(it.next().map(|s| s.timestamp), Some(2));
        assert_eq!(it.next().map(|s| s.timestamp), Some(3));
        assert_eq!(it.next().map(|s| s.timestamp), Some(5));
        assert_eq!(it.next().map(|s| s.timestamp), Some(6));
        assert_eq!(it.next().map(|s| s.timestamp), Some(7));
        assert!(it.next().is_none());
    }
}
