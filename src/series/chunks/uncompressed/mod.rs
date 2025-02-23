use crate::common::{Sample, Timestamp, SAMPLE_SIZE};
use crate::error::{TsdbError, TsdbResult};
use crate::iterators::SampleIter;
use crate::series::chunks::merge::merge_samples;
use crate::series::chunks::utils::get_sample_index_bounds;
use crate::series::chunks::Chunk;
use crate::series::{DuplicatePolicy, SampleAddResult};
use ahash::AHashSet;
use core::mem::size_of;
use get_size::GetSize;

// todo: move to constants
pub const MAX_UNCOMPRESSED_SAMPLES: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct UncompressedChunk {
    pub max_size: usize,
    pub samples: Vec<Sample>,
    pub(crate) max_elements: usize,
}

impl Default for UncompressedChunk {
    fn default() -> Self {
        Self {
            samples: Vec::default(),
            max_size: MAX_UNCOMPRESSED_SAMPLES * SAMPLE_SIZE,
            max_elements: MAX_UNCOMPRESSED_SAMPLES,
        }
    }
}

impl GetSize for UncompressedChunk {
    fn get_size(&self) -> usize {
        size_of::<usize>() +  // self.max_size
        size_of::<usize>() +  // self.max_elements
        self.samples.capacity() * size_of::<Sample>() // todo: add capacity
    }
}

impl UncompressedChunk {
    pub fn new(size: usize, samples: &[Sample]) -> Self {
        let max_elements = size / SAMPLE_SIZE;
        Self {
            samples: samples.to_vec(),
            max_size: size,
            max_elements,
        }
    }

    pub fn with_max_size(size: usize) -> Self {
        Self {
            max_size: size,
            max_elements: size / SAMPLE_SIZE,
            ..Default::default()
        }
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.max_elements
    }

    pub fn clear(&mut self) {
        self.samples.clear();
    }

    pub fn set_data(&mut self, samples: &[Sample]) -> TsdbResult<()> {
        self.samples = samples.to_vec();
        // todo: complain if size > max_size
        Ok(())
    }

    fn handle_insert(&mut self, sample: Sample, policy: DuplicatePolicy) -> TsdbResult<()> {
        let ts = sample.timestamp;

        let (idx, found) = self.get_sample_index(ts);
        if found {
            // update value in case timestamp exists
            let current = self.samples.get_mut(idx).unwrap(); // todo: get_mut_unchecked
            current.value = policy.duplicate_value(ts, current.value, sample.value)?;
        } else if idx < self.samples.len() {
            self.samples.insert(idx, sample);
        } else {
            self.samples.push(sample);
        }
        Ok(())
    }

    pub fn bytes_per_sample(&self) -> usize {
        SAMPLE_SIZE
    }

    pub fn iter(&self) -> impl Iterator<Item = Sample> + '_ {
        self.samples.iter().cloned()
    }

    pub fn range_iter(&self, start_ts: Timestamp, end_ts: Timestamp) -> SampleIter {
        let slice = self.get_range_slice(start_ts, end_ts);
        SampleIter::vec(slice)
    }

    pub fn samples_by_timestamps(&self, timestamps: &[Timestamp]) -> TsdbResult<Vec<Sample>> {
        if self.len() == 0 || timestamps.is_empty() {
            return Ok(vec![]);
        }
        let first_timestamp = timestamps[0];
        if first_timestamp > self.last_timestamp() {
            return Ok(vec![]);
        }

        let mut samples = Vec::with_capacity(timestamps.len());
        for ts in timestamps.iter() {
            let (pos, found) = self.get_sample_index(*ts);
            if found {
                // todo: we know that the index in bounds, so use get_unchecked
                let sample = self.samples.get(pos).unwrap();
                samples.push(*sample);
            }
        }
        Ok(samples)
    }

    fn get_sample_index(&self, ts: Timestamp) -> (usize, bool) {
        get_sample_index(&self.samples, ts)
    }

    fn get_range_slice(&self, start_ts: Timestamp, end_ts: Timestamp) -> Vec<Sample> {
        if let Some((start_idx, end_index)) =
            get_sample_index_bounds(&self.samples, start_ts, end_ts)
        {
            self.samples[start_idx..=end_index].to_vec()
        } else {
            vec![]
        }
    }
}

fn get_sample_index(samples: &[Sample], ts: Timestamp) -> (usize, bool) {
    match samples.binary_search_by(|x| x.timestamp.cmp(&ts)) {
        Ok(pos) => (pos, true),
        Err(idx) => (idx, false),
    }
}

impl Chunk for UncompressedChunk {
    fn first_timestamp(&self) -> Timestamp {
        if self.samples.is_empty() {
            return 0;
        }
        self.samples[0].timestamp
    }

    fn last_timestamp(&self) -> Timestamp {
        if self.samples.is_empty() {
            return i64::MAX;
        }
        self.samples[self.samples.len() - 1].timestamp
    }

    fn len(&self) -> usize {
        self.samples.len()
    }

    fn last_value(&self) -> f64 {
        if self.samples.is_empty() {
            return f64::MAX;
        }
        self.samples[self.samples.len() - 1].value
    }

    fn size(&self) -> usize {
        self.samples.len() * size_of::<Sample>()
    }

    fn max_size(&self) -> usize {
        self.max_size
    }

    fn remove_range(&mut self, start_ts: Timestamp, end_ts: Timestamp) -> TsdbResult<usize> {
        let count = self.samples.len();
        self.samples
            .retain(|sample| -> bool { sample.timestamp < start_ts || sample.timestamp > end_ts });
        Ok(count - self.samples.len())
    }

    fn add_sample(&mut self, sample: &Sample) -> TsdbResult<()> {
        if self.is_full() {
            return Err(TsdbError::CapacityFull(MAX_UNCOMPRESSED_SAMPLES));
        }
        self.samples.push(*sample);
        Ok(())
    }

    fn get_range(&self, start: Timestamp, end: Timestamp) -> TsdbResult<Vec<Sample>> {
        let slice = self.get_range_slice(start, end);
        Ok(slice)
    }

    fn upsert_sample(&mut self, sample: Sample, dp_policy: DuplicatePolicy) -> TsdbResult<usize> {
        let ts = sample.timestamp;

        let count = self.samples.len();
        if self.is_empty() {
            self.samples.push(sample);
        } else {
            let last_sample = self.samples[count - 1];
            let last_ts = last_sample.timestamp;
            if ts > last_ts {
                self.add_sample(&sample)?;
            } else {
                self.handle_insert(sample, dp_policy)?;
            }
        }

        Ok(self.len() - count)
    }

    fn merge_samples(
        &mut self,
        samples: &[Sample],
        dp_policy: Option<DuplicatePolicy>,
    ) -> TsdbResult<Vec<SampleAddResult>> {
        let first = samples[0];

        if samples.len() == 1 {
            if self.is_empty() {
                self.add_sample(&first)?;
            } else {
                self.upsert_sample(first, DuplicatePolicy::KeepLast)?;
            }
            return Ok(vec![SampleAddResult::Ok(first.timestamp)]);
        }

        if self.is_empty() || first.timestamp > self.last_timestamp() {
            self.samples.extend_from_slice(samples);
            let result = samples
                .iter()
                .map(|sample| SampleAddResult::Ok(sample.timestamp))
                .collect();
            return Ok(result);
        }

        struct State {
            dest: Vec<Sample>,
            res: Vec<SampleAddResult>,
        }

        let mut state = State {
            dest: Vec::with_capacity(self.samples.len() + samples.len()),
            res: Vec::with_capacity(samples.len()),
        };

        // eliminate results not related to the new samples
        let mut sample_set: AHashSet<Timestamp> = AHashSet::with_capacity(samples.len());
        for sample in samples.iter() {
            sample_set.insert(sample.timestamp);
        }

        let left_iter = SampleIter::Slice(samples.iter());
        let right_iter = SampleIter::Slice(self.samples.iter());

        let max_len = self.max_elements;

        merge_samples(
            left_iter,
            right_iter,
            dp_policy,
            &mut state,
            |state, sample, duplicate| {
                let is_new = sample_set.remove(&sample.timestamp);
                if state.dest.len() > max_len {
                    return Err(TsdbError::CapacityFull(max_len));
                }
                state.dest.push(sample);
                if is_new {
                    if duplicate {
                        state.res.push(SampleAddResult::Duplicate);
                    } else {
                        state.res.push(SampleAddResult::Ok(sample.timestamp));
                    }
                }
                Ok(())
            },
        )?;

        self.samples = state.dest;
        Ok(state.res)
    }

    fn split(&mut self) -> TsdbResult<Self>
    where
        Self: Sized,
    {
        if self.samples.is_empty() {
            return Ok(self.clone());
        }

        if self.samples.len() == 1 {
            let mut result = self.clone();
            result.samples.clear();
            return Ok(result);
        }

        let half = self.samples.len() / 2;
        let samples = std::mem::take(&mut self.samples);
        let (left, right) = samples.split_at(half);
        self.samples = left.to_vec();

        Ok(Self {
            max_size: self.max_size,
            samples: right.to_vec(),
            max_elements: self.max_elements,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{Sample, SAMPLE_SIZE};
    use crate::series::chunks::{Chunk, UncompressedChunk};
    use crate::series::{DuplicatePolicy, SampleAddResult};

    #[test]
    fn test_remove_range() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Remove middle range
        let removed = chunk.remove_range(25, 45).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Remove range at the beginning
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(0, 15).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 3: Remove range at the end
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(45, 60).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
            ]
        );

        // Test 4: Remove entire range
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(0, 60).unwrap();
        assert_eq!(removed, 5);
        assert!(chunk.samples.is_empty());

        // Test 5: Remove range outside of samples
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(60, 70).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(chunk.samples, samples);

        // Test 6: Remove range with no overlap
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let removed = chunk.remove_range(31, 39).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(chunk.samples, samples);

        // Test 7: Remove range from empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let removed = empty_chunk.remove_range(10, 20).unwrap();
        assert_eq!(removed, 0);
        assert!(empty_chunk.samples.is_empty());
    }

    #[test]
    fn test_upsert_sample() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Upsert a new sample at the end
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 60,
                    value: 6.0,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 2: Upsert a new sample in the middle
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 40,
                    value: 4.0,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 3: Upsert an existing sample with KeepLast policy
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 30,
                    value: 3.5,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 0);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 4: Upsert an existing sample with KeepFirst policy
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 40,
                    value: 4.5,
                },
                DuplicatePolicy::KeepFirst,
            )
            .unwrap();
        assert_eq!(result, 0);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 5: Upsert a sample at the beginning
        let result = chunk
            .upsert_sample(
                Sample {
                    timestamp: 5,
                    value: 0.5,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 5,
                    value: 0.5
                },
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 6: Upsert a sample into an empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let result = empty_chunk
            .upsert_sample(
                Sample {
                    timestamp: 10,
                    value: 1.0,
                },
                DuplicatePolicy::KeepLast,
            )
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            empty_chunk.samples,
            vec![Sample {
                timestamp: 10,
                value: 1.0
            },]
        );

        // Test 7: Attempt to upsert when the chunk is full
        let mut full_chunk = UncompressedChunk::new(SAMPLE_SIZE * 2, &samples);
        let result = full_chunk.upsert_sample(
            Sample {
                timestamp: 70,
                value: 7.0,
            },
            DuplicatePolicy::KeepLast,
        );
        assert!(result.is_err());
        assert_eq!(full_chunk.samples, samples);
    }

    #[test]
    fn test_samples_by_timestamps() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Get existing timestamps
        let timestamps = vec![10, 30, 50];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Get non-existing timestamps
        let timestamps = vec![15, 25, 35];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 3: Mix of existing and non-existing timestamps
        let timestamps = vec![10, 15, 30, 35, 50];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(
            result,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 4: Empty timestamps
        let timestamps = vec![];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 5: All timestamps after the last sample
        let timestamps = vec![60, 70, 80];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 6: All timestamps before the first sample
        let timestamps = vec![1, 5, 9];
        let result = chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);

        // Test 7: Empty chunk
        let empty_chunk = UncompressedChunk::default();
        let timestamps = vec![10, 20, 30];
        let result = empty_chunk.samples_by_timestamps(&timestamps).unwrap();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_merge_samples() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Merge non-overlapping samples
        let new_samples = vec![
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];
        let result = chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(
            result,
            vec![SampleAddResult::Ok(20), SampleAddResult::Ok(40)]
        );
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Merge overlapping samples with KeepLast policy
        let new_samples = vec![
            Sample {
                timestamp: 30,
                value: 3.5,
            },
            Sample {
                timestamp: 60,
                value: 6.0,
            },
        ];
        let result = chunk
            .merge_samples(&new_samples, Some(DuplicatePolicy::KeepLast))
            .unwrap();
        assert_eq!(
            result,
            vec![SampleAddResult::Ok(30), SampleAddResult::Ok(60)]
        );
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.5
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
            ]
        );

        // Test 3: Merge samples into an empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let new_samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
        ];
        let result = empty_chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(
            result,
            vec![SampleAddResult::Ok(10), SampleAddResult::Ok(20)]
        );
        assert_eq!(empty_chunk.samples, new_samples);

        // Test 4: Merge samples with all timestamps greater than the last timestamp in the chunk
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_samples = vec![
            Sample {
                timestamp: 60,
                value: 6.0,
            },
            Sample {
                timestamp: 70,
                value: 7.0,
            },
        ];
        let result = chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(
            result,
            vec![SampleAddResult::Ok(60), SampleAddResult::Ok(70)]
        );
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
                Sample {
                    timestamp: 60,
                    value: 6.0
                },
                Sample {
                    timestamp: 70,
                    value: 7.0
                },
            ]
        );

        // Test 5: Merge a single sample
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_samples = vec![Sample {
            timestamp: 40,
            value: 4.0,
        }];
        let result = chunk.merge_samples(&new_samples, None).unwrap();
        assert_eq!(result, vec![SampleAddResult::Ok(40)]);
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );
    }

    #[test]
    fn test_split() {
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
            Sample {
                timestamp: 50,
                value: 5.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);

        // Test 1: Split a chunk with an odd number of samples
        let new_chunk = chunk.split().unwrap();
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
            ]
        );
        assert_eq!(
            new_chunk.samples,
            vec![
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
                Sample {
                    timestamp: 50,
                    value: 5.0
                },
            ]
        );

        // Test 2: Split a chunk with an even number of samples
        let samples = vec![
            Sample {
                timestamp: 10,
                value: 1.0,
            },
            Sample {
                timestamp: 20,
                value: 2.0,
            },
            Sample {
                timestamp: 30,
                value: 3.0,
            },
            Sample {
                timestamp: 40,
                value: 4.0,
            },
        ];
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_chunk = chunk.split().unwrap();
        assert_eq!(
            chunk.samples,
            vec![
                Sample {
                    timestamp: 10,
                    value: 1.0
                },
                Sample {
                    timestamp: 20,
                    value: 2.0
                },
            ]
        );
        assert_eq!(
            new_chunk.samples,
            vec![
                Sample {
                    timestamp: 30,
                    value: 3.0
                },
                Sample {
                    timestamp: 40,
                    value: 4.0
                },
            ]
        );

        // Test 3: Split a chunk with a single sample
        let samples = vec![Sample {
            timestamp: 10,
            value: 1.0,
        }];
        let mut chunk = UncompressedChunk::new(1000, &samples);
        let new_chunk = chunk.split().unwrap();
        assert_eq!(
            chunk.samples,
            vec![Sample {
                timestamp: 10,
                value: 1.0
            },]
        );
        assert!(new_chunk.samples.is_empty());

        // Test 4: Split an empty chunk
        let mut empty_chunk = UncompressedChunk::default();
        let new_chunk = empty_chunk.split().unwrap();
        assert!(empty_chunk.samples.is_empty());
        assert!(new_chunk.samples.is_empty());
    }
}
