use crate::common::hash::IntMap;
use crate::common::parallel::{Parallel, ParallelExt};
use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::error_consts;
use crate::series::chunks::{Chunk, TimeSeriesChunk};
use crate::series::index::get_series_key_by_id;
use crate::series::{find_last_ge_index, DuplicatePolicy, SampleAddResult, TimeSeries};
use smallvec::{smallvec, SmallVec};
use std::ops::DerefMut;
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};

/// Appending to a GorillaChunk is a streaming operation, but adding out-of-order samples is an O(n) operation.
/// For every insert of a sample with a timestamp prior to the last timestamp, the chunk data is rewritten to insert
/// the sample in the correct location.
///
/// To optimize this process, we group samples by the chunk they belong to, then merge them in a single write operation.
/// Represents a collection of samples for a single timeseries, grouped by chunk index.
struct GroupedSamples {
    chunk_index: usize,
    samples: SmallVec<Sample, 8>,
    indices: SmallVec<usize, 8>,
}

impl GroupedSamples {
    fn new(chunk_index: usize) -> Self {
        Self {
            chunk_index,
            samples: SmallVec::new(),
            indices: SmallVec::new(),
        }
    }

    /// Sorts samples by timestamp in ascending order, maintaining the relationship with their original indices.
    fn sort_by_timestamp(&mut self) {
        // Create a vector of (index, sample) pairs
        let mut pairs: SmallVec<(usize, Sample), 8> = self
            .indices
            .iter()
            .copied()
            .zip(self.samples.iter().copied())
            .collect();

        // Sort the pairs by sample timestamp
        pairs.sort_by_key(|(_idx, sample)| sample.timestamp);

        // Clear the existing SmallVecs
        self.indices.clear();
        self.samples.clear();

        // Push sorted elements back into the SmallVecs
        for (idx, sample) in pairs {
            self.indices.push(idx);
            self.samples.push(sample);
        }
    }

    fn add_sample(&mut self, sample: Sample, index: usize) {
        self.samples.push(sample);
        self.indices.push(index);
    }

    fn handle_merge(
        &self,
        chunk: &mut TimeSeriesChunk,
        group: &GroupedSamples,
        policy: DuplicatePolicy,
    ) -> SmallVec<(usize, SampleAddResult), 8> {
        // Merge samples into this chunk
        match chunk.merge_samples(&group.samples, Some(policy)) {
            Ok(chunk_results) => chunk_results
                .iter()
                .zip(group.indices.iter().cloned())
                .map(|(res, index)| (index, *res))
                .collect::<SmallVec<_, 8>>(),
            Err(_e) => {
                let err = SampleAddResult::Error(error_consts::CANNOT_ADD_SAMPLE);
                group
                    .indices
                    .iter()
                    .cloned()
                    .map(|index| (index, err))
                    .collect::<SmallVec<_, 8>>()
            }
        }
    }
}

pub struct IndexedSample {
    pub index: usize,
    pub timestamp: Timestamp,
    pub value: f64,
}

/// A collection of samples for a single timeseries, along with their original indices.
pub struct PerSeriesSamples<'a> {
    series: &'a mut TimeSeries,
    samples: SmallVec<IndexedSample, 6>,
}

impl<'a> PerSeriesSamples<'a> {
    /// Creates a new `PerSeriesSamples` instance.
    pub fn new(series: &'a mut TimeSeries) -> Self {
        Self {
            series,
            samples: SmallVec::new(),
        }
    }

    /// Adds a sample to the collection, along with its original index.
    pub fn add_sample(&mut self, sample: Sample, index: usize) {
        self.samples.push(IndexedSample {
            index,
            timestamp: sample.timestamp,
            value: sample.value,
        });
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn sort_by_timestamp(&mut self) {
        // Sort indexed samples by timestamp
        self.samples.sort_by_key(|sample| sample.timestamp);
    }

    pub fn sort_by_index(&mut self) {
        // Sort indexed samples by original index
        self.samples.sort_by_key(|sample| sample.index);
    }
}

#[derive(Clone)]
struct MergeWorker {
    chunk_results: SmallVec<(usize, SampleAddResult), 8>,
}

impl MergeWorker {
    fn new() -> Self {
        Self {
            chunk_results: Default::default(),
        }
    }
}

impl Parallel for MergeWorker {
    fn create(&self) -> Self {
        Self {
            chunk_results: Default::default(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.chunk_results.extend(other.chunk_results);
    }
}

/// Groups samples by the chunk they belong to, while filtering out old samples.
///
/// NOTE: `samples` **must** be sorted by timestamp before calling this function.
fn group_samples_by_chunk(
    series: &mut TimeSeries,
    samples: &[Sample],
    results: &mut [SampleAddResult],
    earliest_allowed_timestamp: Timestamp,
) -> TsdbResult<IntMap<usize, GroupedSamples>> {
    let mut chunk_groups: IntMap<usize, GroupedSamples> = IntMap::default();

    for (index, &sample) in samples.iter().enumerate() {
        if sample.timestamp < earliest_allowed_timestamp {
            results[index] = SampleAddResult::TooOld;
            continue;
        }

        let adjusted_sample = Sample {
            value: series.adjust_value(sample.value),
            timestamp: sample.timestamp,
        };

        let chunk_index = if series.is_empty() || sample.timestamp < series.first_timestamp {
            0
        } else {
            loop {
                let (index, _) = find_last_ge_index(&series.chunks, sample.timestamp);

                debug_assert!(index < series.chunks.len());
                let chunk = &mut series.chunks[index];

                if chunk.should_split() {
                    chunk.split()?;
                    continue;
                }

                break index;
            }
        };

        chunk_groups
            .entry(chunk_index)
            .or_insert_with(|| GroupedSamples::new(chunk_index))
            .add_sample(adjusted_sample, index);
    }

    for group in chunk_groups.values_mut() {
        group.sort_by_timestamp();
    }

    Ok(chunk_groups)
}

/// Merges a collection of samples into a time series.
///
/// This function efficiently groups samples by the chunks they would belong to
/// and applies the appropriate duplicate policy when merging. If samples are split across
/// multiple chunks, they are possibly processed in parallel to optimize performance.
///
///
/// ### Arguments
///
/// * `samples` - A slice of samples to merge into the time series
/// * `policy_override` - Optional override for the duplicate policy to use when merging
///
/// ### Returns
///
/// A result containing a vector of `SampleAddResult` with the outcome for each sample.
///
pub(super) fn merge_samples(
    series: &mut TimeSeries,
    samples: &[Sample],
    policy_override: Option<DuplicatePolicy>,
) -> TsdbResult<Vec<SampleAddResult>> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    let policy = series.sample_duplicates.resolve_policy(policy_override);
    let earliest_allowed_timestamp = if series.retention.is_zero() {
        0
    } else {
        series.get_min_timestamp()
    };

    let mut results = vec![SampleAddResult::Error("Unknown error"); samples.len()];

    // Ensure there's at least one chunk to work with
    if series.chunks.is_empty() {
        series.append_chunk();
    }

    // Group samples by chunk. Map is chunk_idx -> Vec<(original_index, sample)>
    let chunk_groups =
        group_samples_by_chunk(series, samples, &mut results, earliest_allowed_timestamp)?;
    let mut chunks = std::mem::take(&mut series.chunks);
    let mut worker = MergeWorker::new();
    worker.maybe_par_idx(2, chunks.deref_mut(), |worker, index, chunk| {
        let group = chunk_groups
            .get(&index)
            .expect("Chunk index should exist in chunk groups");

        let results = group.handle_merge(chunk, group, policy);
        worker.chunk_results.extend(results);
    });

    series.chunks = chunks;

    // Map results back to original indices
    for (orig_idx, result) in worker.chunk_results.into_iter() {
        results[orig_idx] = result;

        // Update metadata for successful additions
        if let SampleAddResult::Ok(sample) = result {
            // The first timestamp might need updating
            if sample.timestamp < series.first_timestamp || series.is_empty() {
                series.first_timestamp = sample.timestamp;
            }

            // Update sample count
            series.total_samples += 1;
        }
    }

    // Update last_sample
    series.update_last_sample();

    Ok(results)
}

struct SeriesWorker {
    chunk_results: SmallVec<(usize, SampleAddResult), 8>,
    err: Option<ValkeyError>,
}

impl Parallel for SeriesWorker {
    fn create(&self) -> Self {
        Self {
            chunk_results: Default::default(),
            err: None,
        }
    }

    fn merge(&mut self, other: Self) {
        self.chunk_results.extend(other.chunk_results);
    }
}

/// Merges samples across multiple series, supporting parallel processing when applicable.
///
/// ### Parameters
/// - `groups`: A slice of series with their related samples.
///
/// ### Returns
/// Returns a `ValkeyResult` containing a `SmallVec` of tuples (group index, SampleAddResult) on success.:
/// - The second element is the result of processing the samples for that series.
///
///
pub fn multi_series_merge_samples(
    groups: &mut [PerSeriesSamples],
    ctx: Option<&Context>,
) -> ValkeyResult<SmallVec<(usize, SampleAddResult), 8>> {
    let mut worker = SeriesWorker {
        chunk_results: SmallVec::new(),
        err: None,
    };

    let thread_ctx = ctx.map(|ctx| ThreadSafeContext::with_blocked_client(ctx.block_client()));

    worker.maybe_par(2, groups, |worker, input| {
        if worker.err.is_some() {
            return;
        }

        match add_samples_internal(input, &thread_ctx) {
            Ok(results) => {
                worker.chunk_results.extend(results);
            }
            Err(e) => {
                worker.err = Some(e);
            }
        }
    });

    let Some(err) = worker.err else {
        return Ok(worker.chunk_results);
    };

    Err(err)
}

fn add_samples_internal(
    input: &mut PerSeriesSamples,
    ctx: &Option<ThreadSafeContext<BlockedClient>>,
) -> ValkeyResult<SmallVec<(usize, SampleAddResult), 8>> {
    if input.samples.len() == 1 {
        let sample = input.samples.pop().unwrap();
        let index = sample.index;
        let result = input.series.add(sample.timestamp, sample.value, None);
        handle_compaction(ctx, input.series, &[result]);

        return Ok(smallvec![(index, result)]);
    }

    input.sort_by_timestamp();
    let samples: SmallVec<Sample, 8> = input
        .samples
        .iter()
        .map(|s| Sample::new(s.timestamp, s.value))
        .collect();

    let add_results = input
        .series
        .merge_samples(&samples, None)
        .map_err(|e| ValkeyError::String(format!("{e}")))?;

    // run compaction if needed
    handle_compaction(ctx, input.series, &add_results);

    let mut result: SmallVec<(usize, SampleAddResult), 8> = SmallVec::new();
    for item in add_results
        .iter()
        .zip(input.samples.iter())
        .map(|(res, original)| (original.index, *res))
    {
        result.push(item);
    }

    Ok(result)
}

fn handle_compaction(
    thread_ctx: &Option<ThreadSafeContext<BlockedClient>>,
    series: &mut TimeSeries,
    results: &[SampleAddResult],
) {
    if series.rules.is_empty() {
        return;
    }

    let Some(thread_ctx) = thread_ctx else {
        return;
    };

    let last_timestamp = series.last_sample.map(|last_sample| last_sample.timestamp);

    for sample in results {
        let SampleAddResult::Ok(sample) = sample else {
            continue;
        };
        let ctx = thread_ctx.lock();
        let result = if is_upsert(sample.timestamp, last_timestamp) {
            series.upsert_compaction(&ctx, *sample)
        } else {
            series.run_compaction(&ctx, *sample)
        };
        if let Err(e) = result {
            let key = get_series_key_by_id(&ctx, series.id)
                .unwrap_or_else(|| ctx.create_string("Unknown"));
            let msg = format!("TSDB: error running compaction upsert for key '{key}': {e}",);
            ctx.log_warning(&msg);
        }
    }
}

fn is_upsert(timestamp: Timestamp, last_timestamp: Option<Timestamp>) -> bool {
    if let Some(last_timestamp) = last_timestamp {
        return timestamp <= last_timestamp;
    }
    false
}
