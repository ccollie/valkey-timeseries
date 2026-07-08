use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::series::bulk_add::merge_samples_into_series;
use crate::series::index::get_series_key_by_id;
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries};
use orx_parallel::ParIterResult;
use orx_parallel::{ParIter, ParallelizableCollectionMut};
use smallvec::{SmallVec, smallvec};
use valkey_module::{BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult};

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
        self.samples.sort_unstable_by_key(|s| s.timestamp);
    }

    pub fn sort_by_index(&mut self) {
        // Sort indexed samples by the original index
        self.samples.sort_unstable_by_key(|s| s.index);
    }
}

/// Merges a collection of samples into a time series.
///
/// Delegates to the shared bulk-merge core ([`merge_samples_into_series`]) used by
/// `TS.ADDBULK`, so normalization (retention/rounding/IGNORE), chunk grouping and
/// parallel merging behave identically on both ingest paths.
///
/// ## Note
///
/// `samples` **must** be sorted by timestamp.
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

    let results = merge_samples_into_series(series, samples, policy_override);
    series.split_chunks_if_needed()?;
    Ok(results)
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
    groups: Vec<PerSeriesSamples>,
    ctx: Option<&Context>,
) -> ValkeyResult<SmallVec<(usize, SampleAddResult), 8>> {
    if groups.is_empty() {
        return Ok(smallvec![]);
    }
    let thread_ctx = ctx.map(|ctx| ThreadSafeContext::with_blocked_client(ctx.block_client()));
    let mut groups = groups;

    if groups.len() == 1 {
        return add_samples_internal(&mut groups[0], &thread_ctx);
    }

    let res = groups
        .par_mut()
        .map(|group| add_samples_internal(group, &thread_ctx))
        .into_fallible_result()
        .reduce(|mut acc, item| {
            acc.extend(item);
            acc
        })?
        .unwrap();

    Ok(res)
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
