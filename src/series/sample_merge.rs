use crate::common::{Sample, Timestamp};
use crate::error::TsdbResult;
use crate::series::bulk_add::merge_samples_into_series;
use crate::series::index::get_series_key_by_id;
use crate::series::{DuplicatePolicy, SampleAddResult, TimeSeries};
use orx_parallel::ParIterResult;
use orx_parallel::{ParIter, ParallelizableCollectionMut};
use smallvec::{SmallVec, smallvec};
use valkey_module::{Context, ValkeyError, ValkeyResult};

pub struct IndexedSample {
    pub index: usize,
    pub timestamp: Timestamp,
    pub value: f64,
}

/// A collection of samples for a single timeseries, along with their original indices.
pub struct PerSeriesSamples<'a> {
    series: &'a mut TimeSeries,
    samples: SmallVec<[IndexedSample; 6]>,
    /// Samples accepted by the merge, ascending by timestamp; consumed by the post-merge
    /// compaction pass.
    added: SmallVec<[Sample; 8]>,
    /// The series' last timestamp before the merge: batch compaction uses it to tell
    /// guaranteed-fresh appends apart from samples that may have replaced existing values.
    prev_last: Option<Timestamp>,
}

impl<'a> PerSeriesSamples<'a> {
    /// Creates a new `PerSeriesSamples` instance.
    pub fn new(series: &'a mut TimeSeries) -> Self {
        Self {
            series,
            samples: SmallVec::new(),
            added: SmallVec::new(),
            prev_last: None,
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
/// The merge phase may run on worker threads, but compaction propagation runs afterwards,
/// sequentially on the calling (command) thread: it acquires destination-series guards and
/// must not lock the global context from inside the parallel section (the command thread
/// already holds the GIL, so a `ThreadSafeContext` lock there deadlocks the server).
///
/// ### Parameters
/// - `groups`: A slice of series with their related samples.
/// - `ctx`: Command context; when present, compaction rules are propagated after the merge.
///
/// ### Returns
/// Returns a `ValkeyResult` containing a `SmallVec` of tuples (group index, SampleAddResult) on success.:
/// - The second element is the result of processing the samples for that series.
///
///
pub fn multi_series_merge_samples(
    groups: Vec<PerSeriesSamples>,
    ctx: Option<&Context>,
) -> ValkeyResult<SmallVec<[(usize, SampleAddResult); 8]>> {
    if groups.is_empty() {
        return Ok(smallvec![]);
    }
    let mut groups = groups;

    let res = if groups.len() == 1 {
        add_samples_internal(&mut groups[0])?
    } else {
        groups
            .par_mut()
            .map(add_samples_internal)
            .into_fallible_result()
            .reduce(|mut acc, item| {
                acc.extend(item);
                acc
            })?
            .unwrap()
    };

    if let Some(ctx) = ctx {
        run_group_compactions(ctx, &mut groups);
    }

    Ok(res)
}

fn add_samples_internal(
    input: &mut PerSeriesSamples,
) -> ValkeyResult<SmallVec<[(usize, SampleAddResult); 8]>> {
    input.prev_last = input.series.last_sample.map(|s| s.timestamp);

    if input.samples.len() == 1 {
        let sample = input.samples.pop().unwrap();
        let index = sample.index;
        let result = input.series.add(sample.timestamp, sample.value, None);
        if let SampleAddResult::Ok(added) = result {
            input.added.push(added);
        }

        return Ok(smallvec![(index, result)]);
    }

    input.sort_by_timestamp();
    let samples: SmallVec<[Sample; 8]> = input
        .samples
        .iter()
        .map(|s| Sample::new(s.timestamp, s.value))
        .collect();

    let add_results = input
        .series
        .merge_samples(&samples, None)
        .map_err(|e| ValkeyError::String(format!("{e}")))?;

    // `add_results` follow the sorted input order, so the accepted samples are ascending —
    // exactly what batch compaction requires.
    input.added = add_results
        .iter()
        .filter_map(|res| {
            if let SampleAddResult::Ok(s) = res {
                Some(*s)
            } else {
                None
            }
        })
        .collect();

    let mut result: SmallVec<[(usize, SampleAddResult); 8]> = SmallVec::new();
    for item in add_results
        .iter()
        .zip(input.samples.iter())
        .map(|(res, original)| (original.index, *res))
    {
        result.push(item);
    }

    Ok(result)
}

/// Propagate each group's merged batch to its compaction destinations, one batch per series
/// (the old per-sample path locked the context once per sample and rebuilt the open bucket
/// for every in-order sample).
fn run_group_compactions(ctx: &Context, groups: &mut [PerSeriesSamples]) {
    for group in groups.iter_mut() {
        if group.series.rules.is_empty() || group.added.is_empty() {
            continue;
        }

        if let Err(e) = group
            .series
            .batch_compaction(ctx, &group.added, group.prev_last)
        {
            let key = get_series_key_by_id(ctx, group.series.id)
                .unwrap_or_else(|| ctx.create_string("Unknown"));
            let msg = format!("TSDB: error running compaction for key '{key}': {e}");
            ctx.log_warning(&msg);
        }
    }
}
