# Bulk Sample Ingest: Duplication Review & Refactoring Plan

**Date:** 2026-07-08
**Scope:** Code paths that add or merge samples in bulk — `src/series/bulk_add.rs`,
`src/series/sample_merge.rs`, `src/series/chunks/merge.rs`, and the per-encoding
`ChunkOps::merge_samples` implementations.

## Summary

There are two parallel series-level bulk-ingest pipelines that do nearly the same job
(`bulk_add.rs` backing `TS.ADDBULK`, `sample_merge.rs` backing `TS.MADD`), plus four
near-identical chunk-level `merge_samples` implementations across the chunk encodings.
The duplication has already caused behavioral drift, including a probable
`total_samples` over-count bug on the MADD path.

## 1. Series level: two pipelines doing the same thing

Both `bulk_insert_samples` (`bulk_add.rs`) and `merge_samples` (`sample_merge.rs`)
implement the same sequence:

1. Resolve duplicate policy against series/global defaults.
2. Filter retention (`TooOld`), in-batch duplicate timestamps (`Duplicate`), and the
   IGNORE filter (`Ignored`), applying value rounding.
3. Group surviving samples by destination chunk.
4. Merge into chunks in parallel.
5. Map per-chunk results back to original input indices.
6. Update series metadata (`total_samples`, first/last timestamps).
7. Trigger compaction propagation.

### Verbatim / near-verbatim copies

| What | bulk_add.rs | sample_merge.rs |
|---|---|---|
| Normalization loop (batch dups, TooOld, rounding, IGNORE w/ `running_last`) | `normalize_batch` (~L305) | front half of `group_samples_by_chunk` (~L153) |
| Retention floor (`retention.is_zero() ? 0 : get_min_timestamp()`) | `get_min_allowed_timestamp` (~L127) | inlined (~L240) |
| Error-fill on chunk merge failure (repeat `CANNOT_ADD_SAMPLE`) | `exec_merge` (~L104) | `GroupedSamples::handle_merge` (~L63) |
| Chunk grouping (different data structures, same intent) | `group_samples_by_chunk` → `Vec<ChunkSampleGroup>` (borrowed slices) | `group_samples_by_chunk` → `IntMap<usize, GroupedSamples>` (copied SmallVecs) |

### Drift the duplication already caused

1. **`total_samples` over-count (MADD path).** `sample_merge.rs` incremented
   `total_samples += 1` for every `SampleAddResult::Ok`, but chunk merges return `Ok`
   for an *upsert* of an existing timestamp too (duplicate policy resolved, not
   blocked). A `TS.MADD` with ≥2 samples for one key where one timestamp already
   exists mid-series inflated `total_samples`. `bulk_add.rs` avoids this via
   `recalculate_total_samples()`.
2. **Different parallel-mutation strategies.** `bulk_add` takes disjoint `&mut`
   borrows (`disjoint_get_many_mut_with_pos`); `sample_merge` did
   `std::mem::take` of *all* chunks + a `HashMap<first_timestamp, index>` lookup with
   an `expect(...)` panic surface, O(all chunks) per batch, relying on a fragile
   "first_timestamp is unique" invariant.
3. **Different compaction propagation.** `run_compactions` (bucket rebuild, parallel
   per rule) vs `handle_compaction` (per-sample `upsert_compaction`/`run_compaction`
   under a lock). Two engines computing the same downstream aggregates is a standing
   correctness risk — they can silently disagree.
4. **Redundant work in `bulk_add`.** `normalize_batch` already drops `TooOld`
   samples, yet `group_samples_by_chunk` re-computed the retention floor and
   re-scanned for old samples — dead logic on that call path.

## 2. Chunk level: four copies of the same merge template

`merge_samples` in `gorilla_chunk.rs`, `tsxor_chunk.rs`, `xor2_chunk.rs`,
`uncompressed/mod.rs`, and pco's `merge_internal` all follow the identical shape:

1. Fast path: `is_empty() || samples[0].timestamp > last_timestamp()` → append-all.
2. Build a set of input timestamps (one result per unique input timestamp).
3. Call the shared two-way merge helper with a callback that appends into a fresh
   encoder and pushes `Ok`/`Duplicate` based on `sample_set.remove(...)`.
4. Swap the rebuilt state into `self`.

Only the "append one sample" line differs per encoding. Drifted details:

- **Inconsistent default duplicate policy**: pco defaults to `Block`; the shared
  helper and all other encodings default to `KeepLast`.
- **Uncompressed single-sample path ignores `dp_policy`**: hardcodes `KeepLast` and
  unconditionally reports `Ok`, so `Block` does not block there.
- **Inconsistent set types**: `AHashSet` in three encodings, `IntSet` in xor2.
- **Copy-paste artifacts**: tsxor logs "error in gorilla chunk merge"; gorilla/tsxor
  share an identical nested `add_sample` closure.
- **Dead code in pco**: an unreachable re-check of
  `first.timestamp > last_timestamp()` after the same condition already returned.
- **A second private `merge_samples` free function** in `xor2_chunk.rs` re-implements
  `chunks/merge.rs::merge_samples` almost line-for-line, existing only because xor2's
  callback needs the iterator's `st` value.

## 3. Refactoring plan

- **Step 1 — extract shared normalization.** Move `normalize_batch` +
  `get_min_allowed_timestamp` to a shared location and have both pipelines consume
  a `NormalizedBatch`. Delete the redundant retention re-scan in
  `bulk_add::group_samples_by_chunk`. *(Done — see below.)*
- **Step 2 — one merge executor at the series level.** Route
  `sample_merge::merge_samples` through the `bulk_add` grouping/merge core
  (`ChunkSampleGroup`, disjoint `&mut` borrows, `exec_merge`), fixing the
  `total_samples` over-count and removing the `mem::take` + HashMap approach.
  What stays in `sample_merge.rs` is multi-series orchestration
  (`multi_series_merge_samples`) and the per-sample compaction strategy.
  *(Done — see below.)*
- **Step 3 — unify compaction propagation.** Decide whether incremental
  (`upsert_compaction`/`run_compaction`) or bucket-rebuild (`run_compactions`) is
  authoritative and route both entry points through it. *(Not yet done.)*
- **Step 4 — a chunk-merge template.** Add a generic helper in `chunks/merge.rs`
  that owns the timestamp-set bookkeeping and `Ok`/`Duplicate` result logic; each
  encoding implements only "append one sample" and "commit rebuilt state". Fold in
  drift fixes: one default policy, honor `dp_policy` on the uncompressed
  single-sample path, `IntSet` everywhere, correct log messages. *(Not yet done.)*

## 4. Changes applied on this branch (steps 1 & 2)

- `normalize_batch`, `NormalizedBatch`, and `get_min_allowed_timestamp` moved to a
  shared module (`src/series/ingest_normalize.rs`) used by both pipelines.
  `normalize_batch` now also tolerates unsorted input (stable index sort, first of
  two equal timestamps wins), preserving `TimeSeries::merge_samples`' documented
  tolerance for out-of-order batches.
- The bulk merge core was extracted into
  `bulk_add::merge_samples_into_series(series, samples, policy)`: normalize → group
  by chunk → parallel merge (disjoint `&mut` borrows for existing chunks, parallel
  creation for new chunks) → splice results to input order → refresh metadata.
  `bulk_insert_samples` (TS.ADDBULK) wraps it with chunk splitting, keyspace
  notification and compaction; `sample_merge::merge_samples` (TS.MADD) wraps it with
  chunk splitting only. The old `GroupedSamples`/`IntMap` pipeline, its
  `mem::take`-all-chunks parallel strategy and its `expect(...)` panic path were
  deleted.
- **Bug fixed — `total_samples` over-count (MADD):** totals are now recalculated
  from chunk lengths instead of incremented per `Ok` result, so upserts of existing
  timestamps no longer inflate the count.
- **Bug fixed — overlapping chunks from gap samples (ADDBULK):** grouping now
  routes each sample to the first chunk whose last timestamp is `>= ts` (samples in
  the gap before a chunk upsert into it), instead of bundling everything after a gap
  into a new chunk that could overlap existing ones.
- **Bug fixed — truncated range reads (`find_last_ge_index`):** when the end bound
  fell past a chunk's last timestamp, the linear-scan branch returned `idx - 1`,
  cutting the final chunk out of `get_chunk_index_bounds`. It now returns `idx`,
  consistent with its use as an inclusive end bound.
- **Behavior improved — appends reuse the last chunk:** samples newer than the last
  chunk are appended into it up to its estimated remaining capacity (via the
  previously dead `calculate_capacity`), with overflow going to new chunks. This
  keeps small `TS.MADD`/`TS.ADDBULK` append batches from creating a tiny new chunk
  per call, and preserves the MADD append behavior after unification.
- **Bug fixed — stale `last_sample` metadata:** series metadata is refreshed
  unconditionally after a bulk merge (previously only when new chunks were created),
  so an upsert that changes the value at the current last timestamp is reflected in
  `last_sample`.
- The redundant retention re-scan in `bulk_add::group_samples_by_chunk` was removed
  (normalization is the single retention filter for both pipelines).
