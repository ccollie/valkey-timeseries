# Multi-Aggregation Support for the RANGE Command Family — Implementation Plan

**Status:** Implemented (2026-07-06)
**Scope:** `TS.RANGE`, `TS.REVRANGE`, `TS.MRANGE`, `TS.MREVRANGE` (standalone and cluster)
**Out of scope:** `TS.JOIN` (guarded with a validation error), `TS.CREATERULE` compaction rules, analysis commands.

---

## 1. Overview

The `AGGREGATION` parameter currently accepts a single aggregator. This change extends it to
accept a comma-separated list:

```
TS.RANGE key fromTimestamp toTimestamp
  [LATEST]
  [FILTER_BY_TS ts...]
  [FILTER_BY_VALUE min max]
  [COUNT count]
  [[ALIGN align] AGGREGATION aggregator[,aggregator...] bucketDuration
      [CONDITION op value] [BUCKETTIMESTAMP bt] [EMPTY]]
```

All aggregators in one clause share the bucket parameters (`bucketDuration`, `ALIGN`,
`BUCKETTIMESTAMP`, `EMPTY`). Each bucket produces one output row containing the bucket
timestamp followed by one value per aggregator, **in the order specified**:

```
TS.RANGE temp:tlv - + AGGREGATION avg,max,count 60000
1) 1) (integer) 1652419200000
   2) (double) 22.4        # avg
   3) (double) 31.0        # max
   4) (double) 12          # count
2) ...
```

With a single aggregator the output shape is unchanged (`[timestamp, value]`), so existing
clients are fully backward compatible — a one-element list is syntactically identical to
today's argument.

### Approved design decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | `GROUPBY/REDUCE` × multi-aggregation | **Column-wise reduce.** Each aggregation column is reduced independently across the series of a group; output rows keep the `[ts, agg1, agg2, ...]` shape. |
| 2 | `CONDITION` × multi-aggregation | **Distribute to capable aggregators.** A single `CONDITION` attaches to every aggregator in the list that requires or optionally accepts one (`countif`, `sumif`, `all`, `any`, `none`, `share`, `count`, `sum`); plain aggregators ignore it. Error if no aggregator in the list can use it, or if a condition-requiring aggregator appears without `CONDITION`. |
| 3 | Duplicates / cap | **Reject duplicate aggregation types** (`TSDB: duplicate aggregation '<name>'`). Hard cap of **16** aggregators per clause (`TSDB: too many aggregations (max 16)`). |
| 4 | Scope | **RANGE family only.** `TS.JOIN` rejects lists with `TSDB: multiple aggregations are not supported for TS.JOIN`. |

---

## 2. Current Architecture (baseline)

Understanding what exists, since the plan is deliberately additive:

- **Options model** — `RangeOptions.aggregation: Option<AggregationOptions>` where
  `AggregationOptions` (`src/series/request_types.rs`) holds a single
  `AggregatorConfig { aggregation: AggregationType, value_filter: Option<ValueComparisonFilter> }`
  plus shared bucket parameters. `AggregationOptions` currently derives `Copy`.
- **Parsing** — `parse_aggregation_options()` (`src/commands/command_parser.rs:531`) reads one
  aggregator token, the bucket duration, then optional `CONDITION` / `ALIGN` / `EMPTY` /
  `BUCKETTIMESTAMP`. Shared by RANGE family and `TS.JOIN` (`parse_join_args`).
- **Engine** — `AggregationHelper` inside `src/aggregators/aggregate_iterator.rs` owns **one**
  `Aggregator` (an enum of 23 stateful implementations, `src/aggregators/handlers.rs`) and the
  bucket bookkeeping (start/end, alignment, EMPTY back-fill). `AggregateIterator` wraps it as an
  `Iterator<Item = Sample>`.
- **Pipeline** — everything downstream of the chunk readers is
  `Box<dyn Iterator<Item = Sample>>`. `create_range_iterator()` /
  `create_sample_iterator_adapter()` (`src/iterators/utils.rs`) compose:
  base reader → LATEST chaining → ts/value filters → `AggregateIterator` →
  `ReduceIterator` (GROUPBY) → reverse → `take(count)`.
- **Replies** — `reply_with_samples()` (`src/common/replies/raw_replies.rs:106`) emits 2-element
  `[ts, value]` arrays. `reply_with_mrange_series_result()` (`src/commands/utils.rs:36`) emits
  `[key, labels, samples]` per series, reading samples from `MRangeSeriesResult.data:
  TimeSeriesChunk`.
- **MRANGE local** — `process_mrange_query()` (`src/series/mrange.rs`) resolves series by
  selector, then processes them **in parallel** with `orx_parallel` (`into_par()` per series,
  `iter_into_par()` per group). Grouped queries: per-series aggregation → `MultiSeriesSampleIter`
  (k-way merge by timestamp) → `SampleReducer` (cross-series reduce per timestamp).
- **MRANGE cluster** — `MRangeFanoutCommand` (`src/commands/ts_mrange_fanout_command.rs`).
  Shards strip `COUNT`, `AGGREGATION`, and reversal in `get_local_response()` and return **raw
  filtered samples** as compressed chunks (`SeriesRangeResponse.samples`). The coordinator
  applies aggregation/grouping/count/reverse in `reply()`. Aggregation is therefore a
  **coordinator-side** concern; the request proto carries it but shards ignore it.
- **Protobuf** — `src/commands/fanout.request.proto` / `fanout.response.proto`, compiled with
  `prost-build`. `AggregationOptions.aggregator` is a single `AggregatorConfig`.

Key consequence: because fanout aggregation happens at the coordinator, **response protos need
no changes** — raw samples flow exactly as today. Only the request-side `AggregationOptions`
message and the coordinator's post-processing change.

---

## 3. Data Model Changes

### 3.1 `AggregationOptions` (src/series/request_types.rs)

```rust
#[derive(Debug, Clone, PartialEq)]              // Copy is dropped
pub struct AggregationOptions {
    /// 1..=MAX_AGGREGATIONS entries; index = output column order.
    pub aggregations: SmallVec<AggregatorConfig, 2>,
    pub bucket_duration: u64,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub report_empty: bool,
}

pub const MAX_AGGREGATIONS: usize = 16;

impl AggregationOptions {
    pub fn primary(&self) -> &AggregatorConfig;      // aggregations[0]
    pub fn is_multi(&self) -> bool;                  // len() > 1
    pub fn create_aggregators(&self) -> SmallVec<Aggregator, 2>;
}
```

Dropping `Copy` ripples through ~a dozen call sites (`options.aggregation` copies in
`src/iterators/utils.rs:79`, conversions, tests) — all become `.clone()`. `AggregatorConfig`
itself remains `Copy`, so cloning the SmallVec is a cheap memcpy for inline sizes.

Rationale for one struct with a repeated config (rather than `Vec<AggregationOptions>`): the
grammar shares bucket parameters across the list, and the engine shares bucket boundaries —
one bucketing pass, N accumulators.

### 3.2 Row type (src/common or src/iterators)

```rust
/// One output bucket of a multi-aggregation query: values[i] corresponds to
/// AggregationOptions.aggregations[i].
#[derive(Debug, Clone, PartialEq)]
pub struct MultiSample {
    pub timestamp: Timestamp,
    pub values: SmallVec<f64, 4>,
}
```

Inline capacity 4 keeps rows heap-free for the common 2–4 aggregator case.

### 3.3 `MRangeSeriesResult.data`

`TimeSeriesChunk` can only store `(ts, f64)` pairs, so multi-aggregation results need a second
representation:

```rust
pub(crate) enum SeriesResultData {
    Chunk(TimeSeriesChunk),      // raw / single-aggregation results (today's path, unchanged)
    Rows(Vec<MultiSample>),      // multi-aggregation output
}
```

`MRangeSeriesResult.data` becomes `SeriesResultData` with `Default = Chunk(default)`. The
fanout serialization (`SeriesRangeResponse` ⇄ `MRangeSeriesResult` in
`src/commands/fanout/chunks.rs`) only ever carries raw samples, so it converts to/from the
`Chunk` variant exclusively and `Rows` is rejected there with an internal error (it can never
occur on the wire).

---

## 4. Parser Changes (src/commands/command_parser.rs)

`parse_aggregation_options()` — replace the single-token read:

1. Read the aggregator token, split on `','` (no surrounding whitespace allowed — Valkey
   tokenization already forbids spaces inside one argument).
2. For each element: `AggregationType::try_from(part)?`; empty elements (leading/trailing/double
   commas) → `TSDB: invalid aggregation list`.
3. Enforce `MAX_AGGREGATIONS` and duplicate rejection (types compared after parsing, so
   `AVG,avg` is a duplicate).
4. Parse bucket duration and optional clauses exactly as today.
5. `CONDITION` distribution: for each type in the list build `AggregatorConfig::new(ty, cond)`
   where `cond` is passed only when `ty.is_filtered() || ty.has_filtered_variant()`. Validation:
   - `CONDITION` given but no element is condition-capable → reuse
     `TSDB: aggregation type does not support a filter condition`.
   - Element with `is_filtered()` (countif, sumif, all, any, none, share) but no `CONDITION` →
     existing `TSDB: missing condition for aggregator` (falls out of `AggregatorConfig::new`).

New error constants in `src/error_consts.rs`:
`DUPLICATE_AGGREGATION`, `TOO_MANY_AGGREGATIONS`, `INVALID_AGGREGATION_LIST`,
`MULTI_AGGREGATION_UNSUPPORTED` (for TS.JOIN).

**TS.JOIN guard** — in `parse_join_args` (`Aggregation` arm) and any other non-RANGE caller of
`parse_aggregation_options`, add `if options.is_multi() { return Err(MULTI_AGGREGATION_UNSUPPORTED) }`.
Alternative considered and rejected: a `allow_multi: bool` parameter on
`parse_aggregation_options` — the post-check is simpler and keeps the signature stable.

Existing validation (ALIGN vs `-`/`+` endpoints, ts-filter pruning) is untouched.

---

## 5. Aggregation Engine (src/aggregators/aggregate_iterator.rs)

Generalize `AggregationHelper` in place — one code path for N ≥ 1, no forked logic to drift:

```rust
struct AggregationHelper {
    aggregators: SmallVec<Aggregator, 2>,   // was: aggregator: Aggregator
    // bucket bookkeeping unchanged: duration, ts output, range, alignment, count, ...
}
```

- `new(options, aligned_ts)` builds every aggregator via `create_aggregators()`; the existing
  `Rate::set_window_ms(bucket_duration)` fix-up is applied to each `Rate` instance.
- `update(sample)` feeds all aggregators; `has_samples` becomes true if **any** accepted the
  value (NaN handling stays per-aggregator, matching today's semantics per column).
- `complete_bucket(...)` finalizes each aggregator into a `MultiSample` row; `report_empty`
  back-fill uses each aggregator's own `empty_bucket_value()` per column (sum → 0, min → NaN,
  etc. — per-column semantics identical to running each aggregator alone).
- `AggregateIterator` internals switch to rows. Two public iterator types are exposed:

```rust
/// N >= 1 aggregators, yields rows.
pub struct MultiAggregateIterator<T: Iterator<Item = Sample>> { ... }
impl Iterator for MultiAggregateIterator<..> { type Item = MultiSample; }

/// Existing type, now a thin wrapper over the row iterator for the
/// single-aggregator case (used by JOIN, grouped paths, analysis, tests).
pub struct AggregateIterator<T> { inner: MultiAggregateIterator<T> }
impl Iterator for AggregateIterator<..> { type Item = Sample; }   // row -> Sample, debug-assert len == 1
```

**Correctness invariant** (the cornerstone test, §10): for every aggregation type and option
combination, column *i* of `MultiAggregateIterator` over `[a1, ..., aN]` is byte-identical to
running today's single-aggregator iterator with `ai` alone. This holds structurally because
bucket boundaries/EMPTY logic depend only on shared options, never on aggregator state.

**REVRANGE**: aggregation still consumes ascending input; reversal happens post-aggregation.
`AggregationType::apply_reverse_adjusted()` (First↔Last swap) is applied **per list element**
when building aggregators for a reverse query. A `ReverseRowIter` (clone of
`ReverseSampleIter`, `src/iterators/utils.rs:173`) buffers and reverses rows.

Cost analysis: single pass over samples, N accumulator updates per sample. For N=1 the only
overhead vs. today is iterating a 1-element SmallVec — negligible next to chunk decode. This is
why no separate single-agg engine is kept.

---

## 6. Iterator Pipeline & TS.RANGE / TS.REVRANGE

The raw-sample domain (chunk readers, LATEST chaining, FILTER_BY_TS / FILTER_BY_VALUE) is
untouched — filters apply **pre-aggregation** exactly as today.

Refactor `create_range_iterator()` (`src/iterators/utils.rs`) to expose the pre-aggregation
composition:

```rust
/// base reader (+ ts-filter variant) + LATEST chaining + ts/value filters.
fn create_filtered_sample_iterator<'a>(series, options, latest, should_reverse_iter)
    -> impl Iterator<Item = Sample> + 'a;
```

Then two adapters consume it:

- `create_sample_iterator_adapter(...)` — unchanged behavior for: no aggregation, or single
  aggregation (via the `AggregateIterator` wrapper). **All existing call sites and tests keep
  working unmodified.**
- `create_row_iterator_adapter(...) -> Box<dyn Iterator<Item = MultiSample> + '_>` — new; used
  when `options.aggregation.as_ref().is_some_and(|a| a.is_multi())`:
  filtered samples → `MultiAggregateIterator` → optional `ReverseRowIter` → `take(count)`.
  (`COUNT` limits output rows/buckets — same semantics as today, one row per bucket.)

`TimeSeriesRangeIterator` (`src/iterators/timeseries_range_iterator.rs`) gains a sibling
`TimeSeriesRangeRowIterator` reusing `get_latest_sample()` / `calculate_size_hint()` (both are
aggregation-agnostic; extract them as free functions or keep them on a shared helper).

`ts_range.rs` / `range_internal()` branches once:

```rust
let series = get_timeseries(...)?;
match options.aggregation.as_ref().filter(|a| a.is_multi()) {
    Some(_) => {
        let iter = TimeSeriesRangeRowIterator::new(Some(ctx), &series, &options, is_reverse);
        reply_with_multi_samples(ctx, iter);
    }
    None => { /* existing path, unchanged */ }
}
```

### Reply writers (src/common/replies/raw_replies.rs)

```rust
pub fn reply_with_multi_sample<C: IntoRawCtx>(ctx: C, row: &MultiSample) {
    reply_with_array(ctx, 1 + row.values.len());
    reply_with_integer(ctx, row.timestamp);
    for v in &row.values { raw::reply_with_double(ctx, *v); }
}

pub fn reply_with_multi_samples<C: IntoRawCtx>(ctx: C, rows: impl Iterator<Item = MultiSample>);
// postponed-array pattern, mirrors reply_with_samples()
```

RESP2/RESP3 both render these as arrays of (integer, double...) — no protocol-specific work.

---

## 7. TS.MRANGE / TS.MREVRANGE — Local (non-clustered) Path

`src/series/mrange.rs`:

- **Non-grouped** (`handle_non_grouped`): inside the existing `into_par().map(...)` closure,
  branch on `is_multi()`: build the row pipeline per series and store
  `SeriesResultData::Rows(rows)`. Parallelism across series is inherited from the existing
  `orx_parallel` structure — the multi-aggregation work runs inside each parallel task; **no new
  threading is introduced** (see §9).
- **Grouped** (`handle_grouping` / `get_grouped_samples`): per approved decision #1
  (column-wise reduce):
  1. Per series: row pipeline → `Iterator<Item = MultiSample>` (per-series bucket aggregation).
  2. New `MultiSeriesRowIter`: k-way merge of row iterators by timestamp (structural clone of
     `MultiSeriesSampleIter`, `src/iterators/multi_series_sample_iter.rs`).
  3. New `RowReducer`: for each timestamp group, holds `N` clones of the REDUCE aggregator
     (one per column) and reduces column-wise; NaN inputs are skipped per column exactly as
     `SampleReducer` does today (`src/iterators/sample_reducer.rs`); an all-NaN column yields
     NaN. A series that produced no row for a timestamp simply contributes nothing to any
     column at that timestamp.
  4. Reverse + `COUNT` applied to rows (`collect_samples` gains a row twin `collect_rows`).
- Group labels are unchanged: `__reducer__` still names the single REDUCE aggregator (it is not
  per-column), `sources`/group label logic untouched (`build_mrange_grouped_labels`).
- `sort_mrange_results` and `convert_labels` are data-agnostic — unchanged.

`reply_with_mrange_series_result` (`src/commands/utils.rs`) branches on `SeriesResultData`:
`Chunk` → `reply_with_samples`, `Rows` → `reply_with_multi_samples`.

---

## 8. Cluster Support

### 8.1 Protobuf changes (src/commands/fanout.request.proto)

Evolve `AggregationOptions` — the container messages (`RangeRequest`, `MultiRangeRequest`)
stay untouched:

```proto
message AggregationOptions {
  // Aggregator list in output column order; must contain at least one entry.
  // A single-aggregator query is simply a one-element list.
  repeated AggregatorConfig aggregators = 1;
  uint32 bucket_duration = 2;
  int64 alignment_timestamp = 3;
  BucketTimestampType bucket_timestamp_type = 4;
  BucketAlignmentType bucket_alignment = 5;
  bool report_empty = 6;
}
```

> The project is unreleased, so no legacy single-aggregator field is kept; the
> single-aggregator case is just a one-element list.

No response-proto changes: shards return raw samples (`SeriesRangeResponse.samples`) and the
coordinator aggregates, exactly as today.

### 8.2 Conversions (src/commands/fanout/conversions.rs)

- `From<AggregationOptions> for FanoutAggregationOptions` (now `From<&...>` since `Copy` is
  gone): set `aggregators = all entries`.
- `TryFrom<FanoutAggregationOptions> for AggregationOptions`: convert each entry, re-validating
  non-empty, count ≤ MAX and duplicates — defense against malicious/corrupt peers; an empty list
  errors with `"TSDB: aggregation config is required"`.

### 8.3 Mixed-version cluster analysis

Not applicable: the project is unreleased, so no wire-compatibility with older versions is
maintained. `aggregators` is the only representation on the wire.

### 8.3.1 Interaction with aggregation push-down (implementation note)

**Updated 2026-07-06:** multi-aggregation now participates in shard-side push-down. The response
transport was generalized so bucket rows can cross the wire: `SeriesRangeResponse.samples`
(a single chunk) became `repeated SampleData columns`, one chunk of `(bucket_ts, value_i)` per
aggregation column. Raw samples and single-aggregation buckets are a one-element list; a
multi-aggregation series is a list of N ≥ 2 columns (all columns share identical timestamps and
length by construction); an empty multi-aggregation series is zero columns. `chunks.rs` transposes
`SeriesResultData::Rows` into columns on the way out and zips them back into rows on the way in
(validating equal lengths/timestamps as corrupt-peer defense).

Consequences for the three push-down flags (`MRangeFanoutCommand::new`):

- **Per-series `AGGREGATION` push-down** (`pushdown`) — **enabled for multi.** Shards run the
  per-series row pipeline (`create_row_iterator` → `MultiAggregateIterator`) and ship bucket rows
  as columns. The coordinator detects already-bucketed rows by the arrived `SeriesResultData`
  variant (`Rows`) and skips re-aggregation — `series_rows_ascending` yields the shard rows
  directly, or aggregates raw samples in the legacy/config-off path (`Chunk`). Grouped multi
  queries reduce these per-series rows column-wise via `MultiSeriesRowIter` + `RowReducer`.
- **`COUNT` push-down** (`pushdown_count`) — **enabled for multi**, because the shipped unit is
  now rows (bucket-aligned); the shard truncates head/tail rows and the coordinator re-applies the
  authoritative COUNT, exactly as for single-aggregation buckets.
- **GROUPBY/REDUCE partial-state push-down** (`pushdown_group`) — **still disabled for multi.**
  `ReducePartialState`/`GroupPartialSeries` carry one value per bucket; a per-column form is the
  remaining follow-up (see `mrange-aggregation-pushdown-plan.md` §10). `get_local_response`
  defensively ignores `apply_group_reduce` for multi requests. Grouped multi therefore falls back
  to per-series bucket transport with a coordinator-side column-wise reduce, which already captures
  most of the network saving (buckets vs. raw samples).

`reply()` clears `aggregation` only for single-aggregation push-down; multi keeps its options
(needed for the column count and the multi/data-variant branch).

### 8.4 Fanout command handler (src/commands/ts_mrange_fanout_command.rs)

`get_local_response()` is **unchanged** (it already strips aggregation/count/reverse; stripping
a list is the same operation).

Coordinator-side `reply()` path:

- `process_series_list()` currently returns `Vec<Sample>`; add a row variant
  `process_series_rows() -> Vec<MultiSample>` selected when `is_multi()`. It reuses the same
  three-way branch (pre-sorted reverse merge / single series / `MultiSeriesSampleIter`) with the
  row adapter from §6. `validate_reverse()` logic is unchanged (aggregation present ⇒ aggregate
  ascending, reverse rows afterwards).
- `handle_basic()` / `process_series_samples()`: store `SeriesResultData::Rows` when multi.
- `handle_grouping()` / `process_group()`: replace `SampleReducer` usage with
  `MultiSeriesRowIter` + `RowReducer` (same components as the local grouped path, §7) and store
  `Rows`. `build_mrange_grouped_labels` unchanged.
- Existing `orx_parallel` parallelism over series (`into_par`) and groups (`iter_into_par`) is
  preserved as-is.

Note: `MRangeFanoutCommand.reply()` clears `latest`/filters before post-processing because
shards already applied them — this remains correct; the coordinator's row pipeline runs with
`timestamp_filter/value_filter = None` on already-filtered raw samples.

---

## 9. Performance & Threading

**Design principle: one scan, N accumulators.** Multi-aggregation must not multiply the
dominant cost (chunk decode + iteration). The shared `AggregationHelper` guarantees a single
pass regardless of N; per-sample incremental cost is N enum-dispatched `update()` calls on
inline (SmallVec) state — cache-friendly and branch-predictable.

**Where parallelism applies (and where it deliberately doesn't):**

- `TS.MRANGE`/`TS.MREVRANGE`: already parallel per series and per group via `orx_parallel`
  (`src/series/mrange.rs`, `ts_mrange_fanout_command.rs`). Multi-aggregation executes inside
  those tasks; parallelism scales with series count, which is the right axis — no changes
  needed, no new thread pools.
- Cluster: shards work concurrently by construction; the coordinator's merge/aggregation is
  parallel per series/group as above.
- `TS.RANGE` (single series): stays single-threaded. Splitting one series scan across threads
  would require (a) partitioning at bucket-aligned chunk boundaries, (b) an associative
  `merge()` operation on every aggregator's partial state (avg needs (sum, count); std/var need
  (n, mean, M2); first/last/rate are order-sensitive), which the `AggregationHandler` trait does
  not expose today. The scan is memory-bandwidth-bound and typical ranges are small; the added
  complexity is not justified. Recorded as future work (§11) behind a benchmark gate.

**Memory:** rows are `16 + 8·N` bytes logical; `SmallVec<f64, 4>` keeps ≤4 columns heap-free.
`ReverseRowIter` buffers one series' output rows (bucket count, not sample count) — same order
of magnitude as today's `ReverseSampleIter`.

**Benchmarks to add** (criterion or existing bench setup): N ∈ {1, 3, 8} aggregators vs. N
sequential single-aggregation queries over 1M samples; assert the multi path is ~flat in N and
the N=1 path shows no regression vs. `main`.

---

## 10. Testing Plan

**Parser (`command_parser.rs` tests):**
- `avg,min,max` ordering preserved; case-insensitivity; single element == legacy behavior.
- Errors: empty element (`avg,,max`), trailing comma, unknown type, duplicates (`avg,AVG`),
  > 16 entries, `CONDITION` with zero capable aggregators, `countif` in list without
  `CONDITION`, `TS.JOIN` with a list.
- `CONDITION > 5` with `countif,avg,sum`: countif+sum get the filter, avg does not.

**Engine (aggregate_iterator tests):**
- *Equivalence invariant*: for every `AggregationType`, all option combos (EMPTY on/off, each
  BUCKETTIMESTAMP, alignments, NaN-laden inputs), column *i* of the multi iterator ==
  single-aggregator run of that type. This single parametrized test protects all per-column
  semantics.
- EMPTY back-fill: per-column empty values (`sum→0`, `min→NaN`, `last→previous` semantics as
  encoded in `empty_bucket_value`).
- Reverse: First/Last swap applied per column; row order descending.

**MRANGE local:**
- Non-grouped multi-agg across multiple series; WITHLABELS/SELECTED_LABELS unaffected.
- Grouped column-wise reduce: hand-computed fixture (2 groups × 2 series, `avg,max` +
  `REDUCE sum`); series missing buckets; all-NaN columns; `COUNT` on rows; MREVRANGE ordering.

**Fanout / proto:**
- Conversion round-trip: multi list survives `AggregationOptions ⇄ FanoutAggregationOptions`;
  legacy decode (only field 1 set) yields a 1-element list; both-empty errors; >16 or duplicate
  entries from the wire rejected.
- `MRangeFanoutCommand`: feed synthetic `SeriesRangeResponse` chunks into `on_response` +
  `reply` with multi-agg options, both grouped and basic (unit-testable without a cluster since
  the reply path is pure post-processing).
- Cluster integration (if/where the existing cluster test harness covers MRANGE): 2-shard
  multi-agg MRANGE with GROUPBY, verify shape and values match a standalone run over the same
  data.

**Replies:** RESP shape checks — N=1 emits 2-element rows (byte-compatible with today), N=3
emits 4-element rows.

---

## 11. Non-Goals & Future Work

- **Aggregation push-down for MRANGE fanout.** Every series lives on exactly one shard, so
  non-grouped per-series aggregation could run shard-side and cut network volume (buckets vs.
  raw samples). Deferred: it changes response semantics and interacts with `COUNT`; the proto
  groundwork (repeated `AggregatorConfig` in the request) is already in place.
- **Intra-series parallel aggregation** for very large `TS.RANGE` scans — requires
  `AggregationHandler::merge()` for partial-bucket state (see §9).
- **Per-aggregator CONDITION syntax** (`countif(>5),sumif(<=2)`) — the parser decision (#2)
  keeps room to add this later without breaking the comma-list grammar.
- **TS.JOIN multi-aggregation** — explicitly out of scope; guarded with a clear error.
- Multi-value transport chunks (compressed row encoding) — only needed if push-down happens.

---

## 12. Implementation Order

Phased so the tree builds and all existing tests pass after every phase:

1. **Core types + engine.** `MultiSample`; `AggregationOptions.aggregations` (+ `Copy` removal
   fallout); `AggregationHelper` generalization; `MultiAggregateIterator` +
   `AggregateIterator` wrapper; equivalence tests. *No behavior change anywhere.*
2. **Parser + validation.** List parsing, CONDITION distribution, duplicate/cap errors, new
   `error_consts`, TS.JOIN guard; parser tests.
3. **TS.RANGE / TS.REVRANGE.** `create_filtered_sample_iterator` refactor,
   `create_row_iterator_adapter`, `TimeSeriesRangeRowIterator`, reply writers, command branch.
4. **Local MRANGE / MREVRANGE.** `SeriesResultData` enum, non-grouped rows path,
   `MultiSeriesRowIter` + `RowReducer`, grouped path, reply branch.
5. **Cluster.** Proto field 7 + conversions with legacy fallback, fanout coordinator row paths
   (`handle_basic`/`handle_grouping`/`process_series_rows`), conversion + fanout unit tests.
6. **Docs + polish.** Update `docs/commands/ts.range.md`, `ts.revrange.md`, `ts.mrange.md`,
   `ts.mrevrange.md` (syntax, output description, examples), `docs/COMMANDS.md` if syntax is
   summarized there; benchmarks; cluster integration test.

### Touched-file summary

| Area | Files |
|---|---|
| Types | `src/series/request_types.rs`, `src/common/` (MultiSample) |
| Engine | `src/aggregators/aggregate_iterator.rs`, `src/aggregators/mod.rs` |
| Parser | `src/commands/command_parser.rs`, `src/error_consts.rs` |
| Pipeline | `src/iterators/utils.rs`, `src/iterators/timeseries_range_iterator.rs`, new `src/iterators/{multi_series_row_iter,row_reducer}.rs`, `src/iterators/mod.rs` |
| Commands | `src/commands/ts_range.rs`, `src/commands/ts_mrange.rs`, `src/commands/utils.rs` |
| MRANGE core | `src/series/mrange.rs` |
| Replies | `src/common/replies/raw_replies.rs`, `src/common/replies/mod.rs` |
| Cluster | `src/commands/fanout.request.proto`, `src/commands/fanout/conversions.rs`, `src/commands/fanout/chunks.rs`, `src/commands/ts_mrange_fanout_command.rs` |
| Guard | `src/commands/command_parser.rs` (`parse_join_args`) |
| Docs | `docs/commands/ts.{range,revrange,mrange,mrevrange}.md` |
