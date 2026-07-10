# Shard-Side Aggregation Push-Down for MRANGE Fanout — Implementation Plan

**Status:** Implemented (Phases 1 & 2, 2026-07-06; design approved 2026-07-05)
**Scope:** `TS.MRANGE` / `TS.MREVRANGE` cluster fanout. Phase 1: per-series `AGGREGATION`
push-down. Phase 2: partial `GROUPBY`/`REDUCE` push-down.
**Out of scope (original plan):** `COUNT` push-down (later added), multi-aggregation transport
(later added — see §10), all other fanout commands (already minimal — see §2).

---

## 1. Overview

Today, a clustered `TS.MRANGE` ships **raw filtered samples** from every shard to the
coordinator, which then re-runs aggregation, grouping, count, and reversal locally
(`src/commands/ts_mrange_fanout_command.rs`). For a query like

```
TS.MRANGE - + AGGREGATION avg 3600000 FILTER region=us-east
```

over series with one sample per second, each shard transfers ~3,600 samples per series per
hour of range, only for the coordinator to collapse them to 1 bucket per hour. The network
and coordinator-CPU cost is proportional to **raw sample count**, when it could be
proportional to **bucket count**.

The key structural fact enabling this optimization: **a time series is a single Valkey key
and therefore lives entirely on one shard.** Per-series bucket aggregation needs no data
from any other node, so it can be computed shard-side with *exact* semantics for **all 23
aggregator types** — no decomposability/mergability requirements, no approximation. Only
cross-series operations (`GROUPBY`/`REDUCE`) genuinely require multi-shard data, and even
those can be *partially* reduced shard-side for decomposable reducers (Phase 2).

### Approved design decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | Scope | **Per-series `AGGREGATION` push-down (Phase 1)** and **partial `GROUPBY`/`REDUCE` push-down (Phase 2)**. `COUNT` push-down for non-aggregated queries was considered and deferred. |
| 2 | Sequencing vs. the multi-aggregation plan (`docs/multi-aggregation-plan.md`) | **Push-down first**, designed against the current single-aggregator baseline. When multi-aggregation lands, multi-agg queries bypass push-down (fall back to raw samples) until a follow-up adds multi-column transport (§10). |
| 3 | Mixed-version clusters | **Assume homogeneous cluster.** No per-response compatibility marker. A request-side opt-in flag exists anyway (it is how the kill switch works, §5.2) and incidentally makes the *old-coordinator → new-shard* direction safe; the *new-coordinator → old-shard* direction is unsafe while push-down is enabled mid-upgrade (§8). |
| 4 | Kill switch | **Yes** — module boolean config `ts-fanout-aggregation-pushdown` (default `yes`), checked by the coordinator when building the request. |

---

## 2. Survey: where shard-side aggregation is possible

All seven fanout commands were examined (`grep "impl FanoutClientCommand"`):

| Command | Data on the wire | Push-down opportunity |
|---|---|---|
| `TS.MRANGE`/`TS.MREVRANGE` (`MRangeFanoutCommand`) | **Raw samples** per series (Gorilla chunks) | **Yes — this plan.** |
| `TS.MGET` (`MGetFanoutCommand`) | One sample per series | None; already minimal. |
| `TS.CARD` (`CardFanoutCommand`) | One count per shard | Already pushed down. |
| `TS.MDEL` (`MDelFanoutCommand`) | Deleted count | Already minimal. |
| `TS.QUERYINDEX` | Key names | No aggregation semantics. |
| `TS.LABELNAMES` / `TS.LABELVALUES` / label search | Name/value lists | Set-union only; already minimal. |
| `TS.LABELSTATS` (`LabelStatsFanoutCommand`) | Per-shard stats | Already aggregated per shard. |

`TS.RANGE`/`TS.REVRANGE` are single-key commands routed by the cluster to the owning shard —
no fanout, nothing to change. `TS.JOIN` and `TS.OUTLIERS` have no fanout implementation and
are out of scope.

**Conclusion:** the MRANGE family is the only place where non-aggregated data crosses the
wire. Within it:

1. **Per-series `AGGREGATION`** — always computable shard-side, exactly, for every
   aggregator (`avg`, `std.*`, `rate`, `first`/`last`, … — the shard sees the complete
   series). *Phase 1.*
2. **Cross-series `GROUPBY`/`REDUCE`** — inherently multi-shard, but shards can pre-reduce
   their *local* members of each group into one partial series per (group, shard) when the
   reducer is decomposable. *Phase 2.*
3. **`COUNT`** for non-grouped queries is per-series and could also be pushed down —
   deferred by decision #1; it remains coordinator-side in both phases.

---

## 3. Current architecture (baseline)

The cluster flow for `TS.MRANGE` (`src/commands/ts_mrange_fanout_command.rs`):

- **Coordinator** (`ts_mrange.rs` → `MRangeFanoutCommand::exec`): serializes the full
  `MRangeOptions` into `MultiRangeRequest` — including `AggregationOptions`, which shards
  currently *ignore* — and fans out to one node per shard
  (`FanoutTargetMode::Random`).
- **Shard** (`get_local_response`, ts_mrange_fanout_command.rs:42): deserializes options,
  then strips `count`, `aggregation`, and `is_reverse`; runs `process_mrange_query(ctx,
  options, /* clustered = */ true)`. That path (`src/series/mrange.rs::process_mrange`)
  tags each series with its `GROUPBY` label value, strips `grouping`, and returns **raw
  samples** per series, after applying `LATEST`, `FILTER_BY_TS`, and `FILTER_BY_VALUE`
  shard-side. Samples are encoded as Gorilla chunks (`SeriesRangeResponse.samples`).
- **Coordinator** (`reply`): clears `latest`/`timestamp_filter`/`value_filter` (already
  applied by shards), then re-processes everything else:
  - non-grouped (`handle_basic` → `process_series_samples` → `process_series_list`):
    per-series aggregation + reverse + `COUNT`;
  - grouped (`handle_grouping` → `process_group`): merges **raw** samples of a group via
    `MultiSeriesSampleIter`, aggregates the merged stream, reduces, reverses, applies
    `COUNT`.

Relevant invariants that make push-down safe:

- The wire `DateRange` carries **absolute resolved timestamps** (`TimestampRange ⇄
  DateRange` conversions resolve `-`/`+` at the coordinator,
  `src/commands/fanout/conversions.rs:80`). `ALIGN` resolution
  (`BucketAlignment::get_aligned_timestamp`, `src/aggregators/mod.rs:74`) depends only on
  those two values, so **every shard derives bit-identical bucket boundaries** to the
  coordinator.
- `LATEST` and both filters are already applied shard-side, *before* aggregation — the
  same pre-aggregation position they occupy in the standalone pipeline
  (`create_range_iterator`, `src/iterators/utils.rs`).
- Aggregation always consumes ascending input; reversal is applied to the *output* buckets
  (`validate_reverse`, ts_mrange_fanout_command.rs:236). Shards can therefore aggregate
  ascending and ship ascending chunks (chunk encoders require ascending timestamps), with
  the coordinator reversing bucket order afterwards — byte-identical results.

---

## 4. Phase 1 — per-series `AGGREGATION` push-down

### 4.1 Behavior

When the coordinator builds a `MultiRangeRequest` with `aggregation` present and push-down
enabled, it sets a new request flag. Shards seeing the flag **keep** the aggregation
options instead of stripping them, so `process_mrange_query` runs the existing per-series
pipeline `base reader → LATEST chain → ts/value filters → AggregateIterator` and returns
**buckets** instead of raw samples. Bucket output is `(timestamp, f64)` pairs, ascending,
with regular timestamp deltas — it flows through the existing `SampleData` chunk transport
unchanged (and Gorilla delta-of-delta compresses regular bucket timestamps extremely well).

The coordinator then skips re-aggregation:

- **Non-grouped:** per series, apply only reversal + `COUNT` to the received buckets.
- **Grouped:** merge the per-series *buckets* of a group by timestamp and apply the
  `REDUCE` step (then reversal + `COUNT`). Bucket timestamps are aligned identically
  across series/shards (§3), so the merge-and-reduce is exact.

Eligibility (coordinator side): `options.range.aggregation.is_some()` and config enabled.
Every aggregator type qualifies; there is no per-type restriction in Phase 1. Queries
without `AGGREGATION` keep today's raw-sample flow.

### 4.2 Protobuf changes (`src/commands/fanout.request.proto`)

```proto
message MultiRangeRequest {
  RangeRequest range = 1;
  repeated SeriesSelector filters = 2;
  optional GroupingOptions grouping = 3;
  bool with_labels = 4;
  repeated string selected_labels = 5;
  bool is_reverse = 6;
  // Phase 1: shard applies range.aggregation per series and returns buckets
  // instead of raw samples. Never set when range.aggregation is absent.
  bool apply_aggregation = 7;
  // Phase 2 (reserved here for visibility, added in Phase 2):
  // bool apply_group_reduce = 8;
}
```

**No response-proto changes in Phase 1.** Aggregated buckets are ordinary
`(timestamp, value)` pairs carried in the existing `SeriesRangeResponse.samples` chunk.

Why a flag rather than "aggregate whenever `range.aggregation` is present": today's
coordinators *already* serialize `aggregation` into every request (shards ignore it). An
explicit flag (a) gives the kill switch a wire representation, and (b) means an *old*
coordinator (which never sets field 7 — prost/proto3 decodes absent fields as `false`)
keeps getting raw samples from upgraded shards. Only the new-coordinator → old-shard
direction remains version-sensitive (§8).

### 4.3 Conversions (`src/commands/fanout/conversions.rs`)

`MRangeOptions` does not grow a field for this — the flag is fanout-transport state, not
query semantics. Instead `MRangeFanoutCommand` carries it:

- `MultiRangeRequest ⇄ MRangeOptions` conversions are untouched.
- `MRangeFanoutCommand::generate_request()` sets `apply_aggregation = self.pushdown`
  (computed once in `MRangeFanoutCommand::new` from the config and
  `options.range.aggregation.is_some()`).
- Shard side reads the flag directly off the decoded `MultiRangeRequest` in
  `get_local_response` before converting to `MRangeOptions`.

### 4.4 Shard-side handler (`ts_mrange_fanout_command.rs::get_local_response`)

```rust
fn get_local_response(ctx: &Context, req: MultiRangeRequest) -> ValkeyResult<MultiRangeResponse> {
    let apply_aggregation = req.apply_aggregation;
    let mut options: MRangeOptions = req.try_into()?;
    options.range.count = None;          // COUNT stays coordinator-side (decision #1)
    options.is_reverse = false;          // aggregate/encode ascending; coordinator reverses
    if !apply_aggregation {
        options.range.aggregation = None; // legacy behavior
    }
    let series = process_mrange_query(ctx, options, true)?;
    ...
}
```

No changes are needed in `process_mrange` itself: the clustered path already strips
`grouping` after tagging `group_label_value`, so per-series iterators run with
`(aggregation = Some, grouping = None)` and produce exactly the standalone per-series
bucket stream. `LATEST` chaining happens before aggregation, as in standalone.

### 4.5 Coordinator (`ts_mrange_fanout_command.rs::reply` and helpers)

`reply()` currently clears `latest` and both filters because shards applied them. Push-down
extends the same pattern:

```rust
fn reply(&mut self, ctx: &FanoutContext) -> Status {
    self.options.range.latest = false;
    self.options.range.timestamp_filter = None;
    self.options.range.value_filter = None;
    if self.pushdown {
        // Shards already bucketed each series; do not re-aggregate.
        self.options.range.aggregation = None;
    }
    ...
}
```

That single change makes the existing post-processing do the right thing:

- **`handle_basic` / `process_series_samples`:** with `aggregation = None`,
  `validate_reverse` selects the plain path — buckets are (optionally) reversed and
  `take(count)`-limited. No aggregation work, no `MultiSeriesSampleIter`; the dominant
  coordinator cost becomes chunk decode of *buckets*.
- **`handle_grouping` / `process_group` / `process_series_list`:** with
  `(aggregation = None, grouping = Some)`, the adapter path is
  `MultiSeriesSampleIter` (k-way merge of per-series buckets) → `ReduceIterator`
  (cross-series reduce per bucket timestamp) → reverse → `take(count)`. This is precisely
  the standalone "aggregate per series, then reduce" order — see §7(a) for why this is
  also a correctness *fix*.

When `self.pushdown` is false (config off, or no aggregation in the query), the reply path
is byte-for-byte today's behavior.

### 4.6 Kill switch (`src/config.rs`)

Register alongside `debug-mode` (`register_bool_configuration`, src/config.rs:714):

- Name: `ts-fanout-aggregation-pushdown`, default `yes`, `ConfigurationFlags::DEFAULT`
  (runtime-changeable via `CONFIG SET`).
- Backing store: `pub static FANOUT_AGGREGATION_PUSHDOWN: AtomicBool = AtomicBool::new(true)`.
- Read once per command in `MRangeFanoutCommand::new`; the decision is latched into
  `self.pushdown` so a concurrent `CONFIG SET` cannot desynchronize request and reply
  handling within one command.

Only the **coordinator** consults the config; shards obey the request flag. Flipping the
config off on all nodes restores exactly today's data flow (useful mid-rolling-upgrade,
§8, or as mitigation if a semantic regression ships).

---

## 5. Semantics matrix (Phase 1)

Why each query option remains exact under push-down:

| Option | Where handled today | Under push-down | Why exact |
|---|---|---|---|
| `FILTER_BY_TS` / `FILTER_BY_VALUE` | Shard, pre-aggregation | Unchanged (shard, pre-aggregation) | Same pipeline position as standalone. |
| `LATEST` | Shard (chained before coordinator-side aggregation) | Shard, chained before *shard-side* aggregation | Identical chain order (`create_range_iterator`). |
| `ALIGN` / `BUCKETTIMESTAMP` / bucket duration | Coordinator | Shard | Alignment depends only on the wire-absolute `(start, end)` (§3); all nodes compute identical bucket boundaries. |
| `EMPTY` | Coordinator back-fill | Shard back-fill | Per-series operation; identical iterator. Note: empty buckets may be `NaN` — see NaN transport test, §11. |
| `AGGREGATION <type>` (all 23) | Coordinator | Shard | Whole series is shard-local; same `AggregateIterator` code runs on the same complete input. |
| `COUNT` (non-grouped: per series; grouped: per group) | Coordinator | **Still coordinator** (deferred) | Applied to bucket streams after reversal, as today. |
| `TS.MREVRANGE` | Aggregation ascending at coordinator, buckets reversed after | Aggregation ascending at shard, buckets reversed at coordinator | `validate_reverse` semantics preserved: reversal is a post-aggregation output transform in both layouts. Chunks stay ascending on the wire. |
| `GROUPBY`/`REDUCE` | Coordinator, over raw merged samples | Coordinator, over per-series buckets | Reduce operates per bucket timestamp on per-series aggregated values — the standalone definition (§7a). |
| `WITHLABELS` / `SELECTED_LABELS` | Shard | Unchanged | Label handling untouched. |
| Sparse/empty series | Empty chunk returned | Empty chunk returned | Aggregating an empty range yields no buckets — same as coordinator-side aggregation of zero samples. |

Data-volume note: a shard response is `min(samples, buckets)`-shaped only in the sense
that bucket count is bounded by `range / bucketDuration` (plus `EMPTY` back-fill). For
`bucketDuration` smaller than the sampling interval combined with `EMPTY`, bucket count
can exceed raw sample count; this is the same trade-off the standalone reply already
makes, and no worse than today's coordinator output. No special-casing is planned.

---

## 6. Phase 2 — partial `GROUPBY`/`REDUCE` push-down

### 6.1 Behavior

With `GROUPBY label REDUCE r`, a group's member series may span shards, but each shard can
pre-reduce **its own members** per bucket timestamp and ship one *partial series* per
(group, shard) — carrying reducer *state*, not finalized values. The coordinator merges
partial states across shards per bucket, finalizes, and builds the grouped reply. Transfer
becomes `O(groups × buckets)` per shard instead of `O(series × buckets)`.

Pipeline (shard, when `apply_group_reduce` is set and the reducer is decomposable):

1. Per-series bucket aggregation (Phase 1 machinery; skipped when the query has no
   `AGGREGATION` — then "buckets" are raw samples grouped by exact timestamp, which is the
   standalone `REDUCE`-without-`AGGREGATION` semantics).
2. Group local series by `group_label_value` (already computed for tagging).
3. Per group: `MultiSeriesSampleIter` k-way merge → **partial reducer** that accumulates
   per-timestamp `ReducePartialState` instead of finalizing.

### 6.2 Reducer decomposability

`REDUCE` accepts any `is_groupable()` type except `rate` (parser,
`src/commands/command_parser.rs:598`), optionally with `CONDITION` (which maps
`count`/`sum` to their `*if` variants). Merge analysis:

| Reducer | Partial state | Merge | Finalize |
|---|---|---|---|
| `sum`, `sumif` | `{count, sum}` | add | `sum` (NaN if count = 0) |
| `count`, `countif`, `count_all`, `count_nan` | `{count}` | add | `count` |
| `min` / `max` | `{count, m}` | min/max | `m` |
| `range` | `{count, min, max}` | elementwise | `max − min` |
| `avg` | `{count, sum}` | add | `sum / count` |
| `std.p`, `std.s`, `var.p`, `var.s` | `{count, mean, m2}` | Chan/Welford parallel merge | per-type formula |
| `first` / `last` | `{count, ts, value}` | keep min/max `ts` | `value` |
| `increase`, `irate` | — | **not decomposable** | fall back to Phase 1 |

`increase`/`irate` reduce over same-timestamp values in merge order — order-sensitive
across series, so cross-shard merging cannot reproduce a single interleaving; they simply
don't set the flag. For `first`/`last`, equal-`ts` ties across shards are broken
arbitrarily — this matches today's behavior, where the winner among identical timestamps
is an artifact of `MultiSeriesSampleIter` iterator order.

NaN handling mirrors `SampleReducer`: values an aggregator rejects (NaN, for all reducers
except `count_all`/`count_nan`) don't bump `count`; a bucket whose merged `count` is 0
finalizes to NaN — exactly the "all-NaN group yields NaN" rule.

### 6.3 Protobuf changes

Request (`fanout.request.proto`):

```proto
message MultiRangeRequest {
  ...
  bool apply_aggregation = 7;   // Phase 1
  // Shard pre-reduces its local group members per bucket and returns
  // group_partials instead of per-series results. Implies apply_aggregation
  // semantics for the per-series stage. Only set when grouping is present
  // and the reducer is decomposable (§6.2).
  bool apply_group_reduce = 8;
}
```

Response (`fanout.response.proto`):

```proto
// Reducer state for one bucket; field meaning depends on the reducer type
// (carried in the request), see §6.2.
message ReducePartialState {
  uint64 count = 1;
  double acc1 = 2;   // sum | min | value | mean
  double acc2 = 3;   // max | m2
  int64 ts = 4;      // first/last: contributing sample timestamp
}

// One (group, shard) partial series. Parallel arrays, ascending timestamps.
message GroupPartialSeries {
  string group_label_value = 1;
  repeated string source_keys = 2;        // for the __source__ label
  repeated int64 bucket_timestamps = 3;
  repeated ReducePartialState states = 4;
}

message MultiRangeResponse {
  repeated SeriesRangeResponse series = 1;    // Phase 1 / fallback path
  repeated GroupPartialSeries group_partials = 2;
}
```

A response contains `series` *or* `group_partials`, never both (determined by the request
flags the coordinator itself sent — consistent with the homogeneous-cluster decision).

### 6.4 New component: `PartialReducer` (`src/aggregators/` or `src/iterators/`)

Rather than adding `merge()` to all 23 `AggregationHandler` implementations, add a small
dedicated enum covering the decomposable set:

```rust
pub enum PartialReducer { Sum{..}, Count{..}, Min{..}, Max{..}, Range{..},
                          Avg{..}, Moments{..}, First{..}, Last{..} }

impl PartialReducer {
    fn for_reducer(cfg: &AggregatorConfig) -> Option<Self>; // None => not decomposable
    fn update(&mut self, ts: Timestamp, value: f64);
    fn take_state(&mut self) -> ReducePartialState;
    fn merge(state: &mut ReducePartialState, other: &ReducePartialState, kind: ...);
    fn finalize(state: &ReducePartialState, kind: ...) -> f64;
}
```

`for_reducer` returning `None` is the coordinator's eligibility test for setting
`apply_group_reduce`. `CONDITION` filters (`countif`, `sumif`) are applied in `update`
shard-side, identically to `CountIfAggregator`/`SumIfAggregator`.

A `PartialSampleReducer<I>` iterator (structural clone of `SampleReducer`,
`src/iterators/sample_reducer.rs`) yields `(Timestamp, ReducePartialState)` per timestamp
group.

### 6.5 Shard and coordinator logic

**Shard** (`get_local_response`): when `apply_group_reduce` && `grouping.is_some()`:
short-circuit before the `grouping = None` strip in `process_mrange` — group the tagged
series, run the per-group partial pipeline (§6.1), and return `group_partials` (with
`source_keys` collected per group). Series without the group label are omitted (matches
`construct_group_map` behavior).

**Coordinator** (`MRangeFanoutCommand`):

- `on_response`: accumulate `resp.group_partials` alongside `series`.
- `reply` (group-reduce mode): bucket partials by `group_label_value`; per group, k-way
  merge the shards' `(bucket_timestamps, states)` arrays by timestamp, `merge` states with
  equal timestamps, `finalize` each into a `Sample`; then reverse + `COUNT` per group;
  key = `"{label}={value}"`; labels via `build_mrange_grouped_labels` with the sorted
  union of `source_keys`.

**Fallback matrix** (coordinator picks the deepest applicable mode per query):

| Query | Mode | Wire payload |
|---|---|---|
| `AGGREGATION` + `GROUPBY`, decomposable reducer | Phase 2 | partial states per (group, shard) |
| `GROUPBY` only, decomposable reducer | Phase 2 | partial states (per raw timestamp) |
| `AGGREGATION` (± `GROUPBY` with non-decomposable reducer) | Phase 1 | per-series buckets |
| no `AGGREGATION`, no `GROUPBY` (or config off) | legacy | raw samples |

---

## 7. Pre-existing cluster/standalone divergences (fix or pin with tests first)

Discovered while auditing the paths this plan touches. Each should be captured in an
equivalence test *before* the push-down changes land, because push-down alters (and in two
cases silently *fixes*) the behavior:

a. **Grouped fanout pools raw samples before aggregation.** Standalone
   (`get_grouped_samples`) aggregates *per series*, then reduces across series per bucket.
   The fanout coordinator (`process_series_list` with `(Some(agg), Some(grp))`) merges the
   group's **raw** samples into one stream, aggregates the *pooled* stream, then reduces a
   single value per bucket — e.g. `AGGREGATION avg … GROUPBY r REDUCE max` yields a pooled
   average instead of the max of per-series averages. Cluster results currently diverge
   from standalone. **Phase 1 fixes this structurally** (shards aggregate per series;
   coordinator only reduces); add a regression test codifying standalone as the reference.

b. **`GroupData.keys` is never populated** in `ts_mrange_fanout_command.rs`
   (`construct_group_map` creates it empty; `process_group` then emits `key = ""` and an
   empty `__source__`), whereas standalone emits `key = "label=value"` and joined source
   keys. Fix in Phase 1 while rewriting `process_group`: key from
   `format!("{group_label}={label_value}")`, sources collected from member series.

c. **Standalone grouped path double-reduces**: `get_grouped_samples` builds per-series
   iterators with `grouping` still set, so `create_sample_iterator_adapter` applies a
   per-series `ReduceIterator` *and then* `SampleReducer` reduces across series. For most
   reducers this is idempotent (single-sample groups), but for dispersion reducers
   (`std.*`/`var.*`) the per-series pass collapses each value to a 1-sample std/var before
   the cross-series pass — needs verification with a hand-computed fixture. Whatever the
   verdict, Phase 1/2 must match the *corrected* standalone behavior, and the fix (pass
   `grouping = None` to per-series iterators in `get_grouped_samples`) belongs in the
   prep phase.

---

## 8. Mixed-version cluster analysis

Per decision #3, correctness is guaranteed only for homogeneous clusters. The residual
matrix with the request flag in place:

| Coordinator | Shard | Behavior |
|---|---|---|
| new (push-down on) | new | Flags honored. **Correct.** |
| new (push-down **off**) | any | Flag unset → shards strip aggregation → today's flow. **Correct.** |
| old | new | Old coordinator never sets field 7/8 (decodes as `false` on the shard) → shard strips aggregation → raw samples → old coordinator aggregates. **Correct.** |
| new (push-down on) | old | Shard ignores unknown fields, returns **raw samples**; coordinator assumes buckets and skips aggregation → **wrong results** (raw samples surfaced as buckets / reduce over raw values). |

**Operational guidance (documented in the config's description and release notes):** set
`ts-fanout-aggregation-pushdown no` before starting a rolling upgrade that crosses this
feature boundary; re-enable after all nodes run the new module. The default stays `yes`
for fresh clusters and post-upgrade steady state.

`FANOUT_MESSAGE_VERSION` is not bumped — field-level proto evolution is sufficient, and
the header version has no negotiation mechanism to build on.

---

## 9. Performance expectations

- **Network:** for aggregated queries, per-series payload drops from `O(samples in range)`
  to `O(range / bucketDuration)` — often 100–10,000×. Phase 2 further divides grouped
  payloads by (local series per group).
- **Coordinator CPU:** `handle_basic` becomes decode + optional reverse + `take(count)`;
  aggregation cost moves to shards where it parallelizes across the cluster instead of
  serializing on the coordinator (which today re-aggregates *every* series of the query,
  albeit with `orx_parallel`).
- **Shard CPU:** aggregation is a single streaming pass over samples the shard was already
  iterating and encoding; bucket output means *fewer* samples to chunk-encode. Expect
  neutral-to-positive shard cost.
- **Benchmarks to add:** cluster-simulated round-trip (synthetic `get_local_response` +
  `reply`) for 1M samples/series with `bucketDuration` at 1×, 60×, 3600× the sampling
  interval; measure serialized response bytes and coordinator reply time, push-down on vs
  off. Assert on payload-size ratios (deterministic) rather than wall-clock.

---

## 10. Interaction with the multi-aggregation plan

Per decision #2, push-down landed first, against the single-aggregator
`AggregationOptions`. Multi-aggregation (`docs/multi-aggregation-plan.md`) then landed and
was **extended to participate in per-series push-down** (2026-07-06):

- **Per-series `AGGREGATION` push-down and `COUNT` push-down now apply to multi-agg.** The
  multi-column transport was implemented as `SeriesRangeResponse.samples` → `repeated
  SampleData columns` (column *i* is a chunk of `(bucket_ts, value_i)`); the coordinator zips
  columns back into `MultiSample` rows and detects already-bucketed rows by the arrived
  `SeriesResultData::Rows` variant to skip re-aggregation. See multi-aggregation-plan §8.3.1.
- **Phase 2 (partial `GROUPBY`/`REDUCE` push-down) applies to multi-agg too (2026-07-10).**
  `GroupPartialSeries.states` is flattened row-major with a `column_count = 5` field (bucket
  *i*, column *j* at `states[i * column_count + j]`; single-aggregation is `column_count = 1`).
  A `PartialRowReducer` (row twin of `PartialSampleReducer`, `src/aggregators/partial_reducer.rs`)
  holds N clones of the partial reducer and accumulates each aggregation column independently
  per bucket timestamp; the shard's multi pipeline is `create_row_iterator` →
  `MultiSeriesRowIter` → `PartialRowReducer` (`process_mrange_group_partials`). The coordinator
  merges states column-wise per bucket, finalizes each column, and emits `MultiSample` rows,
  validating the row-major shape against its own expected column count as corrupt-peer defense.
  Non-decomposable reducers fall back to per-series bucket transport exactly as for
  single-aggregation.

Both plans touch `fanout.request.proto`, `fanout.response.proto`, `conversions.rs`,
`chunks.rs`, and `ts_mrange_fanout_command.rs`.

---

## 11. Testing plan

**Cornerstone — standalone/cluster equivalence.** The definitive property: for any query,
the fanout pipeline (`get_local_response` on partitioned data + `on_response` + `reply`)
produces identical results to `process_mrange_query` on the union of the data. Both halves
are unit-testable without a live cluster (the shard handler and reply path are pure
functions of request/response protos). Parametrize over:

- every `AggregationType` × {`EMPTY` on/off} × {`ALIGN` default/start/end/ts} ×
  {`BUCKETTIMESTAMP` −/~/+} × {forward, reverse} × {`COUNT` present/absent};
- `FILTER_BY_TS`/`FILTER_BY_VALUE` combined with aggregation;
- `LATEST` on compaction series with a pending partial bucket;
- `GROUPBY`/`REDUCE` for each groupable reducer, with member series distributed across
  2–3 simulated shards, including a group entirely on one shard and a group spanning all;
- NaN-laden inputs and all-NaN buckets;
- sparse series (empty in range) and single-sample series;
- push-down config on vs off (both must equal standalone).

**Transport:** Gorilla (and each `ChunkEncoding`) round-trips NaN values bit-exactly
(EMPTY buckets produce NaN for min/max/avg-family) and handles bucket-regular timestamps;
empty-chunk round-trip.

**Phase 1 specifics:** shard honors `apply_aggregation = false` (legacy) and absent field
(old coordinator); coordinator `reply` with `pushdown` latched clears aggregation exactly
once; grouped fix regressions for §7(a) and §7(b).

**Phase 2 specifics:** `PartialReducer` property tests — for every decomposable reducer,
`finalize(merge(states…))` equals the single-node `SampleReducer` result over the
concatenated inputs, for arbitrary partitions (proptest-style over random partitions is
cheap and high-value); NaN/count=0 → NaN; `CONDITION` variants; non-decomposable reducers
(`increase`, `irate`) route to Phase 1 mode; `first`/`last` cross-shard tie documented as
arbitrary.

**Config:** registration, default `yes`, `CONFIG SET` toggling mid-stream (latched
`self.pushdown` means in-flight commands are self-consistent).

**Integration:** if the existing cluster test harness covers MRANGE, add a 2-shard
multi-agg… (single-agg) `TS.MRANGE`/`TS.MREVRANGE` with and without `GROUPBY`, comparing
against a standalone node loaded with the same data, push-down on and off.

---

## 12. Implementation order

Phased so the tree builds and existing tests pass after every phase:

1. **Prep / correctness pinning.** Equivalence-test harness (shard handler + reply as pure
   functions); pin §7(a)/(b)/(c) with tests; fix §7(b) (`GroupData.keys`) and §7(c)
   (per-series `grouping = None` in `get_grouped_samples`) as standalone bugfixes. *No
   wire changes.*
2. **Config.** `ts-fanout-aggregation-pushdown` registration + `AtomicBool`; no readers
   yet.
3. **Phase 1 wire + shard.** `apply_aggregation = 7` in the request proto; shard-side
   conditional strip in `get_local_response`; shard unit tests (flag on/off/absent).
4. **Phase 1 coordinator.** `pushdown` latch in `MRangeFanoutCommand::new` +
   `generate_request`; `reply` clears aggregation when latched; grouped/basic paths verified
   against the equivalence suite; payload-size benchmark.
5. **Phase 2 partial reduce.** `PartialReducer` + property tests; response proto
   (`ReducePartialState`, `GroupPartialSeries`, `MultiRangeResponse.group_partials`);
   `apply_group_reduce = 8`; shard group-partial path; coordinator merge/finalize path;
   fallback matrix tests.
6. **Docs + polish.** `docs/overview.md` cluster section, config reference, release-note
   text for the rolling-upgrade guidance (§8); cluster integration tests; benchmarks in CI
   if the harness supports them.

### Touched-file summary

| Area | Files |
|---|---|
| Protos | `src/commands/fanout.request.proto` (field 7, 8), `src/commands/fanout.response.proto` (Phase 2 messages) |
| Fanout command | `src/commands/ts_mrange_fanout_command.rs` (flag latch, shard strip logic, reply, grouped rewrite, Phase 2 merge path) |
| MRANGE core | `src/series/mrange.rs` (§7(c) fix; Phase 2 shard group-partial hook) |
| Serialization | `src/commands/fanout/conversions.rs` (Phase 2 partial-state conversions), `src/commands/fanout/chunks.rs` (unchanged; NaN round-trip tests) |
| Aggregators | new `PartialReducer` (Phase 2) in `src/aggregators/` |
| Iterators | new `PartialSampleReducer` (Phase 2) in `src/iterators/` |
| Config | `src/config.rs` (`ts-fanout-aggregation-pushdown`) |
| Tests | equivalence suite (new module under `src/tests/` or alongside the fanout command), chunk NaN tests, reducer property tests |
| Docs | `docs/overview.md`, config docs, this plan |
