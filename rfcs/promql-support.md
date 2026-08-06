# RFC: PromQL Query Engine for Valkey TimeSeries

- **Status**: Implemented; pending compatibility review and release approval
- **Branch**: `promql`
- **Date**: 2026-08-06
- **Author**: valkey-timeseries team

---

## 1. Summary

This RFC describes the addition of a PromQL-compatible query engine to the
Valkey TimeSeries module. Two new Valkey commands — `TS.QUERY` (instant) and
`TS.QUERYRANGE` (range) — accept PromQL expressions and evaluate them against
time-series data stored in the module. They return RESP maps shaped similarly
to Prometheus query results: a `resultType` plus a scalar, string, instant
vector, or matrix `result`.

The implementation uses the MIT-licensed
[`promql-parser`](https://crates.io/crates/promql-parser) crate for AST
construction. It also contains work adapted from the MIT-licensed OpenData
project, with the required attribution and license retained in the source tree.
It does not depend on RedisTimeSeries or Prometheus server source code.

This is an embedded PromQL evaluator, not a full Prometheus implementation. It
does not provide Prometheus remote-read/write APIs, scrape management,
recording rules, alerting rules, or Prometheus's storage lifecycle. These are explicitly
out of scope for this RFC.

### Motivation

- Give Valkey-native users a query language that is already understood by
  millions of operators, dashboard tools (Grafana), and alerting systems
  (Alertmanager).
- Avoid the cost and latency of an external Prometheus sidecar — queries execute
  inside the database process, close to the data.
- Exploit the Valkey cluster: push-down evaluation of selectors, rollups,
  and aggregations to shards collapses multi-step fanout into a single
  round-trip per query phase.

---

## 2. Public API

### 2.1 `TS.QUERY` — Instant Query

```
TS.QUERY <query>
    [TIME timestamp]
    [LOOKBACK_DELTA lookback]
    [TIMEOUT duration]
```

Evaluates a PromQL expression at a single point in time. Returns a scalar
(JSON number), an instant vector (array of `{metric, value, timestamp}`), or
a string.

**Examples:**

```bash
TS.QUERY "sum(rate(http_requests_total[5m]))" TIME 1672531200000
TS.QUERY "up{job='api-server'}" TIME *
TS.QUERY "absent(nonexistent_metric)" TIME -1h
```

### 2.2 `TS.QUERYRANGE` — Range Query

```
TS.QUERYRANGE <query>
    STEP duration
    [START timestamp]
    [END timestamp]
    [LOOKBACK_DELTA lookback]
    [TIMEOUT duration]
```

Evaluates a PromQL expression at regular intervals over a time range. Returns
a matrix (array of `{metric, values: [[timestamp, value], …]}`).

**Example:**

```bash
TS.QUERYRANGE "rate(http_requests_total[5m])" STEP 15s START 1672531200000 END 1672545600000
```

### 2.3 Timestamp Resolution

Both commands accept RFC 3339 timestamps, decimal seconds, relative durations,
and `*`, `+`, and `-` special values. Integer timestamp handling intentionally
differs by command:

- `TS.QUERY TIME` detects seconds, milliseconds, microseconds, or nanoseconds
  from the integer's magnitude.
- `TS.QUERYRANGE START` and `END` treat a bare integer as milliseconds. A
  decimal numeric timestamp is seconds.

This distinction must remain documented: `1672531200` and `1672531200.0` name
materially different instants in `TS.QUERYRANGE`.

---

## 3. Evaluation Engine

### 3.1 Entry Points

```
evaluate_instant(reader, stmt, eval_time, opts) → QueryValue
evaluate_range(reader, stmt, opts) → Vec<RangeSample>
```

Both functions parse the PromQL string via `promql_parser`, optionally run the
optimizer, construct an `Evaluator`, and delegate to its step-loop.

### 3.2 QueryReader Trait

The engine is decoupled from storage via the `QueryReader` trait:

```rust
trait QueryReader {
    fn query(&self, selector, timestamp, options) → Vec<InstantSample>;
    fn query_range(&self, selector, start, end, options) → Vec<RangeSample>;
    fn query_aggregation(&self, request) → AggregationOutcome;
    fn query_rollup(&self, request) → RollupOutcome;
}
```

- **Production**: `ValkeySeriesQuerier` routes to `SelectorBatchExecutor` or cluster fanout.
- **Testing**: `MemorySeriesQuerier` provides deterministic in-memory data for the `promqltest` framework.
- **Benchmarks**: The same trait allows head-to-head comparison of encoding schemes without a live server.

### 3.3 Evaluation Pipeline

The evaluator uses a four-phase pipeline :

1. **Plan** — Compute concrete time bounds, bucket list, and path-specific parameters
   (instant vector, matrix, or subquery vector-selector fast path).
2. **LoadSamples** — Load sample data for explicit `(bucket, series)` work items,
   using the `SelectorBatchExecutor` for production reads.
3. **ShapeSamples** — Merge, filter, and deduplicate samples into per-series structures
   (`SeriesMap`).
4. **Evaluate** — Run PromQL expression semantics on prepared in-memory inputs.

### 3.4 Range Query Preload

For range queries, the evaluator preloads data in three passes before the step loop:

1. **Instant vector preload** — All selectors that don't participate in rollups are
   preloaded across the entire step grid in parallel batches (`MAX_CONCURRENT_PRELOAD_REQUESTS = 4`).
2. **Rollup preload** — When rollup push-down is enabled, eligible
   `*_over_time`/`rate`/`delta` calls are evaluated on each shard for the full grid
   in a single fanout.
3. **Matrix preload** — Raw matrix spans for selector windows not covered by a
   rollup grid are fetched once, so the step loop slices windows locally instead
   of re-fetching per step.

### 3.5 Subquery Support

Subqueries (`metric[5m:1m]`) evaluate their inner expression over an aligned
resolution independent of the outer query's step. The evaluator:
- Computes the subquery's aligned start, end, and step from the outer `EvalContext`.
- Preloads the subquery's grid via `PreloadGrid::for_subquery`, preserving the
  outer query's `@start()`/`@end()` bounds for `@`-modifier resolution inside
  the subquery.
- Uses a scoped sub-evaluator so preloaded data for the subquery is isolated.

### 3.6 Step Merging

Range query results are folded into the series map in bounded chunks
(`STEP_MERGE_CHUNK_SIZE = 64`) to bound peak intermediate memory, rather than
materializing every step at once.

---

## 4. Supported PromQL Features

### 4.1 Selectors

- Instant vector selectors: `metric_name{label="value"}`
- Range vector selectors: `metric_name{label="value"}[5m]`
- Full Prometheus label matching: `=`, `!=`, `=~` (regex), `!~` (negated regex)
- `@` modifier: `metric @ 1672531200`
- `offset` modifier: `metric offset 1h`
- Subqueries: `metric[5m:1m]`

### 4.2 Binary Operators

| Category | Operators |
|---|---|
| Arithmetic | `+`, `-`, `*`, `/`, `%`, `^` (pow), `atan2` |
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical/Set | `and`, `or`, `unless` |

Vector matching modifiers are implemented:
- `on(<labels>)` / `ignoring(<labels>)`
- `group_left(<labels>)` / `group_right(<labels>)`
- `bool` modifier for comparison operators
- `fill(<value>)` / `fill_left(<value>)` / `fill_right(<value>)` — the native
  fill modifier introduced upstream in Prometheus 3.10+
- Many-to-one and one-to-one cardinality handling

**`fill` modifiers:** ordinarily a binary op drops any sample whose match key
has no partner on the other side (an inner join). `fill(<value>)` substitutes
`<value>` for the missing operand instead of dropping the sample, turning the
match into an outer join; `fill_left`/`fill_right` do the same for only the
left or right side, and `fill_left(<v1>) fill_right(<v2>)` sets both
independently. This applies uniformly to arithmetic and comparison operators
(e.g. `left_vector + fill(0) right_vector`, `a > fill_right(0) b`) and
composes with `on`/`ignoring`/`group_left`/`group_right`. Fill modifiers are
rejected with an evaluation error on the set operators (`and`/`or`/`unless`),
where the concept of a "missing operand value" does not apply.

The vector-matching implementation remains subject to the release criteria in
Section 11.

### 4.3 Aggregation Operators

| Operator | Push-Down | Notes |
|---|---|---|
| `sum` | Yes | Decomposable: coordinator merges partial sums |
| `avg` | Yes | Merged via Welford's algorithm (Kahan summation) |
| `min` | Yes | Coordinator takes min of shard mins |
| `max` | Yes | Coordinator takes max of shard maxs |
| `count` | Yes | Coordinator sums shard counts |
| `group` | Yes | Coordinator unions shard group label sets |
| `stddev` | Yes | Merged via parallel Welford |
| `stdvar` | Yes | Merged via parallel Welford |
| `topk` | Partial | Shards select local top-k; coordinator merges and re-selects |
| `bottomk` | Partial | Same pattern as `topk` |
| `limitk` | Partial | Shards select local top-k by value |
| `limit_ratio` | Partial | Per-shard ratio-based selection; coordinator re-applies |
| `count_values` | Partial | Shards count locally; coordinator sums per-value counts |
| `quantile` | No | Not decomposable; evaluates on coordinator |

Aggregation grouping modifiers (`by` / `without`) are fully supported and
propagated to shards during push-down.

### 4.4 Functions (~72)

**Mathematical (30):**
`abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atanh`, `ceil`, `clamp`,
`clamp_max`, `clamp_min`, `cos`, `cosh`, `deg`, `exp`, `floor`, `ln`, `log10`,
`log2`, `max_of`, `min_of`, `pi`, `rad`, `round`, `sgn`, `sin`, `sinh`, `sqrt`,
`tan`, `tanh`

**Date/Time (10):**
`day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `hour`, `minute`,
`month`, `year`, `timestamp`, `time`

**Range Vector (15):**
`sum_over_time`, `avg_over_time`, `count_over_time`, `min_over_time`,
`max_over_time`, `stddev_over_time`, `stdvar_over_time`, `first_over_time`,
`last_over_time`, `quantile_over_time`, `present_over_time`,
`absent_over_time`, `mad_over_time`, `ts_of_first_over_time`,
`ts_of_last_over_time`, `ts_of_max_over_time`, `ts_of_min_over_time`

**Counter/Rate (5):**
`rate`, `irate`, `delta`, `idelta`, `increase`

**Analysis (4):**
`deriv`, `predict_linear`, `changes`, `resets`

**Histogram (2):**
`histogram_quantile`, `histogram_fraction`

**Smoothing (1):**
`double_exponential_smoothing`

**Label Manipulation (2):**
`label_join`, `label_replace`

**Sorting (4):**
`sort`, `sort_desc`, `sort_by_label`, `sort_by_label_desc`

**Special (7):**
`absent`, `scalar`, `vector`, `start`, `end`, `step`, `range`

### 4.5 Staleness / Lookback

The engine implements Prometheus's lookback delta (default 5 minutes):
- For instant queries, samples within `(t - lookback_delta, t]` are considered.
- The lookback is configurable per-query via `LOOKBACK_DELTA` and globally via
  the `ts-promql-lookback-delta` configuration parameter.

---

## 5. Cluster Integration

### 5.1 Fanout Architecture

In cluster mode, PromQL evaluation pushes work to shards via four fanout commands:

| Fanout Command | Purpose | Wire Reduction |
|---|---|---|
| `InstantVectorSelectorFanoutCommand` | Evaluate a vector selector at a timestamp | Ships matching `(labels, value, timestamp)` per shard |
| `RangeVectorSelectorFanoutCommand` | Evaluate a range selector over a window | Ships raw samples per series |
| `AggregationFanoutCommand` | Push aggregation to shards | Ships partial states (one per group); coordinator merges |
| `RollupFanoutCommand` | Push rollup + optional fused aggregation to shards | Ships final rollup values (one per series per step) or group partials |

### 5.2 Rollup Push-Down

Rollup push-down is the largest cluster optimization. Without it, evaluating
`rate(m[5m])` over 6h at 15s step sends overlapping sample windows to the
coordinator at every step (~1440 fanouts). With push-down, the entire grid is
evaluated in **one fanout** — each shard computes final per-series-per-step
values and the coordinator concatenates.

Eligibility rule: a rollup is eligible if its output depends only on one series'
own samples in the window. The sole exception is `absent_over_time`, which
depends on global series presence/absence and remains coordinator-only.

**Fused aggregation:** when a rollup is wrapped in a decomposable aggregation
(`sum by (job) (rate(m[5m]))`), the outer aggregation is fused into the shard
request, so what crosses the wire is one partial per `(group, step)` per shard
rather than one value per series per step.

### 5.3 Aggregation Push-Down

`sum by (job) (metric)` is pushed to shards: each shard computes partial states,
the coordinator merges them. The coordinator decides whether to push down; shards
that cannot apply the operator return raw data with `applied = false`.

### 5.4 Compatibility Handshake

Version skew between coordinator and shards during rolling upgrades is handled
without version negotiation:

- **Envelope layer**: `required_features` bitmask in the fanout message header.
  A receiver that doesn't support a required feature rejects the message
  explicitly. No feature bits are currently defined.
- **Payload layer**: Self-describing responses. Every push-down response carries
  an `applied` boolean (and `aggregated` for fused rollup+aggregation). A shard
  that predates a push-down returns `applied = false` (proto3 default), and the
  coordinator falls back to local evaluation. `false` always means "did nothing."

The design rule is: **`false` must mean "did nothing."** This ensures old shards
degrade gracefully without any version check.

### 5.5 Wire Protocol

PromQL push-down messages share the `proto/v1/promql.proto` schema with the
existing TS.* fanout types. Key messages:

- `InstantQuery` / `InstantQueryResponse` — Instant vector selector push-down
- `RangeQuery` / `RangeQueryResponse` — Range vector selector push-down
- `AggregationQuery` / `AggregationQueryResponse` — Aggregation push-down with
  `AggregationGroupPartial` for mergeable partials
- `RollupQuery` / `RollupQueryResponse` — Rollup push-down with `RollupSeries`
  for final values and `RollupGroupPartial` for fused aggregation partials

Samples on the wire use the same compression policy as `MRANGE` fanout
(`ChimpChunk` for ≥ 16 samples, `UncompressedChunk` below).

---

## 6. Query Optimization

The optimizer runs before evaluation when
`optimize_queries` is enabled (off by default in cluster mode). It has two passes:

### 6.1 Constant Folding

Evaluates constant sub-expressions at plan time:
- Arithmetic on literals: `1 + 2` → `3`
- `NaN` and `±Inf` propagation
- `(A + A)` → `A * 2`

### 6.2 Filter Push-Down

Pushes label filters from binary expressions and aggregations down into vector
selectors, so the data source reads fewer series:
- `sum by (job) (metric{env="prod"})` — the `env="prod"` filter is already in
  the selector; this pass pushes down filters that are *outside* selectors.
- Works through parentheses, binary operators, and aggregations.
- Respects label scoping from `on()`/`ignoring()`/`by()`/`without()` clauses.

**Example:**

```
http_requests_total{job="checkout-service"} / http_requests_duration_seconds_count
```

`http_requests_duration_seconds_count` is emitted by every service in the fleet
(a shared histogram library), so on its own its selector resolves against the
full label index — e.g. ~200k series across 500 jobs. The binary op uses the
default one-to-one match (no `on()`/`ignoring()`), so a right-hand series can
only ever join with a `job="checkout-service"` left-hand series if its own
`job` label is also `checkout-service`; every other job's series is guaranteed
to be discarded during matching. Filter push-down rewrites the right selector
to `http_requests_duration_seconds_count{job="checkout-service"}` before
evaluation, so the label index returns only the few hundred already-relevant
series instead of the full ~200k. In cluster mode this also shrinks the
fanout: each shard's `InstantVectorSelectorFanoutCommand` scans a much smaller
postings list and ships far fewer `(labels, value)` pairs back to the
coordinator.

---

## 7. Configuration

All PromQL parameters are exposed as Valkey module configuration values under the
`ts-promql-*` prefix. They are backed by atomics and can be changed at runtime
via `CONFIG SET`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ts-promql-max-query-len` | int64 | 4096 | Maximum query string length in bytes (1 KiB–16 KiB) |
| `ts-promql-max-response-series` | int64 | 1000 | Maximum series returned by a query |
| `ts-promql-max-points-per-timeseries` | int64 | 0 (unlimited) | Maximum data points per series in a response |
| `ts-promql-lookback-delta` | duration | 5m | Default lookback delta for instant vector selectors |
| `ts-promql-max-lookback` | duration | 0 (use lookback-delta) | Synonym to Prometheus's `--query.lookback-delta` |
| `ts-promql-max-query-duration` | duration | 30s | Query timeout; aborts long-running evaluations |
| `ts-promql-set-lookback-to-step` | bool | no | If yes, lookback is clamped to the query step |
| `ts-promql-optimize-queries` | bool | no | Enable query optimizer (constant folding + filter push-down) |
| `ts-promql-enable-experimental-functions` | bool | yes | Enable experimental functions (will default to `no` before release) |

Cluster push-down toggles (runtime `CONFIG SET`):

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ts-fanout-aggregation-pushdown` | bool | yes | Push PromQL aggregation to shards |
| `ts-fanout-rollup-pushdown` | bool | no | Push PromQL rollup evaluation to shards (behind feature flag pending soak) |

### 7.1 Per-Query Overrides

`TS.QUERY` and `TS.QUERYRANGE` accept `LOOKBACK_DELTA` and `TIMEOUT` arguments
that override the global configuration for a single query.

---

## 8. Testing Strategy

### 8.1 PromQL Test Framework (`promqltest`)

A Prometheus-compatible `promqltest` harness that reuses the same `promql_parser`
crate used in production. Test files follow the
[Prometheus promqltest format](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).

**Extensions:**
- `ignore` / `resume` directives: skip blocks of commands to mark unimplemented
  features while preserving upstream test files verbatim.
- `clear` wipes loaded data without affecting the ignore state.

Tests are auto-discovered at build time via `build.rs` and generate one
`#[test]` function per file.

### 8.2 Evaluator Unit Tests

- Expression evaluation correctness
- Edge cases (empty input, NaN propagation, ±Inf)
- Label semantics
- Subquery interactions

### 8.3 Optimizer Tests

- Filter push-down correctness
- Constant folding
- Expression simplification

### 8.4 Proptest Regression Seeds

Proptest regression seeds for function tests discovered via property-based testing.

### 8.5 PromQL Engine Benchmarks

A Criterion benchmark suite to measure end-to-end
evaluation performance with realistic datasets.

---

## 9. Performance Considerations

### 9.1 Parallelism

- Range query steps are evaluated in parallel via `orx-parallel`.
- Step results are merged in bounded chunks (`STEP_MERGE_CHUNK_SIZE = 64`) to
  control peak memory.
- Preload requests (instant vector, rollup, matrix) are issued in parallel,
  throttled to `MAX_CONCURRENT_PRELOAD_REQUESTS = 4` to avoid overwhelming the
  cluster with concurrent fanouts.

### 9.2 Memory Efficiency

- **`EvalLabels`**: A copy-on-write label container. The `Shared` variant wraps
  `Arc<[Label]>` from storage; mutation promotes to `Owned` only when needed.
  Cloning is an atomic refcount bump.
- **`halfbrown::HashMap`**: Used for `SeriesMap` and aggregation group maps;
  provides hashbrown-level performance with smaller memory overhead.
- **`BitSet`**: Custom bitset for efficient set operations in vector-vector
  binary operator matching.

### 9.3 Wire Reduction

Rollup and aggregation push-down are the primary mechanisms for reducing
cluster traffic:
- Rollup push-down: from O(steps) fanouts to 1 fanout.
- Aggregation push-down: from shipping all matching series to shipping
  one partial state per group per shard.
- Fused rollup+aggregation: combines both reductions in a single shard request.

### 9.4 Known Trade-offs

- **Wire compression is a bandwidth measure, not a latency win.** On 10–25 Gbps
  in-rack interconnects, Chimp encoding's break-even is ~1.2–1.7 Gbps
  (round-trip), so compression adds CPU overhead for minimal latency benefit
  on fast networks. It remains valuable for cross-AZ links and egress cost
  control.
- **`enable-experimental-functions` currently defaults to `true`.** It will be
  flipped to `false` before release.

---

## 10. Release Criteria, Limitations & Intentional Divergences from Prometheus

### 10.1 Release Criteria

Before release, the project must:

1. Run the generated PromQL conformance fixtures and full unit suite in CI.
2. Soak aggregation and rollup push-down in cluster mode before enabling rollup
   push-down by default.
3. Resolve known vector-vector comparison semantics before claiming complete
   PromQL vector-matching compatibility.
4. Document every unsupported or intentionally divergent PromQL semantic.

### 10.2 Not Yet Implemented

- Certain histogram functions in range contexts

### 10.3 Intentional Divergences

- **Error messages**: May differ from Prometheus for the same error condition.
  The engine aims for semantic equivalence, not textual compatibility.
- **Float formatting**: Minor differences in NaN/Inf representation and
  decimal precision at the 15th significant digit.
- **Staleness**: The engine does not implement the Prometheus head compaction
  staleness model; it relies on the lookback delta to approximate the same
  behavior.

---

## 11. Future Work

1. **Flip `enable-experimental-functions` default to `false`** before the first
   stable release.
2. **Flip `ts-fanout-rollup-pushdown` default to `true`** after sufficient soak
   time in production clusters.
3. **Prometheus remote-read compatibility**: Accept PromQL queries via the
   Prometheus remote-read protocol, allowing drop-in replacement of
   Prometheus-side query evaluation.
4. **Recording rules**: Periodic evaluation of PromQL expressions with results
   written back as new time series.
5. **Alerting rules**: Evaluate PromQL alert conditions and fire notifications.
6. **Query caching**: Cache repeated selector results within and across queries.
7. **Expression planning**: A cost-based planner to choose between push-down and
   coordinator evaluation based on selectivity estimates.
8. **Extended statistics**: Percentile-based query latency histograms,
   per-function call counts, and selectivity estimates.

---

## 12. References

- [PromQL documentation (Prometheus)](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [`promql-parser` crate](https://crates.io/crates/promql-parser) — MIT-licensed PromQL parser
- [OpenData project](https://github.com/opendata-oss/opendata) — MIT-licensed adapted-code attribution
