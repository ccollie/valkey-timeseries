# PromQL query pipeline: resource exhaustion paths and defenses (plan)

**Status:** Findings from a code review on 2026-10-09 of `promql-observability` at
`c49b9d48a`. Nothing below is implemented. One finding (F2, parse cost) is measured; the
others are traced from code and were not reproduced on a live server, so treat their
severities as risk-ranked hypotheses until §8's reproductions are run.
**Scope:** `TS.QUERY` / `TS.QUERYRANGE` from argument parsing to the reply, single node
and cluster, including the shard side of the PromQL fan-out commands. The `FILTER`-taking
`TS.*` commands are covered by [filter-dos-audit.md](../topics/filter-dos-audit.md); the
index machinery they share with PromQL selectors (regex scans over distinct label values,
F3 there) is not repeated here.
**Evidence base:** code reading, plus a parse-time measurement of `promql-parser` 0.11.0
(§6).

---

## Summary

The pipeline already bounds most dimensions, and the bounds are in the right places: the
query text (length, regex size, nesting depth), the grid (1,000,000 steps, 100,000,000
subquery step evaluations), what a read loads (a query-wide sample budget with per-read
early stop, a series cap, bounded lock holds), concurrency (a fixed worker pool with a
bounded queue) and time (a deadline every query carries). §1 lists them.

What remains is a set of paths where **work or memory is proportional to something the
limits do not count**:

- *Produced* points rather than *loaded* samples (F1). One sample per series inside a long
  lookback is replicated to every step of a million-step grid without being charged, on a
  single node and on every shard. The final result of a range query is not capped at all.
- Series *walked* rather than series *returned* (F3). Every read path collects all matches
  before it compares the count with the cap.
- Parse cost on the main thread, before admission (F2). Measured: 54 ms for a 4 KiB query,
  760 ms for a 16 KiB one, superlinear in length.
- A client-supplied lookback that nothing caps (F4). `ts-promql-max-lookback` is documented
  and stored but never read.
- Concurrency (F5, F8). Every per-query limit is sized as if the query ran alone, and one
  client can own every worker and queue slot.
- Work nobody will collect (F6, F7). A disconnected client's query runs to its deadline; a
  shard runs requests whose coordinator has already timed out.

The defenses in §3 are ordered by value per line. The first two (charge produced points,
fail the series cap during the walk) close the paths with the largest multipliers and are
small changes to code that already accounts for the neighbouring quantity.

---

## 1. The pipeline and what already bounds it

| Stage (thread) | Bound today | Where |
|---|---|---|
| Argument parse (main) | query length ≤ `ts-promql-max-query-len` (4 KiB default, 16 KiB max); every `=~`/`!~` pattern compiled under a 64 KiB program / 16 KiB DFA limit before the parser sees it; depth ≤ 500 nodes; `TIMEOUT` clamped to `ts-promql-max-query-duration` (30 s) and always set, even when the client gives none | [command_parser.rs:1818](../../src/commands/command_parser.rs#L1818), [:1864](../../src/commands/command_parser.rs#L1864), [regex.rs:12](../../src/labels/regex.rs#L12), [utils.rs:197](../../src/promql/utils.rs#L197), [engine/mod.rs:73](../../src/promql/engine/mod.rs#L73) |
| Admission (main) | 8 workers (`ts-promql-max-concurrent-queries`), 128 queued (`ts-promql-max-queued-queries`), refused on arrival past that; a query whose deadline passed while queued is answered without evaluation; deny-blocking contexts (MULTI, scripts) refused | [query_workers.rs:71](../../src/promql/engine/query_workers.rs#L71) |
| Planning (worker) | outer grid ≤ 1,000,000 steps (`ts-promql-max-points-per-timeseries`, default 0 = the ceiling only); each subquery grid ≤ 1,000,000 steps; all subqueries ≤ 100,000,000 step evaluations, nesting multiplied through | [utils.rs:54](../../src/promql/utils.rs#L54), [time.rs:123](../../src/promql/time.rs#L123), [utils.rs:135](../../src/promql/utils.rs#L135) |
| Reads (executor, materialize pool, shard lanes) | sample budget `ts-promql-max-samples-per-query` (50,000,000): each read stops early, the evaluator sums every read, each shard applies its own; series cap `ts-promql-max-response-series` (1,000) per read; per-series point cap (off by default); lock holds of ≤ 512 series and ≤ 4 MiB of copied chunks; ≤ 4 preload fan-outs in flight per query; label profiles capped at 50,000 series, 60 values, 4 KiB | [sample_budget.rs](../../src/promql/engine/sample_budget.rs), [batched_querier.rs:51](../../src/series/index/batched_querier.rs#L51), [evaluator/mod.rs:77](../../src/promql/exec/evaluator/mod.rs#L77), [label_profile.rs:41](../../src/promql/engine/label_profile.rs#L41) |
| Evaluation (worker + eval pool) | 64 steps per parallel chunk, folded before the next; deadline checked per outer step, per preload request and per subquery step; `label_replace` regex cache of 64 entries per thread | [promql_engine.rs:63](../../src/promql/engine/promql_engine.rs#L63), [evaluator/mod.rs:340](../../src/promql/exec/evaluator/mod.rs#L340), [subquery.rs:223](../../src/promql/exec/evaluator/subquery.rs#L223) |
| Fan-out (main for the bus, shard lane for the work) | one timer per request (the query timeout); shard lane of 2–8 workers behind a 1,024-deep queue, busy answered at once; header strings bounded before allocation | [cluster_rpc.rs:556](../../src/fanout/cluster_rpc.rs#L556), [workers.rs:16](../../src/fanout/workers.rs#L16), [threads/mod.rs:125](../../src/common/threads/mod.rs#L125) |
| Reply (worker) | written through the blocked client's thread-safe context without the GIL; no size bound of its own | [promql_utils.rs](../../src/commands/promql_utils.rs) |

`INFO ts_promql` exposes `queries_running`, `queries_queued` and `queries_rejected`
([module_info.rs:66](../../src/common/module_info.rs#L66)); no other refusal is counted.

---

## 2. Findings

Each finding gives the mechanism, the worst shape the limits admit under default
configuration, and how it was established.

### F1. Produced points are not budgeted; the final result is not capped — **high**

The budget charges what a read *decodes*. Three places then produce points in proportion
to `series × steps` without charging them:

1. **Single-node stepped preload.** `preload_vector_selector` charges a shard's stepped
   output when the grid push-down answers ([preload.rs:656](../../src/promql/exec/evaluator/preload.rs#L656)),
   but the `Raw` answer every single node gives goes through `cache_stepped_span`, which
   charges only the raw samples ([preload.rs:717](../../src/promql/exec/evaluator/preload.rs#L717))
   and then builds a `StepGridBuilder` sized to the grid for every series
   ([preload.rs:750](../../src/promql/exec/evaluator/preload.rs#L750)). A series with one
   sample inside the lookback of every step gets a slot per step: 16 bytes × steps.
2. **Shard-side grid.** `ShardGrid::add` accumulates every batch's stepped or rolled output
   ([grid_fanout_command.rs:597](../../src/promql/engine/fanout/grid_fanout_command.rs#L597))
   against a local budget that counts only raw samples
   ([query_utils.rs:280](../../src/promql/engine/fanout/query_utils.rs#L280)). The series cap
   is judged after every batch is in ([query_utils.rs:356](../../src/promql/engine/fanout/query_utils.rs#L356)),
   deliberately, so that a read over both limits reports `max_series`. The coordinator
   charges what arrives, after the shard did the work and shipped it.
3. **Range-query result.** Each outer step's result is merged into the series map
   ([promql_engine.rs:291](../../src/promql/engine/promql_engine.rs#L291),
   [exec/utils.rs:12](../../src/promql/exec/utils.rs#L12)) with no charge, and
   `ts-promql-max-response-series` applies to selector reads only, never to the result.
   `count_values`, `label_replace` with a per-sample value, and `or` chains all produce
   more series than any read returned. (The `count_values` benchmark already produces a
   110,000-series result from 100 series.)

Prometheus' `--query.max-samples` differs here: its range evaluation charges each step's
output points to the same budget as the loaded samples.

*Worst admissible shape, defaults:* `TS.QUERYRANGE m START 0 END 1000000 STEP 1
LOOKBACK_DELTA 30d` where `m` matches 1,000 series that each hold one sample before the
window.

| Quantity | Value |
|---|---:|
| series matched | 1,000 |
| steps (the built-in ceiling) | 1,000,000 |
| raw samples charged | 1,000 |
| grid points built | 1,000,000,000 |
| memory for the grid alone | 16 GB |
| reply, on top of that | ~30 GB |

*Evidence:* traced; not run. The reply estimate assumes about 30 bytes per RESP sample.

### F2. Parsing runs on the main thread, before admission, and is superlinear — **high**

`parse_promql_query` ([command_parser.rs:1818](../../src/commands/command_parser.rs#L1818))
runs the regex pre-check, the parser and the depth check on the thread that executes the
command, before a worker slot is reserved. A query the depth check will reject has already
paid the full parse. Measured (§6): a 4 KiB chain of unary minus parses in 54 ms, a
16 KiB one in 760 ms; four times the length costs fourteen times the time. The regex
pre-check adds up to 75 ms per the comments on [regex.rs:12](../../src/labels/regex.rs#L12).

A pipelining client therefore stalls the main thread with valid queries that are never
evaluated and never count against the pool: at 54 ms each, 18 queries per second is the
whole main thread, and nothing in `INFO` shows it.

*Evidence:* measured, release build, promql-parser 0.11.0 alone (§6). Not measured on a
running server, where the same parse runs with the same input.

### F3. The series cap is applied after the full walk — **medium**

Every read path collects all matched series and validates the count afterwards:

- shard instant read, [query_utils.rs:73](../../src/promql/engine/fanout/query_utils.rs#L73);
  shard aggregation read, [query_utils.rs:120](../../src/promql/engine/fanout/query_utils.rs#L120);
  shard range read, [query_utils.rs:429](../../src/promql/engine/fanout/query_utils.rs#L429);
- local reads, [selector_batch_executor.rs:1293](../../src/promql/engine/selector_batch_executor.rs#L1293)
  and [:1305](../../src/promql/engine/selector_batch_executor.rs#L1305). The instant read
  also reserves one slot for every planned series on its first hold
  ([:1186](../../src/promql/engine/selector_batch_executor.rs#L1186)).

A selector matching ten million series on a node costs ten million key opens across the
batched lock holds and a 400 MB vector before the refusal, and the client can repeat it. The
label-profile walk shows the pattern that is missing: it breaks as soon as `matched > cap`
([query_utils.rs:149](../../src/promql/engine/fanout/query_utils.rs#L149)).

The rationale for the late check (count what the query *returns*, not what it matched, so
push-down and plain paths agree) survives an early exit: the returned count only grows, so
once it passes the cap the verdict is fixed.

*Evidence:* traced; the full walk is the documented behaviour of the batched querier.

### F4. Client lookback is uncapped, and the configured cap is dead — **medium**

`LOOKBACK_DELTA` is taken as given for range queries
([command_parser.rs:1981](../../src/commands/command_parser.rs#L1981)) and instant queries
([:2025](../../src/commands/command_parser.rs#L2025)). `ts-promql-max-lookback` is documented
in [promql.md](../topics/promql.md) as "optional upper bound on lookback", stored in
[promql_config.rs:35](../../src/promql/engine/promql_config.rs#L35), and read nowhere.

An instant read is cheap whatever the lookback (`last_sample_in_range` walks back from the
head). A grid read is not: the shard fetches `(first_end − lookback, last_end]` as raw
samples ([query_reader.rs:386](../../src/promql/engine/query_reader.rs#L386)), so
`LOOKBACK_DELTA 10y` reads every matched series' full history, bounded only by the sample
budget. It is also the multiplier behind F1: it is what makes one sample present at every
step.

*Evidence:* traced.

### F5. No memory accounting across queries — **medium**

Per-query limits are sized as if one query ran alone. Under defaults:

| Contribution | Per query | × concurrency |
|---|---:|---:|
| sample budget, peak about twice (`sample_budget.rs` doc) | 1.6 GB | 12.8 GB over 8 workers |
| preload fan-outs in flight | 4 | 32 |
| reply buffer (Valkey's `client-output-buffer-limit normal` is unlimited by default) | result size | unbounded |

Nothing consults the server's used-memory ratio at admission or between phases; `maxmemory`
only governs writes. [filter-dos-audit.md §4](../topics/filter-dos-audit.md#4-are-max-series--max-samples-sufficient)
makes the same point for the `FILTER` commands ("attackers use concurrency").

*Evidence:* traced; the 8 GB swap death that motivated the sample budget is the one data
point ([sample_budget.rs](../../src/promql/engine/sample_budget.rs) header).

### F6. A disconnected client does not cancel its query — **low/medium**

`block_client` ([thread_safe_reply_context.rs:50](../../src/common/replies/thread_safe_reply_context.rs#L50))
blocks with no disconnect callback and no timeout callback. `CLIENT UNBLOCK` or a closed
connection leaves the evaluation running to its deadline on a worker and the evaluation
pool. The queue reservation is released when the job starts, so connection churn costs the
attacker nothing: open, send, close, repeat, and every query still runs for up to 30 s.

*Evidence:* traced.

### F7. Shards work for coordinators that gave up; responses decode on the main thread — **low/medium, cluster only**

- The request carries no deadline ([promql.proto:258](../../proto/v1/promql.proto#L258);
  the header has none either). A request dequeued from the shard lane after the
  coordinator's timer fired still runs to completion and its answer is dropped as a late
  response. The lane queue is 1,024 deep behind 2–8 workers
  ([cluster_rpc.rs:615](../../src/fanout/cluster_rpc.rs#L615)), so a burst of coordinator
  timeouts leaves every shard working through a backlog nobody will read.
- Each fan-out is armed with the query's *relative* timeout
  ([selector_batch_executor.rs:547](../../src/promql/engine/selector_batch_executor.rs#L547)),
  not the time remaining to its deadline, so a query's last fan-out can outlive the query
  by a full timeout.
- A shard's response is deserialized, and the command's `on_response` run, inside the
  cluster-bus callback on the coordinator's main thread
  ([fanout_command.rs:160](../../src/fanout/fanout_command.rs#L160)). The range path keeps
  chunks packed for the pool, but the protobuf decode and label resolution of a large grid
  response are a main-thread stall proportional to its size.

*Evidence:* traced.

### F8. CPU is bounded only by the coarse deadline, and the pool has no fairness — **low**

- `check_deadline` runs per outer step, per preload request and per subquery step. An
  instant query is checked once before and once after the whole evaluation
  ([promql_engine.rs:149](../../src/promql/engine/promql_engine.rs#L149)). Aggregations,
  functions and binops over a large input run to completion regardless.
- The subquery cost cap admits 100,000,000 step evaluations
  ([utils.rs:126](../../src/promql/utils.rs#L126)); its own comment measured 55 s for a
  nested `vector(1)` shape in a debug build. Eight such queries hold the evaluation pool
  for 30 s each.
- The 8 worker and 128 queue slots are global. One pipelining client fills them and every
  other client is refused with "too many queued PromQL queries".

*Evidence:* traced; the 55 s figure is from the code comment.

---

## 3. Defenses, in priority order

| # | Defense | Closes | Size |
|---|---|---|---|
| D1 | charge produced points to the sample budget; cap the final result (done) | F1, F4 (damage) | small |
| D2 | fail the series cap during the walk (done) | F3 | small |
| D3 | parse on the worker; linear pre-check on the main thread | F2 | medium |
| D4 | enforce `ts-promql-max-lookback` | F4 | small |
| D5 | global admission: shared budget, used-memory check, reply cap | F5 | medium |
| D6 | cancel on disconnect (done) | F6 | small |
| D7 | propagate the deadline to shards; decode off the bus callback | F7 | medium |
| D8 | per-client concurrency; finer deadline checks | F8 | small |
| D9 | count every refusal | all (tuning) | small |

### D1. Charge produced points; cap the final result

**Status:** implemented 2026-10-09, with three deviations from the text below.

- **Grids are charged as they are built, a series at a time, for the larger of
  the raw span and the grid.** Charging both, as the text implies, double-counts
  ordinary data, whose grid is the smaller: a test pinned that a raw rollup
  answer is charged once, and two cluster tests pin budgets of 12 against 9 and
  10 grid points. Charging only the excess over the span keeps every existing
  budget boundary, and still charges F1's shape for its grid: three series with
  one sample each across 10,000 steps now cost 30,000, not 3. Every grid the
  preloader builds is covered: the single-node stepped span, the cluster
  stepped answer, and rollup and fused-aggregation answers (charged as their
  grids, holes included, instead of as the points that arrived).
- **No shard-side charge (item 2).** F1's premise for the shard was wrong: a
  shard stages a series only when its span holds at least as many samples as
  there are steps (`ships_raw`), and ships it raw otherwise, so its stage
  output never exceeds the raw samples it already charged. The expansion
  happens on the coordinator, which builds the grid from the raw span and now
  charges it. A trial charge of the shard's output double-counted ordinary
  data and failed the cluster budget tests (`20 > 12`).
- **The range result is capped by series, not charged.** Each step's output is
  built from preloaded grids or live reads that are already charged, and an
  operator returns at most what it reads at that step (an `or`, the sum of its
  sides), so charging the result again would double-count as above. What can
  grow past every read is the *number* of series a range query accumulates
  (`count_values`, `label_replace`, `or`), and `ts-promql-max-response-series`
  — documented as the maximum *returned* series — was never applied to the
  result. It now is, after every chunk of 64 steps and on the instant result.
  The `count_values` and `group_samples` benchmarks lift both limits, since
  they return 110,000 and 2,000 series.
- **`ts-promql-max-points-per-timeseries` keeps its 0 default.** The parameter
  does double duty: besides the outer grid it bounds the raw samples every read
  may return per series, on shards too. 11,000 would refuse `rate(m[1d])` over
  1 s data (86,400 samples per series). An outer-grid cap of Prometheus' 11,000
  needs a parameter of its own; until then the 1,000,000-step ceiling and the
  charged grids bound it.

- In `cache_stepped_span`, charge `num_steps` per series before building its grid, or
  charge the builder's final length after `finish()` (the trimmed length is what is kept;
  charging before is the conservative choice). Both the `Raw` and the `Unsupported` paths
  land here.
- In `ShardGrid::add`, charge each batch's output points to the shard's local budget as the
  batch is added, so the shard stops at its own `ts-promql-max-samples-per-query` instead
  of building the whole grid. Keep `WindowBounds` for the error ordering, but let
  `admit` return early once `returned > max_series` (D2).
- In `evaluate_range`'s chunk loop, charge each step's `InstantVector` length (and 1 per
  scalar step) before merging. Apply `max_response_series` to `series_map.len()` after
  each chunk, not only to reads.
- Give `ts-promql-max-points-per-timeseries` a non-zero default. Prometheus caps the outer
  grid at 11,000 points; that is the obvious value. Dashboards never need more and the
  1,000,000 ceiling stays as the hard upper bound for operators who raise it.

The budget already flows through every reader and sub-evaluator, so this is new call
sites, not new plumbing. The one semantic change: a query that today succeeds with a huge
grid of mostly-absent steps may now be refused; absent steps are not charged (the builder
never stores a leading absence), so only present points count.

### D2. Fail the series cap during the walk

**Status:** implemented 2026-10-09. Every read path now stops once the series it *returns*
pass the limit (`exceeds_max_series` in `query_limits.rs`), and fails through the same
`validate_max_series` as before, so the error kind and its fan-out classification are
unchanged. Where each path stops:

| Path | Stops after |
|---|---|
| shard instant read, shard aggregation push-down | the series that crosses the limit |
| single-node instant and aggregation read | the series that crosses the limit, mid-hold; its buffer is reserved for at most limit + 1 |
| shard range read, shard grid push-down, single-node range and grid read | the batch that crosses it (≤ 512 series or 4 MiB of chunks): whether a copied series returns anything is only known once it is decoded |

The message now reports the count at which the read stopped, not the matched total: a
single-node instant read of 1,500 series against a limit of 10 said `1500 > 10` and now says
`11 > 10`. On a 3-node cluster with 1,300 matching series on one shard, the shard's refusal
went from `1300 > 100` to `101 > 100` (instant) and to one batch (range and grid). The
coordinator's own check, over shards each under the limit, still reports the exact sum.

In each `for_each_series` / cursor loop that feeds a `validate_max_series` at the end, stop
the cursor as soon as the *returned* count exceeds the cap and fail with the same message.
For the instant read, reserve `min(cursor.remaining(), max_series + 1)` rather than every
planned series. The profile path's `ControlFlow::Break` is the template.

### D3. Parse on the worker

**Status:** implemented 2026-10-09. `submit_evaluation` gained a `prepare`
stage that runs on the query worker after the queue-wait deadline check and before the
evaluation pool is entered; its error is the reply as it stands, so every parse error keeps
its old text. The handlers now check only emptiness and length, and carry the text to the
worker in `PromqlStatement`. Two consequences: a malformed query sent inside MULTI or a
script, or to a full queue, gets that refusal instead of its parse error; and a parse panic
is now caught by the worker instead of aborting the server. Not done: the linear pre-parse
guard (only needed if the parse stayed on the main thread), the parsed-statement cache, and
any change to the 16 KiB maximum. A deep 16 KiB query now costs one query worker up to
~0.8 s before it is refused, within the same pool and queue limits as evaluation.

Reorder `ts_query_cmd` / `ts_queryrange_cmd`: read the raw arguments, check the length,
reserve the slot, block the client, then parse, validate regexes and check depth on the
worker, answering a parse error through the thread-safe context. The main thread then does
O(length) work per query.

If parsing must stay on the main thread for reply-ordering reasons, add a linear guard
before it: walk the lexer's tokens and count unary/binary operators, `(` and call openers;
reject when the count exceeds `MAX_QUERY_DEPTH`. That rejects every shape in §6 before the
parser sees it. A small LRU of parsed statements keyed by query text would also remove the
cost for the common case, since dashboards repeat their queries verbatim.

Consider whether the 16 KiB maximum for `ts-promql-max-query-len` is worth keeping; at
that length the parser alone is 0.5–0.8 s per query.

### D4. Enforce `ts-promql-max-lookback`

**Status:** implemented 2026-10-09, with three deviations from the text below.

- **Rejected, not clamped.** A shorter lookback changes which series are stale, so a clamped
  query would silently answer a different question. Every other limit here rejects too. (The
  text's claim that clamping is "what the Prometheus flag does" was wrong: Prometheus has no
  maximum-lookback flag. The parameter's old doc comment was copied from VictoriaMetrics,
  where `maxLookback` *is* the lookback, not a cap on it.)
- **Two lookbacks are always allowed:** the configured `ts-promql-lookback-delta`, so asking
  for the server's own default never fails, and for a range query anything up to `STEP`. A
  lookback ≤ step reads at most one step before `START` and never repeats a sample across
  steps, so it adds neither F1's multiplier nor F4's history read. The step-derived lookback
  (`STEP` larger than the default, or `ts-promql-set-lookback-to-step`) is therefore left
  alone, and with `set-lookback-to-step` the option is ignored and nothing is refused.
- **Default 1h** as proposed; `0` now means unlimited rather than "use lookback-delta". A
  store of hourly or daily series has to raise it.

In `normalize_lookback` and the instant parser, clamp a client lookback to the configured
maximum (or reject above it; clamping is what the Prometheus flag does). Default it to
something finite rather than 0, for example 1 h, and make `set_lookback_to_step` respect it
too. Then the grid fetch span is bounded by configuration, and F1's multiplier is capped
independently of D1.

### D5. Global admission

- A process-wide `SampleBudget` that every query's budget charges through, sized as a
  multiple of the per-query budget (for example `ts-promql-max-samples-total`, default
  2× `max-samples-per-query`). A query that would exceed it is refused with the same
  "too many samples" error; alternatively it waits in the queue, which the existing
  reservation already supports.
- Check `RM_GetUsedMemoryRatio` at `submit_evaluation` and in `check_deadline`; refuse new
  queries, and abort running ones at their next check, above a configurable ratio
  (`ts-promql-memory-ratio-limit`, default 0.9). This is the one control that sees every
  tenant of the process, including the data set.
- Estimate reply size from result points and label bytes before writing, and refuse above
  `ts-promql-max-reply-bytes`. The server will not do this for normal clients.

### D6. Cancel on disconnect

**Status:** implemented 2026-10-09. The disconnect callback is installed on the main thread
straight after blocking, and sets a flag in a process-wide registry keyed by the
blocked-client handle (`common/replies/client_cancellation.rs`). `QueryOptions` is `Copy`, so
it carries a `CancelToken` (the key) rather than the flag, and every place that checks the
deadline reads it: the evaluator's `check_deadline`, each outer step, an instant query before
and after evaluating, each lock hold of a single-node read, and the worker on dequeue. A
cancelled query counts in `INFO ts_promql` as `ts_queries_cancelled`. The callback only ever
sets a registered flag, and the registration is removed in the handle's `Drop` *before* the
client is unblocked, so a later blocked client reusing the address never starts out
cancelled. Measured on a release build (Apple M2): a 22 s query stopped 0–2 ms after its client
closed the connection, in three trials; a queued one is dropped when it is dequeued. Not covered: a cluster
fan-out already in flight waits out its own timeout, which is D7's deadline propagation.

Pass a disconnect callback to `RM_BlockClient` that sets an `AtomicBool` shared with the
query's `QueryOptions` (or the `Evaluator`); have `check_deadline` return `Timeout` (or a
new `Cancelled`) when it is set. Fan-out timers already clean up on their own. A queued
query whose client has gone should be skipped at dequeue, as an expired one is now.

### D7. Propagate the deadline to shards

- Add an absolute deadline to the fan-out request header (the coordinator's clock; shards
  tolerate skew by also carrying the relative timeout and taking the later of the two).
  `on_request_received` records the enqueue time; `process_request_message` drops a request
  whose deadline passed before it started, and `for_each_snapshot_batch` checks it between
  batches.
- Arm each fan-out with `min(options.timeout, deadline − now)`.
- Move `OP::Response::deserialize` and `on_response` out of the cluster-bus callback onto
  the fan-out lane, keeping only the request-id lookup on the main thread.

### D8. Fairness and finer deadline checks

- `ts-promql-max-queries-per-client` (or per ACL user), default 4, checked in
  `submit_evaluation` with a per-client counter released when the job finishes.
- In the aggregation and function kernels, call `check_deadline` every N input series
  (N ≈ 4,096 keeps the check under 0.1 % of the work). The subquery loop already does this.
- Lower `MAX_SUBQUERY_STEP_EVALUATIONS` or make it configurable; 10,000,000 still admits
  the documented "1,000-point range query over a 7-day subquery at a 1-minute step".

### D9. Count every refusal

Add a `promql` section to the metrics registry: `queries_rejected_total{reason}` for
queue-full, timeout, samples, series, points, parse, depth, regex, lookback, memory and
cancelled; histograms of evaluation time and samples loaded; gauges for samples in flight
and the global budget's remaining share; the parse time on the main thread. Without these,
the limits above cannot be tuned, and F2 is invisible today.

---

## 4. Suggested ordering

1. D1 + D2 together (one change to the read/preload layer, with a test per path that
   asserts the refusal happens before the allocation).
2. D4 (one function, plus a doc fix: the parameter is currently documented as working).
3. D3 (the parse move; measure main-thread CPU per query before and after with the §6
   shapes).
4. D6, D8 (small, independent).
5. D5, D7 (larger; D7 needs a proto change and the usual `VALKEY_TS_PROTO_REGEN=1` round
   trip and a rolling-upgrade story for the new header field).
6. D9 alongside each of the above, so every new refusal lands with its counter.

---

## 5. Proposed configuration surface

| Parameter | Default | Purpose |
|---|---:|---|
| `ts-promql-max-points-per-timeseries` | 0 (unchanged; see D1's status) | also caps raw samples per read, so 11,000 is too low for it; an outer-grid cap needs its own parameter |
| `ts-promql-max-lookback` | 1h (from 0, and now enforced; done) | cap on client `LOOKBACK_DELTA` (D4) |
| `ts-promql-max-samples-total` | 2× per-query budget | process-wide in-flight sample budget (D5) |
| `ts-promql-memory-ratio-limit` | 0.9 | refuse/abort queries above this used-memory ratio (D5) |
| `ts-promql-max-reply-bytes` | 256 MiB | estimated reply size cap (D5) |
| `ts-promql-max-queries-per-client` | 4 | in-flight queries per client (D8) |

Every new parameter must also be copied into the `PROMQL_CONFIG` snapshot by
`sync_promql_config`, or `CONFIG SET` accepts it and no query sees it (AGENTS.md).

---

## 6. Measurement: parse cost

Method: a scratch crate depending on `promql-parser = "0.11.0"` (the version in
`Cargo.lock`), release profile, `rustc 1.96.0`, Apple M2. Each query was parsed on a fresh
thread with the given stack, timed around `promql_parser::parser::parse` and around the
drop of the result. Shapes nest `up` under N levels of the construct named.

| Shape | 4 KiB (default max), 8 MiB stack | 16 KiB (config max), 8 MiB stack | 16 KiB, 2 MiB stack |
|---|---:|---:|---:|
| unary minus chain (`----…up`) | 54 ms | 761 ms | 734 ms |
| binary chain (`up+up+…`) | 37 ms | 502 ms | 501 ms |
| `or` chain | 10 ms | 209 ms | 202 ms |
| nested `sum(…)` | 32 ms | 469 ms | 474 ms |
| nested `abs(…)` | 0.6 ms | 1.9 ms | 2.0 ms |
| nested parentheses | 0.3 ms | 1.3 ms | 1.2 ms |

Dropping the tree took under 1 ms in every case. No shape overflowed a 2 MiB stack, so the
parser itself is not the stack risk the depth check guards against (the evaluator is). The
cost is CPU, on the main thread, and grows faster than linearly with length.

---

## 7. Threat-model assumptions

- The client is authenticated and allowed to run `TS.QUERY` / `TS.QUERYRANGE`; nothing here
  is reachable without that. The concern is an authorized but careless or hostile client,
  and the dashboard that accidentally behaves like one.
- Cluster peers are trusted. F7 is about wasted work from honest coordinators, not about
  malicious shards; the wire-level bounds in `fanout_message.rs` cover malformed headers.
- "Memory" means the module's own allocations, which the server does not cap for reads, and
  the client output buffer, which the server does not cap for normal clients by default.
- Severities assume default configuration. An operator who sets
  `ts-promql-max-points-per-timeseries` to a few thousand and a tight
  `ts-promql-max-samples-per-query` has already closed most of F1 and F5.

---

## 8. Reproductions to run before changing policy

1. **F1 single node:** 1,000 series with one sample each at `t=0`;
   `TS.QUERYRANGE m START 0 END 100000 STEP 1 LOOKBACK_DELTA 1d` (100,000 steps, to stay
   under machine memory); watch RSS and `used_memory` against the 1,000 samples charged.
   Expect about 1.6 GB of grid plus the reply.
2. **F1 shard:** the same against a 3-node cluster; watch each shard's RSS and the
   coordinator's main-thread latency (`PING` p99) while the response is decoded.
3. **F2:** pipeline 200 copies of the 4 KiB unary chain; measure `PING` p99 from a second
   connection and main-thread CPU. Expect multi-second stalls.
4. **F3:** 1,000,000 series on one node; `TS.QUERY '{__name__=~".+"}'`; measure time to
   the series-cap error and peak RSS.
5. **F6:** issue a 30 s query (F8's nested subquery shape), close the connection at once,
   observe `queries_running` in `INFO ts_promql` for the next 30 s.
