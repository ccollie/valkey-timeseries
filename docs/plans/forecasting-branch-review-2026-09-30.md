# `feat/forecasting` Branch Review — 2026-09-30

**Status:** In progress (updated 2026-10-01). Landed, each with a dated note under its item:
1.2 (`56ea57dca`), 2.3 (`6377a41c4`), 2.4 (`11228654e`), 2.5 (`5435484a2`), 2.6 (`b0981c02d`),
2.7 (`c0aa9691b`, `e9292203c`), 2.8 (`e9292203c`), 2.11 (`4ea9b87bc`). Everything else is open.
Findings record for the branch (49 commits, ~25k lines vs `main`, head `3ed24b966`). Each item
has a location, why it matters, a fix, and a test or benchmark path. Mark items as they land
with a dated note rather than deleting them.
**Scope:** `TS.FORECAST`, `TS.AUTOFORECAST`, `TS.BACKTEST`, `TS.DECOMPOSE`, `TS.PERIODS`,
`TS.STATIONARITY`, `TS.AUTOCORRELATION`, `TS.XCORR`, `TS.FEATURES`, `TS.STATS`, `TS.FILLGAPS`,
`TS.SANITIZE`, `TS.TREND`, `TS._STORE`, the analysis runner, the analysis pool, `StoreTarget`,
and the new configs. Reviewed by reading the diff and the pinned `anofox-forecast 0.15.10`
sources; no RedisTimeSeries code was consulted.
**Evidence:** "measured" means reproduced during the review (scratch programs described in
§6). Everything else was verified by reading the code; repro commands are given but were not
executed against a server. `cargo fmt --check`, `cargo clippy --profile release --all-targets
-- -D clippy::all` and `cargo test --features enable-system-alloc` (1584 unit + 13 doc tests)
all pass on the branch head.

Severity: **Blocking** = wrong results or divergent replica state; **Important** = a real
failure mode or measurable cost that should land before merge; **Suggestion** = cheap cleanups
and documentation.

---

## 1. Blocking

### 1.1 `TS.XCORR` returns wrong correlations for offset series (measured)

- **Where:** [src/commands/ts_xcorr.rs:165-172](../../src/commands/ts_xcorr.rs#L165-L172)
  calls `anofox_forecast::simd::correlation`.
- **Why:** the crate's `simd` module converts inputs to `f32` and forms
  `cov = dot/n − mx·my`, `var = ss/n − m²` (one-pass formula). Catastrophic cancellation
  appears as soon as the mean is a few hundred times the standard deviation. Run against the
  crate directly with `x = base + sin(i/5)`, `y = base + sin((i+2)/5)`, n = 300, exact
  lag-0 r = 0.9202:

  | base | `simd::correlation` | f64 two-pass |
  |---|---|---|
  | 0 | 0.9202 | 0.9202 |
  | 100 | 0.9196 | 0.9202 |
  | 1e3 | 1.0002 | 0.9202 |
  | 1e4 | NaN | 0.9202 |
  | 1e5 | 0.8599 | 0.9202 |
  | 1e6 | −1.0000 | 0.9202 |

  Byte counters, prices, and temperatures in Kelvin all sit in this range. The existing
  tests use random walks near zero, so they pass.
- **Fix:** an in-repo f64 two-pass Pearson over each lag window (centre each window by its
  own mean, then `Σdxdy / sqrt(Σdx²·Σdy²)`); drop the `anofox_forecast::simd` import.
- **Test:** pytest with the table above; assert `peak_lag == 2`, `peak_correlation ≈ 1.0`,
  lag-0 ≈ 0.920 at `base = 1e4`.
- **Same root cause elsewhere:** the crate's `features::basic::mean` is also the f32 kernel.
  It feeds `features::autocorrelation` (used by `TS.AUTOCORRELATION`,
  [ts_autocorrelation.rs:135-138](../../src/commands/ts_autocorrelation.rs#L135-L138)),
  PACF, `TS.PERIODS` acf, and the `count_above_mean` / `longest_strike_*` /
  `variation_coefficient` features forwarded by
  [features.rs:237-250](../../src/analysis/forecasting/features.rs#L237-L250). The branch
  already fixed this class for mean/variance/skew/kurtosis in
  [stats.rs:124-129](../../src/analysis/forecasting/stats.rs#L124-L129)
  (`test_features_mean_is_double_precision`); extend the same treatment: one in-repo f64
  `acf`/`pacf` (Durbin–Levinson) and route the mean-dependent counting features through
  `moments::mean`. Test: `TS.AUTOCORRELATION` on `1e7 + sin(i/5)` (n = 300) lag 1 ≈ 0.98.

### 1.2 `TS.SANITIZE` replicates verbatim with clock-relative bounds

- **Where:** [src/commands/ts_sanitize.rs:88-164](../../src/commands/ts_sanitize.rs#L88-L164):
  `parse_timestamp_range` → `get_series_range` resolves the bounds on the primary, then
  `ctx.replicate_verbatim()`.
- **Why:** the range grammar accepts `*` (now) and relative offsets (`-1h`, `+30m`,
  [timestamp_range.rs:88-117](../../src/series/timestamp_range.rs#L88-L117)). A replica
  replays `TS.SANITIZE key -1h *` later against its own clock, and an AOF replay does so at
  restart, so the replica keeps NaNs the primary dropped (or drops samples the primary kept).
  `TS.DEL` already solves this by replicating resolved integers
  ([ts_del.rs:49-58](../../src/commands/ts_del.rs#L49-L58)). The doc's "sanitizing is
  deterministic" claim is false for these bounds, and a `SEASONAL auto` period resolved on a
  different architecture could also differ.
- **Fix:** replace `replicate_verbatim` with
  `ctx.replicate("TS.SANITIZE", [key, start_ts, end_ts, POLICY <resolved policy incl. the
  detected SEASONAL period>, <raw STORE tokens>])`.
- **Test:** `test_sanitize_relative_bounds_replicate_resolved` in a ReplicationTestCase: pause
  the replica link longer than the window, run `TS.SANITIZE src -10s *`, resume, compare
  `TS.RANGE src - +`; plus a `DEBUG RELOAD` / AOF restart case.
- **Done 2026-09-30 (`56ea57dca`).** `ts_sanitize.rs` replicates
  `TS.SANITIZE key <start> <end> POLICY <resolved policy> [STORE dst …]` via `ctx.replicate`;
  `StoreTarget::replication_clause()` supplies the raw STORE tokens. Differences from the plan:
  - Resolved bounds are clamped for replication (`replicable_bounds`): the range parser rejects
    negative timestamps, so a start below 0 becomes 0 and an end below 0 becomes the inverted
    range `1 0`. Without this a replica errors, and with `STORE` the destination diverges.
  - The restart case is an AOF restart, not `DEBUG RELOAD` (that path goes through the RDB and
    replays no commands).
  - The stalled replica uses `DEBUG SLEEP 3` rather than pausing the link.
  - Tests: `test_sanitize_relative_bounds_replicate_resolved` and `…_store`
    (`test_ts_store_replication.py`), `test_sanitize_relative_bounds_replay_resolved_from_aof`
    (`test_ts_aofrewrite.py`), three unit tests in `ts_sanitize.rs`. The three relative-bounds
    tests fail on the old verbatim code and pass now.
    `test_sanitize_seasonal_auto_replicates_the_detected_period` passes either way (a replica
    detects the same period on identical data) and is only a guard on the new `SEASONAL <n>`
    replay.
  - Docs: the "sanitizing is deterministic" claim is corrected in `docs/commands/ts.sanitize.md`
    and `AGENTS.md`.
  - Noticed, not fixed: `TS.DEL` (`ts_del.rs:49-58`) replicates its resolved bounds unclamped,
    so a negative end (e.g. `TS.DEL key 0 -1h`) would be rejected by the replica. Read from the
    code, not run. No `STORE` there, so expected impact is a replica-side error, not divergence.

---

## 2. Important

### 2.1 Fan-out inside the analysis pool inflates latency and causes spurious TIMEOUTs (measured)

- **Where:** `map_on_current_pool`
  ([threads/mod.rs:110-130](../../src/common/threads/mod.rs#L110-L130)), used by
  `TS.BACKTEST` folds and `TS.FEATURES`; and the crate's own `par_iter` in AutoForecast,
  AutoARIMA, AutoETS, STL and cross-validation (feature `parallel`).
- **Why:** both run a fork-join on the same pool that `spawn_analysis` queues whole jobs on.
  A rayon worker waiting on its own scope steals from other workers first and then pops the
  injected queue, i.e. it picks up a *new* top-level job and runs it to completion before
  noticing its scope finished. Scratch demo (rayon-core 1.13, pools of 2 and 4 threads, one
  fan-out job of 50 ms sub-tasks queued ahead of 1 s jobs): the fan-out job completed after
  **1.06 s instead of ~50 ms**, on both pool sizes. With a 1 s `TIMEOUT` that is a spurious
  deadline under moderate load. The `ANALYSIS_POOL` doc comment only argues this nesting cannot
  deadlock, which is true; it says nothing about latency.
- **Fix options:** (a) build `anofox-forecast` without `parallel` and make
  `map_on_current_pool` sequential; the pool then becomes an N-way job queue with predictable
  latency (measure AutoForecast and BACKTEST wall time first — per-job parallelism rarely pays
  under concurrency); (b) run fan-outs on a separate pool so a waiting worker can only steal
  sub-tasks.
- **Benchmark:** 20k-sample `TS.BACKTEST … N_FOLDS 8` with four concurrent `TS.AUTOFORECAST`
  clients; report its p99 against the single-client p99.

### 2.2 No admission control on the analysis pool

- **Where:** `spawn_analysis` ([threads/mod.rs:96-103](../../src/common/threads/mod.rs#L96-L103)).
- **Why:** jobs queue without bound; a job abandoned by a client timeout keeps running and
  holds its input copy. A client looping `TS.AUTOFORECAST big - + HORIZON 1 TIMEOUT 1` grows
  the queue and memory indefinitely. `BoundedExecutor::try_spawn`
  ([executor.rs:56](../../src/common/threads/executor.rs#L56)) already implements
  reject-when-full.
- **Fix:** a cap in `spawn_analysis` (counter or `BoundedExecutor`) returning
  `TSDB: analysis pool busy, retry later`; document it next to `ts-analysis-timeout`.
- **Test:** 200 pipelined `TIMEOUT 1` calls, then assert `TS._DEBUG ANALYSIS_JOBS` never
  exceeds the cap and the overflow calls got the busy error.

### 2.3 Inline path has no size cap where blocking is denied

- **Where:** [analysis_runner.rs:151](../../src/commands/analysis_runner.rs#L151):
  `if work <= inline_max || is_blocking_denied(ctx)`.
- **Why:** inside `MULTI`, Lua or `RM_Call` every size runs on the main thread.
  `TS.DECOMPOSE` on 200k samples is seconds by its own comment
  ([ts_decompose.rs:92-93](../../src/commands/ts_decompose.rs#L92-L93)); `TS.FEATURES`
  `fourier_entropy` is a naive O(n²) DFT in the crate (`features/entropy.rs:271-289`) and has
  no `work` measure even in the background (`run_analysis_in_background`), so a 1M-sample
  range is ~5×10¹¹ operations pinned on a worker that `TIMEOUT` cannot cancel. On this path
  the crate's `par_iter` also runs on the **global** rayon pool from the GIL-holding main
  thread, the pattern AGENTS.md (this branch) tells contributors to avoid. Not a deadlock
  today (GIL takers run on detached threads / `BoundedExecutor`), but a saturated global pool
  stalls a MULTI'd AUTOFORECAST.
- **Fix:** when `is_blocking_denied(ctx) && work > inline_max`, return
  `TSDB: range too large to run inside MULTI/Lua; run it outside the transaction` instead of
  stalling; give `TS.FEATURES` a `work` measure (n × max lag for pacf, n² for
  `fourier_entropy`) or cap `fourier_entropy` at a documented n (rustfft is already a
  transitive dependency); fix `docs/commands/ts.features.md` "most features are linear".
- **Test:** `EVAL "return redis.call('TS.DECOMPOSE','s','-','+','SEASONALITY',24)" 0` on 100k
  samples → the error, not a multi-second reply.
- **Done 2026-10-01 (`6377a41c4`).** Where `is_blocking_denied(ctx)`, a call whose work is past
  a per-command ceiling is refused with `TSDB: range too large to run inside MULTI, a script or
  a module call: <n> <unit> exceeds the limit of <max>; run the command outside of it`.
  `WorkLimits { inline_max, unblockable_max, unit }` (`analysis_runner.rs`) carries the ceiling;
  `run_analysis` checks it, and `run_analysis_in_background` takes one too. All ten analysis
  commands have one. Differences from the plan:
  - The ceiling is **not** `inline_max`. `test_ts_analysis_blocking.py` asserts that every
    command runs inline inside `MULTI`/Lua at 6,000 samples, past each inline threshold (XCORR's
    work is 12M against a 10M threshold), so the plan's cap would have broken that and ordinary
    scripted use. Ceilings come from timing each command inline on a live server (release
    build, the fanless 8 GB Mac), chosen so the worst case stalls about a second: `TS.TREND`
    40,000 samples (quadratic: 0.2 s at 20k, 1.5 s at 60k); `TS.DECOMPOSE` 100,000;
    `TS.PERIODS` 1M; `TS.STATIONARITY` 2M samples (since 2.5: 1G sample-passes);
    `TS.AUTOCORRELATION` 100M sample-lags; `TS.XCORR`
    200M; `TS.FEATURES` 100M; `TS.FORECAST` 20,000 samples × models; `TS.AUTOFORECAST` 10,000;
    `TS.BACKTEST` 60,000 samples × models × folds; `TS.OUTLIERS` 1M, 10,000 for `rcf`, 6,000
    for `esd`. The forecast families span ~50× per sample (SES ~0.07 µs, AutoTBATS ~75 µs), so
    those ceilings are a compromise set by the heavy end. They are constants, not configs.
  - `TS.OUTLIERS` is covered too: it has its own inline branch outside the runner and the same
    hazard (`esd` is ~0.8 s at 6k samples and ~9 s at 20k inline). Its inline threshold and its
    ceiling now come from one table (`work_limits`).
  - `TS.FEATURES` got a work measure (`features_work`: samples per feature, samples × lag for
    `pacf`, samples² for `fourier_entropy`), and `fourier_entropy` is capped at 20,000 finite
    samples in **every** context, since the quadratic DFT pins a pool worker as surely as the
    main thread and `TIMEOUT` cannot cancel it. This is a behaviour change: it used to compute
    at any size. The rustfft alternative was not taken (a new direct dependency, and values would
    shift in the low digits). `docs/commands/ts.features.md` "most features are linear" is
    corrected, and every command page and `overview.md` document its ceiling.
  - Tests (`test_ts_analysis_blocking.py`): 11 oversized cases, each refused in Lua and in
    `MULTI` with the rest of the transaction still running; the message names size and limit;
    a range exactly at the limit still runs inline (`pacf:1000` over 100,000 samples) while one
    sample more is refused; the same large range runs outside a transaction; and the
    `fourier_entropy` limit (small range computes, 20,001 refused, non-finite samples not
    counted). Unit tests cover `check_unblockable`, `features_work` and the cap. With the ceiling
    disabled, the 24 refusal and limit tests fail (the work simply runs inline) and the
    outside-a-transaction control passes.
  - **Not tested end to end:** the `TS.PERIODS` (1M) and cheap `TS.OUTLIERS` (1M) ceilings;
    a series that large costs more to build than the case is worth. They share the runner path
    the others exercise. (`TS.STATIONARITY`'s ceiling became reachable through `LAGS` in 2.5 and
    is tested there.)
  - **Not addressed:** the plan's note that the crate's `par_iter` runs on the global rayon pool
    from the GIL-holding main thread on this path. Ranges under a ceiling still do (the
    `TS.FEATURES`/AutoForecast/STL/cross-validation cases in 2.1).

### 2.4 `TS.TREND` inline threshold stalls the main thread ~20 ms per call

- **Where:** [ts_trend.rs:163-165](../../src/commands/ts_trend.rs#L163-L165):
  `INLINE_MAX_SAMPLES = 2_000`, "fitting is ~20 ms here".
- **Why:** 20 ms is roughly 300× the pool handoff cost the inline path exists to avoid, and
  it blocks every other client.
- **Fix:** size the threshold for ~1 ms (≈100–200 samples by the same comment's scaling).
- **Benchmark:** PING p99 while one client loops `TS.TREND` on 2 000 samples, before/after.
- **Done 2026-10-01 (`11228654e`).** `ts_trend.rs` `LIMITS.inline_max` is 400 (was 2,000); the
  doc comment and `docs/commands/ts.trend.md` say so. The numbers differ from the plan's:
  - Measured (release build, default `MODEL AUTO`, fanless 8 GB Mac), the fit is ~0.4 ms plus
    ~2.4 µs a sample: 0.5 ms at 100 samples, 1.2 ms at 500, 4.9 ms at 2,000 (not ~20 ms), so
    ~1 ms is about 400 samples rather than 100–200. A specific `MODEL` is about a third of that
    and shares the threshold. Beyond a few thousand samples it grows faster (~0.2 s at 20k).
  - The pool handoff is not ~70 µs but below what a client round trip resolves: an
    always-background `TS.FORECAST SES` differs by ≤ 0.02 ms between the pool and Lua-inline
    at 100, 500 and 2,000 samples, and `TS.TREND` at 2,100 samples (pool) costs the caller no
    more than at 2,000 (inline). Lowering the threshold costs the caller nothing.
  - Benchmark (one client looping `TS.TREND` on 2,000 samples, another timing PING every
    ~0.5 ms for 6 s; idle PING p50 ≈ 0.15 ms): PING p50 4.15 → 0.11 ms, p90 4.24 → 0.18 ms,
    p99 4.53 → 1.22 ms, max 33.7 → 16.9 ms. The `TS.TREND` caller's median went 4.78 → 4.90 ms
    (1,345 → 1,300 calls in 6 s). The p99 after is noisy: the machine was loaded.
  - Tests (`test_ts_analysis_deadlines.py`, `TestTrendInlineThreshold`): the observable is the
    slowlog, which records only main-thread time; a 2,000-sample fit forced inline through Lua
    appears in it (the control) and the same call handed to the pool does not. A `TIMEOUT 1`
    version was tried first and dropped: deadlines are processed too coarsely to beat ~5 ms of
    work, so it did not distinguish the two paths. With the threshold put back to 2,000 that
    test fails; two more pin that the range still answers with the fit and that a 300-sample
    range stays inline and ignores `TIMEOUT`.
  - **Not re-verified:** two replication tests errored at fixture setup (replica link-up
    timeout) in the final run on the new sources, which took 35 min instead of ~4 with the
    machine saturated by macOS indexing; the other 378 tests in that run passed. They are, in
    `test_ts_store_replication.py`:
    `test_store_merge_with_no_output_leaves_destination[fillgaps]` and
    `test_store_to_the_source_key_is_rejected[TS.AUTOFORECAST]`.
    Three re-runs were stopped at the 30-minute background limit. Unrelated to `TS.TREND` and
    green in an earlier clean run; to be re-run on an idle machine.
  - **Not changed, same defect:** other inline thresholds stall the main thread for several
    ms: `TS.DECOMPOSE` at 2,000 samples ~20 ms (measured), `TS.STATIONARITY` at 50,000 ~9 ms,
    `TS.XCORR` at its 10M-product threshold ~25 ms (the last two extrapolated from probes at
    other sizes). They are not in this item and were left alone.

### 2.5 `TS.STATIONARITY` `work` ignores `LAGS`

- **Where:** [ts_stationarity.rs:127-133](../../src/commands/ts_stationarity.rs#L127-L133)
  passes `values.len()` with `INLINE_MAX_SAMPLES = 50_000`.
- **Why:** ADF lag selection and KPSS are O(n × lags); `LAGS 1000` (the cap) on 50k samples is
  ~10⁸ multiply-adds inline. The command's own `complexity: "O(N*L)"` says so.
- **Fix:** `work = n.saturating_mul(effective_lags + 1)` with the test's default lag when
  `LAGS` is absent, as [ts_autocorrelation.rs:124-127](../../src/commands/ts_autocorrelation.rs#L124-L127)
  already does.
- **Test:** `TS.STATIONARITY s - + TEST kpss LAGS 1000 TIMEOUT 1` on 50k samples should hit
  the pool and time out; today it answers inline.
- **Done 2026-10-01 (`5435484a2`).** `stationarity_work(n, test, lags)` (`ts_stationarity.rs`)
  replaces the bare sample count as the `run_analysis` work, in a new unit, `sample-passes`.
  Measured inline at 50,000 samples (release build), cost is linear in lags as the plan said:
  ADF default lags (36) 6.8 ms, `LAGS 10` 2.4 ms, `LAGS 100` 18 ms, `LAGS 1000` **171 ms**; KPSS
  default (18) 1.5 ms, `LAGS 10` 1.1 ms, `LAGS 100` 5 ms, `LAGS 1000` 43 ms; combined 7.9 ms.
  Differences from the plan:
  - **Weighted, not `n × (lags + 1)`.** ADF costs ~3.5 ns a sample-lag and KPSS ~0.9 ns (~4×),
    which matches the code: ADF's AIC search makes four passes over the data per lag
    (`ADF_PASSES_PER_LAG`), KPSS's autocovariance one. The unweighted formula would either
    under-protect ADF or refuse KPSS four times earlier than it needs.
  - **Default lags** are the crate's own rules, as in the plan: `(n − 1)^(1/3)` for ADF and
    `4 (n / 100)^(1/4)` for KPSS, an explicit `LAGS` held to `n / 2 − 1` / `n / 2` and at least 1
    (anofox-forecast 0.15.10, so a bump of the pinned crate could drift them; the effect would be
    a mis-sized threshold, not a wrong result). The combined test sums both with default lags
    (`LAGS` is rejected with it).
  - **Limits:** `inline_max` 8,500,000, chosen so the combined test with default lags stays
    inline up to exactly 50,000 samples as before (~8 ms); `unblockable_max` 1,000,000,000 (about
    0.9 s, e.g. `TEST adf LAGS 1000` over ~250,000 samples). That replaces the 2M-sample ceiling
    from 2.3, which was extrapolated linearly from 200k and came out low: with default lags the
    cost per sample grows with n, so 2M samples is ~1.1 s, not ~0.5 s.
  - **Behaviour changes:** a `LAGS 1000` call over 50,000 samples used to run inline for ~170 ms
    and now goes to the pool. Single-test calls with default lags move too: KPSS alone stays
    inline up to ~386k samples (it was 50k; ~8 ms there) and ADF alone up to ~57k.
  - **Test:** the plan's `TIMEOUT 1` was replaced by the slowlog, as in 2.4, since a 1 ms
    deadline is processed too coarsely to be a reliable signal. `TestStationarityLagsRouting`
    (`test_ts_analysis_deadlines.py`): `TEST adf LAGS 1000` over 50,000 samples forced inline
    (Lua) shows in the slowlog, the same call outside Lua does not; the helper moved into a
    shared `SlowlogMixin` (also used by the 2.4 test). `TS.STATIONARITY` joined the `MULTI`/Lua
    refusal cases in `test_ts_analysis_blocking.py` (`TEST adf LAGS 1000` over 250,001 samples).
    Eight unit tests: the KPSS lag rule agrees with the crate across sizes and `LAGS` values
    (for ADF the crate reports the lag its AIC search *picked*, so only the bound is asserted),
    the 50,000-sample boundary is unchanged for the combined test, ADF costs more per lag than
    KPSS, and an absurd range saturates. With the old sample-count work, exactly the three new
    integration tests fail and the other 27 oversized/stationarity tests pass; the analysis
    family passes with the change (198 integration, 1613 unit, 13 doc).
  - Docs: `ts.stationarity.md` (the `TIMEOUT` paragraph and `Complexity`) and the `overview.md`
    ceiling row now describe passes. The default-lags inline stall (~8 ms at 50k, the combined
    test) is unchanged; it is the same class of defect as 2.4 and was left alone.

### 2.6 STORE writes and `TS.SANITIZE` bypass compaction

- **Where:** `create_or_update_series_with_samples`
  ([series/utils.rs:264-300](../../src/series/utils.rs#L264-L300)) clears with
  `remove_range` and writes with `TimeSeries::merge_samples`; neither feeds rules
  (`TS.DEL` uses `remove_range_with_compaction`, MADD runs `run_group_compactions`). Yet
  `get_or_create_store_destination` ([series/utils.rs:234-246](../../src/series/utils.rs#L234-L246))
  attaches default compaction rules to a new destination. `TS.SANITIZE` does the same to its
  **source** ([ts_sanitize.rs:150-160](../../src/commands/ts_sanitize.rs#L150-L160)).
- **Why:** with `ts-compaction-policy` set, `TS.INFO dst` lists rules whose child series stay
  empty forever; a user `TS.CREATERULE dst …` is equally dead; a sanitized source leaves its
  buckets computed from the pre-sanitize data (an `avg` that includes the dropped NaN, a
  `count` one too high).
- **Fix:** pick one policy and apply it identically in `TS._STORE` so replicas match: either
  do not create default rules for STORE destinations and document "STORE writes do not feed
  compaction rules", or propagate (`remove_range_with_compaction` plus the ctx-bearing merge).
  For `TS.SANITIZE` the source must propagate.
- **Tests:** `TS.CREATERULE src agg AGGREGATION count 10000; TS.MADD src 1000 1 src 2000 nan
  src 3000 3; TS.SANITIZE src - + POLICY DROP; TS.RANGE agg - +` → bucket 0 should be 2.
  `CONFIG SET ts-compaction-policy …; TS.TREND src - + STORE dst; TS.INFO dst` → either no
  rule, or a fed child series.
- **Done 2026-10-01 (`b0981c02d`).** The "propagate" option was taken: `STORE` destinations and
  `TS.SANITIZE`'s source now feed their compaction rules as `TS.MADD` and `TS.DEL` do.
  - `merge_and_compact` (`series/sample_merge.rs`) merges, then calls `batch_compaction` with
    the accepted samples (stored, rounded values; `prev_last` from before the merge).
    `TimeSeries::merge_samples_with_compaction` is the destination entry point, and
    `overwrite_samples` (2.7) now takes a `ctx` and propagates too. The retention trim follows
    the compaction for the merge, as for `TS.MADD`; the overwrite does none (it cannot advance
    the window). A failure in a rule's destination series is logged, not returned, as `TS.MADD`
    does, since the samples are already stored.
  - `create_or_update_series_with_samples` (`series/utils.rs`) clears with
    `remove_range_with_compaction` and merges with the new method, which covers the overwrite
    clear and the empty-result clear from 2.11. The clear runs up to the series' last timestamp
    rather than `Timestamp::MAX`: the same samples, without asking bucket arithmetic to handle a
    range that ends at the type's limit. `TS._STORE` runs this same function, so a replica's
    rule series follow what the primary wrote.
  - `TS.SANITIZE`'s `write_back` (`ts_sanitize.rs`) uses `remove_range_with_compaction` for
    `DROP` and the propagating overwrite otherwise. `DROP` clears the range and writes the kept
    samples back, so its buckets are recomputed twice; not measured.
  - **The plan's example does not reproduce for NaN.** Compaction aggregators skip NaN (probed:
    `count`, `sum`, `avg`, `first`, `last` over a range with a NaN all ignore it), so a `count`
    bucket is not "one too high" and `DROP` of a NaN changes no bucket. They do not skip
    ±infinity. The fix shows in two cases: an imputed value joins its bucket (`sum` 4 → 6,
    `count` 2 → 3 for `FILL 2`), and a dropped infinity leaves it (`count` 3 → 2, `sum` inf → 4).
    The tests use those.
  - **The default rules come from each node's own `ts-compaction-policy`.** A replica without
    the policy creates its destination without rules, as it already does for `TS.ADD`
    auto-create. Documented in `overview.md`; the tests set it on both nodes.
  - Tests: in `test_ts_sanitize.py`, `DROP` of an infinity under `count` and under `sum`, `FILL`
    under `sum`, and "nothing to fix leaves the buckets alone"; in `test_ts_store_replication.py`,
    `test_store_feeds_the_destinations_compaction_rules` (sanitize, fillgaps, trend, forecast),
    `test_store_overwrite_clears_the_compaction_buckets_too`,
    `test_store_overwrite_with_no_output_empties_the_compaction_buckets` and
    `test_store_merge_adds_to_the_compaction_buckets`. Each STORE test compares the rule's series
    with the buckets of the destination's own samples on both the primary and the replica. With
    the old source, 10 of the 11 fail; "nothing to fix" passes either way and is only a guard.
    The broad regression pass (652 integration, 1613 unit, 13 doc) is green.
  - Docs: `overview.md` (a `STORE` write feeds the destination's rules) and `ts.sanitize.md`.

### 2.7 `TS.SANITIZE` write-back is subject to the source's IGNORE filter

- **Where:** [ts_sanitize.rs:157-158](../../src/commands/ts_sanitize.rs#L157-L158) →
  `normalize_batch` → `SampleDuplicatePolicy::is_duplicate`
  ([types.rs:242-261](../../src/series/types.rs#L242-L261)). With
  `policy_override = Some(KeepLast)` the IGNORE test is *enabled*.
- **Repro:** `TS.CREATE src IGNORE 10000 1.0; TS.MADD src 1000 5 src 2000 nan src 3000 5.5;
  TS.SANITIZE src 2000 3000 POLICY FILL 5.2`. The reply shows 3 samples; `TS.RANGE src - +`
  returns only `1000 5`: after `remove_range(2000, 3000)` the running last sample is
  `1000:5`, so `2000:5.2` (Δv 0.2) and the untouched finite `3000:5.5` (Δv 0.5) are both
  `Ignored`. Rounding is also re-applied to already-stored values.
- **Fix:** re-insert only the changed timestamps (the map the imputation returns) through a
  raw path that bypasses IGNORE and rounding, instead of delete-and-remerge of the whole range.
- **Test:** `test_sanitize_ignore_filter_does_not_drop_rewritten_samples` (the repro).
- **Done 2026-09-30 (`c0aa9691b`, `e9292203c`).** The write-back no longer goes through the
  IGNORE filter, and writes only what changed:
  - `normalize_batch` takes an `IgnoreFilter` (`Apply`/`Bypass`) and the merge core a matching
    `merge_samples_into_series_with`; `TimeSeries::overwrite_samples` is the bypassing entry
    point (retention, rounding and chunk grouping unchanged, stored values replaced whatever the
    duplicate policy). `ts_sanitize.rs` diffs the sanitized result against the range as read
    (`diff_range`) and calls `write_back`.
  - Imputing policies upsert only the timestamps whose value changed and delete nothing; a range
    with nothing missing is left untouched (before, `POLICY ERROR` on clean data could lose
    samples). Differences from the plan:
  - `DROP` still clears the range in one pass and writes the kept samples back, now unfiltered.
    The chunk API has no multi-timestamp removal, so removing each dropped run separately would
    re-encode a chunk per run, a GIL stall on a large `DROP`. The plan's "re-insert the map" was
    this same shape for `DROP`.
  - Rounding is *not* bypassed for imputed values: a series with `DECIMAL_DIGITS 2` should not
    gain `2.3333333` from a `FILLMEAN`. The complaint, re-rounding already-stored values, no
    longer happens since untouched samples are not rewritten.
  - A sanitized sample that is not stored is now an error (before, the per-sample results were
    discarded and the data silently lost).
  - Tests: in `test_ts_sanitize.py`,
    `test_sanitize_ignore_filter_does_not_drop_rewritten_samples` (the repro),
    `test_sanitize_drop_keeps_samples_the_ignore_filter_would_drop`,
    `test_sanitize_fills_a_trailing_gap_under_the_ignore_filter`,
    `test_sanitize_of_clean_data_changes_nothing_under_the_ignore_filter` (`ERROR`, `DROP`,
    `INTERPOLATE`) and `test_sanitize_rewrites_only_the_samples_it_changed`; unit tests in
    `bulk_add.rs` (the bypass, with the filtered `merge_samples` result alongside) and
    `ts_sanitize.rs` (`diff_range`). All but `…_rewrites_only_the_samples_it_changed` fail on the
    old write-back and pass now; that one passes either way and is only a guard.
  - Docs: the "Source rewrite" note in `docs/commands/ts.sanitize.md` describes the new write.

### 2.8 `TS.SANITIZE` partial-mutation and error-after-replicate window

- **Where:** [ts_sanitize.rs:150-170](../../src/commands/ts_sanitize.rs#L150-L170).
- **Why:** `remove_range` succeeds, then a `merge_samples` failure leaves the range deleted
  with an error reply. `replicate_verbatim()` and the `ts.sanitize` event fire before
  `dest.write_unreplicated`, so a destination failure (e.g. `DUPLICATE_SERIES` from a `METRIC`
  that collides in the label index) returns an error after the source was rewritten and the
  command queued for replication. The replica converges (same deterministic error) but the
  client is told a command failed that mutated.
- **Fix:** validate everything that can fail on the destination (type, METRIC collision)
  before touching the source; order compute → write destination → write source → replicate.
- **Done 2026-09-30 (`e9292203c`).** The order is now compute → validate → write destination →
  write source → replicate → notify, so an error reply never follows a replicated command and a
  rejected command has changed nothing.
  - The only failure a client could still cause after the source was rewritten was
    `DUPLICATE_SERIES` from a `METRIC` another series holds: `CHUNK_SIZE` is validated when the
    clause is parsed and default-compaction child failures are logged and skipped, not
    returned. `StoreTarget::check_destination_writable` runs it up front through
    `check_series_creatable` (`series/utils.rs`), which shares `check_metric_name_unique` with
    `create_series` so the two cannot drift.
  - The check only runs when a write follows: an empty result never creates the destination, so
    a colliding `METRIC` there still succeeds (as before), and an existing destination ignores
    `METRIC` (it only applies at creation).
  - Side effect: the destination's `ts.del`/`ts.add` events now fire before `ts.sanitize`.
  - **Not fixed:** a failure *inside* a write (`remove_range`/`merge_samples`, internal errors
    only) can still leave the first key written and nothing replicated. 2.7 narrows it, since
    the imputing policies now delete nothing, but `DROP` keeps its clear-then-write-back gap.
  - Tests: `test_store_metric_collision_fails_before_changing_anything`,
    `test_store_metric_collision_is_ignored_when_nothing_is_written` and
    `test_store_metric_is_ignored_for_an_existing_destination` in `test_ts_sanitize.py`;
    `test_sanitize_rejected_store_is_not_replicated` in `test_ts_store_replication.py`. The
    first and last fail on the old order; the other two pin the behaviours above.

### 2.9 `TS.SANITIZE MOVINGAVERAGE` window is uncapped and inline

- **Where:** [ts_sanitize.rs:201-210](../../src/commands/ts_sanitize.rs#L201-L210) accepts
  any odd window; `impute_moving_average`
  ([imputation.rs:300-326](../../src/analysis/forecasting/imputation.rs#L300-L326)) scans
  `[i−half, i+half]` per missing sample, up to 3 passes, with the GIL held (SANITIZE does not
  go through `run_analysis`), and every replica repeats it.
- **Fix:** cap the window (a constant like the 100k FILLGAPS grid cap) and/or a running-sum
  implementation (O(n) per pass).

### 2.10 `TS.BACKTEST` size options wrap in release builds; work is uncapped

- **Where:** `parse_count` ([ts_backtest.rs:469-488](../../src/commands/ts_backtest.rs#L469-L488))
  accepts any `i64 ≥ minimum`; `run_backtest` (lines 221-239) hands the values to the crate's
  `CvFoldGenerator`, whose `min_series = min_train + purge + gap + horizon` and
  `last_origin = series_len − gap − horizon` are unchecked (`Cargo.toml` has no `[profile]`,
  so release builds wrap).
- **Why:** replicating the generator's arithmetic on a 100-sample series, `HORIZON 5`:
  `PURGE 9223372036854775807 GAP 9223372036854775807` → first fold `train 0..97, test
  95..100`, a *successful* reply with leaked metrics; `INITIAL_WINDOW i64::MAX GAP i64::MAX`
  → every fold reports `IndexOutOfBounds` as a per-model error. Under overflow checks
  (ASAN/debug) the same input panics and becomes `INTERNAL_ERROR`. Separately, `MODELS`
  count is uncapped for FORECAST and BACKTEST (`prepare_model_specs`,
  [model_parser.rs:65-76](../../src/analysis/forecasting/parsers/model_parser.rs#L65-L76)),
  `N_FOLDS` is uncapped with `STEP 1` allowed, so `STEP 1 N_FOLDS 1e8` on 1M samples is O(n²)
  with ~1M `FoldResult`s and a 1M-element reply; and `TIMEOUT` cannot cancel a running job.
- **Fix:** on the main thread once `series.len()` is known, require
  `initial_window.checked_add(purge)…checked_add(horizon) ≤ series.len()` (reuse the "not
  enough data" error); cap models per request (e.g. 16) and `N_FOLDS` (e.g. 1000); consider
  an `AtomicBool` timed-out flag passed into `compute` so `evaluate_model` stops between
  folds and `process_models` between models.
- **Tests:** the two repro inputs above assert an argument error; `N_FOLDS 1001` and 17
  models assert the cap errors; a `TIMEOUT 1` backtest with `STEP 1 N_FOLDS 10000` on 20k
  samples returns the deadline error and `wait_for_analysis_pool_idle` completes in bounded
  time.

### 2.11 STORE overwrite with an empty result leaves stale destination data

- **Where:** [series/utils.rs:264-266](../../src/series/utils.rs#L264-L266) returns early on
  `samples.is_empty()`. Reached from FILLGAPS (no gaps), SANITIZE `DROP` of an all-missing
  range, and TREND when AutoTrend has no winner (`fitted_trend()` empty).
- **Why:** `TS.SANITIZE src - + POLICY DROP STORE clean` on an all-NaN range returns 0 and
  `clean` still holds its previous samples although the mode is "overwrite". The docs say
  "left untouched" one paragraph after "the destination is cleared before writing".
- **Fix:** in overwrite mode clear an existing destination even when nothing is written (and
  replicate the clear); keep "not created when missing".
- **Test:** `test_store_overwrite_with_no_output_clears_destination` for all three commands.
- **Done 2026-10-01 (`4ea9b87bc`).** In overwrite mode an empty result now empties an existing
  destination (`create_or_update_series_with_samples`, `series/utils.rs`) and fires `ts.del`;
  the existing `changed` flag replicates it: `TS.FILLGAPS` as `TS._STORE key "" …` (the
  replica's decoder already takes an empty payload), `TS.SANITIZE` by re-running. A missing
  destination is still not created, `MERGE` still changes nothing, and clearing an
  already-empty destination replicates nothing. `TS.FILLGAPS` lost its `gaps_filled == 0` early
  return, which kept the fix from reaching it. Differences from the plan:
  - `TS.TREND` cannot reach the empty case: `AutoTrend::fit_trend` returns an error when no
    candidate wins (the module propagates it before any store), so `fitted_trend()` is never
    empty, and `HORIZON 0` is rejected, so the forecast commands cannot either. Tested for
    `TS.FILLGAPS` and `TS.SANITIZE` only; the test helper says why `TS.TREND` is absent.
  - Behaviour change: `TS.FILLGAPS … STORE <non-series key>` with no gaps used to succeed with
    `0` and now fails with `WRONGTYPE`, as a non-empty write does (`TS.SANITIZE` already did).
    `MERGE` with an empty result still skips the check.
  - The reply stays `0` when the destination is cleared: it counts samples written.
  - Tests (`test_ts_store_replication.py`): `test_store_overwrite_with_no_output_clears_destination`
    (replica emptied too), `…_merge_with_no_output_leaves_destination`,
    `…_with_no_output_does_not_create_destination`,
    `test_store_overwrite_of_an_empty_destination_is_not_replicated` and
    `…_with_no_output_rejects_a_destination_of_another_type`. On the old code the two clear
    tests and the `TS.FILLGAPS` wrong-type test fail; the rest pin behaviour that must not
    change and pass either way.
  - Docs: `ts.fillgaps.md` and `ts.sanitize.md` no longer say the destination is "left
    untouched".

### 2.12 Non-finite input handling is inconsistent across commands

- `TS.DECOMPOSE` ([ts_decompose.rs:51-52](../../src/commands/ts_decompose.rs#L51-L52)) feeds
  NaN into LOESS and replies `nan` components; robust STL sorts the remainder with
  `partial_cmp(..).unwrap_or(Equal)`, not a total order (a panic is permitted by the `sort_by`
  contract, caught as `INTERNAL_ERROR`).
- `TS.PERIODS` with a NaN in range returns `[]` (the Welch spectrum is all-NaN, no peak
  passes), i.e. "no seasonality", and `TS.DECOMPOSE … SEASONALITY AUTO` then reports
  "at least one seasonality period is required". Also `detect_periods` returns `[]` below 6
  samples while [ts_periods.rs:86-90](../../src/commands/ts_periods.rs#L86-L90) errors only
  below 4.
- `TS.TREND` ([ts_trend.rs:120-121](../../src/commands/ts_trend.rs#L120-L121)) feeds NaN into
  the models; AutoTrend may end with no winner, so STORE silently writes 0 samples and
  `METRICS` fails with a length mismatch.
- `TS.STATIONARITY` rejects; FEATURES/STATS filter; `seasonally_adjust` interpolates.
- **Fix:** one rule (reject with the STATIONARITY message, or interpolate and restore NaN in
  `residual` only) applied in every `parse_series_range_samples` caller; align the PERIODS
  floor to 6; document.
- **Tests:** one NaN sample through DECOMPOSE, PERIODS, TREND, FORECAST, AUTOFORECAST,
  BACKTEST; PERIODS with 4–5 samples.

### 2.13 Operational and documentation defects with user-visible effect

- **`ts-analysis-timeout` description** ([config.rs:1150](../../src/config.rs#L1150)) contains
  a 23-space run and names only `TS.FORECAST`, `TS.AUTOFORECAST`, `TS.BACKTEST`, while
  `AnalysisTimeout::resolve` applies it to every `run_analysis` command. Emitted verbatim by
  `TS._DEBUG LIST_CONFIGS VERBOSE` and copied into `docs/commands/ts._debug.md`. Add
  `assert!(!desc.description.contains("  "))` to the existing config test.
- **`TS.OUTLIERS` contradicts the new AGENTS.md rule.** It still hand-rolls `block_client` +
  global-pool `spawn` ([ts_outliers.rs:171-172](../../src/commands/ts_outliers.rs#L171-L172)):
  no `catch_unwind` (a panic in a rayon-spawned job aborts the process), no `TIMEOUT`, not
  counted by `ANALYSIS_JOBS`. Port it to `run_analysis` or amend the rule.
- **Dead allocator cfg** ([lib.rs:213-227](../../src/lib.rs#L213-L227)): `all(test, doctest)`
  is never true, so the `System` arm is unreachable and bare `cargo test` SIGABRTs
  ("Critical error: the Valkey Allocator isn't available"). The documented
  `--features enable-system-alloc` path works; delete the dead arm and say the feature is the
  only supported way to link outside a server.
- **`store_anchor` always takes the fallback** ([forecast_utils.rs:67-75](../../src/commands/forecast_utils.rs#L67-L75)):
  the crate's `TimeSeries::univariate` never sets `frequency()`, so every STORE allocates an
  n-element `Vec<i64>` and `compute_median_step_ms` allocates and sorts the diffs, O(n log n)
  on the main thread before the client is blocked. Drop the dead branch; use
  `select_nth_unstable`; decide whether STORE should share FILLGAPS'
  `infer_frequency_from_samples` (modal/GCD) and say so in both docs.

---

## 3. Suggestions

- **Pin rationale** `Cargo.toml:14` says `Cargo.lock` is not committed; it is tracked. The
  exact pin is still right (numeric drift across 0.x), fix the comment.
- **Arity** `TS.AUTOFORECAST` declares `arity: -5`; the shortest valid call is 6 tokens.
  Use `-6`, drop the duplicate `args.len()` guards (same duplication in FORECAST and
  BACKTEST), update `tests/test_ts_command.py`.
- **Key-spec flags** all five STORE specs say `[ReadWrite, Update]`; the write creates and
  clears, so add `Insert`. `StoreTarget::new` already checks `UPDATE | INSERT`.
- **`TS.FILLGAPS`/`TS.TREND` without STORE are `Write` + `DenyOOM` + `@write`**: refused on
  read-only replicas, under OOM, and for `+@read`-only users. TREND's comment acknowledges it;
  FILLGAPS' doc says "no side effects without STORE" with no caveat. Document, or split.
- **`TS.AUTOFORECAST MODELS TBATS` / `MSTL` without `SEASONALITY`** blocks the client and
  then fails with `ConvergenceFailure`; validate on the main thread.
- **`METRICS` turns a successful forecast into a hard error** on an interior NaN fitted value
  or a length mismatch ([forecast_utils.rs:168-206](../../src/commands/forecast_utils.rs#L168-L206));
  `ForecastMetrics::Unavailable` → `metrics: null` already exists for GARCH and would fit.
- **`XCORR peak_lag`** ends at `+maxLag` when every lag is null
  ([ts_xcorr.rs:176-179](../../src/commands/ts_xcorr.rs#L176-L179)); pick 0 or null and pin.
- **`TS.STATS`** returns 0 rather than null for statistics over zero finite values
  ([stats.rs:31-36](../../src/analysis/forecasting/stats.rs#L31-L36)); it is also inline and
  unbounded (full-series materialisation, sorted copy for the median, an `IntSet` of all bit
  patterns). Use `select_nth_unstable` and/or route through `run_analysis`.
- **`TS.STATIONARITY LAGS 0`** is silently clamped to 1 by the crate; a perfectly linear
  range gives `se == 0` → null statistic with `conclusion "non_stationary"`. Reject `LAGS 0`;
  handle the degenerate regression explicitly.
- **`reply_with_statistic`** passes ±Inf through while the docs say only NaN is special-cased
  (`abs_energy`/`sum_values` overflow, `range` of extreme values). Decide and document.
- **Trailing-argument errors differ per command** (`unknown argument 'x'`, `Unknown argument:
  x`, `unrecognized option`, `unrecognized argument 'X'`, `invalid argument`); PERIODS'
  `reject_extra_args` call is unreachable. Use `reject_extra_args` everywhere; make DECOMPOSE
  option order independent like STATIONARITY.
- **`TS.AUTOCORRELATION … 0 AGGREGATED mean`** surfaces as "autocorrelation computation
  returned NaN"; validate `lag ≥ 1` for AGGREGATED up front.
- **DECOMPOSE vs OUTLIERS decomposition drift**: `SEASONALITY AUTO` resolves up to 5 periods
  while explicit input caps at 4; DECOMPOSE uses the crate's default 2 MSTL iterations while
  `seasonally_adjust` uses 5. Share one builder.
- **`DenyOOM` policy**: the seven new read-only commands carry it, `TS.OUTLIERS` and
  `TS.RANGE` do not. Pick one policy.
- **`parse_store_clause`** clones the whole remaining argument iterator to recover consumed
  tokens; record `remaining`/`consumed` and re-slice. `parse_store_options` accepts
  `ON_DUPLICATE`, which the clause doc omits.
- **`TS.SANITIZE` rewrites and replicates even when nothing changed** (`POLICY DROP` on clean
  data): short-circuit when the imputation map is empty.
- **`remove_range` early return** ([time_series.rs:921](../../src/series/time_series.rs#L921))
  duplicates what `overlaps()` already does for an empty series.
- **`ClusterMap::local_node_id`** is now written but never read after `get_local_shard`
  switched to the `is_local` scan.
- **`AutoForecast::with_config(options.config.clone())`** ([ts_autoforecast.rs:217](../../src/commands/ts_autoforecast.rs#L217)):
  `options` is owned; move it.
- **`parse_timeseries_for_forecast`** maps every error to "Failed to prepare time series for
  forecasting"; the only real failure is a timestamp outside chrono's range (~±8.2e15 ms),
  which is a storable value. Include it in the message; the docs call it an internal error.
- **Docs:** `ts.forecast.md` says `Log` requires strictly positive input; the crate shifts by
  `−min + 1` instead (BoxCox is the one that rejects). `ts.forecast.md` lacks the hash-tag /
  CROSSSLOT paragraph that `ts.autoforecast.md` has. `ts.features.md` "doubled prefix" note is
  stale and its complexity sentence is wrong (§2.3). `ts.stationarity.md` LAGS range,
  `ts.xcorr.md` all-null `peak_lag` and the precision caveat until §1.1 lands,
  `ts.periods.md` 4-vs-6 floor, `ts.fillgaps.md` `VALUE` error text (code returns
  `parse_float`'s message). `tests/test_ts_trend.py::test_store_on_existing_key_merges` passes
  `MERGE` explicitly; there is no TREND test that plain STORE overwrites. `types.rs:125`
  silences a doctest with `ignore` instead of fixing the path.

---

## 4. Test gaps

- Inline-vs-background equivalence: `tests/test_ts_analysis_blocking.py` compares only
  `type(result)`; assert equality, and add `TS.STATIONARITY` and `TS.STATS` to the list.
- Accepted side of every cap boundary (lag 1000 for `PARTIAL`/`AGGREGATED`, `STATIONARITY
  LAGS 1000`, `XCORR MAXLAG`, `pacf:1000`); only rejections are tested.
- NaN/inf samples through FORECAST, AUTOFORECAST, BACKTEST, DECOMPOSE, PERIODS, TREND (§2.12);
  XCORR with NaN in only some lag windows.
- "Last clause wins" for FORECAST (`MODELS`, `TRANSFORMS`, `HORIZON`, `STORE`) and BACKTEST;
  only AUTOFORECAST has it.
- `STORE` combined with `TRANSFORMS` (stored samples in original units).
- RESP3 null fields for `TS.STATS kurtosis`, `TS.XCORR values`, `TS.FEATURES
  autocorrelation_10` (asserted `None` in RESP2 only).
- `TS.STATS key from` (single bound, `+` implied); `TS.FEATURES FEATURE
  quantile:0.50,quantile:0.5` dedupe; `Difference(0)` accepted as a no-op.
- TOCTOU on the background STORE path: `DEL dst` / `SET dst x` / `FLUSHALL` while a
  20k-sample `TS.TREND … STORE dst` job runs (today: re-created / WRONGTYPE / re-created in
  the empty DB). Defensible, but pin it.
- `TS.FILLGAPS` / `TS.TREND` without STORE on a replica (currently `READONLY`).
- §1.2, §2.6, §2.7, §2.11 each name their test.

---

## 5. Verified OK

- Command registration: every new command has `#[command]` + `acl_categories!` (13 pairs, the
  pinning test passes); readers `ReadOnly, DenyOOM`; STORE-capable `Write, DenyOOM,
  GetkeysApi`; `ts._store` positional with `write deny-oom`, gated like `TS._RESTORE`.
- `STORE_SEARCH_START = 4` is valid for every layout: all five are `cmd key from to …` and
  `parse_timestamp_range` fails on a non-timestamp end token, so `STORE` can never sit at
  argv[3]; matches every `Keyword{startfrom: 4}` spec; all five handlers call
  `report_store_key_positions` before parsing. `tests/test_ts_analysis_cme.py` connects to
  the owning primary and asserts the server's own `CROSSSLOT`, which does exercise the
  GetkeysApi path. `TS.XCORR` is routed by the legacy 1–2 range and tested cross-slot.
- `is_timed_out` is checked under the GIL before every STORE write; the timeout callback runs
  on the main thread so it cannot interleave; after a timeout the server has detached the real
  client, so the job's late reply is discarded. `ctx.replicate` from the locked thread-safe
  context propagates with the blocked client's DB (replication test covers it).
- `TS._STORE` gating excludes client 0, the AOF client and `REPLICATED`; raw STORE tokens are
  re-parsed by the same `parse_store_options`; `METRIC` consumes its value unconditionally so
  it cannot be confused with `MERGE`.
- FILLGAPS grid: cap computed in `i128` before allocation, `freq_ms` bounded, `checked_add`
  in the loop, inverted range selects nothing. Forecast STORE timestamps use checked
  arithmetic and the final timestamp is validated on the main thread.
- Both runner paths catch panics; no raw reply is written before `compute` can fail.
- Docs/index/README list all 13 public commands with resolving links; no stale
  `WITH_METRICS`; the EDA guide's command syntax matches the parsers (reply shapes not
  checked).
- Test harness: no fixed sleeps (only the 50 ms poll in `wait_for_analysis_pool_idle`), no
  elapsed-time asserts, every `CONFIG SET` restored, one server per test class.
- `.gitignore` only lost duplicate lines; `.idea` is still ignored.
- `chrono` and the `strum` move to runtime deps are required by the crate's `TimeSeries` and
  the parser derives. `anofox-forecast` has no `build_global`, `thread::spawn`, or global
  state; ~307 `.unwrap()` outside tests, which the `catch_unwind` guards cover.

---

## 6. Evidence and assumptions

- **§1.1** measured with a scratch crate depending on `anofox-forecast = "=0.15.10"`
  (`parallel`, `seasonal-detection`), calling `simd::correlation` and
  `features::autocorrelation` on the series described, against an f64 two-pass Pearson.
- **§2.1** measured with a scratch crate on `rayon-core = "=1.13.0"`: one spawned job that
  `scope`s N sub-tasks of 50 ms, followed by 2N spawned 1 s jobs; the first job's completion
  time was 1.058 s (2 threads) and 1.055 s (4 threads). Not run inside the module, so the
  magnitude in a live server depends on job mix.
- **§2.10** reproduced by replicating the fold generator's arithmetic with
  `rustc -O -C overflow-checks=off`, not through the command.
- The pytest suites were not executed (`./build.sh` excluded from the review); §2.6, §2.7,
  §2.8, §2.11 and §2.12 come from code reading with the repro commands given.
- Inline timings quoted (TREND 20 ms, DECOMPOSE seconds) are the branch's own comments.
- Valkey server internals (`RM_Replicate` from a thread-safe context, timeout callback
  ordering) are from API knowledge plus the passing replication tests, not from reading
  `module.c`.
