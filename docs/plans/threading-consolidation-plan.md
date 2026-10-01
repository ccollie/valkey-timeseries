# Threading consolidation across `unstable`, `promql` and `feat/forecasting` (plan)

**Status:** Phase A implemented 2026-10-01 on `refactor/threads-consolidation` (from `unstable`
30dc029c0, uncommitted at the time of writing). Phases B and C (the two rebases) and the A/B
measurements of Phase D are not started. Analysis of `unstable` 30dc029c0, `promql` 5481d941c and
`feat/forecasting` 4c15f01c9.

Deviations from the text below, in Phase A:

- **No `analysis_lane_stats()`.** Nothing on `unstable` reads the lane's load, so it is left for
  the forecasting rebase (`TS._DEBUG ANALYSIS_JOBS`) rather than shipped as dead code.
- **`run_on_analysis_lane` refuses a deny-blocking client** with an error instead of relying on
  the caller's check alone: blocking one is a server assert, so the helper guards it too.
- **`GilToken` is public within the crate** (`common::threads::GilToken::take`), and `GilGuard`
  is a token plus a `DetachedContextGuard`, so any other guard over a raw lock call is checked
  the same way.
- **TS.OUTLIERS busy path has no integration test** (as on `promql`): filling 2–8 workers and a
  256-deep queue deterministically costs more than it proves; the executor unit tests cover
  rejection. The panic-reply and MULTI-inline paths do have integration tests.
- **The JOIN/LABELSTATS A/B (A3) was not run.**

**Goal.** Land on `unstable` the smallest threading substrate that (a) makes the deadlock and
starvation rules enforceable there, (b) fixes the hazards `unstable` has today, and (c) lets both
feature branches rebase with their threading deltas reduced to feature-specific pools and lanes.
Everything PromQL- or forecasting-specific stays on its branch.

The rules themselves are not new: they are the R1–R5 set already implemented on `promql`
(`src/common/threads/mod.rs` there, and [threading-refactor-plan.md](threading-refactor-plan.md)).
This plan is about which parts of that implementation are generic, how the forecasting model maps
onto them, and in what order to move.

---

## 1. Where each branch stands

Merge bases: `promql` forked from `unstable` at 54ad464de (the commit that gave orx-parallel a
persistent rayon-core pool). `feat/forecasting` forked at 82703430a, **before** that commit, so it
still runs the older pool arrangement.

| | `unstable` | `feat/forecasting` | `promql` |
|---|---|---|---|
| orx-parallel runner | `persistent-pool-rayon`: one rayon-core pool sized by `ts-num-threads` (`ORX_NUM_THREADS`, capped at cores) | `transient-pool`: **OS threads spawned per `.par()` call** | same as `unstable` |
| rayon's global pool | alive, unsized (built lazily at core count); used by `threads::spawn`, `join`, `spawn_scoped` | built explicitly by `ThreadPoolBuilder::build_global()` sized by `ts-num-threads`; used by `spawn`, `join`, and anofox's `par_iter` | **retired**: `spawn`/`spawn_scoped`/`join_scoped` deleted, `join` routes to orx's pool |
| extra compute pools | none | `ANALYSIS_POOL` (rayon, N threads): whole analysis jobs via `spawn_analysis` (unbounded queue) plus in-job fork-join via `map_on_current_pool` and anofox `parallel` | `EVAL_POOL`, `MATERIALIZE_POOL` (pinned rayon pools, roles set in `start_handler`) |
| blocking threads | `spawn_background` (plain thread) for trim / stale ids / index sweep / ASM; `BoundedExecutor` v1 (`LazyLock`, `sync_channel`, `try_spawn`) for two fan-out lanes | same as `unstable` | `spawn_background` (+ role, panic log), `spawn_background_single` + `SingleFlight`; `BoundedExecutor` v2 (`const` static, lazy start, `Capacity::{Fixed,Dynamic}`, `try_reserve`/`Reservation`, `stats`) for fan-out lanes, `ANALYSIS_EXECUTOR` (TS.OUTLIERS), `QUERY_WORKERS`; the selector processor thread |
| GIL entry | raw `MODULE_CONTEXT.lock()` at 7 sites | raw `MODULE_CONTEXT.lock()` at 7 sites, plus `ThreadSafeReplyContext::lock()` (raw `ThreadSafeContextLock`) from analysis-pool workers | `LockGil::lock_gil()` only; `GilGuard` marks the thread; R1/re-entry checked at lock time |
| rule enforcement | prose in doc comments | prose + a paragraph in AGENTS.md | `clippy.toml` `disallowed-methods` on every raw pool/GIL entry point; runtime checks R1–R3 in release builds (log once per rule) and debug (panic) |
| parallel entry points | bare `.par()` / `.into_par()` / `.iter_into_par()` / `.par_mut()` at ~25 sites | same | `*_rayon` adapters via `ModulePool` (pinned-aware), `into_par_on(pool)`; bare forms forbidden |
| TS.OUTLIERS | `block_client` then `threads::spawn` on rayon's global pool: no back-pressure, no `catch_unwind` (a panic aborts the process), client blocked before admission | same, plus `WorkLimits` ceilings and inline for deny-blocking clients | `ANALYSIS_EXECUTOR` lane: `try_reserve` before `block_client`, inline for deny-blocking clients, busy → error reply |
| blocked-client safety | a dropped job leaves the client blocked forever | `TimeoutWatcher` + `block_client_with_timeout` (server-side deadline, `is_timed_out` flag) | `ThreadSafeReplyContext` is `Send`-only and answers `NO_REPLY_WRITTEN` on drop |
| background-task guards | hand-rolled `TrimRunGuard`, `acquire_run_lock`; **`optimize_indices` has none** and runs as a rayon pool job | same | `SingleFlight` for all three; all three on `spawn_background_single` |

### Hazards on `unstable` today (what the consolidation fixes there)

1. **Two compute pools, one unsized.** `threads::join` (TS.JOIN handler on the main thread,
   TS.LABELSTATS fan-out) and `threads::spawn` go to rayon's global registry, which nobody sizes
   or owns; orx's pool is the one `ts-num-threads` controls. (`promql` plan §2.1.)
2. **TS.OUTLIERS** (`src/commands/ts_outliers.rs:169-194`): unbounded spawns; a panic in a
   `rayon_core::spawn` job aborts the server; the client is blocked before the job is accepted,
   and a job that is dropped never answers it.
3. **`optimize_indices_for_db`** (`src/series/tasks/optimize_indices.rs`): a pool job that takes
   the postings write lock, started on every cron tick with no single-flight guard, so a slow run
   overlaps the next and the cursor restarts the db from the top.
4. **No reply-on-drop**: any blocked client whose job is dropped (executor full after the client
   was blocked, a panic) hangs until it disconnects.
5. **Rules are prose** across seven doc comments; nothing stops the next `MODULE_CONTEXT.lock()`
   inside a `.par()` closure.
6. Dead code: `spawn_scoped`, `join_scoped`, `spawn_with_context`, `try_spawn_in_context`,
   `run_on_main_thread`, `run_on_main_thread_with_context` have no callers outside the module.

### Hazards on `feat/forecasting` (what its rebase must resolve)

1. **Pre-54ad464de pool model.** `transient-pool` spawns OS threads per `.par()`; AGENTS.md there
   tells contributors so. Rebasing onto `unstable` takes the persistent pool automatically
   (Cargo.toml conflict → take `unstable`), and the AGENTS.md sentence becomes wrong.
2. **Analysis-pool fork-join latency** ([review §2.1](forecasting-branch-review-2026-09-30.md)):
   a rayon worker waiting on its own `scope` pops a *whole new* analysis job from the injected
   queue and runs it to completion first; measured 50 ms → 1.06 s. Still open.
3. **No admission control** on `spawn_analysis` (review §2.2). Still open.
4. **GIL from a pool worker.** `AnalysisCtx::with_locked_context` (STORE writes in
   `forecast_utils.rs:98`, `ts_trend.rs:149`) takes the GIL on an `ANALYSIS_POOL` rayon worker.
   Not a live deadlock today (no GIL holder ever waits on that pool), but it is exactly the R1
   shape, and `promql`'s runtime check would flag it on sight.
5. **anofox `parallel`** builds the crate with `rayon`, whose `par_iter` runs on rayon's global
   registry when called off-pool — including from the inline path on the GIL-holding main
   thread (review §2.3, "not addressed"). After consolidation that would be the only remaining
   user of a pool the module does not own or size.
6. **TS.OUTLIERS** is still the hand-rolled spawn (review §2.13).

`promql` has no open threading hazards; its plan's "not done" list is measurement and docs
(TS.JOIN/LABELSTATS A/B, thread-count comparison, ASAN pass, INFO fields for lanes).

---

## 2. Target model on `unstable`

Stated generically, so it holds for a build with neither PromQL nor forecasting.

**Threads**

| Role | What | May block | May take the GIL |
|---|---|---|---|
| `Main` | the server's main thread | no | holds it |
| `Blocking` | `BoundedExecutor` lane workers, `spawn_background` threads, any per-feature processor thread | yes | yes |
| `SharedPool` | orx's rayon-core pool, sized by `ts-num-threads` | no | no |
| `BlockingPool` | a pinned pool whose jobs may wait for a blocking thread's answer (PromQL: the evaluation pool) | on blocking threads, and cold on an isolated pool | no |
| `IsolatedPool` | a pinned pool whose jobs never block (PromQL: the materialization pool) | no | no |
| `ForeignPool` / `Other` | a rayon pool the module did not build (tests); server I/O threads | — | — |

On `unstable` alone only the first three exist at runtime. The two pinned roles are in the enum
so the rules and checks are stated once; a feature branch adds a pool, not a rule.

**Rules** (unchanged from `promql`; R2 renamed to its generic form)

- **R1.** A pool job never takes the GIL and never blocks on anything but jobs of its own pool.
  `BlockingPool` is the sole exception. Waiting on a data lock (an index's postings) is allowed
  when R5 bounds it.
- **R2.** A GIL holder waits only on a pool that obeys R1 without exception: the shared pool or
  an isolated pool, never a blocking pool.
- **R3.** Only blocking threads and blocking-pool workers wait for another thread's answer, and
  never while holding the GIL.
- **R4.** Parallel work started on a pinned worker stays on its pool: enter through the
  `*_rayon` adapters and `threads::join`, never the bare orx/rayon entry points.
- **R5.** No lock guard (GIL, postings, series) is held across a wait on a pool except where R2
  allows the GIL. Lock order is GIL then postings, never the reverse.

**Enforcement**: `clippy.toml` `disallowed-methods` (as on `promql`, unchanged); runtime checks
in `gil.rs` (`lock_gil` → R1 + re-entry; `check_may_wait_on_blocking_pool` → R2;
`check_may_block` → R3), release builds log once per rule with a backtrace, debug builds panic.

**Lanes** (bounded blocking work; one `BoundedExecutor` static each, domain-owned):

| Lane | Owner | On `unstable` | Added by |
|---|---|---|---|
| `ts-fanout-local`, `ts-fanout-request` | `src/fanout/workers.rs` | yes | — |
| `ts-analysis` | `src/commands/analysis_runner.rs` (new, small) | yes: TS.OUTLIERS | forecasting layers `WorkLimits`, `TIMEOUT`, `AnalysisCtx` on top |
| `ts-promql-query` | `src/promql/engine/query_workers.rs` | no | promql |

**Retired everywhere**: rayon's global registry (`spawn`, `spawn_scoped`, `join_scoped`,
`build_global`), the forecasting `ANALYSIS_POOL`, `spawn_analysis`, `map_on_current_pool`,
`analysis_jobs_in_flight`, and orx's `transient-pool` runner.

---

## 3. The minimal set to move to `unstable`

Taken from `promql`; source commits listed so the port can cherry-pick or re-express. Each item
is generic (no PromQL type appears in it once the two role names are generalized).

| # | Item | From (promql) | Why it is in the minimal set |
|---|---|---|---|
| M1 | `threads/role.rs` (`ThreadRole`, `set_thread_role`, `current_role`, `on_pinned_worker`, `on_pool_worker`) with `BlockingPool`/`IsolatedPool` in place of `EvalPool`/`MaterializePool` | a37c91ca9 | every check is stated in terms of the role |
| M2 | `threads/gil.rs` (`LockGil`, `GilGuard`, `holds_gil`, `check_may_block`, `check_may_wait_on_blocking_pool`, `violated` logging), plus a reusable `GilToken` (see §4 A1) so a blocked client's thread-safe context can take the GIL under the same checks | a37c91ca9 | the only way to catch the next R1 break before it deadlocks a server |
| M3 | `clippy.toml` as on `promql`, `#![allow(clippy::disallowed_methods)]` in `threads/mod.rs`, and `cargo clippy` already runs with `-D clippy::all` in CI (`disallowed_methods` is a `style` lint, so it is in) | a37c91ca9, 1882bfa8c | makes M1/M2 unbypassable |
| M4 | `threads/orx_pool.rs` (`ModulePool`, `ParWithPool`, `par_rayon`/`par_mut_rayon`/`into_par_rayon`/`iter_into_par_rayon`) and the ~25 call-site renames; **not** `RayonPool`/`into_par_on` | 639efe8b6, 45457b081, 1882bfa8c | the clippy gate on bare `.par()` needs a sanctioned replacement; renaming on `unstable` first turns ~30 files of future rebase conflicts into identical hunks |
| M5 | `BoundedExecutor` v2 (`const fn new`, lazy start, `Capacity`, `Rejected`, `try_reserve`/`Reservation`, `stats`, worker role, panic logging) and `lane_workers()`; `src/fanout/workers.rs` ported to statics | a37c91ca9, 15a743851 | reserve-before-block is the fix for hazards 2 and 4; `stats` is what replaces forecasting's job counter |
| M6 | `spawn_background` with role + `catch_unwind` logging; `SingleFlight` + `spawn_background_single`; `series_trim`, `stale_ids`, `optimize_indices` ported | a37c91ca9, 1c0f8a332, 1882bfa8c | fixes hazard 3, deletes two hand-rolled guards |
| M7 | `threads::join` role-aware over orx's pool; delete `spawn`, `spawn_scoped`, `join_scoped`, `spawn_with_context`, `try_spawn_in_context`, `run_on_main_thread*` | a37c91ca9 | retires the unsized pool (hazard 1, 6) |
| M8 | `ThreadSafeReplyContext` `Send`-only, `answered` flag, reply `NO_REPLY_WRITTEN` on drop (`BlockedClient` too) | ae9fbb824, 63be820c7 | hazard 4; forecasting's `TimeoutWatcher` layers on this cleanly |
| M9 | TS.OUTLIERS on an `ANALYSIS_EXECUTOR` lane: `try_reserve` → `block_client` → `slot.spawn`; inline when `is_blocking_denied`; busy → `TSDB: outlier detection: too many queued jobs (limit 256)`; a `TS._DEBUG PANIC_NEXT_ANALYSIS_JOB` hook (debug mode) for the reply-on-drop integration test | a37c91ca9, 63be820c7 (adapted) | hazard 2; the one command both branches touch, so its shape must be fixed on `unstable` |
| M10 | Module docs: the table and rules in `threads/mod.rs` (generic wording), an AGENTS.md "Warnings" bullet pointing at them, `docs/commands/ts.outliers.md` busy error | a37c91ca9, 7a9b079a9 | rules stop being prose spread over seven comments |

**Explicitly not moved** (stays on `promql`): `pools.rs` (`EVAL_POOL`, `MATERIALIZE_POOL`),
`run_on_eval_pool`, `run_on_pool_cold`, `RayonPool`/`into_par_on`, the selector processor
thread, `query_workers.rs`, `INFO promql`, `PANIC_NEXT_EVALUATION`, the `reply()` byte-safe
rewrite and `ReplyContext` deref removal (Phase 5.2 there; replies, not threading).

**Explicitly not moved** (stays on `feat/forecasting`): `TimeoutWatcher`,
`block_client_with_timeout`, `is_timed_out`, `ContextGuard`, `ts-analysis-timeout`,
`WorkLimits`, `AnalysisCtx`, `run_analysis`/`run_analysis_in_background`, `TS._STORE`.

**Not ported from either**: forecasting's `ANALYSIS_POOL`/`spawn_analysis`/`map_on_current_pool`
(replaced by the lane + `par_rayon`), `init_thread_pool` via `build_global` (superseded by
54ad464de).

---

## 4. Work items

### Phase A — `unstable`: substrate (one PR, or one PR per step; each step builds and passes `./build.sh`)

**A1. Roles and the GIL gate** (M1, M2, M3 for the GIL and raw rayon entries).
- Add `role.rs`, `gil.rs`. In `gil.rs` split `GilGuard` into a `GilToken` (does the R1 and
  re-entry checks, sets `HOLDS_GIL`, clears on drop) and `GilGuard { token, ctx: DetachedContextGuard }`.
  Expose `GilToken::take()` as `pub(crate)` so other GIL-taking guards (forecasting's
  `ContextGuard`, the fan-out context) are checked the same way without going through
  `DetachedContext`.
- Replace the 7 raw `MODULE_CONTEXT.lock()` sites with `lock_gil()`: `fanout/cluster_rpc.rs`
  (3), `series/index/asm.rs`, `series/index/persistence.rs` (2), `series/tasks/series_trim.rs`;
  `FanoutContext::lock` and `cluster_map.rs` as on `promql`.
- `clippy.toml`: the `DetachedContext::lock`, `ThreadSafeContext::lock`, `rayon_core::*` and
  `rayon_core::ThreadPool::*` entries. (The orx entries come with A3.)
- Tests: port `role.rs`/`gil.rs` unit tests. Replace `EvalPool`/`MaterializePool` with the new
  names.
- Acceptance: `cargo clippy --all-targets -- -D clippy::all` clean; integration suite passes
  with **zero** `threading rule broken` lines in the server logs (grep after `./build.sh`).

**A2. Executors and background threads** (M5, M6).
- `executor.rs` v2 verbatim; `fanout/workers.rs` to `const` statics with `lane_workers`
  (`src/fanout/fanout_command.rs:433` and `cluster_rpc.rs:614` keep `try_spawn`; the result type
  changes from `ExecutorBusy` to `Rejected`, which has a `Display`).
- `spawn_background` with role + panic logging; `single_flight.rs`; the three tasks as on
  `promql` (`optimize_indices` moves off the pool onto `spawn_background_single`).
- Acceptance: executor and single-flight unit tests; the existing trim/stale-id integration
  tests; a new unit test that two back-to-back `optimize_indices_for_db()` calls run once.

**A3. One parallel entry point** (M4, M7, rest of M3).
- `orx_pool.rs` without `RayonPool`/`into_par_on`; rename the ~25 bare sites (`series/time_series.rs`,
  `mrange.rs`, `bulk_add.rs`, `compaction.rs`, `multi_del.rs`, `sample_merge.rs`,
  `index/querier.rs`, `tasks/series_trim.rs`, `commands/ts_mrange_fanout_command.rs`,
  `analysis/outliers/rcf_outlier_detector.rs`); `join` → `orx_parallel::Pool::global().join`
  unless pinned; delete the dead helpers and rayon-global users; add the orx entries to
  `clippy.toml`.
- Acceptance: `cargo test` (the three pool-placement tests in `threads/mod.rs`); a short A/B of
  TS.JOIN and TS.LABELSTATS before/after (`tools/latency_report.sh` or the server bench), since
  both move from a C-thread pool to the N-thread shared pool. This is `promql`'s open A/B; do
  it here, once.

**A4. Blocked-client safety** (M8).
- `thread_safe_reply_context.rs` as on `promql` (`Send`-only, `answered`, reply-on-drop),
  `error_consts::NO_REPLY_WRITTEN`.
- Acceptance: unit test that dropping an unanswered `ThreadSafeReplyContext` writes the error
  (needs the integration hook in A5 for an end-to-end check).

**A5. TS.OUTLIERS on the analysis lane** (M9).
- New `src/commands/analysis_runner.rs` holding `ANALYSIS_EXECUTOR` (`"ts-analysis"`,
  `lane_workers`, `Capacity::Fixed(256)`) and one helper,
  `run_on_analysis_lane(ctx, job: FnOnce(ThreadSafeReplyContext)) -> ValkeyResult`, that does
  reserve → block → spawn and maps `Rejected` to the error reply. TS.OUTLIERS uses it; the
  inline branch for deny-blocking clients as on `promql`.
- `TS._DEBUG PANIC_NEXT_ANALYSIS_JOB` (debug mode only) → integration test: the client gets
  `NO_REPLY_WRITTEN`, the lane keeps serving. Port `tests/test_ts_outliers.py` additions from
  a37c91ca9 (busy error, MULTI/Lua inline).
- Docs: `docs/commands/ts.outliers.md` (busy error, inline inside MULTI/scripts). Not an RTS
  command, so no compat entry.

**A6. Docs** (M10): `threads/mod.rs` header (the §2 table and rules), AGENTS.md Warnings bullet
("Threading rules R1–R5 live in `src/common/threads/mod.rs`; take the GIL with `lock_gil()`,
enter parallel work through `*_rayon`/`join`, blocking or GIL work goes on a `BoundedExecutor`
or `spawn_background`"), and a link to this plan.

### Phase B — `promql` rebase onto `unstable` (after A)

Use the scratch-worktree merge recipe from the 2026-08/09 rebases. Expected delta after the
rebase, i.e. what stays PromQL-specific:
- `pools.rs` with `ThreadRole::BlockingPool` / `IsolatedPool`; `run_on_eval_pool` (calls
  `check_may_wait_on_blocking_pool`), `run_on_pool_cold`, `RayonPool`, `into_par_on`.
- `query_workers.rs`, `selector_batch_executor.rs`, `INFO promql`, `PANIC_NEXT_EVALUATION`.
- Rename `EvalPool`→`BlockingPool`, `MaterializePool`→`IsolatedPool`,
  `check_may_wait_on_eval_pool`→`check_may_wait_on_blocking_pool` in `promql` files and tests
  (about 20 occurrences; `gil.rs` and `role.rs` tests already renamed in A1).
- Conflicts to expect: none in `threads/` beyond the renames; the 25 `*_rayon` sites and the
  three tasks resolve to identical hunks; `ts_outliers.rs` resolves to `unstable`'s
  (`run_on_analysis_lane`) with no PromQL-side change.

### Phase C — `feat/forecasting` rebase onto `unstable` (after A)

- **Cargo.toml**: take `unstable` (`persistent-pool-rayon`). Decide anofox `parallel` (§6 D2);
  the recommendation is to drop it.
- **`threads/mod.rs`**: drop `ANALYSIS_POOL`, `spawn_analysis`, `ANALYSIS_JOBS_IN_FLIGHT`,
  `InFlightJob`, `analysis_jobs_in_flight`, `map_on_current_pool`, the `build_global`
  `init_thread_pool`. Nothing forecasting-specific remains in the module.
- **`analysis_runner.rs`**: merge onto `unstable`'s file. `run_analysis_job` becomes
  `ANALYSIS_EXECUTOR.try_reserve()` **then** `block_client_with_timeout` then `slot.spawn` (the
  worker already catches panics and logs; keep the job-level `catch_unwind` only for the
  "answer with INTERNAL_ERROR" reply). A refusal is `TSDB: analysis: too many queued jobs
  (limit N)` via `Rejected`'s `Display`, not a new string. This closes review §2.2.
- **Fan-out inside a job**: `map_on_current_pool(items, f)` → `items.par_rayon().map(f).collect()`.
  The job runs on a lane worker (a `Blocking` thread) that holds no GIL during `compute`, so
  waiting on the shared pool is R2/R3-clean, and the shared pool only ever holds short
  sub-tasks, never whole analysis jobs: review §2.1 disappears by construction. (If D2 keeps
  anofox `parallel`, its `par_iter` must be wrapped so it nests on the shared pool; see D2.)
- **`TS._DEBUG ANALYSIS_JOBS`** → `ANALYSIS_EXECUTOR.stats()` (`queued + running`).
  `wait_for_analysis_pool_idle` in `tests/common.py` keeps working unchanged.
- **`ThreadSafeReplyContext::lock()`** → hold a `GilToken` in `ContextGuard` so the background
  STORE path is checked like every other GIL taker (hazard 4 is then an enforced non-event: lane
  workers are `Blocking`). `BlockedClient` gains `timed_out` beside `unstable`'s `answered`; the
  server discards a worker reply after its timeout callback has answered, so the two flags do
  not interact.
- **TS.OUTLIERS**: forecasting's `work_limits`/`check_unblockable` layered on `unstable`'s lane
  version. Review §2.13 is closed by the rebase.
- **AGENTS.md**: rewrite the "Fan out with `map_on_current_pool` … not orx `.par()` (spawns OS
  threads per call)" sentence to point at `par_rayon` and the module rules.
- **Docs**: the eleven `docs/commands/ts.*.md` pages that say "a dedicated pool of analysis
  worker threads (sized by `ts-num-threads`)" → "the analysis lane (`lane_workers` threads, 2–8)
  with a bounded queue; a full queue is refused with …". `docs/commands/ts._debug.md`
  `ANALYSIS_JOBS` wording.

### Phase D — verification (on `unstable` after A; repeat the relevant parts after B and C)

- `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all`
  (the gate) and `cargo test --features enable-system-alloc`.
- `SERVER_VERSION=unstable ./build.sh` serial and `--parallel=auto`; then
  `grep -r "threading rule broken" <server logs>` must be empty.
- `ASAN_BUILD=true SERVER_VERSION=unstable ./build.sh` (open item on `promql`'s plan; not yet
  run against this model anywhere).
- A/B: TS.JOIN, TS.LABELSTATS (A3), TS.OUTLIERS `rcf` at 10k samples × 8 clients (lane vs
  today's unbounded spawn), PING p99 under the same load.
- After C: `TS.BACKTEST … N_FOLDS 8` on 20k samples with four concurrent `TS.AUTOFORECAST`
  clients, p99 vs single-client p99 (the §2.1 benchmark); 200 pipelined `TIMEOUT 1` calls →
  `queued` never exceeds the cap and the overflow gets the busy error (the §2.2 test).

---

## 5. Why the consolidated model cannot deadlock or starve (wait-edge walk)

Every wait edge after Phase A, and after each rebase. "→" means "waits on".

- **Main (GIL) → shared pool** (`par_rayon` in RANGE/MRANGE/MADD/compaction/trim-under-lock,
  `join` in TS.JOIN/LABELSTATS). Shared-pool jobs take no GIL (R1, checked at `lock_gil`) and
  block only on bounded postings reads (R5: writers hold the postings lock for one batch and
  never take the GIL or a pool while holding it). Every job finishes; the wait ends. R2 ✓.
- **Blocking thread (no GIL) → shared pool** (analysis `compute` fan-out, fan-out decode). A
  plain thread waits without stealing, so it cannot pick up a job that needs its own answer. ✓
- **Blocking thread (GIL) → shared pool** (`series_trim` `par_mut_rayon` under the lock, fan-out
  local share). Same as the main-thread case. R2 ✓.
- **Blocking thread → another blocking thread** (fan-out coordinator ↔ lanes) is callback and
  timer driven, not a blocking wait; the one blocking wait, PromQL's `wait_for_result`, is on
  `Blocking`/`BlockingPool` threads only and never under the GIL (R3, checked). ✓
- **Pool worker → GIL**: impossible by construction (`lock_gil` on a pool worker is a rule break
  logged in release, a panic in debug) and by lint (no raw lock outside `threads/`). Lane
  workers and processors are plain threads, so "take the GIL from a worker" is simply not a
  pool worker any more. ✓
- **Lane exhaustion**: each lane has 2–8 workers and a fixed queue; a full queue refuses on
  arrival, *before* the client is blocked (`try_reserve`), so there is never an accepted job
  nobody will run and never a blocked client nobody will answer (reply-on-drop covers panics and
  the "workers gone" path). Jobs of one lane never wait on jobs of the same lane (fan-out has two
  lanes for exactly this; analysis jobs are independent). ✓
- **Shared-pool starvation of latency-sensitive work** by analysis fan-outs: bounded by the
  analysis lane width (≤ 8 concurrent jobs' sub-tasks) and by the sub-tasks being short. If the
  §2.1 benchmark still shows PING or TS.RANGE p99 moving, the escape hatch is an `IsolatedPool`
  for analysis fan-out (R1-clean, so GIL holders may still wait on it) — a feature-branch
  addition, not a rule change.
- **Abandoned jobs** (forecasting `TIMEOUT`): keep running on a lane worker, so their cost is
  bounded by lane width + queue, not by client behaviour; `is_timed_out` is checked under the
  GIL before a STORE, and the timeout callback runs on the main thread, so it cannot interleave
  with the check. ✓
- **Cron re-entry**: every periodic task is behind `SingleFlight`, so a run that outlasts its
  interval (waiting for the GIL on a busy server) is not duplicated. ✓
- **The one pool nobody owns** — rayon's global registry — exists after Phase A only if some
  crate drags `rayon` in. On `unstable` and `promql` nothing does (only `orx-parallel` depends on
  `rayon-core`). anofox `parallel` would reintroduce it (D2).

---

## 6. Decisions

**D1. Role names.** Ship `ThreadRole::{BlockingPool, IsolatedPool}` on `unstable` (recommended)
and rename in `promql` during its rebase, rather than shipping `EvalPool`/`MaterializePool`
into a branch that has no evaluator. The rule R2 is about "a pool whose jobs block", and
`unstable`'s docs should say so in those words. Cost: ~20 renames on `promql`.

**D2. anofox `parallel`.** Drop the feature on the forecasting rebase (recommended) and measure
AUTOFORECAST / BACKTEST wall time single-client; the review's own expectation is that per-job
parallelism rarely pays under concurrency, and the lane already runs up to 8 jobs at once. If
measurement says otherwise, the alternative is a sanctioned `run_on_shared_pool(work)` wrapper
in `threads/` (orx's pool `install`, allowed from a `Blocking` thread holding no GIL) so the
crate's `par_iter` nests on the shared pool, and a `cargo tree -i rayon` check in CI so no
other path to the global registry appears. Keeping `parallel` without the wrapper is not an
option: it is the §1 hazard 5 pool, unsized and unchecked.

**D3. Analysis lane home.** `src/commands/analysis_runner.rs` on `unstable` with just the
static and `run_on_analysis_lane` (recommended), so forecasting's richer runner merges onto a
file of the same name and purpose instead of relocating `ANALYSIS_EXECUTOR` out of
`ts_outliers.rs`. Alternative: keep the static in `ts_outliers.rs` as `promql` does and let
forecasting move it; that is one more conflict for no benefit.

**D4. Busy-error wording.** All lanes report through `Rejected`'s `Display`
("too many queued jobs (limit N)" / "worker threads are not running") prefixed by the
command's own context, as `promql` does for PromQL and OUTLIERS. Forecasting's planned
"analysis pool busy, retry later" is dropped for consistency.

**D5. Order of rebases.** `promql` first (its delta is small and already in this shape), then
`feat/forecasting` (the larger re-expression). Neither should start before Phase A is on
`unstable`, or both will carry a second copy of the substrate.

---

## 7. Risks and what is deliberately left out

- **`join` moves pools** (A3): TS.JOIN and TS.LABELSTATS go from C threads to N (default 4). The
  A/B in A3 decides whether `ts-num-threads`' default needs revisiting; the model does not.
- **Lazy lane start** costs 40–120 µs on the first submission per lane (measured on `promql`);
  accepted so a node that never runs OUTLIERS or PromQL starts no extra threads.
- **Inline OUTLIERS for deny-blocking clients** can stall the main thread on a large input until
  forecasting's `WorkLimits::unblockable_max` lands (Phase C). Today's behaviour (blocking a
  deny-blocking client, which the server asserts on) is worse, so A5 does not wait for it.
- **Not in scope**: INFO fields for the fan-out and analysis lanes (`BoundedExecutor::stats` is
  there; add when a dashboard wants them); `promql`'s reply-stack rewrite (Phase 5.2 there);
  the compat suite (no RTS-surface behaviour changes in Phase A).
- **Verification debt carried over**: the ASAN pass and the thread-count comparison from
  `promql`'s plan are scheduled in Phase D rather than assumed.
