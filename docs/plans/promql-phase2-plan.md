# PromQL refactoring — Phase 2 implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Land the Phase 2 items of the PromQL refactoring assessment — immutable prepared queries, shared raw reads, and the cluster push-down cleanups — one bisectable commit per task.

**Architecture:** `src/promql/exec/evaluator/` (A1, A2), `src/promql/engine/selector_batch_executor.rs` (B1), `src/fanout/` + `src/promql/engine/fanout/` (C1, C2), `src/promql/exec/partial_aggregation.rs` (C3). Each task ends with the full unit suite (normal and optimizer-on conformance) green and clippy clean.

**Tech Stack:** Rust 2024, `cargo test --lib --features enable-system-alloc`, `cargo clippy --all-targets -- -D clippy::all`.

**Spec:** [promql-refactoring-assessment.md](promql-refactoring-assessment.md) — A1, A2, B1, C1, C2, C3.

## Global Constraints

- Conventional Commits; subject plus at most a 2-line body; no co-author trailer.
- No change to PromQL results. A2 and C2 change how much is read and when the budget is charged, never the answer.
- Threading rules R1–R6: no new pool entry points; nothing new under the module GIL.
- Disk is tight (~1–2 GB): delete this crate's `target/debug/deps/{lib,}valkey_timeseries*` between heavy builds.

## Review Focus

- A prepared query must be identical whether a subquery union was prepared, declined, or reused by a twin: same answers, same sample charges (A1).
- `sum(a) + avg(a)` and `rate(a[5m]) / increase(a[5m])` on a single node read `a`'s span once and charge it once; in a cluster the fused requests still go out separately (A2).
- A shard's raw answer over the budget is refused before it is decoded (C2).
- A fan-out command that answered must deliver exactly what it did before `Default` was dropped (C1).

---

### Task 1: A1 — immutable prepared queries

**Files:** `src/promql/exec/evaluator/{mod.rs,preload.rs,pushdown.rs,subquery.rs}`, `src/promql/exec/preloader.rs`, `src/promql/engine/promql_engine.rs`, `evaluator_tests.rs`.

**Interfaces:**
- `PreparedQuery { instant, grids, matrices, subqueries: plain maps; node_keys: NodeKeys; any_preloaded: bool; budget: Arc<SampleBudget> }`; `SubqueryPreloadMap = AHashMap<SubqueryKey, Arc<PreparedQuery>>`.
- `Preloads` (preparation only): the four maps behind `RwLock`.
- `Evaluator { reader, prepared: Arc<PreparedQuery>, preloads: Preloads, options, deadline }`; `Evaluator::with_prepared(reader, options, Arc<PreparedQuery>)` replaces `with_prepared` + `with_shared`.
- `Preloader::prepare(plan) -> EvalResult<Arc<PreparedQuery>>`; `Evaluator::finish_preparation(&mut self, expr, twins)` freezes `preloads` and computes `NodeKeys`.

- [ ] Characterization: existing preload/fallback/subquery/budget tests (`evaluator_tests.rs`, `nested_subquery_tests.rs`, `grid_fallback_tests.rs`, `counting_query_reader.rs`) are the guard.
- [ ] Implement; per-step lookups take no lock; a subquery outer step clones one `Arc`.
- [ ] Full suite PASS; commit `refactor(promql): make prepared queries immutable after preparation`.

### Task 2: A2 — share raw reads across consumers of one selector

**Files:** `src/promql/exec/evaluator/preload.rs`, tests in `src/promql/engine/counting_query_reader.rs`.

- [ ] Tests (counting reader, single node): `sum(a) + avg(a)` over a range reads `a` once; `rate(a[5m]) / increase(a[5m])` reads `a`'s span once; results equal the unshared evaluation. Expected RED: two reads.
- [ ] A single-node `GridOutcome::Raw` for a stepped aggregation reuses an already-cached stepped span for the same selector; rollups over one `(selector, range)` share one raw read, keeping each request's own evaluation and charging the shared span once.
- [ ] Full suite PASS; commit `perf(promql): read a selector's span once for every rollup and aggregation over it`.

### Task 3: B1 — one cluster fan-out starter

**Files:** `src/promql/engine/selector_batch_executor.rs`.

- [ ] Generic `fan_out(ctx, cmd, hash_tags, timeout, responder, on_ok)`; the five `execute_cluster_*` become calls to it; drop the `Arc<SyncSender>`; `execute_selector_task` stops rebuilding the task it destructured.
- [ ] Full suite PASS; commit `refactor(promql): start every cluster selector fan-out through one function`.

### Task 4: C1 — fan-out commands without `Default`

**Files:** `src/fanout/fanout_command.rs`, every `FanoutCommand` impl's `Default`.

- [ ] `FanoutStateInner` holds `Option<OP>` and `take()`s it; drop the `Default` bound and the placeholder impls.
- [ ] Full suite PASS; commit `refactor(fanout): drop the Default bound fan-out commands needed only for mem::take`.

### Task 5: C2 — charge a shard's raw grid answer before decoding it

**Files:** `src/promql/engine/fanout/grid_fanout_command.rs`, `type_conversions.rs`.

- [ ] Test: a raw answer whose chunk lengths alone pass the budget is refused without decoding (decode counter or a chunk that would fail to decode). Expected RED.
- [ ] Charge from `chunk.len()` before `range_sample_from_proto`, as the range path does.
- [ ] Full suite PASS; commit `fix(fanout): charge a raw grid answer before decoding it`.

### Task 6: C3 — index fused partial states by step

**Files:** `src/promql/exec/partial_aggregation.rs`, grid fold callers.

- [ ] `SteppedPartialGroups` keyed by step index over the request's window ends instead of a `BTreeMap<Timestamp, _>`; output order unchanged.
- [ ] Full suite PASS; commit `perf(promql): index fused partial states by step`.

### Task 7: Final verification and whole-branch review
