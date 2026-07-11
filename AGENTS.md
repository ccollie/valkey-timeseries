# AGENTS: Guidance for AI coding agents

Purpose

- Short, focused instructions to help an AI model become productive in this codebase quickly.

Quick start (commands you can run)

- Build + checks:
  `cargo fmt --check && cargo clippy --profile release --all-targets -- -D clippy::all && RUSTFLAGS="-D warnings" cargo build --all --all-targets --release`
- Local dev script (recommended):
    - `SERVER_VERSION=unstable ./build.sh`  # builds module, builds valkey-server, runs unit & integration tests
    - To run ASAN integration pass: `ASAN_BUILD=true SERVER_VERSION=unstable ./build.sh`
    - Run a subset of Python integration tests: `TEST_PATTERN="test_ts_add" SERVER_VERSION=unstable ./build.sh`

Key ENV and behavior (from `./build.sh`)

- `SERVER_VERSION` (required): controls which valkey-server is cloned/built and stored at
  `tests/build/binaries/$SERVER_VERSION/valkey-server`. Defaults to `unstable` if not set, which tracks the latest main or branch.
- `ASAN_BUILD`: when set runs tests with LeakSanitizer checks and fails on leaks.
- `TEST_PATTERN`: passed to pytest `-k` to select tests.
- `MODULE_PATH` exported after build: `target/release/libvalkey_timeseries{.so,.dylib}` depending on OS.

Setup & Environment Notes

- Rust version: The project requires a minimum Rust version of `1.92`.
- Python tests: Integration tests use Python. Dependencies are in `requirements.txt` (or via `uv sync`). The `build.sh`
  script handles this, but if running `pytest` manually, ensure packages are installed.
- Running manually: To manually start a server with the module loaded, run
  `valkey-server --loadmodule ./target/release/libvalkey_timeseries.so` (requires building the module first).

High-level architecture (big picture)

- This is a Valkey module (Rust crate) exposing TS.* commands to the Valkey server via the `valkey_module!` macro (
  `src/lib.rs`).
- Command implementations live in `src/commands/*` and are registered in `src/lib.rs` with a one-to-one mapping to
  Valkey commands. Example:
    - `["TS.ADD", commands::ts_add_cmd, "write deny-oom", 1, 1, 1, "write timeseries"]`
- Time-series core lives under `src/series` (storage, encoding, background tasks, indexes). Index/init helpers:
  `init_croaring_allocator()` and `init_background_tasks()` are invoked from `src/lib.rs`.
  - `src/series/chunks/` implements five encoding formats: **Gorilla** (default), **PCO**, **TsXOR**, **Uncompressed**,
    **XOR2**. The default is controlled by `DEFAULT_CHUNK_ENCODING` in `src/config.rs`.
  - ACL filtering per series: `src/series/acl.rs`.
- Cross-node fanout / clustering patterns: `src/fanout` and `src/commands/*_fanout_command.rs` use protobuf (
  `src/commands/fanout.*.proto`) and explicit fanout registration (`register_fanout_operations`) to implement
  cluster-wide queries.
- Outlier detection: `src/analysis/outliers/` — multiple algorithms (ESD, CUSUM, EWMA, IQR, MAD, modified z-score, RCF
  variants) exposed via the `TS.OUTLIERS` command.
- Aggregation: `src/aggregators/` — aggregation handlers and iterators used by range queries.
- Supporting subsystems (all referenced from `src/lib.rs`):
  - `src/common/` — shared utilities: encoding, logging, thread pool, RDB helpers, string interning.
  - `src/labels/` — `Label` type, label filter evaluation, regex helpers.
  - `src/parser/` — Prometheus-compatible filter syntax, metric name, timestamp, and duration parsing.
  - `src/iterators/` — sample and row iterators consumed by range and multi-range queries.
  - `src/join/` — ASOF join logic backing `TS.JOIN`.

Project-specific conventions and patterns

- All Valkey commands are declared in the `valkey_module!` macro in `src/lib.rs`; change there to add/remove commands.
- Command files follow `ts_<command>.rs` naming and export `ts_<command>_cmd` functions (see `src/commands/mod.rs`).
- Fanout pattern: synchronous local implementation + `*_fanout_command.rs` files which marshal/unmarshal protobuf
  messages for cluster aggregation. Seven operations are currently registered (see `register_fanout_operations` in
  `src/commands/mod.rs`): `LabelStatsFanoutCommand`, `CardFanoutCommand`, `LabelSearchFanoutCommand`,
  `MDelFanoutCommand`, `MGetFanoutCommand`, `MRangeFanoutCommand`, `QueryIndexFanoutCommand`.
- Initialization sequence (inside `initialize()` in `src/lib.rs`): `init_croaring_allocator` → `register_config` →
  `init_fanout` + `register_fanout_operations` (cluster only) → `register_server_events` → `init_thread_pool` →
  `init_background_tasks`.
- Minimum supported Valkey server version: `[8, 0, 0]` (enforced in `preload()` via
  `config::TIMESERIES_MIN_SUPPORTED_VERSION`).

- Allocator in tests: tests compile with `enable-system-alloc` feature in unit runs (see `build.sh`), and the crate
  switches allocators under `#[cfg(test)]`.

Testing & debugging notes

- Unit tests: `cargo test --features enable-system-alloc`.
- Doc tests: `cargo test --doc --features enable-system-alloc`.
- Integration tests: Python pytest under `tests/` and rely on a built `valkey-server` and `tests/valkeytestframework`
  helper files (populated by `./build.sh`).
- To reproduce integration runs locally: run `SERVER_VERSION=unstable ./build.sh` — this will clone/build Valkey and
  copy the server binary to `tests/build/binaries/`.
- Leak detection: when `ASAN_BUILD` is set, the build script scans pytest output for LeakSanitizer output and fails if
  leaks are detected.

Where to look first (key files & directories)

- `src/lib.rs` — module entrypoint, command registration, lifecycle (preload/init/deinit).
- `src/commands/` — implementations and command parsing utilities (`command_parser.rs`).
- `src/series/` — core storage, encodings, indexes, background tasks.
- `src/fanout/` — cluster communication primitives.
- `src/analysis/` — outlier detection algorithms (`src/analysis/outliers/`).
- `src/aggregators/` — aggregation handlers for range queries.
- `src/common/` — shared utilities (encoding, logging, thread pool, RDB, string interning).
- `src/labels/` — label types and filter evaluation.
- `src/parser/` — filter syntax, metric name, timestamp, and duration parsers.
- `src/iterators/` — sample and row iterators.
- `src/join/` — ASOF join for TS.JOIN.
- `src/tests/` — synthetic data generators (mackey_glass, rand) for unit tests.
- `build.sh` — canonical developer flow for formatting, linting, building, and running tests.
- `README.md` and `docs/commands/` — human-facing command descriptions and examples.
- `docs/topics/` — deep-dive topics: `filter-syntax.md`, `label-discovery.md`, `filter-dos-audit.md`.

Quick tips for code changes

- Add new commands: create `src/commands/ts_<name>.rs`, add function `ts_<name>_cmd`, then register in `valkey_module!`
  in `src/lib.rs`. Currently registered commands: TS.CREATE, TS.ALTER, TS.ADD, TS.ADDBULK, TS.GET, TS.MGET, TS.MADD,
  TS.DEL, TS.DECRBY, TS.INCRBY, TS.JOIN, TS.MDEL, TS.MRANGE, TS.MREVRANGE, TS.RANGE, TS.REVRANGE, TS.INFO,
  TS.QUERYINDEX, TS.CARD, TS.LABELNAMES, TS.LABELVALUES, TS.METRICNAMES, TS.LABELSTATS, TS.CREATERULE,
  TS.DELETERULE, TS.OUTLIERS, TS._DEBUG (hidden admin command, no user-facing docs needed).
- Documentation: When adding or modifying commands, remember to update the human-facing docs in `docs/commands/` and the
  supported list in `README.md`. TS._DEBUG is intentionally undocumented.
- When making cluster changes, search for `*_fanout_command.rs` to copy the fanout pattern and add protobuf messages in
  `src/commands/fanout.*.proto`.

Limitations of this document

- Focused on discoverable, executable patterns. It does not cover domain rationale beyond what is visible in
  source/docs.

If you need more context, inspect:

- `build.sh`, `Cargo.toml`, `src/lib.rs`, `src/commands/*`, `src/series/*`, `src/analysis/*`, and `tests/`.
