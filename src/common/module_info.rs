//! The module's `INFO` sections: `ts_memory` and `ts_compaction`.
//!
//! Module sections are emitted only for an explicit `INFO modules`, `INFO everything`/`INFO all`,
//! or `INFO <section>` — never for a bare `INFO` — which is what makes it acceptable for the
//! memory section to walk the term dictionary.

use crate::common::string_interner::InternedString;
use crate::promql::engine::{admission, query_cache, query_workers};
use crate::series::index::index_memory_usage;
use crate::series::{COMPACTION_FAILURES, COMPACTION_STATE_REBUILDS};
use valkey_module::{InfoContext, ValkeyResult};
use valkey_module_macros::info_command_handler;

/// `INFO ts_memory` and `INFO ts_compaction`.
///
/// `ts_memory` is the module's global heap, which no per-key figure can show.
/// `ts_compaction` counts the compaction rule applications that failed and the dirty rules that
/// were rebuilt afterwards, since module start.
///
/// The sections are declared as `memory` and `compaction`; the server prefixes each with the
/// module name, so they land as `# ts_memory` and `# ts_compaction` with every field prefixed
/// `ts_`.
///
/// `MEMORY USAGE <key>` and `TS.INFO memoryUsage` report one series: its chunks, its labels'
/// amortized share of the interner, and its own struct. The label index is not attached to any
/// key, so neither of them can see it at all, and on a high-cardinality keyspace it is routinely
/// the larger half of what the module holds. Everything reported here is *in addition to* the
/// per-key numbers, with one exception noted on `interned_strings_bytes`.
///
/// `terms_bytes` and `id_to_key_bytes` count the entries an adaptive radix tree and a `BTreeMap`
/// hold without their internal node overhead, so the totals are a floor.
#[info_command_handler]
fn module_info(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    let index = index_memory_usage();

    ctx.builder()
        .add_section("memory")
        .field("index_total_bytes", index.total_bytes() as u64)?
        .field("index_terms_bytes", index.terms_bytes as u64)?
        .field("index_postings_bytes", index.postings_bytes as u64)?
        .field("index_id_to_key_bytes", index.id_to_key_bytes as u64)?
        .field("index_bookkeeping_bytes", index.bookkeeping_bytes as u64)?
        .field("index_terms", index.term_count as u64)?
        .field("index_series", index.series_count as u64)?
        .field("index_databases", index.db_count as u64)?
        // The pool total, not a per-series share: this is the whole interner, which the
        // `memoryUsage` of the series holding those labels already divides amongst themselves.
        // Adding it to the sum of every key's `MEMORY USAGE` therefore double-counts it.
        //
        // Both of these are O(1) reads. `TS._DEBUG STRINGPOOLSTATS` has the distribution and the
        // savings figure, which cost a walk of the pool.
        .field("interned_strings", InternedString::interned_count() as u64)?
        .field(
            "interned_strings_bytes",
            InternedString::memory_used() as u64,
        )?
        .build_section()?
        .add_section("compaction")
        .field(
            "compaction_failures",
            COMPACTION_FAILURES.load(std::sync::atomic::Ordering::Relaxed),
        )?
        .field(
            "compaction_state_rebuilds",
            COMPACTION_STATE_REBUILDS.load(std::sync::atomic::Ordering::Relaxed),
        )?
        .build_section()?
        .build_info()?;

    Ok(())
}

/// `INFO ts_promql`: the `TS.QUERY` / `TS.QUERYRANGE` worker pool.
///
/// `queries_running` is at most `ts-promql-max-concurrent-queries`; `queries_queued` is
/// at most `ts-promql-max-queued-queries`. `queries_rejected` counts, since startup, the
/// queries refused on arrival because the backlog was full, and `queries_cancelled` those
/// abandoned, queued or running, because their client disconnected. `samples_in_flight` is
/// what running queries and the shard reads served for them have charged against
/// `ts-promql-max-samples-in-flight`: running totals, not samples resident now. A non-zero,
/// growing `queries_queued` with `queries_rejected` climbing is the pool being overrun:
/// either the queries are too slow or there are too few workers for the load.
///
/// The `query_cache_*` fields describe the parsed-query cache: `entries` and `bytes` (the
/// estimated memory charged for them) now, and since startup the `hits` and `misses` of
/// lookups made while it was enabled and the `evictions` that kept it within its limits.
#[info_command_handler]
fn promql_info(ctx: &InfoContext, _for_crash_report: bool) -> ValkeyResult<()> {
    let pool = query_workers::stats();
    let cache = query_cache::stats();

    ctx.builder()
        .add_section("promql")
        .field("queries_running", pool.running as u64)?
        .field("queries_queued", pool.queued as u64)?
        .field("queries_rejected", pool.rejected)?
        .field("queries_cancelled", query_workers::cancelled_count())?
        .field("samples_in_flight", admission::samples_in_flight() as u64)?
        .field("query_cache_entries", cache.entries as u64)?
        .field("query_cache_bytes", cache.bytes as u64)?
        .field("query_cache_hits", cache.hits)?
        .field("query_cache_misses", cache.misses)?
        .field("query_cache_evictions", cache.evictions)?
        .build_section()?
        .build_info()?;

    Ok(())
}
