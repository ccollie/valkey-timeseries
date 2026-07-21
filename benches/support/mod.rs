#![allow(unused_imports)]

//! Facade over the crate's shared test/bench utilities (`src/tests`, gated
//! behind the `test-utils` feature). Everything here is defined once in the
//! library so benchmarks and `tools/compression_report.rs` stay in sync.

pub use valkey_timeseries::tests::chunk_utils::{
    CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K, build_chunk, chunk_size_id, filled_prefix,
};
pub use valkey_timeseries::tests::generators::{
    DATASET_SAMPLES, DEFAULT_SEED, DatasetKey, DatasetRegistry, TimestampModel, ValueWorkload,
    benchmark_dataset_keys, generate_dataset,
};
