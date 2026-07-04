use std::collections::HashMap;
use valkey_timeseries::common::Sample;
use valkey_timeseries::config::DEFAULT_CHUNK_SIZE_BYTES;
use valkey_timeseries::series::chunks::{Chunk, ChunkEncoding, TimeSeriesChunk};

pub mod generators;

pub use generators::{
    DATASET_SAMPLES, DatasetKey, benchmark_dataset_keys, generate_dataset,
};

pub const CHUNK_SIZE_1K: usize = 1024;
pub const CHUNK_SIZE_4K: usize = DEFAULT_CHUNK_SIZE_BYTES;
pub const CHUNK_SIZE_64K: usize = 64 * 1024;

pub const DEFAULT_SEED: u64 = 0x7EA1_DA7A_5EED;

pub struct DatasetRegistry {
    datasets: HashMap<DatasetKey, Vec<Sample>>,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        let mut datasets = HashMap::new();
        for (idx, key) in benchmark_dataset_keys().into_iter().enumerate() {
            let seed = DEFAULT_SEED.wrapping_add(idx as u64 * 0x9E37_79B9);
            datasets.insert(key, generate_dataset(key, DATASET_SAMPLES, seed));
        }
        Self { datasets }
    }

    pub fn all_keys(&self) -> impl Iterator<Item = DatasetKey> + '_ {
        self.datasets.keys().copied()
    }

    pub fn dataset(&self, key: DatasetKey) -> &[Sample] {
        self.datasets
            .get(&key)
            .map(Vec::as_slice)
            .expect("dataset key missing")
    }
}

impl Default for DatasetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub fn chunk_size_id(chunk_size: usize) -> &'static str {
    match chunk_size {
        CHUNK_SIZE_1K => "1k",
        CHUNK_SIZE_4K => "4k",
        CHUNK_SIZE_64K => "64k",
        _ => "custom",
    }
}

pub fn build_chunk(encoding: ChunkEncoding, chunk_size: usize, data: &[Sample]) -> TimeSeriesChunk {
    let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
    if matches!(encoding, ChunkEncoding::Pco) {
        // PcoChunk::add_sample decompresses+recompresses on every call (O(n²)
        // allocations).  For constant/repeating workloads PCO compresses so well
        // that is_full() never triggers, causing OOM on large datasets.
        // set_data is bulk O(n) and avoids this.
        chunk
            .set_data(data)
            .expect("set_data should succeed");
    } else {
        for sample in data {
            if chunk.is_full() {
                break;
            }
            chunk
                .add_sample(sample)
                .expect("sample should append to benchmark chunk");
        }
    }
    chunk
}

pub fn filled_prefix(data: &[Sample], encoding: ChunkEncoding, chunk_size: usize) -> &[Sample] {
    if matches!(encoding, ChunkEncoding::Pco) {
        // PcoChunk::add_sample is O(n²); use binary search + set_data
        // to find the largest prefix that fits within chunk_size.
        let mut lo = 0;
        let mut hi = data.len();
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
            chunk
                .set_data(&data[..mid])
                .expect("set_data should succeed");
            if chunk.is_full() {
                hi = mid - 1;
            } else {
                lo = mid;
            }
        }
        &data[..lo]
    } else {
        let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
        let mut count = 0;
        for sample in data {
            if chunk.is_full() {
                break;
            }
            chunk
                .add_sample(sample)
                .expect("sample should append to benchmark chunk");
            count += 1;
        }
        &data[..count]
    }
}
