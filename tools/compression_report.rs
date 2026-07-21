use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use rand::SeedableRng;
use rand::prelude::StdRng;
use rand_distr::{Distribution, Exp, Normal, Poisson, Uniform};

use get_size2::GetSize;
use valkey_timeseries::common::{Sample, Timestamp};
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps, TimeSeriesChunk};

// -------- Workload & dataset generation (deterministic) --------

const DATASET_SAMPLES: usize = 64 * 1024; // 64k
const DEFAULT_START_TS: Timestamp = 1_700_000_000_000;
const DEFAULT_INTERVAL_MS: i64 = 1_000; // 1s
const DEFAULT_SEED: u64 = 0x7EA1_DA7A_5EED;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum ValueWorkload {
    Constant,
    ConstantInt,
    Drift,
    Periodic,
    Noisy,
    Bursty,
    Counter,
    Discrete,
}

impl ValueWorkload {
    const fn id(self) -> &'static str {
        match self {
            Self::Constant => "constant",
            Self::ConstantInt => "constant_int",
            Self::Drift => "drift",
            Self::Periodic => "periodic",
            Self::Noisy => "noisy",
            Self::Bursty => "bursty",
            Self::Counter => "counter",
            Self::Discrete => "discrete",
        }
    }

    const fn all() -> &'static [ValueWorkload] {
        &[
            Self::Constant,
            Self::ConstantInt,
            Self::Drift,
            Self::Periodic,
            Self::Noisy,
            Self::Bursty,
            Self::Counter,
            Self::Discrete,
        ]
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum TimestampModel {
    Regular,
    Jitter,
    Irregular,
}

impl TimestampModel {
    const fn id(self) -> &'static str {
        match self {
            Self::Regular => "ts_regular",
            Self::Jitter => "ts_jitter",
            Self::Irregular => "ts_irregular",
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct DatasetKey {
    workload: ValueWorkload,
    ts_model: TimestampModel,
}

impl DatasetKey {
    const fn new(workload: ValueWorkload, ts_model: TimestampModel) -> Self {
        Self { workload, ts_model }
    }
}

fn benchmark_dataset_keys() -> Vec<DatasetKey> {
    let mut keys = Vec::with_capacity(ValueWorkload::all().len() + 4);
    for wl in ValueWorkload::all() {
        keys.push(DatasetKey::new(*wl, TimestampModel::Regular));
    }
    for wl in [ValueWorkload::Drift, ValueWorkload::Noisy] {
        keys.push(DatasetKey::new(wl, TimestampModel::Jitter));
        keys.push(DatasetKey::new(wl, TimestampModel::Irregular));
    }
    keys
}

fn generate_dataset(key: DatasetKey, sample_count: usize, seed: u64) -> Vec<Sample> {
    let mut rng = StdRng::seed_from_u64(seed);
    let timestamps = generate_timestamps(key.ts_model, sample_count, &mut rng);
    let values = generate_values(key.workload, sample_count, &mut rng);
    timestamps
        .into_iter()
        .zip(values)
        .map(|(timestamp, value)| Sample { timestamp, value })
        .collect()
}

fn generate_timestamps(
    model: TimestampModel,
    sample_count: usize,
    rng: &mut StdRng,
) -> Vec<Timestamp> {
    let mut timestamps = Vec::with_capacity(sample_count);
    let mut ts = DEFAULT_START_TS;
    timestamps.push(ts);

    let jitter = Uniform::new_inclusive(-50_i64, 50_i64).expect("valid uniform");
    let irregular = Exp::new(1.0 / DEFAULT_INTERVAL_MS as f64).expect("valid exp lambda");

    for _ in 1..sample_count {
        let delta = match model {
            TimestampModel::Regular => DEFAULT_INTERVAL_MS,
            TimestampModel::Jitter => (DEFAULT_INTERVAL_MS + jitter.sample(rng)).max(1),
            TimestampModel::Irregular => irregular.sample(rng).round().max(1.0) as i64,
        };
        ts += delta;
        timestamps.push(ts);
    }
    timestamps
}

fn generate_values(workload: ValueWorkload, sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    match workload {
        ValueWorkload::Constant => vec![42.0; sample_count],
        ValueWorkload::ConstantInt => vec![1000.0; sample_count],
        ValueWorkload::Drift => drift_values(sample_count, rng),
        ValueWorkload::Periodic => periodic_values(sample_count, rng),
        ValueWorkload::Noisy => noisy_values(sample_count, rng),
        ValueWorkload::Bursty => bursty_values(sample_count, rng),
        ValueWorkload::Counter => counter_values(sample_count, rng),
        ValueWorkload::Discrete => discrete_values(sample_count, rng),
    }
}

fn drift_values(sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    let noise = Normal::new(0.0, 0.01).expect("valid normal");
    let mut value = 100.0;
    let min: f64 = 95.0;
    let max: f64 = 105.0;
    let mut out = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let next: f64 = value + noise.sample(rng);
        value = next.max(min).min(max);
        out.push(value);
    }
    out
}

fn periodic_values(sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    let amplitude = 50.0;
    let period = 3600.0; // one hour of 1s samples
    let noise = Normal::new(0.0, amplitude / 100.0).expect("valid normal");
    let sawtooth_every = 5_000;
    let mut out = Vec::with_capacity(sample_count);
    for idx in 0..sample_count {
        let base = if (idx / sawtooth_every) % 2 == 0 {
            (2.0 * std::f64::consts::PI * (idx as f64) / period).sin() * amplitude
        } else {
            let pos = (idx % 512) as f64 / 512.0;
            (2.0 * pos - 1.0) * amplitude
        };
        out.push(base + noise.sample(rng));
    }
    out
}

fn noisy_values(sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    let dist = Normal::new(100.0, 25.0).expect("valid normal");
    (0..sample_count).map(|_| dist.sample(rng)).collect()
}

fn bursty_values(sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    let quiet_noise = Normal::new(0.0, 0.01).expect("valid normal");
    let burst_noise = Normal::new(0.0, 20.0).expect("valid normal");
    let burst_len = Uniform::new_inclusive(200_usize, 500_usize).expect("valid uniform");
    let mut out = Vec::with_capacity(sample_count);
    let mut value: f64 = 100.0;
    let mut idx = 0;

    while idx < sample_count {
        let quiet_segment = ((sample_count - idx) as f64 * 0.8).round() as usize;
        let quiet_segment = quiet_segment.clamp(50, 4_000).min(sample_count - idx);
        for _ in 0..quiet_segment {
            value = (value + quiet_noise.sample(rng)).clamp(95.0, 105.0);
            out.push(value);
        }
        idx += quiet_segment;
        if idx >= sample_count {
            break;
        }

        let len = burst_len.sample(rng).min(sample_count - idx);
        for _ in 0..len {
            out.push((value * 10.0) + burst_noise.sample(rng));
        }
        idx += len;
    }

    out
}

fn counter_values(sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    let inc = Poisson::new(10.0).expect("valid poisson");
    let reset_every = 50_000;
    let mut value = 0.0;
    let mut out = Vec::with_capacity(sample_count);
    for i in 0..sample_count {
        if i > 0 && i % reset_every == 0 {
            value = 0.0;
        }
        value += inc.sample(rng);
        out.push(value);
    }
    out
}

fn discrete_values(sample_count: usize, rng: &mut StdRng) -> Vec<f64> {
    let choices = [0.0, 0.25, 0.5, 1.0];
    let pick = Uniform::new(0usize, choices.len()).expect("valid uniform");
    (0..sample_count)
        .map(|_| {
            let idx = pick.sample(rng);
            choices[idx]
        })
        .collect()
}

// -------- Report structures --------

#[derive(Debug, Clone)]
struct RowKey {
    encoding: ChunkEncoding,
    workload: ValueWorkload,
    ts_model: TimestampModel,
    chunk_size: usize,
}

impl RowKey {
    fn id(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.encoding.name(),
            self.workload.id(),
            self.ts_model.id(),
            chunk_size_id(self.chunk_size)
        )
    }
}

#[derive(Debug, Clone, Default)]
struct RowVals {
    len: usize,
    data_size: usize,
    size: usize,
    bytes_per_sample: f64,
}

impl RowVals {
    fn ratio(&self) -> f64 {
        if self.data_size == 0 {
            return f64::INFINITY;
        }
        (self.len as f64 * 16.0) / self.data_size as f64
    }
}

const CHUNK_SIZE_1K: usize = 1024;
const CHUNK_SIZE_4K: usize = valkey_timeseries::config::DEFAULT_CHUNK_SIZE_BYTES;
const CHUNK_SIZE_64K: usize = 64 * 1024;

fn chunk_size_id(size: usize) -> &'static str {
    match size {
        CHUNK_SIZE_1K => "1k",
        CHUNK_SIZE_4K => "4k",
        CHUNK_SIZE_64K => "64k",
        _ => "custom",
    }
}

fn filled_prefix_len(data: &[Sample], encoding: ChunkEncoding, chunk_size: usize) -> usize {
    let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
    let mut count = 0;
    for s in data {
        if chunk.is_full() {
            break;
        }
        chunk.add_sample(s).expect("append into benchmark chunk");
        count += 1;
    }
    count
}

/// Build a chunk by appending from `data` until it becomes full (or data ends),
/// returning the constructed chunk along with the number of samples consumed.
fn build_chunk_until_full(
    encoding: ChunkEncoding,
    chunk_size: usize,
    data: &[Sample],
) -> (TimeSeriesChunk, usize) {
    let mut chunk = TimeSeriesChunk::new(encoding, chunk_size);
    let mut count = 0;
    for s in data {
        if chunk.is_full() {
            break;
        }
        chunk.add_sample(s).expect("append into benchmark chunk");
        count += 1;
    }
    (chunk, count)
}

fn encode_matrix_chunk_sizes(key: &DatasetKey) -> Vec<usize> {
    match key.workload {
        ValueWorkload::Drift | ValueWorkload::Noisy => {
            vec![CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K]
        }
        _ => vec![CHUNK_SIZE_4K],
    }
}

fn encodings() -> [ChunkEncoding; 4] {
    [
        ChunkEncoding::Uncompressed,
        ChunkEncoding::Gorilla,
        ChunkEncoding::TsXor,
        ChunkEncoding::Xor2,
    ]
}

// -------- Baseline checking --------

fn load_baseline(path: &Path) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    if !path.exists() {
        return map;
    }
    let Ok(text) = fs::read_to_string(path) else {
        return map;
    };
    for (i, line) in text.lines().enumerate() {
        if i == 0 {
            continue;
        } // header
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 9 {
            continue;
        }
        let id = format!("{}/{}/{}/{}", cols[0], cols[1], cols[2], cols[3]);
        if let Ok(r) = cols[8].parse::<f64>() {
            map.insert(id, r);
        }
    }
    map
}

// -------- Output writers --------

fn ensure_dir(p: &Path) -> std::io::Result<()> {
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_csv(
    path: &Path,
    rows: &[(RowKey, RowVals)],
    capacity4k: &HashMap<(String, ValueWorkload), usize>,
) -> std::io::Result<()> {
    ensure_dir(path)?;
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    writeln!(
        w,
        "encoding,workload,ts_model,chunk_size,len,data_size,size,bytes_per_sample,ratio,capacity_4k"
    )?;
    for (k, v) in rows {
        let cap_key = (k.encoding.name().to_string(), k.workload);
        let cap = capacity4k.get(&cap_key).cloned().unwrap_or_default();
        writeln!(
            w,
            "{},{},{},{},{},{},{},{:.6},{:.6},{}",
            k.encoding.name(),
            k.workload.id(),
            k.ts_model.id(),
            chunk_size_id(k.chunk_size),
            v.len,
            v.data_size,
            v.size,
            v.bytes_per_sample,
            v.ratio(),
            cap
        )?;
    }
    Ok(())
}

fn write_markdown(path: &Path, rows: &[(RowKey, RowVals)]) -> std::io::Result<()> {
    ensure_dir(path)?;
    let file = File::create(path)?;
    let mut w = BufWriter::new(file);
    writeln!(
        w,
        "| encoding | workload | ts_model | chunk | len | data_size | size | bytes/sample | ratio |"
    )?;
    writeln!(w, "|---|---|---|---|---:|---:|---:|---:|---:|")?;
    for (k, v) in rows {
        writeln!(
            w,
            "| {} | {} | {} | {} | {} | {} | {} | {:.6} | {:.6} |",
            k.encoding.name(),
            k.workload.id(),
            k.ts_model.id(),
            chunk_size_id(k.chunk_size),
            v.len,
            v.data_size,
            v.size,
            v.bytes_per_sample,
            v.ratio(),
        )?;
    }
    Ok(())
}

// -------- Main --------

fn main() {
    // Flags: --check (exit non-zero on regression beyond tolerance), --baseline <path>
    let mut check = false;
    let mut baseline_path: Option<PathBuf> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" => check = true,
            "--baseline" => baseline_path = args.next().map(PathBuf::from),
            _ => {}
        }
    }

    let baseline_path = baseline_path
        .unwrap_or_else(|| PathBuf::from("benches/baselines/compression_baseline.csv"));
    let baseline = load_baseline(&baseline_path);

    // Build datasets once
    let mut datasets: HashMap<DatasetKey, Vec<Sample>> = HashMap::new();
    for (i, key) in benchmark_dataset_keys().into_iter().enumerate() {
        let seed = DEFAULT_SEED.wrapping_add(i as u64 * 0x9E37_79B9);
        datasets.insert(key, generate_dataset(key, DATASET_SAMPLES, seed));
    }

    // Capacity@4k per (encoding, workload)
    let mut capacity4k: HashMap<(String, ValueWorkload), usize> = HashMap::new();

    // Collect rows
    let mut rows: Vec<(RowKey, RowVals)> = Vec::new();

    for key in benchmark_dataset_keys() {
        let data = datasets.get(&key).expect("dataset present");
        for encoding in encodings() {
            // capacity at 4k (only compute once per (enc, workload) pair)
            let cap_key = (encoding.name().to_string(), key.workload);
            capacity4k
                .entry(cap_key)
                .or_insert_with(|| filled_prefix_len(data, encoding, CHUNK_SIZE_4K));

            for chunk_size in encode_matrix_chunk_sizes(&key) {
                // Reuse the chunk constructed during the prefix search instead of rebuilding it
                let (chunk, _needed) = build_chunk_until_full(encoding, chunk_size, data);

                let len = chunk.len();
                let data_size = chunk.size();
                // Prefer structural size via GetSize for footprint reporting
                let footprint = chunk.get_size();
                let bps = if len > 0 {
                    data_size as f64 / len as f64
                } else {
                    0.0
                };
                let vals = RowVals {
                    len,
                    data_size,
                    size: footprint,
                    bytes_per_sample: bps,
                };
                let key_row = RowKey {
                    encoding,
                    workload: key.workload,
                    ts_model: key.ts_model,
                    chunk_size,
                };
                rows.push((key_row, vals));
            }
        }
    }

    // Sort rows by id for stable output
    rows.sort_by(|a, b| a.0.id().cmp(&b.0.id()));

    // Write outputs
    let out_csv = PathBuf::from("target/bench-reports/compression.csv");
    let out_md = PathBuf::from("target/bench-reports/compression.md");
    if let Err(e) = write_csv(&out_csv, &rows, &capacity4k) {
        eprintln!("Failed to write CSV: {e}");
    }
    if let Err(e) = write_markdown(&out_md, &rows) {
        eprintln!("Failed to write Markdown: {e}");
    }

    // Check against baseline if requested
    if check && !baseline.is_empty() {
        let mut failed = false;
        let tol = 0.05; // 5% relative drop tolerable? No: fail if drop > 5%
        for (k, v) in &rows {
            let id = k.id();
            if let Some(&base_ratio) = baseline.get(&id) {
                let ratio = v.ratio();
                // Fail if ratio < base_ratio * (1 - tol)
                if ratio + f64::EPSILON < base_ratio * (1.0 - tol) {
                    eprintln!(
                        "Regression: {} ratio {:.6} < baseline {:.6} (>{:.0}% drop)",
                        id,
                        ratio,
                        base_ratio,
                        tol * 100.0
                    );
                    failed = true;
                }
            }
        }
        if failed {
            std::process::exit(1);
        }
    } else if check && baseline.is_empty() {
        eprintln!(
            "--check requested but baseline file not found: {}",
            baseline_path.display()
        );
        std::process::exit(2);
    }

    println!(
        "Compression report written to {} and {} (rows: {})",
        out_csv.display(),
        out_md.display(),
        rows.len()
    );
}
