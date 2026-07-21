use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use get_size2::GetSize;
use valkey_timeseries::series::chunks::{ChunkEncoding, ChunkOps};
use valkey_timeseries::tests::chunk_utils::{
    CHUNK_SIZE_1K, CHUNK_SIZE_4K, CHUNK_SIZE_64K, build_chunk_until_full, chunk_size_id,
    filled_prefix_len,
};
use valkey_timeseries::tests::generators::{
    DATASET_SAMPLES, DatasetKey, DatasetRegistry, TimestampModel, ValueWorkload,
    benchmark_dataset_keys,
};

// -------- Report structures --------

#[derive(Debug, Clone)]
struct RowKey {
    encoding: ChunkEncoding,
    workload: ValueWorkload,
    timestamp_model: TimestampModel,
    chunk_size: usize,
}

impl RowKey {
    fn id(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.encoding.name(),
            self.workload.id(),
            self.timestamp_model.id(),
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
            k.timestamp_model.id(),
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
            k.timestamp_model.id(),
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
    let datasets = DatasetRegistry::with_samples(DATASET_SAMPLES);

    // Capacity@4k per (encoding, workload)
    let mut capacity4k: HashMap<(String, ValueWorkload), usize> = HashMap::new();

    // Collect rows
    let mut rows: Vec<(RowKey, RowVals)> = Vec::new();

    for key in benchmark_dataset_keys() {
        let data = datasets.dataset(key);
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
                    timestamp_model: key.timestamp_model,
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
