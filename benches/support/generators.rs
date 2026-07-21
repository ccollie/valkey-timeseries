use rand::SeedableRng;
use rand::prelude::{IndexedRandom, StdRng};
use rand_distr::{Distribution, Exp, Normal, Poisson, Uniform};
use valkey_timeseries::common::{Sample, Timestamp};

pub const DATASET_SAMPLES: usize = 64 * 1024;
pub const DEFAULT_START_TS: Timestamp = 1_700_000_000_000;
const DEFAULT_INTERVAL_MS: i64 = 1_000;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ValueWorkload {
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
    pub const fn id(self) -> &'static str {
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

    pub const fn all() -> &'static [ValueWorkload] {
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
pub enum TimestampModel {
    Regular,
    Jitter,
    Irregular,
}

impl TimestampModel {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Regular => "ts_regular",
            Self::Jitter => "ts_jitter",
            Self::Irregular => "ts_irregular",
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DatasetKey {
    pub workload: ValueWorkload,
    pub timestamp_model: TimestampModel,
}

impl DatasetKey {
    pub const fn new(workload: ValueWorkload, timestamp_model: TimestampModel) -> Self {
        Self {
            workload,
            timestamp_model,
        }
    }

    pub fn id(self) -> String {
        format!("{}/{}", self.workload.id(), self.timestamp_model.id())
    }
}

pub fn benchmark_dataset_keys() -> Vec<DatasetKey> {
    let mut keys = Vec::with_capacity(ValueWorkload::all().len() + 4);
    for workload in ValueWorkload::all() {
        keys.push(DatasetKey::new(*workload, TimestampModel::Regular));
    }
    for workload in [ValueWorkload::Drift, ValueWorkload::Noisy] {
        keys.push(DatasetKey::new(workload, TimestampModel::Jitter));
        keys.push(DatasetKey::new(workload, TimestampModel::Irregular));
    }
    keys
}

pub fn generate_dataset(key: DatasetKey, sample_count: usize, seed: u64) -> Vec<Sample> {
    let mut rng = StdRng::seed_from_u64(seed);
    let timestamps = generate_timestamps(key.timestamp_model, sample_count, &mut rng);
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

    let jitter = Uniform::new_inclusive(-50_i64, 50_i64).expect("valid uniform distribution");
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
    let period = 3600.0;
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
    let burst_length =
        Uniform::new_inclusive(200_usize, 500_usize).expect("valid uniform distribution");
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

        let len = burst_length.sample(rng).min(sample_count - idx);
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
    (0..sample_count)
        .map(|_| *choices.choose(rng).expect("choices non-empty"))
        .collect()
}
