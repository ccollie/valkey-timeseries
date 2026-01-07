use crate::common::rounding::{round_to_decimal_digits, round_to_sig_figs};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::tests::generators::generator::{
    DerivativeGenerator, MackeyGlassGenerator, StdNormalGenerator, UniformGenerator,
};
use bon::bon;
use std::ops::Range;
use std::time::Duration;

#[derive(Debug, Copy, Clone, Default)]
pub enum RandAlgo {
    #[default]
    Uniform,
    StdNorm,
    MackeyGlass,
    Deriv,
}

/// GeneratorOptions contains the parameters for generating random time series data.
#[derive(Debug, Clone)]
pub struct DataGenerator {
    start: Timestamp,
    end: Option<Timestamp>,
    /// Interval between samples.
    interval: Option<Duration>,
    /// Range of values.
    values: Range<f64>,
    /// Number of samples.
    samples: usize,
    /// Seed for random number generator.
    seed: Option<u64>,
    /// Type of random number generator.
    pub typ: RandAlgo,
    /// Number of significant digits.
    pub significant_digits: Option<usize>,
    /// Number of decimal digits.
    pub decimal_digits: Option<usize>,
}

#[bon]
impl DataGenerator {
    #[builder]
    pub fn new(
        start: Timestamp,
        end: Option<Timestamp>,
        interval: Option<Duration>,
        #[builder(default = 0.0..1.0)] values: Range<f64>,
        samples: usize,
        seed: Option<u64>,
        #[builder(default = RandAlgo::StdNorm)] algorithm: RandAlgo,
        significant_digits: Option<usize>,
        decimal_digits: Option<usize>,
    ) -> Self {
        let mut res = DataGenerator {
            start,
            end,
            samples,
            seed,
            interval,
            values,
            typ: algorithm,
            significant_digits,
            decimal_digits,
        };
        res.fixup();
        res
    }

    fn fixup(&mut self) {
        if self.samples == 0 {
            self.samples = 10;
        }
        if let Some(end) = self.end
            && end < self.start
        {
            // swap start and end
            let tmp = self.start;
            self.start = end;
            self.end = Some(tmp);
        }
        if self.values.is_empty() {
            self.values = 0.0..1.0;
        }
        self.start = self.start / 10 * 10;
    }

    pub fn generate(&self) -> Vec<Sample> {
        generate_series_data(self)
    }
}

const ONE_DAY_IN_SECS: u64 = 24 * 60 * 60;

impl Default for DataGenerator {
    fn default() -> Self {
        let now = current_time_millis() / 10 * 10;
        let start = now - Duration::from_secs(ONE_DAY_IN_SECS).as_millis() as i64;
        let mut res = Self {
            start,
            end: None,
            interval: None,
            values: 0.0..1.0,
            samples: 100,
            seed: None,
            typ: RandAlgo::StdNorm,
            significant_digits: None,
            decimal_digits: None,
        };
        res.fixup();
        res
    }
}

fn get_generator_impl(
    typ: RandAlgo,
    seed: Option<u64>,
    range: &Range<f64>,
) -> Box<dyn Iterator<Item = f64>> {
    match typ {
        RandAlgo::Uniform => Box::new(UniformGenerator::new(seed, range)),
        RandAlgo::StdNorm => Box::new(StdNormalGenerator::new(seed, range)),
        RandAlgo::Deriv => Box::new(DerivativeGenerator::new(seed, range)),
        RandAlgo::MackeyGlass => Box::new(MackeyGlassGenerator::new(17, seed, range)),
    }
}

// Generates time series data from the given type.
pub fn generate_series_data(options: &DataGenerator) -> Vec<Sample> {
    let interval = if let Some(interval) = options.interval {
        interval.as_millis() as i64
    } else {
        let end = if let Some(end) = options.end {
            end
        } else {
            options.start + (options.samples * 1000 * 60) as i64
        };
        (end - options.start) / options.samples as i64
    };

    let generator = get_generator_impl(options.typ, options.seed, &options.values);

    let mut values = generator.take(options.samples).collect::<Vec<f64>>();
    let timestamps = generate_timestamps(
        options.samples,
        options.start,
        Duration::from_millis(interval as u64),
    );

    if let Some(significant_digits) = options.significant_digits {
        for v in values.iter_mut() {
            let rounded = round_to_sig_figs(*v, significant_digits as u8);
            *v = rounded;
        }
    } else if let Some(decimal_digits) = options.decimal_digits {
        for v in values.iter_mut() {
            let rounded = round_to_decimal_digits(*v, decimal_digits as u8);
            *v = rounded;
        }
    }

    timestamps
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(timestamp, value)| Sample { timestamp, value })
        .collect::<Vec<Sample>>()
}

pub fn generate_timestamps(count: usize, start: Timestamp, interval: Duration) -> Vec<Timestamp> {
    let interval_millis = interval.as_millis() as i64;
    let mut res = Vec::with_capacity(count);
    let mut t = start;

    for _ in 0..count {
        res.push(t);
        t += interval_millis;
    }
    res
}
