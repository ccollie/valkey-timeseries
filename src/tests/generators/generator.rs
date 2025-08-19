use crate::tests::generators::create_rng;
use crate::tests::generators::mackey_glass::mackey_glass;
use rand::Rng;
use rand::prelude::StdRng;
use std::ops::Range;

pub struct UniformGenerator {
    rng: StdRng,
    range: Range<f64>,
}

impl UniformGenerator {
    pub fn new(seed: Option<u64>, range: &Range<f64>) -> Self {
        let rng = create_rng(seed);
        Self {
            rng,
            range: range.clone(),
        }
    }
}

impl Iterator for UniformGenerator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.rng.random_range(self.range.start..self.range.end))
    }
}

fn get_value_in_range(rng: &mut StdRng, r: &Range<f64>) -> f64 {
    rng.random_range(r.start..r.end)
}

pub struct StdNormalGenerator {
    rng: StdRng,
    range: Range<f64>,
    last_value: f64,
}
impl StdNormalGenerator {
    pub fn new(seed: Option<u64>, range: &Range<f64>) -> Self {
        let rng = create_rng(seed);
        Self {
            rng,
            range: range.clone(),
            last_value: 0.0,
        }
    }
}

impl Iterator for StdNormalGenerator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        self.last_value = get_value_in_range(&mut self.rng, &self.range);
        Some(self.last_value)
    }
}

pub struct DerivativeGenerator {
    p: f64,
    n: f64,
}

impl DerivativeGenerator {
    pub fn new(seed: Option<u64>, range: &Range<f64>) -> Self {
        let mut rng = create_rng(seed);
        let c = get_value_in_range(&mut rng, range);
        let p = c;
        let n = c + get_value_in_range(&mut rng, range);
        Self { p, n }
    }
}

impl Iterator for DerivativeGenerator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        let v = (self.n - self.p) / 2.0;
        self.p = self.n;
        self.n += v;
        Some(v)
    }
}

pub struct MackeyGlassGenerator {
    buf: Vec<f64>,
    pub tau: usize,
    seed: Option<u64>,
    index: usize,
    pub range: Range<f64>,
}

impl MackeyGlassGenerator {
    pub fn new(tau: usize, seed: Option<u64>, range: &Range<f64>) -> Self {
        Self {
            buf: vec![],
            tau,
            seed,
            index: 0,
            range: range.clone(),
        }
    }
}

impl Default for MackeyGlassGenerator {
    fn default() -> Self {
        let range = Range {
            start: 0.0,
            end: 1.0,
        };
        Self::new(17, None, &range)
    }
}

impl Iterator for MackeyGlassGenerator {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.buf.len() {
            self.index = 0;
            self.buf = mackey_glass(20, Some(self.tau), self.seed);
        }
        let v = self.buf[self.index];
        self.index += 1;
        Some(self.range.start + (self.range.end - self.range.start) * v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;

    fn validate_range(iter: impl Iterator<Item = f64>, r: &Range<f64>) {
        let values = iter.take(1000).collect::<Vec<f64>>();
        for v in values {
            assert!(v >= r.start && v < r.end, "value {v} not in range {r:?}");
        }
    }

    #[test]
    fn test_random_generator() {
        let r = Range {
            start: 0.0,
            end: 1.0,
        };
        let iter = UniformGenerator::new(None, &r);
        validate_range(iter, &r);
    }

    #[test]
    fn test_std_normal_generator() {
        let r = Range {
            start: 1.0,
            end: 99.0,
        };
        let iter = StdNormalGenerator::new(None, &r);
        validate_range(iter, &r);
    }

    #[test]
    fn test_mackey_glass_generator() {
        let r = Range {
            start: 1.0,
            end: 99.0,
        };
        let iter = MackeyGlassGenerator::new(17, None, &r);
        validate_range(iter, &r);
    }
}
