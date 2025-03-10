use crate::common::time::current_time_millis;
use crate::common::Sample;
use rand::{Rng, SeedableRng};
use std::time::Duration;

const SECS_PER_DAY: u64 = 86400;

pub fn generate_random_samples(seed: u64, vec_size: usize) -> Vec<Sample> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let secs_past = rng.random_range(1..10) * SECS_PER_DAY;
    let now = current_time_millis();

    let mut timestamp = now.wrapping_sub(Duration::from_secs(secs_past).as_millis() as i64);
    let mut vec = Vec::with_capacity(vec_size);

    let mut value: f64 = if rng.random_bool(0.5) {
        rng.random_range(-100000000.0..1000000.0)
    } else {
        rng.random_range(-10000.0..10000.0)
    };
    vec.push(Sample { timestamp, value });

    for _ in 1..vec_size {
        timestamp += rng.random_range(1..30) * 1000;
        if rng.random_bool(0.33) {
            value += 1.0;
        } else if rng.random_bool(0.33) {
            value = rng.random();
        }
        vec.push(Sample { timestamp, value });
    }

    vec
}
