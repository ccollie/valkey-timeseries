mod generator;
mod mackey_glass;
mod rand;

use ::rand::prelude::StdRng;
use ::rand::{rng, SeedableRng};
#[cfg(test)]
pub use generator::*;
#[cfg(test)]
pub use rand::*;

pub fn create_rng(seed: Option<u64>) -> StdRng {
    if let Some(seed) = seed {
        StdRng::seed_from_u64(seed)
    } else {
        let mut r = rng();
        StdRng::from_rng(&mut r)
    }
}
