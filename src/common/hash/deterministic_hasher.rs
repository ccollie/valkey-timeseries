use ahash::{AHasher, RandomState};
use core::hash::BuildHasher;

pub struct DeterministicHasher(RandomState);

impl Default for DeterministicHasher {
    fn default() -> Self {
        Self(RandomState::with_seeds(0, 0, 0, 0))
    }
}

impl BuildHasher for DeterministicHasher {
    type Hasher = AHasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.0.build_hasher()
    }
}

impl DeterministicHasher {
    pub fn new() -> Self {
        Self::default()
    }
}
