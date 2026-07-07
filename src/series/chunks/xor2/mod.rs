mod xor2_chunk;
mod xor2_iterator;
#[cfg(test)]
mod xor2_test;

pub(crate) use xor2_chunk::Xor2Chunk;
pub(crate) use xor2_iterator::{Xor2Iterator, Xor2RangeIterator};
