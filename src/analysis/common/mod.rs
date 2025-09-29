mod array_2d;
mod error;
mod confidence_interval;
pub mod moments;

/// Probability value in [0, 1]
pub type Probability = f64;

pub use array_2d::*;
pub use error::*;
pub use confidence_interval::*;