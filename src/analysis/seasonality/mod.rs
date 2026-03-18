pub mod mstl;
mod periodogram;
pub mod stl;
mod test_data;

pub use periodogram::{
    Builder as PeriodogramDetectorBuilder, Detector as PeriodogramDetector, Periodogram,
};

/// A detector of periodic signals in a time series.
pub trait SeasonalityDetector {
    /// Detects the periods of a time series.
    fn detect(&self, data: &[f64]) -> Vec<u32>;
}
