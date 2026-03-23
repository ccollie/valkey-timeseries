pub mod mstl;
mod periodogram;
pub mod stl;
mod test_data;

use crate::analysis::seasonality::mstl::Mstl;
use crate::analysis::seasonality::stl::Stl;
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
pub use periodogram::Detector as PeriodogramDetector;

/// A detector of periodic signals in a time series.
pub trait SeasonalityDetector {
    /// Detects the periods of a time series.
    fn detect(&self, data: &[f64]) -> Vec<u32>;
}

#[derive(Clone, Debug)]
pub enum Seasonality {
    Auto,
    Periods(Vec<usize>),
}

/// Seasonal adjustment using (M)Stl decomposition
pub fn seasonally_adjust(
    ts: &[f64],
    seasonality: &Seasonality,
) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let periods = match seasonality {
        Seasonality::Periods(periods) => periods.clone(),
        Seasonality::Auto => {
            // use periodogram to detect periods
            let detector = PeriodogramDetector::default();
            detector.detect(ts).iter().map(|&x| x as usize).collect()
        }
    };

    if periods.is_empty() {
        return Ok(ts.to_vec());
    }

    let n = ts.len();
    if periods.len() == 1 {
        let required = 2 * periods[0];
        validate_insufficient_data::<Vec<f64>>(required, n)?;

        Stl::new(periods[0])
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("STL decomposition failed".to_string())
            })
    } else {
        let max_period = periods[periods.len() - 1];
        let required = 2 * max_period;
        validate_insufficient_data::<Vec<f64>>(required, n)?;

        Mstl::new(periods.to_vec())
            .robust()
            .decompose(ts)
            .map(|res| res.remainder)
            .ok_or_else(|| {
                TimeSeriesAnalysisError::DecompositionError("MSTL decomposition failed".to_string())
            })
    }
}

fn validate_insufficient_data<T: Default>(
    required: usize,
    actual: usize,
) -> TimeSeriesAnalysisResult<T> {
    if actual >= required {
        return Ok(T::default());
    }
    Err(TimeSeriesAnalysisError::InsufficientData {
        message: "TSDB: insufficient samples for anomaly detection".to_string(),
        required,
        actual,
    })
}
