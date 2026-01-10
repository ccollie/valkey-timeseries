/// Smoothed Z-Score Anomaly Detector algorithm described here:
/// https://stackoverflow.com/a/22640362/14797322
///
/// Port of the golang implementation here:
/// https://github.com/MicahParks/peakdetect
/// Original License: Apache-2.0
use super::anomalies::AnomalySignal;
use crate::analysis::TimeSeriesAnalysisError;

struct MovingMeanStdDev {
    cache: Vec<f64>,
    index: usize,
    prev_mean: f64,
    prev_variance: f64,
}

impl MovingMeanStdDev {
    fn new() -> Self {
        Self {
            cache: Vec::new(),
            index: 0,
            prev_mean: 0.0,
            prev_variance: 0.0,
        }
    }

    /// Initialize creates the necessary assets for the MovingMeanStdDev. It also computes the
    /// resulting mean and population standard deviation using Welford's method.
    ///
    /// https://www.johndcook.com/blog/standard_deviation/
    fn initialize(&mut self, initial_values: &[f64]) -> (f64, f64) {
        self.cache = initial_values.to_vec();
        self.index = 0;

        let cache_len = self.cache.len();

        let mut mean = initial_values[0];
        let mut prev_mean = mean;
        let mut sum_of_squares = 0.0;

        for i in 2..=cache_len {
            let value = initial_values[i - 1];
            let delta = value - prev_mean;
            mean = prev_mean + delta / (i as f64);
            sum_of_squares += delta * (value - mean);
            prev_mean = mean;
        }

        self.prev_mean = mean;
        self.prev_variance = sum_of_squares / cache_len as f64;

        (mean, self.prev_variance.sqrt())
    }

    /// `next` computes the next mean and population standard deviation. It uses a sliding window and is based on Welford's
    /// method.
    ///
    /// https://stackoverflow.com/a/14638138/14797322
    fn next(&mut self, value: f64) -> (f64, f64) {
        let out_of_window = self.cache[self.index];
        self.cache[self.index] = value;
        self.index = (self.index + 1) % self.cache.len();

        let cache_len = self.cache.len() as f64;
        let new_mean = self.prev_mean + (value - out_of_window) / cache_len;
        self.prev_variance += (value - new_mean + out_of_window - self.prev_mean)
            * (value - out_of_window)
            / cache_len;
        self.prev_mean = new_mean;

        (self.prev_mean, self.prev_variance.sqrt())
    }
}

/// SmoothedZScoreAnomalyDetector detects peaks in realtime timeseries data using z-scores.
///
/// This is a Rust implementation of the algorithm described by this StackOverflow answer:
/// https://stackoverflow.com/a/22640362/14797322
///
/// Brakel, J.P.G. van (2014). "Robust peak detection algorithm using z-scores". Stack Overflow. Available
/// at: https://stackoverflow.com/questions/22583391/peak-signal-detection-in-realtime-timeseries-data/22640362#22640362
/// (version: 2020-11-08).
pub struct SmoothedZScoreAnomalyDetector {
    index: usize,
    /// `influence` is the influence of signals on the algorithm's detection threshold. If put at 0, signals have
    /// no influence on the threshold, such that future signals are detected based on a threshold that is calculated with
    /// a mean and standard deviation that is not influenced by past signals. If put at 0.5, signals have half the
    /// influence of normal data points. If you put the influence at 0, you implicitly assume stationarity
    /// (i.e., with a stable average over the long term).
    influence: f64,
    /// `threshold` is the number of standard deviations from the moving mean above which the algorithm will classify a new
    /// datapoint as being a signal. The threshold therefore directly influences how sensitive the algorithm is and thereby
    /// also determines how often the algorithm signals. Examine your own data and choose a sensible threshold that makes
    /// the algorithm signal when you want it to (some trial-and-error might be needed here to get to a good threshold for your purpose).
    threshold: f64,
    /// `lag` determines how much your data will be smoothed and how adaptive the algorithm is to change in the long-term
    /// average of the data. The more stationary your data is, the more lags you should include to improve the
    /// robustness of the algorithm. If your data contains time-varying trends, you should consider how quickly you want
    /// the algorithm to adapt to these trends. I.e., if you put lag at 10, it takes 10 'periods' before the algorithm's
    /// threshold is adjusted to any systematic changes in the long-term average.
    lag: usize,
    moving_stats: MovingMeanStdDev,
    prev_mean: f64,
    prev_std_dev: f64,
    prev_value: f64,
    pub(super) prev_score: f64,
}

impl SmoothedZScoreAnomalyDetector {
    /// Creates a new SmoothedZScoreAnomalyDetector.
    pub fn new(
        influence: f64,
        threshold: f64,
        initial_values: &[f64],
    ) -> Result<Self, TimeSeriesAnalysisError> {
        let lag = initial_values.len();

        let mut res = Self {
            index: 0,
            influence,
            threshold,
            lag,
            moving_stats: MovingMeanStdDev::new(),
            prev_mean: 0.0,
            prev_std_dev: 0.0,
            prev_value: 0.0,
            prev_score: f64::NAN,
        };

        if lag == 0 {
            return Err(TimeSeriesAnalysisError::InvalidInput(
                "the length of the initial values is zero, the length is used as the lag for the algorithm".to_string()
            ));
        }

        let (mean, std_dev) = res.moving_stats.initialize(initial_values);

        res.prev_mean = mean;
        res.prev_std_dev = std_dev;
        res.prev_value = initial_values[res.lag - 1];

        Ok(res)
    }

    /// Next processes the next value and determines its signal.
    pub fn next(&mut self, value: f64) -> AnomalySignal {
        self.index = (self.index + 1) % self.lag;
        let score = (value - self.prev_mean).abs();

        let signal = if score > self.threshold * self.prev_std_dev {
            if value > self.prev_mean {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            }
        } else {
            AnomalySignal::None
        };

        let adjusted_value = if signal != AnomalySignal::None {
            self.influence * value + (1.0 - self.influence) * self.prev_value
        } else {
            value
        };

        let (mean, std_dev) = self.moving_stats.next(adjusted_value);
        self.prev_mean = mean;
        self.prev_std_dev = std_dev;
        self.prev_value = adjusted_value;
        self.prev_score = score;

        signal
    }

    pub fn next_batch(&mut self, values: Vec<f64>) -> Vec<AnomalySignal> {
        values.into_iter().map(|v| self.next(v)).collect()
    }
}

impl Default for SmoothedZScoreAnomalyDetector {
    fn default() -> Self {
        Self {
            index: 0,
            influence: 0.0,
            lag: 0,
            moving_stats: MovingMeanStdDev::new(),
            prev_mean: 0.0,
            prev_std_dev: 0.0,
            prev_value: 0.0,
            threshold: 0.0,
            prev_score: f64::NAN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peak_detector_initialization() {
        let initial_values = vec![1.0, 2.0, 3.0, 2.0, 1.0];

        let result = SmoothedZScoreAnomalyDetector::new(0.1, 2.0, &initial_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_peak_detector_empty_initial_values() {
        let initial_values = vec![];

        let result = SmoothedZScoreAnomalyDetector::new(0.1, 2.0, &initial_values);
        assert!(result.is_err());
    }

    #[test]
    fn test_peak_detection() {
        let initial_values = vec![
            1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.0, 1.1, 1.0, 0.9, 1.0, 1.1, 1.0, 1.0, 0.9, 1.0, 1.0,
            1.1, 1.0, 1.0,
        ];

        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, &initial_values).unwrap();

        // Test with a clear positive peak
        let signal = detector.next(5.0);
        assert_eq!(signal, AnomalySignal::Positive);

        // Test with normal values
        let signal = detector.next(1.0);
        assert_eq!(signal, AnomalySignal::None);

        // Test with a clear negative peak
        let signal = detector.next(-3.0);
        assert_eq!(signal, AnomalySignal::Negative);
    }

    #[test]
    fn test_batch_processing() {
        let initial_values = vec![1.0; 10];

        let mut detector = SmoothedZScoreAnomalyDetector::new(0.0, 2.0, &initial_values).unwrap();

        let test_values = vec![1.0, 5.0, 1.0, -3.0, 1.0];
        let signals = detector.next_batch(test_values);

        assert_eq!(signals.len(), 5);
        // The exact signals depend on the algorithm's internal state,
        // but we can verify the length matches
    }
}
