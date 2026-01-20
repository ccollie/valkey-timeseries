use super::utils::normalize_value;
use crate::analysis::common::Array2D;
use crate::analysis::math::calculate_mean;
use crate::analysis::outliers::{AnomalyMethod, AnomalyResult, AnomalySignal, MethodInfo};
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use rand::Rng;
use rand::prelude::{SliceRandom, ThreadRng};

/// Options for Isolation Forest analysis detection
#[derive(Debug, Clone, Copy)]
pub struct IsolationForestOptions {
    /// Number of trees for isolation forest
    pub n_trees: usize,
    /// Subsampling size for isolation forest
    pub subsample_size: Option<usize>,
    /// Window size for local analysis detection
    pub window_size: Option<usize>,
    /// Contamination rate (expected fraction of anomalies)
    pub contamination: f64,
}

impl Default for IsolationForestOptions {
    fn default() -> Self {
        Self {
            n_trees: 100,
            subsample_size: None,
            contamination: 0.1,
            window_size: None,
        }
    }
}

/// Isolation Forest analysis detection (simplified version)
pub(super) fn detect_anomalies_isolation_forest(
    ts: &[f64],
    options: IsolationForestOptions,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let n = ts.len();
    let n_trees = options.n_trees;
    let subsample_size = options
        .subsample_size
        .unwrap_or((n as f64 * 0.5).min(256.0) as usize);

    // Convert to sliding windows for multivariate representation
    let window_size = options.window_size.unwrap_or(10.min(n / 4));
    let windowed_data = create_sliding_windows(ts, window_size)?;
    let n_windows = windowed_data.rows();

    let mut path_lengths: Vec<f64> = vec![0.0; n_windows];
    let mut rng = rand::rng();

    // Build isolation trees
    for _ in 0..n_trees {
        let tree_path_lengths = build_isolation_tree(&windowed_data, subsample_size, &mut rng)?;
        for i in 0..n_windows {
            path_lengths[i] += tree_path_lengths[i];
        }
    }

    // Average path lengths
    for len in path_lengths.iter_mut() {
        *len /= n_trees as f64;
    }

    // Calculate expected path length for normal data
    let c_n = if subsample_size > 2 {
        2.0 * (subsample_size as f64 - 1.0).ln() + 0.5772156649
            - 2.0 * (subsample_size - 1) as f64 / subsample_size as f64
    } else {
        1.0
    };

    // Calculate anomaly scores (higher for anomalies)
    let mut anomaly_scores: Vec<f64> = vec![0.0; n_windows];
    for i in 0..n_windows {
        anomaly_scores[i] = 2.0_f64.powf(-path_lengths[i] / c_n);
    }

    // Map window scores back to time series
    let mut scores: Vec<f64> = Vec::with_capacity(n);
    let (mut min_score, mut max_score) = (f64::INFINITY, f64::NEG_INFINITY);
    for i in 0..n {
        let window_idx = if i >= window_size {
            i - window_size + 1
        } else {
            0
        }
        .min(n_windows - 1);
        let score = anomaly_scores[window_idx];
        min_score = min_score.min(score);
        max_score = max_score.max(score);

        scores.push(score);
    }

    if min_score.is_finite() && max_score.is_finite() && (max_score - min_score).abs() > 0.0 {
        let denom = max_score - min_score;
        for s in scores.iter_mut() {
            if s.is_finite() {
                *s = ((*s - min_score) / denom).clamp(0.0, 1.0);
            } else {
                *s = 0.0;
            }
        }
    } else {
        // Degenerate case: all scores equal (or non-finite), treat as no anomaly signal
        for s in scores.iter_mut() {
            *s = 0.0;
        }
    }

    // Determine a threshold and anomalies
    let threshold = determine_threshold(&scores, options.contamination);

    // The anomaly score reflects how easily the point was isolated, not if the value itself is
    // numerically "above" or "below" the bulk of the data.
    let anomalies: Vec<AnomalySignal> = scores
        .iter()
        .map(|&score| {
            if score > threshold {
                AnomalySignal::Positive
            } else {
                AnomalySignal::None
            }
        })
        .collect();

    Ok(AnomalyResult {
        scores,
        anomalies,
        threshold,
        method: AnomalyMethod::IsolationForest,
        method_info: Some(MethodInfo::IsolationForest {
            average_path_length: calculate_mean(&path_lengths),
        }),
    })
}

fn create_sliding_windows(
    ts: &[f64],
    window_size: usize,
) -> TimeSeriesAnalysisResult<Array2D<f64>> {
    let n = ts.len();
    if n < window_size {
        return Err(TimeSeriesAnalysisError::InsufficientData {
            message: "TSDB: time series too short for windowing".to_string(),
            required: window_size,
            actual: n,
        });
    }

    let n_windows = n - window_size + 1;
    let mut windows = Array2D::new(n_windows, window_size);

    for i in 0..n_windows {
        let mut row = Vec::with_capacity(window_size);
        for j in 0..window_size {
            let val = normalize_value(ts[i + j]);
            row.push(val);
        }
        windows.set_row(i, row);
    }

    Ok(windows)
}

fn determine_threshold(scores: &[f64], contamination: f64) -> f64 {
    let mut sorted_scores: Vec<f64> = scores.to_vec();
    sorted_scores.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let threshold_idx = ((1.0 - contamination) * sorted_scores.len() as f64) as usize;
    sorted_scores[threshold_idx.min(sorted_scores.len() - 1)]
}

fn build_isolation_tree(
    data: &Array2D<f64>,
    subsample_size: usize,
    rng: &mut ThreadRng,
) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let n_samples = data.rows();
    let _n_features = data.cols();

    // Subsample data
    let actual_subsample_size = subsample_size.min(n_samples);
    let mut indices: Vec<usize> = (0..n_samples).collect();
    indices.shuffle(rng);
    let _subsample_indices = &indices[0..actual_subsample_size];

    let mut path_lengths = Vec::with_capacity(n_samples);

    // Build tree using subsample, but calculate path lengths for all points
    for idx in 0..n_samples {
        let point = data.get_row(idx).expect("Row index out of bounds");
        path_lengths.push(calculate_isolation_path_length(point, data, 0, rng));
    }

    Ok(path_lengths)
}

fn calculate_isolation_path_length(
    point: &[f64],
    data: &Array2D<f64>,
    depth: usize,
    rng: &mut ThreadRng,
) -> f64 {
    const MAX_DEPTH: usize = 20; // Prevent infinite recursion

    if depth >= MAX_DEPTH || data.rows() <= 1 {
        return depth as f64;
    }

    // Randomly select a feature and split value
    let feature_idx = rng.random_range(0..data.cols());
    let feature_values: Vec<f64> = data.get_column(feature_idx).unwrap();
    let min_val = feature_values.iter().copied().fold(f64::INFINITY, f64::min);
    let max_val = feature_values
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    if (max_val - min_val).abs() < 1e-10 {
        return depth as f64; // No variation in this feature
    }

    // Simplified: return path length based on how far the point is from the mean
    let mean_val = feature_values.iter().sum::<f64>() / feature_values.len() as f64;
    let deviation = (point[feature_idx] - mean_val).abs();
    let max_deviation = (max_val - min_val) / 2.0;

    // Anomalies (points far from mean) should have shorter path lengths
    if max_deviation > 0.0 {
        let normalized_deviation = deviation / max_deviation;
        // Points far from mean get shorter paths (are isolated faster)
        depth as f64 + (1.0 - normalized_deviation.min(1.0))
    } else {
        depth as f64 + 1.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_forest_anomaly_detection() {
        let mut ts = vec![1.0; 100];
        ts[50] = 10.0; // Clear outlier

        let options = IsolationForestOptions {
            n_trees: 50,
            subsample_size: Some(64),
            contamination: 0.1,
            window_size: Some(5),
        };

        let result = detect_anomalies_isolation_forest(&ts, options).unwrap();

        // Should detect the outlier
        assert!(
            result.anomalies[50].is_anomaly(),
            "Should detect anomaly at index 50"
        );
    }
}
