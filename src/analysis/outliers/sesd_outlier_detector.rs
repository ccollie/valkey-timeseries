use crate::analysis::math::{calculate_mean, calculate_std_dev};
use crate::analysis::outliers::{Anomaly, AnomalyMethod, AnomalyResult, AnomalySignal};
use crate::analysis::seasonality::stl::Stl;
use crate::analysis::{TimeSeriesAnalysisError, TimeSeriesAnalysisResult};
use statrs::distribution::{ContinuousCDF, StudentsT};

/// SESD (Seasonal Extreme Studentized Deviate) anomaly detector
#[derive(Clone, Debug)]
pub struct SESDOutlierOptions {
    /// Significance level for the statistical test
    pub alpha: f64,
    /// Whether to use the hybrid ESD test
    pub hybrid: bool,
    /// Period of the seasonal pattern
    pub period: Option<usize>,
    /// Maximum number of outliers to detect
    pub max_outliers: Option<usize>,
}

impl Default for SESDOutlierOptions {
    fn default() -> Self {
        SESDOutlierOptions {
            alpha: 0.05,
            hybrid: false,
            period: None,
            max_outliers: None,
        }
    }
}

pub(super) fn detect_anomalies_sesd(
    data: &[f64],
    options: Option<SESDOutlierOptions>,
) -> TimeSeriesAnalysisResult<AnomalyResult> {
    let opts = options.unwrap_or_default();
    let n = data.len();

    // Parameter check: max_outliers
    let max_outliers = match opts.max_outliers {
        Some(m) if m >= n / 2 => {
            let message = format!(
                "max_outliers must be less than n/2. Got max_outliers = {} and n = {}",
                m, n
            );
            let err = TimeSeriesAnalysisError::InvalidParameter {
                name: "max_outliers".to_string(),
                message,
            };
            return Err(err);
        }
        Some(m) => m,
        None => 1,
    };

    // Perform ESD test
    let (anomalies, scores) = if let Some(period) = opts.period {
        // If a period is specified, we can perform a seasonal decomposition and then apply ESD to the residuals.
        let residuals = calculate_stl_residual(data, period)?;
        esd_test(&residuals, opts.alpha, opts.hybrid, max_outliers)?
    } else {
        // If no period is specified, we can apply ESD directly to the original data.
        esd_test(data, opts.alpha, opts.hybrid, max_outliers)?
    };

    Ok(AnomalyResult {
        anomalies,
        scores,
        method_info: None,
        threshold: max_outliers as f64, // ESD doesn't have a fixed threshold, so we can return the max outlier count as info
        method: AnomalyMethod::SESD,
    })
}

/// Calculate STL (Seasonal and Trend decomposition using Loess) residuals.
fn calculate_stl_residual(data: &[f64], period: usize) -> TimeSeriesAnalysisResult<Vec<f64>> {
    let mut decomposer = Stl::new(period);
    decomposer = decomposer.robust();
    if let Some(decomposition) = decomposer.decompose(data) {
        Ok(decomposition.remainder)
    } else {
        Err(TimeSeriesAnalysisError::DecompositionError(
            "STL decomposition failed".to_string(),
        ))
    }
}

fn esd_normalized_score(stat: f64, crit: f64) -> f64 {
    if !stat.is_finite() || !crit.is_finite() || stat <= 0.0 {
        return 0.0;
    }
    let raw = (stat - crit) / stat;
    raw.clamp(0.0, 1.0)
}

fn esd_score_from_p_value(p: f64) -> f64 {
    if !p.is_finite() {
        return 0.0;
    }
    (1.0 - p).clamp(0.0, 1.0)
}

/// About scoring
///
fn esd_test(
    residuals: &[f64],
    alpha: f64,
    hybrid: bool,
    max_outliers: usize,
) -> TimeSeriesAnalysisResult<(Vec<Anomaly>, Vec<f64>)> {
    use statrs::distribution::{ContinuousCDF, StudentsT};

    let n = residuals.len();
    let mut anomalies = Vec::new();
    let mut data = residuals.to_vec();
    let mut indices: Vec<usize> = (0..n).collect();

    // We'll accumulate per-observation scores here; default to 0.
    let mut scores = vec![0.0_f64; n];

    for _k in 0..max_outliers {
        if data.len() < 3 {
            break;
        }

        let mean = calculate_mean(&data);
        let std_dev = calculate_std_dev(&data);

        if std_dev == 0.0 {
            break;
        }
        // Robust location and scale: median + MAD
        let med = median(&data);
        let mad = mad(&data, med);

        // Avoid division by zero; if MAD ~ 0, we can't meaningfully scale.
        if mad == 0.0 {
            break;
        }

        // Compute z-scores for all remaining points
        let z_scores: Vec<f64> = data.iter().map(|&v| ((v - med) / mad).abs()).collect();

        // Find max deviation
        let (max_idx, max_val) = z_scores
            .iter()
            .enumerate()
            .map(|(i, &z)| (i, z))
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .unwrap();

        // Calculate critical value (lambda)
        let p = if hybrid {
            1.0 - alpha / (2.0 * (n - anomalies.len()) as f64)
        } else {
            1.0 - alpha / (2.0 * n as f64)
        };

        let df = (data.len() - 2) as f64;
        let t_dist = StudentsT::new(0.0, 1.0, df).map_err(|e| {
            TimeSeriesAnalysisError::InvalidParameter {
                name: "df".to_string(),
                message: format!("Failed to create t-distribution: {}", e),
            }
        })?;

        let t = t_dist.inverse_cdf(p);
        let lambda = ((data.len() - 1) as f64 * t)
            / ((data.len() as f64 - 2.0 + t.powi(2)).sqrt() * data.len() as f64);

        if max_val > lambda {
            // Normalized score: stat / (stat + lambda) maps smoothly to (0, 1).
            let score = if lambda > 0.0 {
                max_val / (max_val + lambda)
            } else {
                1.0
            };

            let original_idx = indices[max_idx];
            let value = data[max_idx];
            scores[original_idx] = score;

            // Determine a signal direction based on whether the value is above or below mean
            let signal = if value > mean {
                AnomalySignal::Positive
            } else {
                AnomalySignal::Negative
            };

            anomalies.push(Anomaly {
                signal,
                value: residuals[original_idx],
                score,
                index: original_idx,
            });

            data.remove(max_idx);
            indices.remove(max_idx);
        } else {
            // Even the most extreme remaining point didn't exceed the threshold.
            // Assign sub-threshold scores to all remaining points for completeness.
            for (i, &z) in z_scores.iter().enumerate() {
                let score = if lambda > 0.0 { z / (z + lambda) } else { 0.0 };
                scores[indices[i]] = score;
            }
            break;
        }
    }

    Ok((anomalies, scores))
}

pub fn sh_esd_mad(
    y: &[f64],
    period: usize,
    max_anoms_frac: f64,
    alpha: f64,
) -> TimeSeriesAnalysisResult<Vec<Anomaly>> {
    assert!(!y.is_empty());
    assert!(period > 0);
    assert!((0.0..=1.0).contains(&max_anoms_frac));
    assert!((0.0..1.0).contains(&alpha));

    let n = y.len();
    let k_max = (max_anoms_frac * n as f64).floor() as usize;
    if k_max == 0 {
        return Ok(Vec::new());
    }

    let resid = calculate_stl_residual(y, period)?;

    let mut x: Vec<f64> = resid.clone();
    let mut remaining_idx: Vec<usize> = (0..n).collect();

    // (orig_idx, R_i, lambda_i, Direction, score)
    let mut candidates: Vec<(usize, f64, f64, AnomalySignal, f64)> = Vec::with_capacity(k_max);

    for i in 0..k_max {
        if x.len() <= 2 {
            break;
        }

        let med = median(&x);
        let mad = mad(&x, med);
        if mad == 0.0 {
            break;
        }

        let mut max_dev = f64::NEG_INFINITY;
        let mut max_dir = AnomalySignal::None;
        let mut j_star = 0usize;

        for (j, &val) in x.iter().enumerate() {
            let base = (val - med) / mad; // signed
            let dev = base.abs();
            if dev > max_dev {
                max_dev = dev;
                max_dir = if base >= 0.0 {
                    AnomalySignal::Positive
                } else {
                    AnomalySignal::Negative
                };
                j_star = j;
            }
        }

        if max_dev <= 0.0 {
            break;
        }

        let r_i = max_dev;

        let m = n - i;
        if m <= 2 {
            break;
        }
        let p = 1.0 - alpha / (2.0 * (m as f64));
        let df = (m - 2) as f64;
        let t_dist = StudentsT::new(0.0, 1.0, df).unwrap();
        let t = t_dist.inverse_cdf(p);

        let m_f = m as f64;
        let lambda = ((m_f - 1.0) * t) / (((m_f - 2.0 + t * t) * m_f).sqrt());

        let orig_idx = remaining_idx[j_star];
        let score = if lambda > 0.0 {
            max_dev / (max_dev + lambda)
        } else {
            1.0
        };
        candidates.push((orig_idx, r_i, lambda, max_dir, score));

        x.remove(j_star);
        remaining_idx.remove(j_star);
    }

    let mut k_star = 0usize;
    for (idx, &(_, r_i, lambda_i, _, _)) in candidates.iter().enumerate() {
        if r_i > lambda_i {
            k_star = idx + 1;
        }
    }
    if k_star == 0 {
        return Ok(Vec::new());
    }

    let mut selected: Vec<Anomaly> = candidates[..k_star]
        .iter()
        .map(|(idx, _r, _lambda_i, dir, score)| Anomaly {
            signal: *dir,
            value: resid[*idx],
            score: *score,
            index: *idx,
        })
        .collect();

    selected.sort_by_key(|a| a.index);
    Ok(selected)
}

fn median(x: &[f64]) -> f64 {
    let n = x.len();
    assert!(n > 0);
    let mut v: Vec<f64> = x.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if n % 2 == 1 {
        v[n / 2]
    } else {
        let a = v[n / 2 - 1];
        let b = v[n / 2];
        0.5 * (a + b)
    }
}

fn mad(x: &[f64], med: f64) -> f64 {
    if x.is_empty() {
        return 0.0;
    }
    let mut devs: Vec<f64> = x.iter().map(|v| (v - med).abs()).collect();
    devs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let m = median(&devs);
    // Scale factor for consistency with normal distribution (optional).
    1.4826 * m
}

#[cfg(test)]
mod tests {}
