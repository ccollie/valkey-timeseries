//! Error types for the time series module

use crate::error::TsdbError;
use thiserror::Error;

pub const INSUFFICIENT_DATA_ERROR: &str = "TSDB: insufficient samples for anomaly detection";

/// Error type for time series analysis operations
#[derive(Debug, Error)]
pub enum TimeSeriesAnalysisError {
    /// Invalid input data
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Insufficient data for operation
    #[error("Insufficient data: {message}. Need at least {required} observations, got {actual}")]
    InsufficientData {
        /// Error message
        message: String,
        /// Required number of observations
        required: usize,
        /// Actual number of observations
        actual: usize,
    },

    /// Invalid model configuration
    #[error("Invalid model configuration: {0}")]
    InvalidModel(String),

    /// Model fitting error
    #[error("Model fitting error: {0}")]
    FittingError(String),

    /// Forecasting error
    #[error("Forecasting error: {0}")]
    ForecastingError(String),

    /// Decomposition error
    #[error("Decomposition error: {0}")]
    DecompositionError(String),

    /// Feature extraction error
    #[error("Feature extraction error: {0}")]
    FeatureExtractionError(String),

    /// Statistical error
    #[error("Statistical error: {0}")]
    StatisticalError(String),

    /// Anomaly detection error
    #[error("Anomaly detection error: {0}")]
    AnomalyDetectionError(String),

    /// Optimization error
    #[error("Optimization error: {0}")]
    OptimizationError(String),

    /// Convergence error
    #[error("Failed to converge after {iterations} iterations")]
    ConvergenceError {
        /// Number of iterations attempted
        iterations: usize,
    },

    /// Computation error
    #[error("Computation error: {0}")]
    ComputationError(String),

    /// Dimension mismatch
    #[error("Dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch {
        /// Expected dimension
        expected: usize,
        /// Actual dimension
        actual: usize,
    },

    /// Invalid parameter value
    #[error("Invalid parameter '{name}': {message}")]
    InvalidParameter {
        /// Parameter name
        name: String,
        /// Error message
        message: String,
    },

    /// Model not fitted
    #[error("Model not fitted: {0}")]
    ModelNotFitted(String),

    /// Not implemented
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Other error
    #[error("Error: {0}")]
    Other(String),

    /// Core error
    #[error("Core error: {0}")]
    CoreError(#[from] TsdbError),

    #[error("Model not trained")]
    NotTrained,
}

/// Result type for time series operations
pub type TimeSeriesAnalysisResult<T = ()> = Result<T, TimeSeriesAnalysisError>;
