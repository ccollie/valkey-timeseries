use crate::common::Sample;
use metricsql_runtime::types::MetricName;
use std::fmt::Display;

#[derive(Debug)]
pub struct InstantQueryResult {
    pub metric: MetricName,
    pub sample: Sample,
}

#[derive(Debug, Clone)]
pub struct RangeQueryResult {
    pub metric: MetricName,
    pub samples: Vec<Sample>,
}

impl Display for RangeQueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RangeQueryResult {{ metric: {}, samples: {:?} }}",
            self.metric, self.samples
        )
    }
}

#[derive(Debug, Default)]
pub struct InstantResult(pub Vec<InstantQueryResult>);

impl InstantResult {
    pub fn remove(&mut self, index: usize) -> InstantQueryResult {
        self.0.remove(index)
    }

    pub fn into_iter(self) -> impl Iterator<Item = InstantQueryResult> {
        self.0.into_iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &InstantQueryResult> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Result represents the expected response from the provider
#[derive(Debug, Default, Clone)]
pub struct RangeResult {
    /// Data contains a list of received Metric
    pub data: Vec<RangeQueryResult>,
}

impl RangeResult {
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
