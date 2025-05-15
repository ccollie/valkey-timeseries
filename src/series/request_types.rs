use crate::aggregators::{Aggregation, BucketAlignment, BucketTimestamp};
use crate::common::{Sample, Timestamp};
use crate::labels::matchers::Matchers;
use crate::labels::Label;
use crate::series::{TimestampRange, ValueFilter};
use valkey_module::{ValkeyString, ValkeyValue};

#[derive(Debug, Clone)]
pub struct AggregationOptions {
    pub aggregation: Aggregation,
    pub bucket_duration: u64,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub report_empty: bool,
}

#[derive(Default, Clone, Debug)]
pub struct MatchFilterOptions {
    pub date_range: Option<TimestampRange>,
    pub matchers: Vec<Matchers>,
    pub limit: Option<usize>,
}

impl From<Vec<Matchers>> for MatchFilterOptions {
    fn from(matchers: Vec<Matchers>) -> Self {
        Self {
            matchers,
            ..Default::default()
        }
    }
}

impl From<Matchers> for MatchFilterOptions {
    fn from(matcher: Matchers) -> Self {
        Self {
            matchers: vec![matcher],
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub(crate) aggregation: Aggregation,
    pub(crate) group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub aggregation: Option<AggregationOptions>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
    pub filters: Vec<Matchers>,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub grouping: Option<RangeGroupingOptions>,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct MRangeSeriesResult {
    pub(crate) key: String,
    pub(crate) group_label_value: Option<String>,
    pub(crate) labels: Vec<Option<Label>>,
    pub(crate) samples: Vec<Sample>,
}

impl From<MRangeSeriesResult> for ValkeyValue {
    fn from(series: MRangeSeriesResult) -> Self {
        let labels: Vec<_> = series
            .labels
            .into_iter()
            .map(|label| match label {
                Some(label) => label.into(),
                None => ValkeyValue::Null,
            })
            .collect();

        let samples: Vec<_> = series.samples.into_iter().map(|s| s.into()).collect();
        let series = vec![
            ValkeyValue::BulkString(series.key),
            ValkeyValue::Array(labels),
            ValkeyValue::Array(samples),
        ];
        ValkeyValue::Array(series)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MGetRequest {
    pub with_labels: bool,
    pub filters: Vec<Matchers>,
    pub selected_labels: Vec<String>,
}

pub struct MGetSeriesData {
    pub series_key: ValkeyString,
    pub labels: Vec<Option<Label>>,
    pub sample: Option<Sample>,
}

impl From<MGetSeriesData> for ValkeyValue {
    fn from(series: MGetSeriesData) -> Self {
        let labels: Vec<_> = series
            .labels
            .into_iter()
            .map(|label| match label {
                Some(label) => label.into(),
                None => ValkeyValue::Null,
            })
            .collect();

        let sample_value: ValkeyValue = if let Some(sample) = series.sample {
            sample.into()
        } else {
            ValkeyValue::Array(vec![])
        };
        let series = vec![
            ValkeyValue::from(series.series_key),
            ValkeyValue::Array(labels),
            sample_value,
        ];
        ValkeyValue::Array(series)
    }
}

#[cfg(test)]
mod tests {}
