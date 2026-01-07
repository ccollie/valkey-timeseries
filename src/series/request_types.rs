pub(crate) use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::series::chunks::TimeSeriesChunk;
use crate::series::{TimestampRange, ValueFilter};
use valkey_module::{ValkeyResult, ValkeyString, ValkeyValue};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AggregationOptions {
    pub aggregation: AggregationType,
    pub bucket_duration: u64,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub report_empty: bool,
}

#[derive(Default, Clone, Debug)]
pub struct MatchFilterOptions {
    pub date_range: Option<TimestampRange>,
    pub matchers: Vec<SeriesSelector>,
    pub limit: Option<usize>,
}

impl From<Vec<SeriesSelector>> for MatchFilterOptions {
    fn from(matchers: Vec<SeriesSelector>) -> Self {
        Self {
            matchers,
            ..Default::default()
        }
    }
}

impl From<SeriesSelector> for MatchFilterOptions {
    fn from(matcher: SeriesSelector) -> Self {
        Self {
            matchers: vec![matcher],
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct RangeGroupingOptions {
    pub aggregation: AggregationType,
    pub group_label: String,
}

#[derive(Debug, Default, Clone)]
pub struct RangeOptions {
    pub date_range: TimestampRange,
    pub count: Option<usize>,
    pub latest: bool,
    pub aggregation: Option<AggregationOptions>,
    pub timestamp_filter: Option<Vec<Timestamp>>,
    pub value_filter: Option<ValueFilter>,
}

impl RangeOptions {
    pub fn get_timestamp_range(&self) -> (Timestamp, Timestamp) {
        self.date_range.get_timestamps(None)
    }

    pub fn with_range(start_ts: Timestamp, end_ts: Timestamp) -> ValkeyResult<Self> {
        Ok(Self {
            date_range: TimestampRange::from_timestamps(start_ts, end_ts)?,
            ..Default::default()
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct MRangeOptions {
    pub range: RangeOptions,
    pub filters: Vec<SeriesSelector>,
    pub with_labels: bool,
    pub selected_labels: Vec<String>,
    pub grouping: Option<RangeGroupingOptions>,
    pub is_reverse: bool,
}

#[derive(Default, Clone, Debug)]
pub(crate) struct MRangeSeriesResult {
    pub key: String,
    pub group_label_value: Option<String>,
    pub labels: Vec<Label>,
    pub data: TimeSeriesChunk,
}

impl From<MRangeSeriesResult> for ValkeyValue {
    fn from(series: MRangeSeriesResult) -> Self {
        let labels: Vec<_> = series
            .labels
            .into_iter()
            .map(|label| label.into())
            .collect();

        let samples: Vec<_> = series.data.iter().map(|s| s.into()).collect();
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
    pub filters: Vec<SeriesSelector>,
    pub selected_labels: Vec<String>,
    pub latest: bool,
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
