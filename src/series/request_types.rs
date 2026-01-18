use crate::aggregators::{
    Aggregator, AllAggregator, AnyAggregator, CountIfAggregator, NoneAggregator, ShareAggregator,
    SumIfAggregator,
};
use crate::common::binop::ComparisonOperator;
use crate::common::hash::hash_f64;
use crate::common::{Sample, Timestamp};
use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::series::chunks::TimeSeriesChunk;
use crate::series::{TimestampRange, ValueFilter};
use get_size2::GetSize;
use std::hash::Hash;
use valkey_module::{RedisModuleIO, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

pub use crate::aggregators::{AggregationType, BucketAlignment, BucketTimestamp};
use crate::common::rdb::{rdb_load_f64, rdb_load_u8, rdb_save_f64, rdb_save_u8};

#[derive(Debug, Copy, Clone, GetSize)]
pub struct ValueComparisonFilter {
    pub operator: ComparisonOperator,
    pub value: f64,
}

impl ValueComparisonFilter {
    pub(crate) fn save_to_rdb(&self, rdb: *mut RedisModuleIO) {
        let op = self.operator as u8;
        rdb_save_u8(rdb, op);
        rdb_save_f64(rdb, self.value);
    }

    pub fn load_from_rdb(rdb: *mut RedisModuleIO) -> ValkeyResult<Self> {
        let op_byte = rdb_load_u8(rdb)?;
        let operator: ComparisonOperator = op_byte.try_into()?;
        let value = rdb_load_f64(rdb)?;
        Ok(Self { operator, value })
    }
}

impl Hash for ValueComparisonFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.operator.hash(state);
        hash_f64(self.value, state);
    }
}

impl PartialEq for ValueComparisonFilter {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && (self.value.is_nan() && other.value.is_nan() || self.value == other.value)
    }
}

impl Default for ValueComparisonFilter {
    fn default() -> Self {
        Self {
            operator: ComparisonOperator::NotEqual,
            value: f64::NAN,
        }
    }
}

impl ValueComparisonFilter {
    pub fn compare(&self, sample_value: f64) -> bool {
        self.operator.compare(sample_value, self.value)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq)]
pub struct AggregatorConfig {
    pub(crate) aggregation: AggregationType,
    pub(crate) value_filter: Option<ValueComparisonFilter>,
}

impl AggregatorConfig {
    pub fn new(
        aggregation: AggregationType,
        value_filter: Option<ValueComparisonFilter>,
    ) -> ValkeyResult<Self> {
        if aggregation.is_filtered() && value_filter.is_none() {
            return Err(ValkeyError::Str("TSDB: missing condition for aggregator"));
        } else if !(aggregation.is_filtered() || aggregation.has_filtered_variant())
            && value_filter.is_some()
        {
            return Err(ValkeyError::Str(
                "TSDB: aggregation type does not support a filter condition",
            ));
        }
        Ok(Self {
            aggregation,
            value_filter,
        })
    }

    pub fn aggregation_type(&self) -> AggregationType {
        self.aggregation
    }

    pub fn aggregation_name(&self) -> &'static str {
        self.aggregation.name()
    }

    pub fn filter(&self) -> Option<ValueComparisonFilter> {
        self.value_filter
    }

    pub fn create_aggregator(&self) -> Aggregator {
        let aggr_type = self.aggregation;
        if let Some(filter) = self.value_filter {
            match self.aggregation {
                AggregationType::All => {
                    Aggregator::All(AllAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Any => {
                    Aggregator::Any(AnyAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Count | AggregationType::CountIf => {
                    Aggregator::CountIf(CountIfAggregator::new(filter.operator, filter.value))
                }
                AggregationType::None => {
                    Aggregator::None(NoneAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Share => {
                    Aggregator::Share(ShareAggregator::new(filter.operator, filter.value))
                }
                AggregationType::Sum | AggregationType::SumIf => {
                    Aggregator::SumIf(SumIfAggregator::new(filter.operator, filter.value))
                }
                _ => aggr_type.into(),
            }
        } else {
            aggr_type.into()
        }
    }
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            aggregation: AggregationType::Avg,
            value_filter: None,
        }
    }
}

// Allow easy conversion from AggregationType to AggregatorConfig without filter
impl From<AggregationType> for AggregatorConfig {
    fn from(aggregation: AggregationType) -> Self {
        Self {
            aggregation,
            value_filter: None,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct AggregationOptions {
    pub aggregation: AggregatorConfig,
    pub bucket_duration: u64,
    pub timestamp_output: BucketTimestamp,
    pub alignment: BucketAlignment,
    pub report_empty: bool,
}

impl Default for AggregationOptions {
    fn default() -> Self {
        Self {
            aggregation: AggregatorConfig::default(),
            bucket_duration: 60_000, // default 1 minute
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Default,
            report_empty: false,
        }
    }
}

impl AggregationOptions {
    pub fn create_aggregator(&self) -> Aggregator {
        self.aggregation.create_aggregator()
    }
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
    pub aggregation: AggregatorConfig,
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
