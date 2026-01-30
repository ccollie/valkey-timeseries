use super::filters::{deserialize_matchers_list, serialize_matchers_list};
use super::generated::{
    AggregationOptions as FanoutAggregationOptions, AggregationType as FanoutAggregationType,
    AggregatorConfig as FanoutAggregatorConfig, BucketAlignmentType, BucketTimestampType,
    ComparisonOperator as FanoutComparisonOperator, CompressionType as FanoutChunkEncoding,
    DateRange, GroupingOptions as FanoutGroupingOptions, Label as FanoutLabel,
    MetaDateRangeFilter as FanoutMetaDateRangeFilter, MultiRangeRequest,
    PostingStat as FanoutPostingStat, RangeRequest, Sample as FanoutSample,
    SeriesSelector as FanoutSeriesSelector, StatsResponse,
    ValueComparisonFilter as FanoutValueComparisonFilter, ValueRange as FanoutValueFilter,
};
use crate::commands::fanout::MGetValue;
use crate::common::binop::ComparisonOperator;
use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::series::chunks::ChunkEncoding;
use crate::series::request_types::{
    AggregationOptions, AggregationType, AggregatorConfig, BucketAlignment, MGetSeriesData,
    MRangeOptions, MatchFilterOptions, MetaDateRangeFilter, RangeGroupingOptions, RangeOptions,
    ValueComparisonFilter,
};
use crate::series::{TimestampRange, ValueFilter};
use crate::{
    aggregators::BucketTimestamp,
    error_consts,
    series::index::{PostingStat, PostingsStats},
};
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

impl From<ComparisonOperator> for FanoutComparisonOperator {
    fn from(value: ComparisonOperator) -> Self {
        match value {
            ComparisonOperator::Equal => FanoutComparisonOperator::Eq,
            ComparisonOperator::NotEqual => FanoutComparisonOperator::Neq,
            ComparisonOperator::GreaterThan => FanoutComparisonOperator::Gt,
            ComparisonOperator::GreaterThanOrEqual => FanoutComparisonOperator::Gte,
            ComparisonOperator::LessThan => FanoutComparisonOperator::Lt,
            ComparisonOperator::LessThanOrEqual => FanoutComparisonOperator::Lte,
        }
    }
}

impl From<FanoutComparisonOperator> for ComparisonOperator {
    fn from(value: FanoutComparisonOperator) -> Self {
        match value {
            FanoutComparisonOperator::Eq => ComparisonOperator::Equal,
            FanoutComparisonOperator::Neq => ComparisonOperator::NotEqual,
            FanoutComparisonOperator::Gt => ComparisonOperator::GreaterThan,
            FanoutComparisonOperator::Gte => ComparisonOperator::GreaterThanOrEqual,
            FanoutComparisonOperator::Lt => ComparisonOperator::LessThan,
            FanoutComparisonOperator::Lte => ComparisonOperator::LessThanOrEqual,
        }
    }
}

impl From<ChunkEncoding> for FanoutChunkEncoding {
    fn from(value: ChunkEncoding) -> Self {
        match value {
            ChunkEncoding::Uncompressed => FanoutChunkEncoding::Uncompressed,
            ChunkEncoding::Gorilla => FanoutChunkEncoding::Gorilla,
            ChunkEncoding::Pco => FanoutChunkEncoding::Pco,
        }
    }
}

impl From<FanoutChunkEncoding> for ChunkEncoding {
    fn from(value: FanoutChunkEncoding) -> Self {
        match value {
            FanoutChunkEncoding::Uncompressed => ChunkEncoding::Uncompressed,
            FanoutChunkEncoding::Gorilla => ChunkEncoding::Gorilla,
            FanoutChunkEncoding::Pco => ChunkEncoding::Pco,
        }
    }
}

impl From<TimestampRange> for DateRange {
    fn from(value: TimestampRange) -> Self {
        let (start, end) = value.get_timestamps(None);
        DateRange { start, end }
    }
}

impl From<DateRange> for TimestampRange {
    fn from(value: DateRange) -> Self {
        TimestampRange::from_timestamps(value.start, value.end)
            .expect("Invalid date range in decode_date_range")
    }
}

impl From<FanoutPostingStat> for PostingStat {
    fn from(value: FanoutPostingStat) -> Self {
        PostingStat {
            name: value.name,
            count: value.count,
        }
    }
}

impl From<PostingStat> for FanoutPostingStat {
    fn from(value: PostingStat) -> Self {
        FanoutPostingStat {
            name: value.name,
            count: value.count,
        }
    }
}

impl From<PostingsStats> for StatsResponse {
    fn from(value: PostingsStats) -> Self {
        StatsResponse {
            cardinality_metric_stats: value
                .cardinality_metrics_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            cardinality_label_stats: value
                .cardinality_label_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            label_value_stats: value
                .label_value_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            label_value_pairs_stats: value
                .label_value_pairs_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            num_label_pairs: value.num_label_pairs as u64,
            num_labels: value.num_labels as u64,
            series_count: value.series_count,
        }
    }
}

impl From<StatsResponse> for PostingsStats {
    fn from(value: StatsResponse) -> Self {
        PostingsStats {
            cardinality_metrics_stats: value
                .cardinality_metric_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            cardinality_label_stats: value
                .cardinality_label_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            label_value_stats: value
                .label_value_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            label_value_pairs_stats: value
                .label_value_pairs_stats
                .into_iter()
                .map(|s| s.into())
                .collect(),
            num_label_pairs: value.num_label_pairs as usize,
            num_labels: value.num_labels as usize,
            series_count: value.series_count,
        }
    }
}

impl From<BucketTimestamp> for BucketTimestampType {
    fn from(value: BucketTimestamp) -> Self {
        match value {
            BucketTimestamp::Start => BucketTimestampType::Start,
            BucketTimestamp::End => BucketTimestampType::End,
            BucketTimestamp::Mid => BucketTimestampType::Mid,
        }
    }
}

impl From<BucketTimestampType> for BucketTimestamp {
    fn from(value: BucketTimestampType) -> Self {
        match value {
            BucketTimestampType::Start => BucketTimestamp::Start,
            BucketTimestampType::End => BucketTimestamp::End,
            BucketTimestampType::Mid => BucketTimestamp::Mid,
        }
    }
}

impl From<BucketAlignmentType> for BucketAlignment {
    fn from(value: BucketAlignmentType) -> Self {
        match value {
            BucketAlignmentType::Default => BucketAlignment::Default,
            BucketAlignmentType::AlignStart => BucketAlignment::Start,
            BucketAlignmentType::AlignEnd => BucketAlignment::End,
            BucketAlignmentType::Timestamp => BucketAlignment::Timestamp(0),
        }
    }
}

impl From<AggregationType> for FanoutAggregationType {
    fn from(value: AggregationType) -> Self {
        match value {
            AggregationType::All => FanoutAggregationType::All,
            AggregationType::Any => FanoutAggregationType::Any,
            AggregationType::Avg => FanoutAggregationType::Avg,
            AggregationType::Count => FanoutAggregationType::Count,
            AggregationType::CountIf => FanoutAggregationType::CountIf,
            AggregationType::First => FanoutAggregationType::First,
            AggregationType::Increase => FanoutAggregationType::Increase,
            AggregationType::IRate => FanoutAggregationType::Irate,
            AggregationType::Last => FanoutAggregationType::Last,
            AggregationType::Min => FanoutAggregationType::Min,
            AggregationType::Max => FanoutAggregationType::Max,
            AggregationType::None => FanoutAggregationType::None,
            AggregationType::Sum => FanoutAggregationType::Sum,
            AggregationType::SumIf => FanoutAggregationType::SumIf,
            AggregationType::Range => FanoutAggregationType::Range,
            AggregationType::Rate => FanoutAggregationType::Rate,
            AggregationType::Share => FanoutAggregationType::ShareIf,
            AggregationType::StdP => FanoutAggregationType::StdP,
            AggregationType::StdS => FanoutAggregationType::StdS,
            AggregationType::VarP => FanoutAggregationType::VarP,
            AggregationType::VarS => FanoutAggregationType::VarS,
        }
    }
}

impl From<AggregatorConfig> for FanoutAggregationType {
    fn from(value: AggregatorConfig) -> Self {
        value.aggregation_type().into()
    }
}

impl From<FanoutAggregationType> for FanoutAggregatorConfig {
    fn from(value: FanoutAggregationType) -> Self {
        FanoutAggregatorConfig {
            aggregator_type: value as i32,
            value_filter: None,
        }
    }
}

impl From<FanoutAggregationType> for AggregationType {
    fn from(value: FanoutAggregationType) -> Self {
        match value {
            FanoutAggregationType::All => AggregationType::All,
            FanoutAggregationType::Any => AggregationType::Any,
            FanoutAggregationType::Avg => AggregationType::Avg,
            FanoutAggregationType::Count => AggregationType::Count,
            FanoutAggregationType::CountIf => AggregationType::CountIf,
            FanoutAggregationType::First => AggregationType::First,
            FanoutAggregationType::Increase => AggregationType::Increase,
            FanoutAggregationType::Irate => AggregationType::IRate,
            FanoutAggregationType::Last => AggregationType::Last,
            FanoutAggregationType::Max => AggregationType::Max,
            FanoutAggregationType::Min => AggregationType::Min,
            FanoutAggregationType::None => AggregationType::None,
            FanoutAggregationType::Range => AggregationType::Range,
            FanoutAggregationType::Rate => AggregationType::Rate,
            FanoutAggregationType::ShareIf => AggregationType::Share,
            FanoutAggregationType::Sum => AggregationType::Sum,
            FanoutAggregationType::SumIf => AggregationType::SumIf,
            FanoutAggregationType::StdP => AggregationType::StdP,
            FanoutAggregationType::StdS => AggregationType::StdS,
            FanoutAggregationType::VarP => AggregationType::VarP,
            FanoutAggregationType::VarS => AggregationType::VarS,
        }
    }
}

impl TryFrom<FanoutAggregatorConfig> for AggregatorConfig {
    type Error = ValkeyError;

    fn try_from(value: FanoutAggregatorConfig) -> Result<Self, Self::Error> {
        let aggr_type: FanoutAggregationType = value
            .aggregator_type
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?;
        let aggregation_type: AggregationType = aggr_type.into();

        let filter = value.value_filter.map(|f| f.into());

        AggregatorConfig::new(aggregation_type, filter)
    }
}

impl TryFrom<&FanoutGroupingOptions> for RangeGroupingOptions {
    type Error = ValkeyError;

    fn try_from(value: &FanoutGroupingOptions) -> Result<RangeGroupingOptions, ValkeyError> {
        let aggregation: AggregatorConfig = value
            .aggregation
            .unwrap_or_default()
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?; // todo: serialization error

        Ok(RangeGroupingOptions {
            aggregation,
            group_label: value.group_label.clone(),
        })
    }
}

impl TryFrom<FanoutGroupingOptions> for RangeGroupingOptions {
    type Error = ValkeyError;

    fn try_from(value: FanoutGroupingOptions) -> Result<RangeGroupingOptions, ValkeyError> {
        let aggregation_input = value.aggregation.unwrap_or_default();
        let aggregation = aggregation_input.try_into()?;

        Ok(RangeGroupingOptions {
            aggregation,
            group_label: value.group_label,
        })
    }
}

impl From<&RangeGroupingOptions> for FanoutGroupingOptions {
    fn from(value: &RangeGroupingOptions) -> Self {
        let aggregation: FanoutAggregatorConfig = value.aggregation.into();
        FanoutGroupingOptions {
            aggregation: Some(aggregation),
            group_label: value.group_label.clone(),
        }
    }
}

impl From<RangeGroupingOptions> for FanoutGroupingOptions {
    fn from(value: RangeGroupingOptions) -> Self {
        let aggregation: FanoutAggregatorConfig = value.aggregation.into();
        FanoutGroupingOptions {
            aggregation: Some(aggregation),
            group_label: value.group_label,
        }
    }
}

impl From<MGetSeriesData> for MGetValue {
    fn from(value: MGetSeriesData) -> Self {
        let labels = value
            .labels
            .into_iter()
            .map(|l| l.map_or_else(FanoutLabel::default, |l| l.into()))
            .collect();

        let sample = value.sample.map(|s| FanoutSample {
            timestamp: s.timestamp,
            value: s.value,
        });

        MGetValue {
            key: value.series_key.to_string_lossy(),
            labels,
            sample,
        }
    }
}

pub fn deserialize_match_filter_options(
    range: Option<FanoutMetaDateRangeFilter>,
    filters: Option<Vec<FanoutSeriesSelector>>,
) -> ValkeyResult<MatchFilterOptions> {
    let date_range: Option<MetaDateRangeFilter> = range.map(|r| r.into());
    let matchers: Vec<SeriesSelector> = deserialize_matchers_list(filters)?;
    Ok(MatchFilterOptions {
        date_range,
        matchers,
        limit: None,
    })
}

impl From<FanoutLabel> for Label {
    fn from(value: FanoutLabel) -> Self {
        let name = value.name.to_string();
        let value = value.value.to_string();
        Label { name, value }
    }
}

impl From<&Label> for FanoutLabel {
    fn from(value: &Label) -> Self {
        FanoutLabel {
            name: value.name.clone(),
            value: value.value.clone(),
        }
    }
}

impl From<Label> for FanoutLabel {
    fn from(value: Label) -> Self {
        FanoutLabel {
            name: value.name,
            value: value.value,
        }
    }
}

impl From<FanoutSample> for ValkeyValue {
    fn from(value: FanoutSample) -> Self {
        let row = vec![
            ValkeyValue::from(value.timestamp),
            ValkeyValue::from(value.value),
        ];
        ValkeyValue::from(row)
    }
}

impl From<ValueComparisonFilter> for FanoutValueComparisonFilter {
    fn from(value: ValueComparisonFilter) -> Self {
        let fanout_operator: FanoutComparisonOperator = value.operator.into();
        FanoutValueComparisonFilter {
            operator: fanout_operator.into(),
            value: value.value,
        }
    }
}

impl From<FanoutValueComparisonFilter> for ValueComparisonFilter {
    fn from(value: FanoutValueComparisonFilter) -> Self {
        let fanout_operator: FanoutComparisonOperator = value.operator.try_into().unwrap();
        let operator: ComparisonOperator = fanout_operator.into();
        ValueComparisonFilter {
            operator,
            value: value.value,
        }
    }
}

impl From<AggregatorConfig> for FanoutAggregatorConfig {
    fn from(value: AggregatorConfig) -> Self {
        let aggr_type: FanoutAggregationType = value.aggregation_type().into();
        FanoutAggregatorConfig {
            aggregator_type: aggr_type.into(),
            value_filter: value.filter().map(|filter| filter.into()),
        }
    }
}

impl From<AggregationOptions> for FanoutAggregationOptions {
    fn from(value: AggregationOptions) -> Self {
        let aggregator: FanoutAggregatorConfig = value.aggregation.into();
        let bucket_timestamp_type: BucketTimestampType = value.timestamp_output.into();

        let (bucket_alignment, alignment_timestamp) = match value.alignment {
            BucketAlignment::Default => (BucketAlignmentType::Default, 0),
            BucketAlignment::Start => (BucketAlignmentType::AlignStart, 0),
            BucketAlignment::End => (BucketAlignmentType::AlignEnd, 0),
            BucketAlignment::Timestamp(ts) => (BucketAlignmentType::Timestamp, ts),
        };

        FanoutAggregationOptions {
            aggregator: Some(aggregator),
            bucket_duration: value.bucket_duration as u32,
            bucket_timestamp_type: bucket_timestamp_type.into(),
            bucket_alignment: bucket_alignment.into(),
            alignment_timestamp,
            report_empty: value.report_empty,
        }
    }
}

impl TryFrom<FanoutAggregationOptions> for AggregationOptions {
    type Error = ValkeyError;

    fn try_from(value: FanoutAggregationOptions) -> Result<Self, Self::Error> {
        let aggregation: AggregatorConfig = if let Some(agg) = value.aggregator {
            agg.try_into()?
        } else {
            return Err(ValkeyError::Str("TSDB: aggregation config is required"));
        };
        let bucket_duration = value.bucket_duration as u64;
        if bucket_duration == 0 {
            return Err(ValkeyError::Str("TSDB: bucket duration must be positive"));
        }
        let timestamp_output: BucketTimestampType = value
            .bucket_timestamp_type
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_BUCKET_TIMESTAMP_TYPE))?;
        let fanout_alignment: BucketAlignmentType = value
            .bucket_alignment
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::INVALID_BUCKET_ALIGNMENT))?;

        let mut alignment: BucketAlignment = fanout_alignment.into();
        if matches!(alignment, BucketAlignment::Timestamp(_)) {
            let timestamp = value.alignment_timestamp;
            alignment = BucketAlignment::Timestamp(timestamp);
        }

        let report_empty = value.report_empty;

        Ok(AggregationOptions {
            aggregation,
            bucket_duration,
            timestamp_output: timestamp_output.into(),
            alignment,
            report_empty,
        })
    }
}

impl TryFrom<RangeRequest> for RangeOptions {
    type Error = ValkeyError;

    fn try_from(value: RangeRequest) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&RangeRequest> for RangeOptions {
    type Error = ValkeyError;

    fn try_from(value: &RangeRequest) -> Result<Self, Self::Error> {
        let date_range: TimestampRange = match value.range {
            Some(r) => r.into(),
            None => {
                return Err(ValkeyError::Str("TSDB: date range is required"));
            }
        };

        let count = if value.count == 0 {
            None
        } else {
            Some(value.count as usize)
        };

        let aggregation = if let Some(aggregation) = value.aggregation {
            let options = aggregation.try_into()?;
            Some(options)
        } else {
            None
        };

        let timestamp_filter = if value.timestamp_filter.is_empty() {
            None
        } else {
            Some(value.timestamp_filter.clone())
        };

        let value_filter: Option<ValueFilter> = value.value_filter.map(|filter| ValueFilter {
            min: filter.min,
            max: filter.max,
        });

        let latest = value.latest;

        Ok(RangeOptions {
            date_range,
            count,
            aggregation,
            timestamp_filter,
            value_filter,
            latest,
        })
    }
}

impl From<&RangeOptions> for RangeRequest {
    fn from(value: &RangeOptions) -> Self {
        let range: DateRange = value.date_range.into();

        let count = match value.count {
            Some(c) => c as u32,
            None => 0,
        };

        let aggregation = if let Some(aggregation) = value.aggregation {
            let options: FanoutAggregationOptions = aggregation.into();
            Some(options)
        } else {
            None
        };

        let timestamp_filter = match value.timestamp_filter {
            Some(ref ts) => ts.clone(),
            None => vec![],
        };

        let value_filter: Option<FanoutValueFilter> =
            value.value_filter.map(|filter| FanoutValueFilter {
                min: filter.min,
                max: filter.max,
            });

        RangeRequest {
            range: Some(range),
            count,
            aggregation,
            timestamp_filter,
            value_filter,
            latest: value.latest,
        }
    }
}

impl TryFrom<&MultiRangeRequest> for MRangeOptions {
    type Error = ValkeyError;

    fn try_from(value: &MultiRangeRequest) -> Result<Self, Self::Error> {
        let range: RangeOptions = if let Some(r) = &value.range {
            r.try_into()?
        } else {
            return Err(ValkeyError::Str("TSDB: range is required"));
        };

        let mut filters: Vec<SeriesSelector> = Vec::with_capacity(value.filters.len());
        for filter in value.filters.iter() {
            filters.push(filter.try_into()?);
        }
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels.clone();

        let grouping: Option<RangeGroupingOptions> = match &value.grouping {
            Some(group) => Some(group.try_into()?),
            None => None,
        };

        let is_reverse = value.is_reverse;

        Ok(MRangeOptions {
            range,
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse,
        })
    }
}

impl TryFrom<MultiRangeRequest> for MRangeOptions {
    type Error = ValkeyError;

    fn try_from(value: MultiRangeRequest) -> Result<Self, Self::Error> {
        let range: RangeOptions = if let Some(r) = value.range {
            r.try_into()?
        } else {
            return Err(ValkeyError::Str("TSDB: range is required"));
        };
        let filters = deserialize_matchers_list(Some(value.filters))?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels;

        let grouping: Option<RangeGroupingOptions> = match value.grouping {
            Some(group) => Some(group.try_into()?),
            None => None,
        };

        let is_reverse = value.is_reverse;

        Ok(MRangeOptions {
            range,
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse,
        })
    }
}

impl TryFrom<&MRangeOptions> for MultiRangeRequest {
    type Error = ValkeyError;
    fn try_from(value: &MRangeOptions) -> Result<Self, Self::Error> {
        let range: RangeRequest = (&value.range).into();
        let filters: Vec<FanoutSeriesSelector> = serialize_matchers_list(&value.filters)?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels.clone();

        let grouping: Option<FanoutGroupingOptions> =
            value.grouping.as_ref().map(|group| group.into());

        Ok(MultiRangeRequest {
            range: Some(range),
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse: value.is_reverse,
        })
    }
}

impl TryFrom<MRangeOptions> for MultiRangeRequest {
    type Error = ValkeyError;
    fn try_from(value: MRangeOptions) -> Result<Self, Self::Error> {
        let range: RangeRequest = (&value.range).into();
        let filters: Vec<FanoutSeriesSelector> = serialize_matchers_list(&value.filters)?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels;

        let grouping: Option<FanoutGroupingOptions> = value.grouping.map(|group| group.into());

        Ok(MultiRangeRequest {
            range: Some(range),
            filters,
            with_labels,
            selected_labels,
            grouping,
            is_reverse: value.is_reverse,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::BucketAlignment;
    use crate::aggregators::BucketTimestamp;
    use crate::series::request_types::AggregationType;

    #[test]
    fn test_aggregation_options_to_fanout_full() {
        let options = AggregationOptions {
            aggregation: AggregatorConfig::new(
                AggregationType::CountIf,
                Some(ValueComparisonFilter {
                    operator: ComparisonOperator::GreaterThan,
                    value: 10.0,
                }),
            )
            .unwrap(),
            bucket_duration: 1000,
            timestamp_output: BucketTimestamp::Start,
            alignment: BucketAlignment::Timestamp(555),
            report_empty: true,
        };

        let fanout: FanoutAggregationOptions = options.into();

        let f_aggr = fanout.aggregator.unwrap();
        assert_eq!(
            f_aggr.aggregator_type,
            FanoutAggregationType::CountIf as i32
        );
        let filter = f_aggr.value_filter.unwrap();
        assert_eq!(filter.operator, FanoutComparisonOperator::Gt as i32);
        assert_eq!(filter.value, 10.0);
        assert_eq!(fanout.bucket_duration, 1000);
        assert_eq!(
            fanout.bucket_timestamp_type,
            BucketTimestampType::Start as i32
        );
        assert_eq!(
            fanout.bucket_alignment,
            BucketAlignmentType::Timestamp as i32
        );
        assert_eq!(fanout.alignment_timestamp, 555);
        assert!(fanout.report_empty);
        assert_eq!(filter.operator, FanoutComparisonOperator::Gt as i32);
        assert_eq!(filter.value, 10.0);
    }

    #[test]
    fn test_fanout_to_aggregation_options_alignments() {
        let alignments = vec![
            (BucketAlignmentType::Default, BucketAlignment::Default),
            (BucketAlignmentType::AlignStart, BucketAlignment::Start),
            (BucketAlignmentType::AlignEnd, BucketAlignment::End),
        ];

        for (fanout_type, expected) in alignments {
            let aggregator = FanoutAggregatorConfig {
                aggregator_type: FanoutAggregationType::Max as i32,
                value_filter: None,
            };
            let fanout = FanoutAggregationOptions {
                aggregator: Some(aggregator),
                bucket_duration: 10,
                bucket_timestamp_type: BucketTimestampType::End as i32,
                bucket_alignment: fanout_type as i32,
                alignment_timestamp: 0,
                report_empty: false,
            };

            let options: AggregationOptions = fanout.try_into().unwrap();
            assert_eq!(options.alignment, expected);
        }
    }

    #[test]
    fn test_fanout_to_aggregation_options_invalid_duration() {
        let aggregator = FanoutAggregatorConfig {
            aggregator_type: FanoutAggregationType::Count as i32,
            value_filter: None,
        };
        let fanout = FanoutAggregationOptions {
            aggregator: Some(aggregator),
            bucket_duration: 0, // Invalid duration
            bucket_timestamp_type: BucketTimestampType::Mid as i32,
            bucket_alignment: BucketAlignmentType::Default as i32,
            alignment_timestamp: 0,
            report_empty: false,
        };

        let result: Result<AggregationOptions, ValkeyError> = fanout.try_into();
        assert!(result.is_err());
        if let Err(ValkeyError::Str(s)) = result {
            assert!(s.contains("bucket duration must be positive"));
        }
    }

    #[test]
    fn test_range_request_to_range_options_full() {
        let request = RangeRequest {
            range: Some(DateRange {
                start: 1000,
                end: 2000,
            }),
            count: 10,
            aggregation: Some(FanoutAggregationOptions {
                aggregator: Some(FanoutAggregationType::Avg.into()),
                bucket_duration: 60,
                bucket_timestamp_type: BucketTimestampType::Mid.into(),
                bucket_alignment: BucketAlignmentType::AlignStart.into(),
                alignment_timestamp: 0,
                report_empty: true,
            }),
            timestamp_filter: vec![1050, 1100],
            value_filter: Some(FanoutValueFilter {
                min: 10.5,
                max: 20.5,
            }),
            latest: true,
        };

        let options: RangeOptions = (&request)
            .try_into()
            .expect("Should convert to RangeOptions");

        assert_eq!(options.date_range.get_timestamps(None), (1000, 2000));
        assert_eq!(options.count, Some(10));

        let agg = options.aggregation.unwrap();
        assert_eq!(agg.aggregation.aggregation_type(), AggregationType::Avg);
        assert_eq!(agg.bucket_duration, 60);
        assert_eq!(agg.timestamp_output, BucketTimestamp::Mid);
        assert_eq!(agg.alignment, BucketAlignment::Start);
        assert!(agg.report_empty);

        assert_eq!(options.timestamp_filter, Some(vec![1050, 1100]));
        let val_filter = options.value_filter.unwrap();
        assert_eq!(val_filter.min, 10.5);
        assert_eq!(val_filter.max, 20.5);
        assert!(options.latest);
    }

    #[test]
    fn test_range_options_to_range_request_minimal() {
        let options = RangeOptions {
            date_range: TimestampRange::from_timestamps(500, 1500).unwrap(),
            count: None,
            aggregation: None,
            timestamp_filter: None,
            value_filter: None,
            latest: false,
        };

        let request: RangeRequest = (&options).into();

        assert_eq!(request.range.unwrap().start, 500);
        assert_eq!(request.range.unwrap().end, 1500);
        assert_eq!(request.count, 0);
        assert!(request.aggregation.is_none());
        assert!(request.timestamp_filter.is_empty());
        assert!(request.value_filter.is_none());
        assert!(!request.latest);
    }

    #[test]
    fn test_range_request_missing_range_fails() {
        let request = RangeRequest {
            range: None,
            ..Default::default()
        };

        let result: Result<RangeOptions, ValkeyError> = (&request).try_into();
        assert!(result.is_err());
        if let Err(ValkeyError::Str(s)) = result {
            assert!(s.contains("date range is required"));
        }
    }

    #[test]
    fn test_round_trip_conversion() {
        let aggregation = AggregatorConfig::new(
            AggregationType::CountIf,
            Some(ValueComparisonFilter {
                operator: ComparisonOperator::LessThan,
                value: 50.0,
            }),
        )
        .unwrap();
        let original_options = RangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            count: Some(5),
            aggregation: Some(AggregationOptions {
                aggregation,
                bucket_duration: 10,
                timestamp_output: BucketTimestamp::End,
                alignment: BucketAlignment::Timestamp(123),
                report_empty: false,
            }),
            timestamp_filter: None,
            value_filter: Some(ValueFilter { min: 1.0, max: 2.0 }),
            latest: false,
        };

        let request: RangeRequest = (&original_options).into();
        let back_to_options: RangeOptions = (&request).try_into().expect("Round trip failed");

        assert_eq!(
            back_to_options.date_range.get_timestamps(None),
            original_options.date_range.get_timestamps(None)
        );
        assert_eq!(back_to_options.count, original_options.count);
        assert_eq!(
            back_to_options.aggregation.unwrap().alignment,
            BucketAlignment::Timestamp(123)
        );
        assert_eq!(back_to_options.value_filter.unwrap().min, 1.0);
        assert_eq!(back_to_options.value_filter.unwrap().max, 2.0);
    }
}
