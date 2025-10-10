use super::generated::{
    AggregationOptions as FanoutAggregationOptions, AggregationType as FanoutAggregationType,
    BucketAlignmentType, BucketTimestampType, CompressionType as FanoutChunkEncoding, DateRange,
    GroupingOptions as FanoutGroupingOptions, Label as FanoutLabel, Matchers as FanoutMatchers,
    MultiRangeRequest, PostingStat as FanoutPostingStat, Sample as FanoutSample, StatsResponse,
};
use super::matchers::deserialize_matchers_list;
use crate::labels::Label;
use crate::labels::filters::SeriesSelector;
use crate::series::chunks::ChunkEncoding;
use crate::series::request_types::{
    AggregationOptions, AggregationType, BucketAlignment, MRangeOptions, MatchFilterOptions,
    RangeGroupingOptions,
};
use crate::series::{TimestampRange, ValueFilter};
use crate::{
    aggregators::BucketTimestamp,
    error_consts,
    series::index::{PostingStat, PostingsStats},
};
use valkey_module::{ValkeyError, ValkeyResult, ValkeyValue};

impl From<ChunkEncoding> for FanoutChunkEncoding {
    fn from(value: ChunkEncoding) -> Self {
        match value {
            ChunkEncoding::Uncompressed => FanoutChunkEncoding::None,
            ChunkEncoding::Gorilla => FanoutChunkEncoding::Gorilla,
            ChunkEncoding::Pco => FanoutChunkEncoding::Pco,
        }
    }
}

impl From<FanoutChunkEncoding> for ChunkEncoding {
    fn from(value: FanoutChunkEncoding) -> Self {
        match value {
            FanoutChunkEncoding::None => ChunkEncoding::Uncompressed,
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
            AggregationType::Sum => FanoutAggregationType::Sum,
            AggregationType::Min => FanoutAggregationType::Min,
            AggregationType::Max => FanoutAggregationType::Max,
            AggregationType::Avg => FanoutAggregationType::Avg,
            AggregationType::Count => FanoutAggregationType::Count,
            AggregationType::First => FanoutAggregationType::First,
            AggregationType::Last => FanoutAggregationType::Last,
            AggregationType::StdS => FanoutAggregationType::StdS,
            AggregationType::StdP => FanoutAggregationType::StdP,
            AggregationType::VarP => FanoutAggregationType::VarP,
            AggregationType::VarS => FanoutAggregationType::VarS,
            AggregationType::Range => FanoutAggregationType::Range,
        }
    }
}

impl From<FanoutAggregationType> for AggregationType {
    fn from(value: FanoutAggregationType) -> Self {
        match value {
            FanoutAggregationType::Sum => AggregationType::Sum,
            FanoutAggregationType::Avg => AggregationType::Avg,
            FanoutAggregationType::Min => AggregationType::Min,
            FanoutAggregationType::Max => AggregationType::Max,
            FanoutAggregationType::First => AggregationType::First,
            FanoutAggregationType::Last => AggregationType::Last,
            FanoutAggregationType::Count => AggregationType::Count,
            FanoutAggregationType::Range => AggregationType::Range,
            FanoutAggregationType::StdS => AggregationType::StdS,
            FanoutAggregationType::StdP => AggregationType::StdP,
            FanoutAggregationType::VarS => AggregationType::VarS,
            FanoutAggregationType::VarP => AggregationType::VarP,
        }
    }
}

impl TryFrom<&FanoutGroupingOptions> for RangeGroupingOptions {
    type Error = ValkeyError;

    fn try_from(value: &FanoutGroupingOptions) -> Result<RangeGroupingOptions, ValkeyError> {
        let aggregation: FanoutAggregationType = value
            .aggregation
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?; // todo: serialization error
        Ok(RangeGroupingOptions {
            aggregation: aggregation.into(),
            group_label: value.group_label.clone(),
        })
    }
}

impl TryFrom<FanoutGroupingOptions> for RangeGroupingOptions {
    type Error = ValkeyError;

    fn try_from(value: FanoutGroupingOptions) -> Result<RangeGroupingOptions, ValkeyError> {
        let aggregation: FanoutAggregationType = value
            .aggregation
            .try_into()
            .map_err(|_| ValkeyError::Str(error_consts::UNKNOWN_AGGREGATION_TYPE))?; // todo: serialization error
        Ok(RangeGroupingOptions {
            aggregation: aggregation.into(),
            group_label: value.group_label,
        })
    }
}

impl From<&RangeGroupingOptions> for FanoutGroupingOptions {
    fn from(value: &RangeGroupingOptions) -> Self {
        let aggregation: FanoutAggregationType = value.aggregation.into();
        FanoutGroupingOptions {
            aggregation: aggregation.into(),
            group_label: value.group_label.clone(),
        }
    }
}

impl From<RangeGroupingOptions> for FanoutGroupingOptions {
    fn from(value: RangeGroupingOptions) -> Self {
        let aggregation: FanoutAggregationType = value.aggregation.into();
        FanoutGroupingOptions {
            aggregation: aggregation.into(),
            group_label: value.group_label,
        }
    }
}

pub fn deserialize_match_filter_options(
    range: Option<DateRange>,
    filters: Option<Vec<FanoutMatchers>>,
) -> ValkeyResult<MatchFilterOptions> {
    let date_range: Option<TimestampRange> = range.map(|r| r.into());
    let matchers: Vec<SeriesSelector> = deserialize_matchers_list(filters)?;
    Ok(MatchFilterOptions {
        date_range,
        matchers,
        ..Default::default()
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

impl From<FanoutSample> for ValkeyValue {
    fn from(value: FanoutSample) -> Self {
        let row = vec![
            ValkeyValue::from(value.timestamp),
            ValkeyValue::from(value.value),
        ];
        ValkeyValue::from(row)
    }
}

impl From<AggregationOptions> for FanoutAggregationOptions {
    fn from(value: AggregationOptions) -> Self {
        let aggregator: FanoutAggregationType = value.aggregation.into();
        let bucket_timestamp_type: BucketTimestampType = value.timestamp_output.into();

        let (bucket_alignment, alignment_timestamp) = match value.alignment {
            BucketAlignment::Default => (BucketAlignmentType::Default, 0),
            BucketAlignment::Start => (BucketAlignmentType::AlignStart, 0),
            BucketAlignment::End => (BucketAlignmentType::AlignEnd, 0),
            BucketAlignment::Timestamp(ts) => (BucketAlignmentType::Timestamp, ts),
        };

        FanoutAggregationOptions {
            aggregator: aggregator.into(),
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
        let fanout_type: FanoutAggregationType = value.aggregator.try_into()?;
        let aggregation: AggregationType = fanout_type.into();
        let bucket_duration = value.bucket_duration as u64;
        if bucket_duration == 0 {
            return Err(ValkeyError::Str("TSDB: bucket duration must be positive"));
        }
        let timestamp_output: BucketTimestampType = value
            .bucket_timestamp_type
            .try_into()
            .map_err(|_| ValkeyError::Str("TSDB: invalid bucket timestamp type"))?; // todo: put in error_consts
        let fanout_alignment: BucketAlignmentType = value
            .bucket_alignment
            .try_into()
            .map_err(|_| ValkeyError::Str("TSDB: invalid bucket alignment"))?; // todo: put in error_consts

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

impl TryFrom<MultiRangeRequest> for MRangeOptions {
    type Error = ValkeyError;

    fn try_from(value: MultiRangeRequest) -> Result<Self, Self::Error> {
        let date_range: TimestampRange = match value.range {
            Some(r) => r.into(),
            None => {
                return Err(ValkeyError::Str("TSDB: date range is required"));
            }
        };

        let count = value.count.map(|c| c as usize);

        let aggregation = if let Some(aggregation) = value.aggregation {
            let options = aggregation.try_into()?;
            Some(options)
        } else {
            None
        };

        let timestamp_filter = Some(value.timestamp_filter);

        let value_filter: Option<ValueFilter> = value.value_filter.map(|filter| ValueFilter {
            min: filter.min,
            max: filter.max,
        });

        let filters = deserialize_matchers_list(Some(value.filters))?;
        let with_labels = value.with_labels;

        let selected_labels = value.selected_labels;

        let grouping: Option<RangeGroupingOptions> = match value.grouping {
            Some(group) => Some(group.try_into()?),
            None => None,
        };

        Ok(MRangeOptions {
            date_range,
            count,
            aggregation,
            timestamp_filter,
            value_filter,
            filters,
            with_labels,
            selected_labels,
            grouping,
        })
    }
}
