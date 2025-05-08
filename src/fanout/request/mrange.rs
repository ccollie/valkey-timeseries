use super::common::{
    decode_label, deserialize_timestamp_range, load_flatbuffers_object, serialize_timestamp_range,
};
use super::matchers::{deserialize_matchers, serialize_matchers};
use super::request_generated::{
    AggregationOptions as FBAggregationOptions, AggregationOptionsBuilder, AggregationType,
    BucketAlignmentType, BucketTimestampType, GroupingOptions, GroupingOptionsArgs,
    MultiRangeRequest as FBMultiRangeRequest, MultiRangeRequestArgs, ValueRangeFilter,
};
use super::response_generated::{
    Label as ResponseLabel, LabelArgs, MultiRangeResponse as FBMultiRangeResponse,
    MultiRangeResponseArgs, Sample as ResponseSample, SeriesResponse as FBSeriesResponse,
    SeriesResponseArgs,
};
use crate::aggregators::{Aggregator, BucketAlignment, BucketTimestamp};
use crate::commands::process_mrange_query;
use crate::common::{Sample, Timestamp};
use crate::fanout::request::serialization::{Deserialized, Serialized};
use crate::fanout::{ClusterMessageType, MultiShardCommand, TrackerEnum};
use crate::labels::{Label, SeriesLabel};
use crate::series::request_types::{AggregationOptions, RangeGroupingOptions, RangeOptions};
use crate::series::ValueFilter;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use smallvec::SmallVec;
use valkey_module::{Context, ValkeyError, ValkeyResult};

impl Serialized for RangeOptions {
    fn serialize(&self, buf: &mut Vec<u8>) {
        serialize_multi_range_request(buf, self);
    }
}

impl Deserialized for RangeOptions {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        deserialize_multi_range_request(buf)
    }
}

pub struct MultiRangeCommand;

impl MultiShardCommand for MultiRangeCommand {
    type REQ = RangeOptions;
    type RES = MultiRangeResponse;

    fn request_type() -> ClusterMessageType {
        ClusterMessageType::MultiRangeQuery
    }

    fn exec(ctx: &Context, req: Self::REQ) -> ValkeyResult<Self::RES> {
        match process_mrange_query(ctx, req, false) {
            Ok(values) => {
                let series = values
                    .into_iter()
                    .map(|x| SeriesResponse {
                        key: x.key,
                        labels: x.labels,
                        samples: x.samples,
                    })
                    .collect();
                Ok(MultiRangeResponse { series })
            }
            Err(e) => {
                // Handle error
                Err(e)
            }
        }
    }

    fn update_tracker(tracker: &TrackerEnum, res: Self::RES) {
        if let TrackerEnum::MultiRangeQuery(ref t) = tracker {
            t.update(res);
        } else {
            panic!("BUG: Invalid MultiRangeRequest tracker type");
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SeriesResponse {
    pub key: String,
    pub labels: Vec<Option<Label>>,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Debug, Default)]
pub struct MultiRangeResponse {
    pub series: Vec<SeriesResponse>,
}

impl Serialized for MultiRangeResponse {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let mut bldr = FlatBufferBuilder::with_capacity(512);

        let mut series = Vec::with_capacity(self.series.len());
        for item in &self.series {
            let value = encode_series_response(&mut bldr, item);
            series.push(value);
        }

        // todo: use gorilla on samples
        let sample_values = bldr.create_vector(&series);
        let response_args = MultiRangeResponseArgs {
            series: Some(sample_values),
        };

        let obj = FBMultiRangeResponse::create(&mut bldr, &response_args);
        bldr.finish(obj, None);

        let data = bldr.finished_data();
        buf.extend_from_slice(data);
    }
}

impl Deserialized for MultiRangeResponse {
    fn deserialize(buf: &[u8]) -> ValkeyResult<Self> {
        // Get access to the root:
        let req = load_flatbuffers_object::<FBMultiRangeResponse>(buf, "MultiRangeResponse")?;
        let mut result: MultiRangeResponse = MultiRangeResponse::default();

        if let Some(values) = req.series() {
            for item in values.iter() {
                let value = decode_series_response(&item);
                result.series.push(value);
            }
        }

        Ok(result)
    }
}

pub(super) fn serialize_multi_range_request(buf: &mut Vec<u8>, request: &RangeOptions) {
    let mut bldr = FlatBufferBuilder::with_capacity(512);

    let range = serialize_timestamp_range(&mut bldr, Some(request.date_range));
    let mut labels: SmallVec<_, 8> = SmallVec::new();
    for label in &request.selected_labels {
        let name = bldr.create_string(label.as_str());
        labels.push(name);
    }
    let selected_labels = bldr.create_vector(&labels);
    let timestamp_filter = request
        .timestamp_filter
        .as_ref()
        .map(|timestamps| bldr.create_vector(timestamps.as_slice()));

    let value_filter_value = if let Some(value_filter) = &request.value_filter {
        ValueRangeFilter::new(value_filter.min, value_filter.max)
    } else {
        ValueRangeFilter::new(0.0, f64::MAX)
    };

    let aggregation = if let Some(agg) = &request.aggregation {
        let aggregation = serialize_aggregation_options(&mut bldr, agg);
        Some(aggregation)
    } else {
        None
    };

    let grouping = if let Some(group) = &request.grouping {
        let aggregator = encode_aggregator_enum(&group.aggregator);
        let group_label = bldr.create_string(group.group_label.as_str());
        let options = GroupingOptions::create(
            &mut bldr,
            &GroupingOptionsArgs {
                group_label: Some(group_label),
                aggregator,
            },
        );
        Some(options)
    } else {
        None
    };

    let count = request.count.unwrap_or(0) as u32;

    let filter = serialize_matchers(&mut bldr, &request.series_selector);
    let args = MultiRangeRequestArgs {
        range,
        with_labels: request.with_labels,
        selected_labels: Some(selected_labels),
        filter: Some(filter),
        value_filter: if request.value_filter.is_some() {
            Some(&value_filter_value)
        } else {
            None
        },
        timestamp_filter,
        count,
        aggregation,
        grouping,
    };

    let obj = FBMultiRangeRequest::create(&mut bldr, &args);
    bldr.finish(obj, None);

    let data = bldr.finished_data();
    buf.extend_from_slice(data);
}

fn serialize_aggregation_options<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    agg: &AggregationOptions,
) -> WIPOffset<FBAggregationOptions<'a>> {
    let aggregator = encode_aggregator_enum(&agg.aggregator);
    let (bucket_alignment, timestamp) = encode_bucket_alignment(agg.alignment);
    let alignment_timestamp = timestamp.unwrap_or(0);
    let timestamp_output = encode_bucket_timestamp(agg.timestamp_output);
    let mut builder = AggregationOptionsBuilder::new(bldr);

    builder.add_aggregator(aggregator);
    builder.add_bucket_duration(agg.bucket_duration);
    builder.add_timestamp_output(timestamp_output);
    builder.add_alignment_timestamp(alignment_timestamp);
    builder.add_bucket_alignment(bucket_alignment);
    builder.add_report_empty(agg.report_empty);

    builder.finish()
}

fn encode_bucket_timestamp(bucket_timestamp: BucketTimestamp) -> BucketTimestampType {
    match bucket_timestamp {
        BucketTimestamp::Start => BucketTimestampType::Start,
        BucketTimestamp::End => BucketTimestampType::End,
        BucketTimestamp::Mid => BucketTimestampType::Mid,
    }
}

fn decode_bucket_timestamp(timestamp: BucketTimestampType) -> BucketTimestamp {
    match timestamp {
        BucketTimestampType::Start => BucketTimestamp::Start,
        BucketTimestampType::End => BucketTimestamp::End,
        BucketTimestampType::Mid => BucketTimestamp::Mid,
        _ => unreachable!("Unsupported bucket timestamp type in decode_bucket_timestamp"),
    }
}

fn encode_bucket_alignment(
    bucket_alignment: BucketAlignment,
) -> (BucketAlignmentType, Option<Timestamp>) {
    match bucket_alignment {
        BucketAlignment::Default => (BucketAlignmentType::Default, None),
        BucketAlignment::Start => (BucketAlignmentType::Start, None),
        BucketAlignment::End => (BucketAlignmentType::End, None),
        BucketAlignment::Timestamp(ts) => (BucketAlignmentType::Timestamp, Some(ts)),
    }
}

fn decode_bucket_alignment(alignment: BucketAlignmentType) -> BucketAlignment {
    match alignment {
        BucketAlignmentType::Default => BucketAlignment::Default,
        BucketAlignmentType::Start => BucketAlignment::Start,
        BucketAlignmentType::End => BucketAlignment::End,
        BucketAlignmentType::Timestamp => BucketAlignment::Timestamp(0),
        BucketAlignmentType(i8::MIN..=-1_i8) | BucketAlignmentType(4_i8..=i8::MAX) => todo!(),
    }
}

fn encode_aggregator_enum(aggregator: &Aggregator) -> AggregationType {
    match aggregator {
        Aggregator::Sum(_) => AggregationType::Sum,
        Aggregator::Min(_) => AggregationType::Min,
        Aggregator::Max(_) => AggregationType::Max,
        Aggregator::Avg(_) => AggregationType::Avg,
        Aggregator::Count(_) => AggregationType::Count,
        Aggregator::First(_) => AggregationType::First,
        Aggregator::Last(_) => AggregationType::Last,
        Aggregator::StdS(_) => AggregationType::StdS,
        Aggregator::StdP(_) => AggregationType::StdP,
        Aggregator::VarP(_) => AggregationType::VarP,
        Aggregator::VarS(_) => AggregationType::VarS,
        Aggregator::Range(_) => AggregationType::Range,
    }
}

fn decode_aggregator(agg_type: AggregationType) -> Aggregator {
    match agg_type {
        AggregationType::Sum => Aggregator::Sum(Default::default()),
        AggregationType::Avg => Aggregator::Avg(Default::default()),
        AggregationType::Min => Aggregator::Min(Default::default()),
        AggregationType::Max => Aggregator::Max(Default::default()),
        AggregationType::First => Aggregator::First(Default::default()),
        AggregationType::Last => Aggregator::Last(Default::default()),
        AggregationType::Count => Aggregator::Count(Default::default()),
        AggregationType::Range => Aggregator::Range(Default::default()),
        AggregationType::StdS => Aggregator::StdS(Default::default()),
        AggregationType::StdP => Aggregator::StdP(Default::default()),
        AggregationType::VarS => Aggregator::VarS(Default::default()),
        AggregationType::VarP => Aggregator::VarP(Default::default()),
        _ => unreachable!("Unsupported aggregator type in decode_aggregator"),
    }
}

fn deserialize_aggregation_options(agg: FBAggregationOptions) -> ValkeyResult<AggregationOptions> {
    let _type = agg.aggregator();
    let aggregator = decode_aggregator(_type);
    let bucket_duration = agg.bucket_duration();
    if bucket_duration == 0 {
        return Err(ValkeyError::Str("TSDB: bucket duration must be positive"));
    }
    let timestamp_output = decode_bucket_timestamp(agg.timestamp_output());

    let _align = agg.bucket_alignment();
    let mut alignment = decode_bucket_alignment(_align);
    if _align == BucketAlignmentType::Timestamp {
        let timestamp = agg.alignment_timestamp();
        alignment = BucketAlignment::Timestamp(timestamp);
    }

    let report_empty = agg.report_empty();

    Ok(AggregationOptions {
        aggregator,
        bucket_duration,
        timestamp_output,
        alignment,
        report_empty,
    })
}

fn decode_grouping_options(reader: &GroupingOptions) -> RangeGroupingOptions {
    let group_label = reader.group_label().unwrap_or_default().to_string();
    let aggregator = decode_aggregator(reader.aggregator());
    RangeGroupingOptions {
        group_label,
        aggregator,
    }
}

pub fn deserialize_multi_range_request(buf: &[u8]) -> ValkeyResult<RangeOptions> {
    let req = load_flatbuffers_object::<FBMultiRangeRequest>(buf, "MultiRangeRequest")?;

    let date_range = deserialize_timestamp_range(req.range())?.unwrap_or_default();

    let count = if req.count() > 0 {
        Some(req.count() as usize)
    } else {
        None
    };

    let aggregation = if let Some(aggregation) = req.aggregation() {
        let options = deserialize_aggregation_options(aggregation)?;
        Some(options)
    } else {
        None
    };

    let timestamp_filter = req
        .timestamp_filter()
        .map(|filter| filter.iter().collect::<Vec<_>>());

    let value_filter = req.value_filter().map(|filter| ValueFilter {
        min: filter.min(),
        max: filter.max(),
    });

    let series_selector = if let Some(filter) = req.filter() {
        deserialize_matchers(&filter)?
    } else {
        // Default or error handling if filter is mandatory
        return Err(ValkeyError::Str("Missing mandatory filter expression"));
    };

    let with_labels = req.with_labels();

    let selected_labels = if let Some(label_list) = req.selected_labels() {
        label_list
            .iter()
            .map(|label| label.to_string())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let grouping = if let Some(group) = req.grouping() {
        let grouping = decode_grouping_options(&group);
        Some(grouping)
    } else {
        None
    };

    Ok(RangeOptions {
        date_range,
        count,
        aggregation,
        timestamp_filter,
        value_filter,
        series_selector,
        with_labels,
        selected_labels,
        grouping,
    })
}

fn encode_series_response<'a>(
    bldr: &mut FlatBufferBuilder<'a>,
    response: &SeriesResponse,
) -> WIPOffset<super::response_generated::SeriesResponse<'a>> {
    let key = bldr.create_string(&response.key);
    // todo: use gorilla on samples and send the buffer
    let mut samples = Vec::with_capacity(response.labels.len());
    for sample in &response.samples {
        let sample = ResponseSample::new(sample.timestamp, sample.value);
        samples.push(sample);
    }
    let samples = bldr.create_vector(&samples);

    let mut labels = Vec::with_capacity(response.labels.len());
    for label in &response.labels {
        if let Some(label) = label {
            let name = bldr.create_string(label.name.as_str());
            let value = bldr.create_string(label.value.as_str());
            let label = ResponseLabel::create(
                bldr,
                &LabelArgs {
                    name: Some(name),
                    value: Some(value),
                },
            );
            labels.push(label);
        } else {
            labels.push(ResponseLabel::create(
                bldr,
                &LabelArgs {
                    name: None,
                    value: None,
                },
            ));
        }
    }

    let labels = bldr.create_vector(&labels);
    FBSeriesResponse::create(
        bldr,
        &SeriesResponseArgs {
            key: Some(key),
            samples: Some(samples),
            labels: Some(labels),
        },
    )
}

fn decode_series_response(reader: &FBSeriesResponse) -> SeriesResponse {
    let key = reader.key().unwrap_or_default().to_string();
    let samples = reader
        .samples()
        .map(|sample| {
            sample
                .iter()
                .map(|x| Sample::new(x.timestamp(), x.value()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let labels = reader
        .labels()
        .map(|labels| {
            labels
                .iter()
                .map(|x| {
                    let label = decode_label(x);
                    if !label.name().is_empty() {
                        Some(label)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    SeriesResponse {
        key,
        samples,
        labels,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::{Aggregator, BucketAlignment, BucketTimestamp};
    use crate::labels::matchers::{
        Matcher, MatcherSetEnum, Matchers, PredicateMatch, PredicateValue,
    };
    use crate::series::TimestampRange;

    fn make_sample_matchers() -> Matchers {
        Matchers {
            name: Some("mrange-test".to_string()),
            matchers: MatcherSetEnum::And(vec![
                Matcher {
                    label: "mrange-foo".to_string(),
                    matcher: PredicateMatch::Equal(PredicateValue::String("bar".to_string())),
                },
                Matcher {
                    label: "mrange-baz".to_string(),
                    matcher: PredicateMatch::NotEqual(PredicateValue::String("qux".to_string())),
                },
            ]),
        }
    }

    #[test]
    fn test_range_options_request_simple_serialize_deserialize() {
        let req = RangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            series_selector: make_sample_matchers(),
            with_labels: true,
            selected_labels: vec!["label1".to_string(), "label2".to_string()],
            timestamp_filter: None,
            value_filter: None,
            aggregation: None,
            grouping: None,
            count: None,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = RangeOptions::deserialize(&buf).expect("deserialization failed");
        assert_eq!(req.date_range, req2.date_range);
        assert_eq!(req.with_labels, req2.with_labels);
        assert_eq!(req.selected_labels, req2.selected_labels);
        assert_eq!(req.timestamp_filter, req2.timestamp_filter);
        assert_eq!(req.count, req2.count);
        assert_eq!(req.series_selector, req2.series_selector);
        assert!(req2.aggregation.is_none());
        assert!(req2.grouping.is_none());
    }

    #[test]
    fn test_multi_range_request_with_filters_serialize_deserialize() {
        let req = RangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            series_selector: make_sample_matchers(),
            with_labels: true,
            selected_labels: vec!["label1".to_string(), "label2".to_string()],
            timestamp_filter: Some(vec![105, 110, 115]),
            value_filter: Some(ValueFilter {
                min: 10.0,
                max: 100.0,
            }),
            aggregation: None,
            grouping: None,
            count: Some(50),
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = RangeOptions::deserialize(&buf).expect("deserialization failed");
        assert_eq!(
            req.timestamp_filter.as_ref().unwrap(),
            req2.timestamp_filter.as_ref().unwrap()
        );

        let vf1 = req.value_filter.as_ref().unwrap();
        let vf2 = req2.value_filter.as_ref().unwrap();
        assert_eq!(vf1.min, vf2.min);
        assert_eq!(vf1.max, vf2.max);

        assert_eq!(req.count, req2.count);
    }

    #[test]
    fn test_multi_range_request_with_aggregation_serialize_deserialize() {
        let agg_options = AggregationOptions {
            aggregator: Aggregator::Avg(Default::default()),
            bucket_duration: 60,
            timestamp_output: BucketTimestamp::Mid,
            alignment: BucketAlignment::Start,
            report_empty: true,
        };

        let req = RangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            series_selector: make_sample_matchers(),
            with_labels: true,
            selected_labels: vec!["label1".to_string()],
            timestamp_filter: None,
            value_filter: None,
            aggregation: Some(agg_options),
            grouping: None,
            count: None,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = RangeOptions::deserialize(&buf).expect("deserialization failed");

        let agg1 = req.aggregation.as_ref().unwrap();
        let agg2 = req2.aggregation.as_ref().unwrap();

        assert_eq!(agg1.bucket_duration, agg2.bucket_duration);
        assert_eq!(agg1.timestamp_output, agg2.timestamp_output);
        assert_eq!(agg1.report_empty, agg2.report_empty);

        match (&agg1.aggregator, &agg2.aggregator) {
            (Aggregator::Avg(_), Aggregator::Avg(_)) => {}
            _ => panic!("Wrong aggregator type after deserialization"),
        }

        match (agg1.alignment, agg2.alignment) {
            (BucketAlignment::Start, BucketAlignment::Start) => {}
            _ => panic!("Wrong bucket alignment after deserialization"),
        }
    }

    #[test]
    fn test_multi_range_request_with_grouping_serialize_deserialize() {
        let grouping = RangeGroupingOptions {
            group_label: "category".to_string(),
            aggregator: Aggregator::Sum(Default::default()),
        };

        let req = RangeOptions {
            date_range: TimestampRange::from_timestamps(100, 200).unwrap(),
            series_selector: make_sample_matchers(),
            with_labels: true,
            selected_labels: vec!["label1".to_string()],
            timestamp_filter: None,
            value_filter: None,
            aggregation: None,
            grouping: Some(grouping),
            count: None,
        };

        let mut buf = Vec::new();
        req.serialize(&mut buf);

        let req2 = RangeOptions::deserialize(&buf).expect("deserialization failed");

        let grp1 = req.grouping.as_ref().unwrap();
        let grp2 = req2.grouping.as_ref().unwrap();

        assert_eq!(grp1.group_label, grp2.group_label);

        match (&grp1.aggregator, &grp2.aggregator) {
            (Aggregator::Sum(_), Aggregator::Sum(_)) => {}
            _ => panic!("Wrong aggregator type after deserialization"),
        }
    }

    #[test]
    fn test_multi_range_response_serialize_deserialize() {
        let sample1 = SeriesResponse {
            key: "series1".to_string(),
            labels: vec![
                Some(Label::new("name", "series1")),
                Some(Label::new("region", "us-west")),
            ],
            samples: vec![
                Sample::new(100, 1.0),
                Sample::new(110, 2.0),
                Sample::new(120, 3.0),
            ],
        };

        let sample2 = SeriesResponse {
            key: "series2".to_string(),
            labels: vec![
                Some(Label::new("name", "series2")),
                Some(Label::new("region", "us-east")),
            ],
            samples: vec![
                Sample::new(105, 10.0),
                Sample::new(115, 20.0),
                Sample::new(125, 30.0),
            ],
        };

        let resp = MultiRangeResponse {
            series: vec![sample1, sample2],
        };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiRangeResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());

        assert_eq!(resp.series[0].key, resp2.series[0].key);
        assert_eq!(resp.series[0].labels, resp2.series[0].labels);
        assert_eq!(resp.series[0].samples, resp2.series[0].samples);

        assert_eq!(resp.series[1].key, resp2.series[1].key);
        assert_eq!(resp.series[1].labels, resp2.series[1].labels);
        assert_eq!(resp.series[1].samples, resp2.series[1].samples);
    }

    #[test]
    fn test_multi_range_response_empty_serialize_deserialize() {
        let resp = MultiRangeResponse { series: vec![] };

        let mut buf = Vec::new();
        resp.serialize(&mut buf);

        let resp2 = MultiRangeResponse::deserialize(&buf).expect("deserialization failed");
        assert_eq!(resp.series.len(), resp2.series.len());
    }
}
