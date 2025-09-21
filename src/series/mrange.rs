use crate::common::Sample;
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::error_consts;
use crate::fanout::is_clustered;
use crate::iterators::{MultiSeriesSampleIter, SampleIter};
use crate::labels::Label;
use crate::series::acl::check_metadata_permissions;
use crate::series::index::series_by_matchers;
use crate::series::range_utils::{
    aggregate_samples, get_multi_series_range, get_series_labels, group_reduce,
};
use crate::series::request_types::{
    AggregationOptions, MRangeOptions, MRangeSeriesResult, RangeGroupingOptions,
};
use crate::series::{SeriesSampleIterator, TimeSeries, TimestampValue};
use ahash::AHashMap;
use orx_parallel::{IterIntoParIter, ParIter};
use valkey_module::{Context, ValkeyError, ValkeyResult};

pub struct MRangeSeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: String,
    group_label_value: Option<String>,
}

pub fn process_mrange_query(
    ctx: &Context,
    options: MRangeOptions,
    reverse: bool,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    check_metadata_permissions(ctx)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }
    let mut options = options;

    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    options.date_range.start = TimestampValue::Specific(start_ts);
    options.date_range.end = TimestampValue::Specific(end_ts);

    let series_guards = series_by_matchers(ctx, &options.filters, None)?;

    let series_metas: Vec<MRangeSeriesMeta> = series_guards
        .iter()
        .map(|guard| MRangeSeriesMeta {
            series: guard,
            source_key: guard.get_key().to_string(),
            group_label_value: None,
        })
        .collect();

    let is_clustered = is_clustered(ctx);
    let mut result = process_mrange(series_metas, options, is_clustered);

    if reverse {
        for series in result.iter_mut() {
            series.samples.reverse();
        }
    }

    Ok(result)
}

fn process_mrange(
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    is_clustered: bool,
) -> Vec<MRangeSeriesResult> {
    let mut options = options;
    let mut metas = metas;
    if let Some(grouping) = &options.grouping {
        collect_group_labels(&mut metas, grouping);
        // Is_clustered here means that we are being called from a remote node. Since grouping
        // is cross-series, it can only be done when all results are available in the caller node.
        // However, we need to tag each series with the grouping label value (done above), so we can
        // group them by the grouping label when they are returned to the client.
        //
        // We remove grouping from the options here. The full request options are available in the done
        // handler, so we can use them to group the series.
        if is_clustered {
            options.grouping = None;
        }
    }
    match (&options.grouping, &options.aggregation) {
        (Some(groupings), Some(aggr_options)) => {
            handle_aggregation_and_grouping(metas, &options, groupings, aggr_options)
        }
        (Some(groupings), None) => handle_grouping(metas, &options, groupings),
        (_, _) => handle_basic(metas, &options),
    }
}

fn collect_group_labels(metas: &mut Vec<MRangeSeriesMeta>, grouping: &RangeGroupingOptions) {
    for meta in metas.iter_mut() {
        meta.group_label_value = meta
            .series
            .label_value(&grouping.group_label)
            .map(|s| s.to_string());
    }
}

pub(crate) fn build_mrange_grouped_labels(
    group_label_name: &str,
    group_label_value: &str,
    reducer_name_str: &str,
    source_identifiers: &[String],
) -> Vec<Option<Label>> {
    let sources = source_identifiers.join(",");
    vec![
        Some(Label {
            name: group_label_name.into(),
            value: group_label_value.to_string(),
        }),
        Some(Label {
            name: REDUCER_KEY.into(),
            value: reducer_name_str.into(),
        }),
        Some(Label {
            name: SOURCE_KEY.into(),
            value: sources,
        }),
    ]
}

fn handle_aggregation_and_grouping(
    metas: Vec<MRangeSeriesMeta>,
    options: &MRangeOptions,
    groupings: &RangeGroupingOptions,
    aggregations: &AggregationOptions,
) -> Vec<MRangeSeriesResult> {
    let grouped_series_map = group_series_by_label_internal(metas, groupings);

    grouped_series_map
        .into_iter()
        .iter_into_par()
        .map(|(label_value, group_data)| {
            let series_refs = group_data
                .series
                .into_iter()
                .map(|meta| (meta.series, meta.source_key))
                .collect::<Vec<_>>();

            let group = RawGroupedSeries {
                label_value,
                series: series_refs,
                labels: group_data.labels,
            };

            let key = format!("{}={}", groupings.group_label, group.label_value);
            let aggregates = aggregate_grouped_samples(&group, options, aggregations);
            let samples = group_reduce(aggregates.into_iter(), groupings.aggregation);

            MRangeSeriesResult {
                key,
                group_label_value: Some(group.label_value),
                labels: group.labels,
                samples,
            }
        })
        .collect()
}

fn handle_grouping(
    metas: Vec<MRangeSeriesMeta>,
    options: &MRangeOptions,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeSeriesResult> {
    fn handle_one(
        grouping: &RangeGroupingOptions,
        options: &MRangeOptions,
        label_value: String,
        group_data: GroupedSeriesData<'_>,
    ) -> MRangeSeriesResult {
        let key = format!("{}={}", grouping.group_label, label_value);
        let samples = get_grouped_raw_samples(&group_data.series, options, grouping);
        let labels = group_data.labels;
        MRangeSeriesResult {
            key,
            group_label_value: None,
            labels,
            samples,
        }
    }

    let grouped_series_map = group_series_by_label_internal(metas, grouping);

    if grouped_series_map.is_empty() {
        return vec![];
    }

    if grouped_series_map.len() == 1 {
        let (label_value, group_data) = grouped_series_map.into_iter().next().unwrap();
        return vec![handle_one(grouping, options, label_value, group_data)];
    }

    grouped_series_map
        .into_iter()
        .iter_into_par()
        .map(|(label_value, group_data)| handle_one(grouping, options, label_value, group_data))
        .collect::<Vec<_>>()
}

fn convert_labels(
    series: &TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<Label>> {
    get_series_labels(series, with_labels, selected_labels)
        .into_iter()
        .map(|x| x.map(|label_ref| Label::new(label_ref.name, label_ref.value)))
        .collect::<Vec<Option<Label>>>()
}

fn handle_basic(metas: Vec<MRangeSeriesMeta>, options: &MRangeOptions) -> Vec<MRangeSeriesResult> {
    let mut metas = metas;
    let context_values: Vec<(String, Option<String>, Vec<Option<Label>>)> = metas
        .iter_mut()
        .map(|meta| {
            (
                std::mem::take(&mut meta.source_key),
                std::mem::take(&mut meta.group_label_value),
                convert_labels(meta.series, options.with_labels, &options.selected_labels),
            )
        })
        .collect();

    let series_refs: Vec<&TimeSeries> = metas.iter().map(|meta| meta.series).collect();
    let all_samples = get_multi_series_range(&series_refs, options);

    all_samples
        .into_iter()
        .zip(context_values)
        .map(
            |(samples, (key, group_label_value, labels))| MRangeSeriesResult {
                group_label_value,
                key,
                labels,
                samples,
            },
        )
        .collect::<Vec<_>>()
}

fn get_grouped_raw_samples(
    series_metas: &[MRangeSeriesMeta],
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
) -> Vec<Sample> {
    let iterators = get_sample_iterators(series_metas, options);
    let multi_iter = MultiSeriesSampleIter::new(iterators);
    let aggregator = grouping_options.aggregation;
    group_reduce(multi_iter, aggregator)
}

fn aggregate_grouped_samples(
    group: &RawGroupedSeries<'_>,
    options: &MRangeOptions,
    aggregation_options: &AggregationOptions,
) -> Vec<Sample> {
    fn get_raw_group_sample_iterators<'a>(
        series_refs: &'a [(&'a TimeSeries, String)],
        options: &'a MRangeOptions,
    ) -> Vec<SampleIter<'a>> {
        let (start_ts, end_ts) = options.date_range.get_timestamps(None);
        series_refs
            .iter()
            .map(|(ts, _)| {
                SeriesSampleIterator::new(
                    ts,
                    start_ts,
                    end_ts,
                    &options.value_filter,
                    &options.timestamp_filter,
                )
                .into()
            })
            .collect::<Vec<SampleIter<'a>>>()
    }

    let iterators = get_raw_group_sample_iterators(&group.series, options);
    let iter = MultiSeriesSampleIter::new(iterators);
    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    aggregate_samples(iter, start_ts, end_ts, aggregation_options)
}

fn get_sample_iterators<'a>(
    series_metas: &'a [MRangeSeriesMeta],
    range_options: &'a MRangeOptions,
) -> Vec<SampleIter<'a>> {
    let (start_ts, end_ts) = range_options.date_range.get_timestamps(None);
    series_metas
        .iter()
        .map(|meta| {
            SeriesSampleIterator::new(
                meta.series,
                start_ts,
                end_ts,
                &range_options.value_filter,
                &range_options.timestamp_filter,
            )
            .into()
        })
        .collect::<Vec<SampleIter<'a>>>()
}

struct GroupedSeriesData<'a> {
    series: Vec<MRangeSeriesMeta<'a>>,
    labels: Vec<Option<Label>>,
}

struct RawGroupedSeries<'a> {
    label_value: String,
    series: Vec<(&'a TimeSeries, String)>,
    labels: Vec<Option<Label>>,
}

fn group_series_by_label_internal<'a>(
    metas: Vec<MRangeSeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
) -> AHashMap<String, GroupedSeriesData<'a>> {
    let mut grouped: AHashMap<String, GroupedSeriesData<'a>> = AHashMap::new();
    let group_by_label_name = &grouping.group_label;
    let reducer_name = grouping.aggregation.name();

    for meta in metas.into_iter() {
        if let Some(label_value_str) = &meta.group_label_value {
            let entry =
                grouped
                    .entry(label_value_str.clone())
                    .or_insert_with(|| GroupedSeriesData {
                        series: Vec::new(),
                        labels: Vec::new(),
                    });
            entry.series.push(meta);
        }
    }

    for (label_value_str, group_data) in grouped.iter_mut() {
        let source_keys: Vec<String> = group_data
            .series
            .iter()
            .map(|m| m.source_key.clone())
            .collect();
        group_data.labels = build_mrange_grouped_labels(
            group_by_label_name,
            label_value_str,
            reducer_name,
            &source_keys,
        );
    }
    grouped
}
