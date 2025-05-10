use super::parse_range_options;
use super::range_utils::{aggregate_samples, get_series_labels, group_reduce};
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::common::parallel::join;
use crate::common::{Sample, Timestamp};
use crate::fanout::cluster::is_clustered;
use crate::fanout::{
    perform_remote_mrange_request, MultiRangeResponse,
};
use crate::iterators::{MultiSeriesSampleIter, SampleIter};
use crate::labels::Label;
use crate::series::index::series_by_matchers;
use crate::series::request_types::{
    AggregationOptions, MRangeSeriesResult, RangeGroupingOptions, RangeOptions,
};
use crate::series::{SeriesGuard, SeriesSampleIterator, TimeSeries, TimestampValue};
use ahash::AHashMap;
use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use smallvec::SmallVec;
use std::collections::BTreeMap;
use valkey_module::{
    BlockedClient, Context, NextArg, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString,
    ValkeyValue,
};

struct MRangeSeriesMeta {
    series: SeriesGuard,
    source_key: String,
}

/// TS.MRANGE fromTimestamp toTimestamp
//   [LATEST]
//   [FILTER_BY_TS ts...]
//   [FILTER_BY_VALUE min max]
//   [WITHLABELS | <SELECTED_LABELS label...>]
//   [COUNT count]
//   [[ALIGN align] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
//   FILTER filterExpr...
//   [GROUPBY label REDUCE reducer]
pub fn mrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, false)
}

pub fn mrevrange(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    mrange_internal(ctx, args, true)
}

fn mrange_internal(ctx: &Context, args: Vec<ValkeyString>, reverse: bool) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_range_options(&mut args)?;

    if options.series_selector.is_empty() {
        return Err(ValkeyError::Str("TSDB: no FILTER given"));
    }

    args.done()?;

    if is_clustered(ctx) {
        perform_remote_mrange_request(ctx, options, on_mrange_request_done)?;
        return Ok(ValkeyValue::NoReply);
    }

    let result_rows = process_mrange_query(ctx, options, reverse)?;
    let result = result_rows
        .into_iter()
        .map(|series| series.into())
        .collect::<Vec<ValkeyValue>>();

    Ok(ValkeyValue::from(result))
}

fn on_mrange_request_done(
    ctx: &ThreadSafeContext<BlockedClient>,
    req: RangeOptions,
    results: Vec<MultiRangeResponse>,
) {
    let all_series = results
        .into_iter()
        .flat_map(|result| result.series.into_iter())
        .collect::<Vec<_>>();
    
    let series = if let Some(grouping) = req.grouping {
        group_sharded_series(all_series, &grouping)
    } else {
        all_series
    };
    
    let result = ValkeyValue::Array(series.into_iter().map(|x| x.into()).collect());
    ctx.reply(Ok(result));
}

pub fn process_mrange_query(
    ctx: &Context,
    options: RangeOptions,
    reverse: bool,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    if options.series_selector.is_empty() {
        return Err(ValkeyError::Str("TSDB: no FILTER given"));
    }
    let mut options = options;

    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    options.date_range.start = TimestampValue::Specific(start_ts);
    options.date_range.end = TimestampValue::Specific(end_ts);

    let matchers = std::mem::take(&mut options.series_selector);
    let series = series_by_matchers(ctx, &[matchers], None, true, true)?;

    let metas = series
        .into_iter()
        .map(|guard| {
            let source_key = guard.get_key().to_string_lossy();
            MRangeSeriesMeta {
                series: guard,
                source_key,
            }
        })
        .collect();

    let mut result = process_mrange_command(metas, &options);

    if reverse {
        result.reverse();
    }

    Ok(result)
}

fn process_mrange_command(
    metas: Vec<MRangeSeriesMeta>,
    options: &RangeOptions,
) -> Vec<MRangeSeriesResult> {
    match (&options.grouping, &options.aggregation) {
        (Some(groupings), Some(aggr_options)) => {
            handle_aggregation_and_grouping(metas, options, groupings, aggr_options)
        }
        (Some(groupings), None) => {
            // group raw samples
            handle_grouping(metas, options, groupings)
        }
        (_, _) => handle_basic(metas, options), // also handles aggregation
    }
}

fn handle_aggregation_and_grouping(
    metas: Vec<MRangeSeriesMeta>,
    options: &RangeOptions,
    groupings: &RangeGroupingOptions,
    aggregations: &AggregationOptions,
) -> Vec<MRangeSeriesResult> {
    let mut grouped_series = group_series_by_label(metas, groupings);

    fn process_group(
        group: &RawGroupedSeries<'_>,
        options: &RangeOptions,
        groupings: &RangeGroupingOptions,
        aggregations: &AggregationOptions,
    ) -> MRangeSeriesResult {
        // according to docs, the GROUPBY/REDUCE is applied post-aggregation stage.
        let key = format!("{}={}", groupings.group_label, group.label_value);
        let aggregates = aggregate_grouped_samples(group,options, aggregations);
        let samples = group_reduce(aggregates.into_iter(), groupings.aggregator.clone());
        MRangeSeriesResult {
            key,
            group_label_value: None,
            labels: group.labels.clone(),
            samples,
        }
    }

    fn process(
        groups: &[RawGroupedSeries<'_>],
        options: &RangeOptions,
        groupings: &RangeGroupingOptions,
        aggregations: &AggregationOptions,
    ) -> Vec<MRangeSeriesResult> {
        match groups {
            [] => vec![],
            [group] => {
                vec![process_group(group, options, groupings, aggregations)]
            }
            [first, second] => {
                let (first_row, second_row) = join(
                    || process_group(first, options, groupings, aggregations),
                    || process_group(second, options, groupings, aggregations),
                );
                vec![first_row, second_row]
            }
            _ => {
                let (first, rest) = groups.split_at(groups.len() / 2);
                let (mut first_samples, rest_samples) = join(
                    || process(first, options, groupings, aggregations),
                    || process(rest, options, groupings, aggregations),
                );

                first_samples.extend(rest_samples);
                first_samples
            }
        }
    }

    let raw_series: SmallVec<RawGroupedSeries, 8> = grouped_series
        .iter_mut()
        .map(|group| {
            let series = group
                .series
                .iter()
                .map(|meta| {
                    let ts: &TimeSeries = &meta.series;
                    (ts, meta.source_key.as_str())
                })
                .collect::<Vec<_>>();
            RawGroupedSeries {
                label_value: std::mem::take(&mut group.label_value),
                series,
                labels: std::mem::take(&mut group.labels),
            }
        })
        .collect();

    process(&raw_series, options, groupings, aggregations)
}

fn handle_grouping(
    metas: Vec<MRangeSeriesMeta>,
    options: &RangeOptions,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeSeriesResult> {
    // group raw samples
    let grouped_series = group_series_by_label(metas, grouping);
    // todo par_iter
    grouped_series
        .into_iter()
        .map(|group| {
            let key = format!("{}={}", grouping.group_label, group.label_value);
            let samples = get_grouped_raw_samples(&group.series, options, grouping);
            MRangeSeriesResult {
                key,
                group_label_value: None,
                labels: group.labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn convert_labels(
    series: &TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Option<Label>> {
    get_series_labels(series, with_labels, selected_labels)
        .into_iter()
        .map(|x| x.map(|x| Label::new(x.name, x.value)))
        .collect::<Vec<Option<Label>>>()
}

fn handle_basic(metas: Vec<MRangeSeriesMeta>, options: &RangeOptions) -> Vec<MRangeSeriesResult> {
    let all_samples = get_all_series_samples(&metas, options);
    all_samples
        .into_iter()
        .zip(metas)
        .map(|(samples, meta)| {
            let labels =
                convert_labels(&meta.series, options.with_labels, &options.selected_labels);
            MRangeSeriesResult {
                group_label_value: None,
                key: meta.source_key,
                labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn get_grouped_raw_samples(
    series: &[MRangeSeriesMeta],
    options: &RangeOptions,
    grouping_options: &RangeGroupingOptions,
) -> Vec<Sample> {
    // todo: maybe use rayon/chili rather than the multi iterator to read multiple chunks
    // concurrently
    let iterators = get_sample_iterators(series, options);
    let multi_iter = MultiSeriesSampleIter::new(iterators);
    let aggregator = grouping_options.aggregator.clone();
    group_reduce(multi_iter, aggregator)
}

fn aggregate_grouped_samples(
    group: &RawGroupedSeries<'_>,
    options: &RangeOptions,
    aggregation_options: &AggregationOptions,
) -> Vec<Sample> {
    fn get_sample_iterators<'a>(
        series: &'a [(&TimeSeries, &str)],
        options: &'a RangeOptions,
    ) -> Vec<SampleIter<'a>> {
        let (start_ts, end_ts) = options.date_range.get_timestamps(None);
        series
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

    // todo: maybe use rayon/chili rather than the multi iterator. Could significantly speed up
    // the process if we go beyond a small number of chunks
    
    let iterators = get_sample_iterators(&group.series, options);
    let iter = MultiSeriesSampleIter::new(iterators);
    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    aggregate_samples(iter, start_ts, end_ts, aggregation_options)
}

fn get_raw_samples(
    series: &TimeSeries,
    start_ts: Timestamp,
    end_ts: Timestamp,
    options: &RangeOptions,
) -> Vec<Sample> {
    series.get_range_filtered(
        start_ts,
        end_ts,
        options.timestamp_filter.as_deref(),
        options.value_filter,
    )
}

fn get_series_iterator<'a>(
    meta: &'a MRangeSeriesMeta,
    options: &'a RangeOptions,
) -> SeriesSampleIterator<'a> {
    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    SeriesSampleIterator::new(
        &meta.series,
        start_ts,
        end_ts,
        &options.value_filter,
        &options.timestamp_filter,
    )
}

fn get_sample_iterators<'a>(
    series: &'a [MRangeSeriesMeta],
    range_options: &'a RangeOptions,
) -> Vec<SampleIter<'a>> {
    series
        .iter()
        .map(|meta| get_series_iterator(meta, range_options).into())
        .collect::<Vec<SampleIter<'a>>>()
}

fn get_all_series_samples(
    meta: &[MRangeSeriesMeta],
    range_options: &RangeOptions,
) -> Vec<Vec<Sample>> {
    fn fetch_one(
        series: &TimeSeries,
        start_ts: Timestamp,
        end_ts: Timestamp,
        range_options: &RangeOptions,
    ) -> Vec<Sample> {
        let samples = get_raw_samples(series, start_ts, end_ts, range_options);
        if let Some(agg_options) = &range_options.aggregation {
            aggregate_samples(samples.into_iter(), start_ts, end_ts, agg_options)
        } else {
            samples
        }
    }

    fn get_raw_internal(
        series: &[&TimeSeries],
        start_ts: Timestamp,
        end_ts: Timestamp,
        range_options: &RangeOptions,
    ) -> Vec<Vec<Sample>> {
        match series {
            [] => vec![],
            [meta] => {
                let samples = fetch_one(meta, start_ts, end_ts, range_options);
                vec![samples]
            }
            [first, second] => {
                let (first_samples, second_samples) = join(
                    || fetch_one(first, start_ts, end_ts, range_options),
                    || fetch_one(second, start_ts, end_ts, range_options),
                );
                vec![first_samples, second_samples]
            }
            _ => {
                let (first, rest) = series.split_at(series.len() / 2);
                let (mut first_samples, rest_samples) = join(
                    || get_raw_internal(first, start_ts, end_ts, range_options),
                    || get_raw_internal(rest, start_ts, end_ts, range_options),
                );

                first_samples.extend(rest_samples);
                first_samples
            }
        }
    }

    let (start_ts, end_ts) = range_options.date_range.get_timestamps(None);
    let series: SmallVec<_, 8> = meta
        .iter()
        .map(|meta| {
            let ts: &TimeSeries = &meta.series;
            ts
        })
        .collect();

    get_raw_internal(&series, start_ts, end_ts, range_options)
}

struct GroupedSeries {
    label_value: String,
    series: Vec<MRangeSeriesMeta>,
    labels: Vec<Option<Label>>,
}

struct RawGroupedSeries<'a> {
    label_value: String,
    series: Vec<(&'a TimeSeries, &'a str)>,
    labels: Vec<Option<Label>>,
}

fn group_series_by_label(
    metas: Vec<MRangeSeriesMeta>,
    grouping: &RangeGroupingOptions,
) -> Vec<GroupedSeries> {
    let mut grouped: AHashMap<String, Vec<MRangeSeriesMeta>> = AHashMap::new();
    let label = &grouping.group_label;

    for meta in metas.into_iter() {
        if let Some(label_value) = meta.series.label_value(label) {
            // note that `grouped.entry()` is not used here because it would require a clone of
            // the label_value even in the case where the key already exists
            if let Some(list) = grouped.get_mut(label_value) {
                list.push(meta);
            } else {
                grouped.insert(label_value.to_string(), vec![meta]);
            }
        }
    }

    let reducer = grouping.aggregator.name();

    grouped
        .into_iter()
        .map(|(label_value, series)| {
            let capacity = series.iter().map(|x| x.source_key.len()).sum::<usize>() + series.len();
            let mut sources: String = String::with_capacity(capacity);
            for (i, meta) in series.iter().enumerate() {
                sources.push_str(&meta.source_key);
                if i < series.len() - 1 {
                    sources.push(',');
                }
            }
            let labels = vec![
                Some(Label {
                    name: label.into(),
                    value: label_value.clone(),
                }),
                Some(Label {
                    name: REDUCER_KEY.into(),
                    value: reducer.into(),
                }),
                Some(Label {
                    name: SOURCE_KEY.into(),
                    value: sources,
                }),
            ];
            GroupedSeries {
                label_value,
                series,
                labels,
            }
        })
        .collect()
}


/// Apply GROUPBY/REDUCE to the series coming from remote nodes
fn group_sharded_series(
    metas: Vec<MRangeSeriesResult>,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeSeriesResult> {

    fn handle_reducer(
        series: &[MRangeSeriesResult],
        grouping: &RangeGroupingOptions,
    ) -> Vec<Sample> {
        let mut iterators: Vec<SampleIter> = Vec::with_capacity(series.len());
        for meta in series.iter() {
            let iter = SampleIter::Slice(meta.samples.iter());
            iterators.push(iter);
        }
        let multi_iter = MultiSeriesSampleIter::new(iterators);
        let aggregator = grouping.aggregator.clone();
        group_reduce(multi_iter, aggregator)
    }
    
    let mut grouped: BTreeMap<String, Vec<MRangeSeriesResult>> = BTreeMap::new();

    for meta in metas.into_iter() {
        if let Some(label_value) = &meta.group_label_value {
            let key = format!("{}={}", grouping.group_label, label_value);
            // note that `grouped.entry()` is not used here because it would require a clone of
            // the label_value even in the case where the key already exists
            if let Some(list) = grouped.get_mut(&key) {
                list.push(meta);
            } else {
                grouped.insert(key, vec![meta]);
            }
        }
    }

    let reducer = grouping.aggregator.name();
    let label = &grouping.group_label;
    
    grouped
        .par_iter()
        .map(|(label_value, series)| {
            let capacity = series.iter().map(|x| x.key.len()).sum::<usize>() + series.len();
            let mut sources: String = String::with_capacity(capacity);
            for (i, meta) in series.iter().enumerate() {
                sources.push_str(&meta.key);
                if i < series.len() - 1 {
                    sources.push(',');
                }
            }
            
            // get the value of the label (the string past the =)
            let group_label_value = label_value.split('=').nth(1).unwrap_or("");
            
            let samples = handle_reducer(series, grouping);
            let labels = vec![
                Some(Label {
                    name: label.into(),
                    value: group_label_value.to_string(),
                }),
                Some(Label {
                    name: REDUCER_KEY.into(),
                    value: reducer.into(),
                }),
                Some(Label {
                    name: SOURCE_KEY.into(),
                    value: sources,
                }),
            ];
            MRangeSeriesResult {
                group_label_value: None,
                key: label_value.clone(),
                samples,
                labels,
            }
        })
        .collect::<Vec<_>>()
}