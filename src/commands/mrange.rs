use super::arg_types::{RangeGroupingOptions, RangeOptions};
use super::parse_range_options;
use super::range_utils::{aggregate_samples, get_series_labels, group_samples_internal};
use crate::aggregators::{AggOp, AggregationOptions, Aggregator};
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::common::parallel::join;
use crate::common::{Sample, Timestamp};
use crate::iterators::{MultiSeriesSampleIter, SampleIter};
use crate::labels::Label;
use crate::series::index::series_keys_by_matchers;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesSampleIterator, TimeSeries};
use ahash::AHashMap;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

pub(crate) struct MRangeSeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: String,
    start_ts: Timestamp,
    end_ts: Timestamp,
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
    let mut options = parse_range_options(&mut args)?;

    if options.series_selector.is_empty() {
        return Err(ValkeyError::Str("TSDB: no FILTER given"));
    }

    args.done()?;

    let matchers = std::mem::take(&mut options.series_selector);
    let keys = series_keys_by_matchers(ctx, &[matchers], None)?;

    // needed to keep valkey keys alive below
    let db_keys = keys.iter().map(|key| ctx.open_key(key)).collect::<Vec<_>>();

    let (start_ts, end_ts) = options.date_range.get_timestamps(None);
    let metas = db_keys
        .iter()
        .zip(keys)
        .filter_map(|(db_key, source_key)| {
            if let Ok(Some(series)) = db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                let meta = MRangeSeriesMeta {
                    series,
                    source_key: source_key.to_string_lossy(),
                    start_ts,
                    end_ts,
                };
                Some(meta)
            } else {
                None
            }
        })
        .collect();

    let result_rows = process_mrange_command(metas, &options);
    let mut result = result_rows
        .into_iter()
        .map(result_row_to_value)
        .collect::<Vec<_>>();

    if reverse {
        result.reverse();
    }

    Ok(ValkeyValue::from(result))
}

#[derive(Default)]
pub(crate) struct MRangeResultRow {
    key: String,
    labels: Vec<Option<Label>>,
    samples: Vec<Sample>,
}

pub fn process_mrange_command(
    metas: Vec<MRangeSeriesMeta>,
    options: &RangeOptions,
) -> Vec<MRangeResultRow> {
    match (&options.grouping, &options.aggregation) {
        (Some(groupings), Some(aggr_options)) => {
            handle_aggregation_and_grouping(metas, options, groupings, aggr_options)
        }
        (Some(groupings), None) => {
            // group raw samples
            handle_grouping(metas, options, groupings)
        }
        (None, Some(_)) => handle_aggregation(metas, options),
        (None, None) => handle_raw(metas, options),
    }
}

fn handle_aggregation_and_grouping(
    metas: Vec<MRangeSeriesMeta<'_>>,
    options: &RangeOptions,
    groupings: &RangeGroupingOptions,
    aggregations: &AggregationOptions,
) -> Vec<MRangeResultRow> {
    let grouped_series = group_series_by_label(metas, groupings);

    fn process_group(
        group: &GroupedSeries<'_>,
        options: &RangeOptions,
        groupings: &RangeGroupingOptions,
        aggregations: &AggregationOptions,
    ) -> MRangeResultRow {
        // according to docs, the GROUPBY/REDUCE is applied post aggregation stage.
        let key = format!("{}={}", groupings.group_label, group.label_value);
        let aggregator = aggregations.aggregator.clone();
        let aggregates = aggregate_grouped_samples(group, options, aggregator);
        let samples = group_samples_internal(aggregates.into_iter(), groupings);
        MRangeResultRow {
            key,
            labels: group.labels.clone(),
            samples,
        }
    }

    fn process(
        groups: &[GroupedSeries<'_>],
        options: &RangeOptions,
        groupings: &RangeGroupingOptions,
        aggregations: &AggregationOptions,
    ) -> Vec<MRangeResultRow> {
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

    process(&grouped_series, options, groupings, aggregations)
}

fn handle_grouping(
    metas: Vec<MRangeSeriesMeta<'_>>,
    options: &RangeOptions,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeResultRow> {
    // group raw samples
    let grouped_series = group_series_by_label(metas, grouping);
    grouped_series
        .into_iter()
        .map(|group| {
            let key = format!("{}={}", grouping.group_label, group.label_value);
            let samples = get_grouped_raw_samples(&group.series, options, grouping);
            MRangeResultRow {
                key,
                labels: group.labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn handle_aggregation(
    metas: Vec<MRangeSeriesMeta<'_>>,
    options: &RangeOptions,
) -> Vec<MRangeResultRow> {
    let data = get_all_series_samples(&metas, options);
    data.into_iter()
        .zip(metas)
        .map(|(samples, meta)| {
            let labels =
                get_series_labels(meta.series, options.with_labels, &options.selected_labels);
            let labels = labels
                .into_iter()
                .map(|x| x.map(|x| Label::new(x.name, x.value)))
                .collect::<Vec<Option<Label>>>();
            MRangeResultRow {
                key: meta.source_key,
                labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn handle_raw(metas: Vec<MRangeSeriesMeta>, options: &RangeOptions) -> Vec<MRangeResultRow> {
    let all_samples = get_all_series_samples(&metas, options);
    all_samples
        .into_iter()
        .zip(metas)
        .map(|(samples, meta)| {
            let labels =
                get_series_labels(meta.series, options.with_labels, &options.selected_labels);
            let labels = labels
                .into_iter()
                .map(|x| x.map(|x| Label::new(x.name, x.value)))
                .collect::<Vec<Option<Label>>>();

            MRangeResultRow {
                key: meta.source_key,
                labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn result_row_to_value(row: MRangeResultRow) -> ValkeyValue {
    let samples: Vec<ValkeyValue> = row.samples.into_iter().map(|x| x.into()).collect();
    let labels: Vec<_> = row
        .labels
        .into_iter()
        .map(|label| match label {
            Some(label) => ValkeyValue::Array(vec![
                ValkeyValue::from(label.name),
                ValkeyValue::from(label.value),
            ]),
            None => ValkeyValue::Null,
        })
        .collect();
    ValkeyValue::Array(vec![
        ValkeyValue::from(row.key),
        ValkeyValue::Array(labels),
        ValkeyValue::from(samples),
    ])
}

fn get_grouped_raw_samples(
    series: &[MRangeSeriesMeta<'_>],
    options: &RangeOptions,
    grouping_options: &RangeGroupingOptions,
) -> Vec<Sample> {
    // todo: maybe use rayon/chili rather than the multi iterator to read multiple chunks
    // concurrently
    let iterators = get_sample_iterators(series, options);
    let multi_iter = MultiSeriesSampleIter::new(iterators);
    group_samples_internal(multi_iter, grouping_options)
}

fn aggregate_grouped_samples(
    group: &GroupedSeries,
    options: &RangeOptions,
    aggr: Aggregator,
) -> Vec<Sample> {
    let mut aggregator = aggr;

    #[inline]
    fn update(aggr: &mut Aggregator, value: f64) -> bool {
        if value.is_nan() {
            false
        } else {
            aggr.update(value);
            true
        }
    }

    fn flush(
        aggregator: &mut Aggregator,
        timestamp: Timestamp,
        is_nan: bool,
        res: &mut Vec<Sample>,
    ) {
        res.push(Sample {
            timestamp,
            value: if is_nan {
                f64::NAN
            } else {
                aggregator.finalize()
            },
        });
        aggregator.reset()
    }

    // todo: maybe use rayon/chili rather than the multi iterator. Could significantly speed up
    // the process if we go beyond a small number of chunks

    let mut count = options.count.unwrap_or(usize::MAX);
    let iterators = get_sample_iterators(&group.series, options);
    let mut iter = MultiSeriesSampleIter::new(iterators);

    let mut res: Vec<Sample> = Vec::with_capacity(iter.size_hint().0);

    let (mut sample, mut is_nan) = if let Some(sample) = iter.next() {
        (sample, !update(&mut aggregator, sample.value))
    } else {
        return res;
    };

    let mut unsaved: usize = 0;

    for next_sample in iter {
        if sample.timestamp == next_sample.timestamp {
            if !update(&mut aggregator, sample.value) {
                is_nan = true;
            }
            unsaved += 1;
        } else {
            flush(&mut aggregator, sample.timestamp, is_nan, &mut res);
            is_nan = !update(&mut aggregator, sample.value);
            sample = next_sample;

            unsaved = 1;
            count -= 1;
            if count == 0 {
                // we have all the buckets we need. Don't add the last one
                unsaved = 0;
                break;
            }
        }
    }

    if unsaved > 0 {
        flush(&mut aggregator, sample.timestamp, is_nan, &mut res);
    }

    res
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
    meta: &MRangeSeriesMeta<'a>,
    options: &'a RangeOptions,
) -> SeriesSampleIterator<'a> {
    SeriesSampleIterator::new(
        meta.series,
        meta.start_ts,
        meta.end_ts,
        &options.value_filter,
        &options.timestamp_filter,
    )
}

fn get_sample_iterators<'a>(
    series: &[MRangeSeriesMeta<'a>],
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
    fn fetch_one(meta: &MRangeSeriesMeta, range_options: &RangeOptions) -> Vec<Sample> {
        let start_ts = meta.start_ts;
        let end_ts = meta.end_ts;
        if let Some(agg_options) = &range_options.aggregation {
            get_series_sample_aggregates(meta, start_ts, end_ts, range_options, agg_options)
        } else {
            get_raw_samples(meta.series, start_ts, end_ts, range_options)
        }
    }

    fn get_raw_internal(
        series: &[MRangeSeriesMeta],
        range_options: &RangeOptions,
    ) -> Vec<Vec<Sample>> {
        match series {
            [] => vec![],
            [meta] => {
                let samples = fetch_one(meta, range_options);
                vec![samples]
            }
            [first, second] => {
                let (first_samples, second_samples) = join(
                    || fetch_one(first, range_options),
                    || fetch_one(second, range_options),
                );
                vec![first_samples, second_samples]
            }
            _ => {
                let (first, rest) = series.split_at(series.len() / 2);
                let (mut first_samples, rest_samples) = join(
                    || get_raw_internal(first, range_options),
                    || get_raw_internal(rest, range_options),
                );

                first_samples.extend(rest_samples);
                first_samples
            }
        }
    }

    get_raw_internal(meta, range_options)
}

fn get_series_sample_aggregates(
    meta: &MRangeSeriesMeta<'_>,
    start_ts: Timestamp,
    end_ts: Timestamp,
    range_options: &RangeOptions,
    aggregation_options: &AggregationOptions,
) -> Vec<Sample> {
    let samples = get_raw_samples(meta.series, start_ts, end_ts, range_options);
    aggregate_samples(samples.into_iter(), start_ts, end_ts, aggregation_options)
}

struct GroupedSeries<'a> {
    label_value: String,
    series: Vec<MRangeSeriesMeta<'a>>,
    labels: Vec<Option<Label>>,
}

fn group_series_by_label<'a>(
    metas: Vec<MRangeSeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
) -> Vec<GroupedSeries<'a>> {
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
