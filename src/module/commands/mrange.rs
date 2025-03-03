use crate::aggregators::{AggOp, Aggregator};
use crate::arg_types::{RangeGroupingOptions, RangeOptions};
use crate::common::{Sample, Timestamp};
use crate::iterators::aggregator::AggregationOptions;
use crate::iterators::{MultiSeriesSampleIter, SampleIter};
use crate::module::commands::range_arg_parse::parse_range_options;
use crate::module::commands::range_utils::{
    aggregate_samples, get_series_labels, group_samples_internal,
};
use crate::module::result::sample_to_value;
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::index::{series_keys_by_matchers, with_timeseries_index};
use crate::series::{SeriesSampleIterator, TimeSeries};
use ahash::AHashMap;
use valkey_module::{Context, NextArg, ValkeyResult, ValkeyString, ValkeyValue};
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};

struct SeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: ValkeyString,
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

    args.done()?;

    with_timeseries_index(ctx, move |index| {
        let matchers = std::mem::take(&mut options.series_selector);
        let keys = series_keys_by_matchers(ctx, index, &[matchers])?;

        // needed to keep valkey keys alive below
        let db_keys = keys.iter().map(|key| ctx.open_key(key)).collect::<Vec<_>>();

        let metas = db_keys.iter()
            .zip(keys)
            .filter_map(|(db_key, source_key)| {
                if let Ok(Some(series)) = db_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                    let (start_ts, end_ts) = options.date_range.get_series_range(series, None,false);
                    let meta = SeriesMeta {
                        series,
                        source_key,
                        start_ts,
                        end_ts,
                    };
                    Some(meta)
                } else {
                    None
                }
            }).collect();

        let result_rows = process_command(metas, &options);
        let mut result = result_rows
            .into_iter()
            .map(result_row_to_value)
            .collect::<Vec<_>>();

        if reverse {
            result.reverse();
        }
        
        Ok(ValkeyValue::from(result))
    })
}

struct ResultRow {
    key: ValkeyValue,
    labels: Vec<ValkeyValue>,
    samples: Vec<Sample>,
}

/// calculate the max and min timestamps
fn calculate_timestamp_range<'a>(metas: &'a Vec<SeriesMeta<'a>>) -> (Timestamp, Timestamp) {
    let mut start_ts = i64::MAX;
    let mut end_ts = i64::MIN;
    for meta in metas {
        start_ts = start_ts.min(meta.start_ts);
        end_ts = end_ts.max(meta.end_ts);
    }
    (start_ts, end_ts)
}

fn process_command(metas: Vec<SeriesMeta>, options: &RangeOptions) -> Vec<ResultRow> {
    match (&options.grouping, &options.aggregation) {
        (Some(groupings), Some(aggr_options)) => {
            handle_aggregation_and_grouping(metas, options, groupings, aggr_options)
        }
        (Some(groupings), None) => {
            // group raw samples
            handle_grouping(metas, options, groupings)
        }
        (None, Some(aggr_options)) => handle_aggregation(metas, options, aggr_options),
        (None, None) => handle_raw(metas, options),
    }
}

fn handle_aggregation_and_grouping(
    metas: Vec<SeriesMeta<'_>>,
    options: &RangeOptions,
    groupings: &RangeGroupingOptions,
    aggregations: &AggregationOptions,
) -> Vec<ResultRow> {
    let grouped_series = group_series_by_label(metas, groupings);

    grouped_series
        .into_iter()
        .map(|group| {
            let group_key = format!("{}={}", groupings.group_label, group.label_value);
            let key = ValkeyValue::from(group_key);
            let aggregator = aggregations.aggregator.clone();
            let aggregates = aggregate_grouped_samples(&group, options, aggregator);
            let samples = group_samples_internal(aggregates.into_iter(), groupings);
            ResultRow {
                key,
                labels: group.labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn handle_grouping(
    metas: Vec<SeriesMeta<'_>>,
    options: &RangeOptions,
    grouping: &RangeGroupingOptions,
) -> Vec<ResultRow> {
    // group raw samples
    let grouped_series = group_series_by_label(metas, grouping);
    grouped_series
        .into_iter()
        .map(|group| {
            // todo: we need to account for the fact that valkey strings are binary safe,
            // we should probably restrict labels to utf-8 on construction
            let group_key = format!("{}={}", grouping.group_label, group.label_value);
            let key = ValkeyValue::from(group_key);
            let samples = get_grouped_raw_samples(&group.series, options, grouping);
            ResultRow {
                key,
                labels: group.labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn handle_aggregation(
    metas: Vec<SeriesMeta<'_>>,
    options: &RangeOptions,
    aggregation: &AggregationOptions,
) -> Vec<ResultRow> {
    let (start_ts, end_ts) = calculate_timestamp_range(&metas);
    let data = get_raw_sample_aggregates(&metas, start_ts, end_ts, options, aggregation);
    data.into_iter()
        .zip(metas)
        .map(|(samples, meta)| {
            let labels =
                get_series_labels(meta.series, options.with_labels, &options.selected_labels);
            ResultRow {
                key: ValkeyValue::from(meta.source_key),
                labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn handle_raw(metas: Vec<SeriesMeta>, options: &RangeOptions) -> Vec<ResultRow> {
    let mut iterators = get_sample_iterators(&metas, options);
    // todo: maybe rayon
    iterators
        .iter_mut()
        .zip(metas)
        .map(|(iter, meta)| {
            let labels =
                get_series_labels(meta.series, options.with_labels, &options.selected_labels);
            let samples = iter.collect::<Vec<Sample>>();
            ResultRow {
                key: ValkeyValue::from(meta.source_key),
                labels,
                samples,
            }
        })
        .collect::<Vec<_>>()
}

fn result_row_to_value(row: ResultRow) -> ValkeyValue {
    let samples: Vec<_> = row.samples.into_iter().map(sample_to_value).collect();
    ValkeyValue::Array(vec![
        row.key,
        ValkeyValue::from(row.labels),
        ValkeyValue::from(samples),
    ])
}

fn get_grouped_raw_samples(
    series: &[SeriesMeta<'_>],
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

fn get_series_iterator<'a>(
    meta: &SeriesMeta<'a>,
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
    series: &[SeriesMeta<'a>],
    range_options: &'a RangeOptions,
) -> Vec<SampleIter<'a>> {
    series
        .iter()
        .map(|meta| get_series_iterator(meta, range_options).into())
        .collect::<Vec<SampleIter<'a>>>()
}

fn get_raw_sample_aggregates(
    series: &[SeriesMeta],
    start_ts: Timestamp,
    end_ts: Timestamp,
    range_options: &RangeOptions,
    aggregation_options: &AggregationOptions,
) -> Vec<Vec<Sample>> {
    // todo: rayon
    series
        .iter()
        .map(|meta| {
            get_series_sample_aggregates(meta, start_ts, end_ts, range_options, aggregation_options)
        })
        .collect::<Vec<_>>()
}

fn get_series_sample_aggregates(
    series: &SeriesMeta<'_>,
    start_ts: Timestamp,
    end_ts: Timestamp,
    range_options: &RangeOptions,
    aggregation_options: &AggregationOptions,
) -> Vec<Sample> {
    let iter = get_series_iterator(series, range_options);
    aggregate_samples(
        iter,
        start_ts,
        end_ts,
        aggregation_options,
        range_options.count,
    )
}


struct GroupedSeries<'a> {
    label_value: String,
    series: Vec<SeriesMeta<'a>>,
    labels: Vec<ValkeyValue>,
}

fn group_series_by_label<'a>(
    metas: Vec<SeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
) -> Vec<GroupedSeries<'a>> {
    let mut grouped: AHashMap<String, Vec<SeriesMeta>> = AHashMap::new();
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
                sources.push_str(&meta.source_key.to_string());
                if i < series.len() - 1 {
                    sources.push(',');
                }
            }
            let labels: Vec<ValkeyValue> = vec![
                ValkeyValue::Array(vec![
                    ValkeyValue::from(label),
                    ValkeyValue::from(label_value.clone()),
                ]),
                ValkeyValue::Array(vec![
                    ValkeyValue::from(REDUCER_KEY),
                    ValkeyValue::from(reducer),
                ]),
                ValkeyValue::Array(vec![
                    ValkeyValue::from(SOURCE_KEY),
                    ValkeyValue::from(sources),
                ]),
            ];
            GroupedSeries {
                label_value,
                series,
                labels,
            }
        })
        .collect()
}
