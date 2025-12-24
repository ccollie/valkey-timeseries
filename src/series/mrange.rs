use crate::aggregators::AggregateIterator;
use crate::common::Sample;
use crate::common::constants::{REDUCER_KEY, SOURCE_KEY};
use crate::error_consts;
use crate::iterators::ReduceIterator;
use crate::iterators::{MultiSeriesSampleIter, TimeSeriesRangeIterator};
use crate::labels::Label;
use crate::series::TimeSeries;
use crate::series::acl::check_metadata_permissions;
use crate::series::chunks::{Chunk, GorillaChunk, TimeSeriesChunk, UncompressedChunk};
use crate::series::index::series_by_selectors;
use crate::series::request_types::{
    AggregationOptions, MRangeOptions, MRangeSeriesResult, RangeGroupingOptions,
};
use ahash::AHashMap;
use logger_rust::log_debug;
use orx_parallel::{IntoParIter, IterIntoParIter, ParIter};
use valkey_module::{Context, ValkeyError, ValkeyResult};

pub(crate) struct MRangeSeriesMeta<'a> {
    series: &'a TimeSeries,
    source_key: String,
    group_label_value: Option<String>,
}

pub fn process_mrange_query(
    ctx: &Context,
    options: MRangeOptions,
    clustered: bool,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    check_metadata_permissions(ctx)?;

    if options.filters.is_empty() {
        return Err(ValkeyError::Str(error_consts::MISSING_FILTER));
    }

    let series_guards = series_by_selectors(ctx, &options.filters, None)?;

    let series_metas: Vec<MRangeSeriesMeta> = series_guards
        .iter()
        .map(|guard| MRangeSeriesMeta {
            series: guard,
            source_key: guard.get_key().to_string(),
            group_label_value: None,
        })
        .collect();

    Ok(process_mrange(Some(ctx), series_metas, options, clustered))
}

fn process_mrange(
    ctx: Option<&Context>,
    metas: Vec<MRangeSeriesMeta>,
    options: MRangeOptions,
    is_clustered: bool,
) -> Vec<MRangeSeriesResult> {
    let mut options = options;
    let mut metas = metas;

    log_debug!("process_mrange ");
    if let Some(grouping) = &options.grouping {
        collect_group_label_values(&mut metas, grouping);
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
    // if we're clustered, don't group or aggregate here - do it in the caller node
    let mut items = if is_clustered {
        options.range.aggregation = None;
        // also disable count - it will be applied in the caller node
        options.range.count = None;
        return handle_basic(ctx, metas, &options, is_clustered);
    } else if options.grouping.is_some() {
        handle_grouping(ctx, metas, &options)
    } else {
        handle_basic(ctx, metas, &options, is_clustered)
    };

    if !is_clustered {
        sort_mrange_results(&mut items, options.grouping.is_some());
    }

    items
}

pub(crate) fn sort_mrange_results(results: &mut [MRangeSeriesResult], is_grouped: bool) {
    if is_grouped {
        results.sort_by(|a, b| a.group_label_value.cmp(&b.group_label_value));
    } else {
        results.sort_by(|a, b| a.key.cmp(&b.key));
    }
}

fn handle_basic(
    ctx: Option<&Context>,
    metas: Vec<MRangeSeriesMeta>,
    options: &MRangeOptions,
    clustered: bool,
) -> Vec<MRangeSeriesResult> {
    fn process_series(
        ctx: Option<&Context>,
        meta: MRangeSeriesMeta,
        options: &MRangeOptions,
        clustered: bool,
    ) -> MRangeSeriesResult {
        let range = &options.range;
        let iter = TimeSeriesRangeIterator::new(ctx, meta.series, range, true);
        let data = if clustered {
            let mut chunk = GorillaChunk::with_max_size(16 * 1024); // 16KB - todo: make configurable?
            for sample in iter {
                let _ = chunk.add_sample(&sample);
            }
            TimeSeriesChunk::Gorilla(chunk)
        } else {
            let samples = collect_samples(options, iter);
            let chunk = UncompressedChunk::from_vec(samples);
            TimeSeriesChunk::Uncompressed(chunk)
        };

        let labels = convert_labels(meta.series, options.with_labels, &options.selected_labels);

        MRangeSeriesResult {
            group_label_value: meta.group_label_value,
            key: meta.source_key,
            labels,
            data,
        }
    }

    if options.range.latest {
        // when LATEST is specified, we cannot use parallel processing since Context is not Send
        metas
            .into_iter()
            .map(|meta| process_series(ctx, meta, options, clustered))
            .collect()
    } else {
        metas
            .into_par()
            .map(|meta| process_series(None, meta, options, clustered))
            .collect()
    }
}

fn handle_grouping(
    ctx: Option<&Context>,
    metas: Vec<MRangeSeriesMeta>,
    options: &MRangeOptions,
) -> Vec<MRangeSeriesResult> {
    fn process_group<'a>(
        ctx: Option<&Context>,
        grouping: &RangeGroupingOptions,
        options: &MRangeOptions,
        label_value: String,
        group_data: GroupedSeriesData<'a>,
    ) -> MRangeSeriesResult {
        let data = get_grouped_samples(ctx, &group_data.series, options, grouping);
        let labels = group_data.labels;
        let key = format!("{}={}", grouping.group_label, label_value);
        let chunk = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(data));
        MRangeSeriesResult {
            key,
            group_label_value: Some(label_value),
            labels,
            data: chunk,
        }
    }

    let Some(grouping) = &options.grouping else {
        panic!("Grouping options should be present");
    };

    let grouped_series_map = group_series_by_label(metas, grouping, options.with_labels);

    if grouped_series_map.is_empty() {
        return vec![];
    }

    // if LATEST is specified, we run sequentially since we have a live Context which is not Send + Sync
    // Fortunately, LATEST queries are expected to be fast anyway
    if options.range.latest {
        return grouped_series_map
            .into_iter()
            .map(|(label_value, group_data)| {
                process_group(ctx, grouping, options, label_value, group_data)
            })
            .collect::<Vec<_>>();
    }

    grouped_series_map
        .into_iter()
        .iter_into_par()
        .map(|(label_value, group_data)| {
            process_group(None, grouping, options, label_value, group_data)
        })
        .collect::<Vec<_>>()
}

fn get_grouped_samples(
    ctx: Option<&Context>,
    series_metas: &[MRangeSeriesMeta],
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
) -> Vec<Sample> {
    // This function gets the raw samples from all series in the group, then applies the grouping
    // reducer across the samples.
    // todo: choose approach based on data size and available memory?
    let iterators = series_metas
        .iter()
        .map(|meta| TimeSeriesRangeIterator::new(ctx, meta.series, &options.range, false))
        .collect::<Vec<_>>();

    let multi_iter = MultiSeriesSampleIter::new(iterators);
    let reducer = ReduceIterator::new(multi_iter, grouping_options.aggregation);

    collect_samples(options, reducer)
}

pub(crate) fn collect_samples<I: Iterator<Item = Sample>>(
    options: &MRangeOptions,
    iter: I,
) -> Vec<Sample> {
    if options.is_reverse {
        // we collect instead of .rev() because the reducer is not double-ended
        let mut samples = iter.collect::<Vec<_>>();
        samples.reverse();
        if let Some(count) = options.range.count {
            samples.truncate(count);
        }
        samples
    } else if let Some(count) = options.range.count {
        iter.take(count).collect::<Vec<_>>()
    } else {
        iter.collect::<Vec<_>>()
    }
}

fn convert_labels(
    series: &TimeSeries,
    with_labels: bool,
    selected_labels: &[String],
) -> Vec<Label> {
    if !with_labels && selected_labels.is_empty() {
        return Vec::new();
    }

    if selected_labels.is_empty() {
        return series.labels.iter().map(|l| l.into()).collect();
    }

    selected_labels
        .iter()
        .map(|name| {
            series
                .get_label(name)
                .map(|label| label.into())
                .unwrap_or_else(|| Label {
                    name: name.clone(),
                    value: String::new(),
                })
        })
        .collect()
}

pub(crate) fn build_mrange_grouped_labels(
    group_label_name: &str,
    group_label_value: &str,
    reducer_name_str: &str,
    source_identifiers: &[String],
) -> Vec<Label> {
    let sources = source_identifiers.join(",");
    vec![
        Label {
            name: group_label_name.into(),
            value: group_label_value.to_string(),
        },
        Label {
            name: REDUCER_KEY.into(),
            value: reducer_name_str.into(),
        },
        Label {
            name: SOURCE_KEY.into(),
            value: sources,
        },
    ]
}

fn collect_group_label_values(metas: &mut Vec<MRangeSeriesMeta>, grouping: &RangeGroupingOptions) {
    for meta in metas.iter_mut() {
        meta.group_label_value = meta
            .series
            .label_value(&grouping.group_label)
            .map(|s| s.to_string());
    }
}

struct GroupedSeriesData<'a> {
    series: Vec<MRangeSeriesMeta<'a>>,
    labels: Vec<Label>,
}

fn group_series_by_label<'a>(
    metas: Vec<MRangeSeriesMeta<'a>>,
    grouping: &RangeGroupingOptions,
    with_labels: bool,
) -> AHashMap<String, GroupedSeriesData<'a>> {
    let mut grouped: AHashMap<String, GroupedSeriesData<'a>> = AHashMap::new();
    let group_by_label_name = &grouping.group_label;
    let reducer_name = grouping.aggregation.name();

    for mut meta in metas.into_iter() {
        if let Some(label_value_str) = meta.group_label_value.take() {
            let entry = grouped
                .entry(label_value_str)
                .or_insert_with(|| GroupedSeriesData {
                    series: Vec::new(),
                    labels: Vec::new(),
                });
            entry.series.push(meta);
        }
    }

    if with_labels {
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
    }

    grouped
}

pub fn create_aggregate_iterator<I>(
    iter: I,
    options: &MRangeOptions,
    aggregation: &AggregationOptions,
) -> AggregateIterator<I>
where
    I: Iterator<Item = Sample>,
{
    let (start_ts, end_ts) = options.range.get_timestamp_range();
    let aligned_timestamp = aggregation
        .alignment
        .get_aligned_timestamp(start_ts, end_ts);
    AggregateIterator::new(iter, aggregation, aligned_timestamp)
}

macro_rules! apply_iter_limit {
    ($iter:expr, $limit:expr) => {
        if let Some(limit) = $limit {
            Box::new($iter.take(limit as usize)) as Box<dyn Iterator<Item = _> + '_>
        } else {
            Box::new($iter) as Box<dyn Iterator<Item = _> + '_>
        }
    };
}

pub fn create_mrange_iterator_adapter<'a>(
    base_iter: impl Iterator<Item = Sample> + 'a,
    options: &MRangeOptions,
) -> Box<dyn Iterator<Item = Sample> + 'a> {
    // only use count if were not reversed. Reversed queries are fixed in post-processing
    let count = if options.is_reverse {
        None
    } else {
        options.range.count
    };
    match (&options.range.aggregation, &options.grouping) {
        (Some(agg), Some(grouping)) => {
            let grouping_aggregation = grouping.aggregation;
            let aggr_iter = create_aggregate_iterator(base_iter, options, agg);
            let reducer = ReduceIterator::new(aggr_iter, grouping_aggregation);
            apply_iter_limit!(reducer, count)
        }
        (None, Some(grouping)) => {
            let aggregation = grouping.aggregation;
            let reducer = ReduceIterator::new(base_iter, aggregation);
            apply_iter_limit!(reducer, count)
        }
        (Some(aggregation), None) => {
            let aggr_iter = create_aggregate_iterator(base_iter, options, aggregation);
            apply_iter_limit!(aggr_iter, count)
        }
        _ => {
            apply_iter_limit!(base_iter, count)
        }
    }
}
