use super::fanout::generated::{
    DateRange, MultiRangeRequest, MultiRangeResponse, SeriesResponse, ValueRange,
};
use crate::commands::fanout::matchers::serialize_matchers_list;
use crate::common::Sample;
use crate::fanout::FanoutTarget;
use crate::fanout::{FanoutOperation, exec_fanout_request_base};
use crate::iterators::{MultiSeriesSampleIter, SampleIter};
use crate::series::mrange::{build_mrange_grouped_labels, process_mrange_query};
use crate::series::range_utils::group_reduce;
use crate::series::request_types::{MRangeOptions, MRangeSeriesResult, RangeGroupingOptions};
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use orx_parallel::{IntoParIter, IterIntoParIter};
use std::collections::BTreeMap;
use valkey_module::{Context, ValkeyResult, ValkeyValue};

#[derive(Default)]
pub struct MultiRangeFanoutOperation {
    options: MRangeOptions,
    series: Vec<MultiRangeResponse>,
}

impl MultiRangeFanoutOperation {
    pub fn new(options: MRangeOptions) -> Self {
        Self {
            options,
            series: Vec::new(),
        }
    }
}

pub(super) fn execute_mrange_fanout_operation(
    ctx: &Context,
    options: MRangeOptions,
) -> ValkeyResult<ValkeyValue> {
    let operation = MultiRangeFanoutOperation::new(options);
    exec_fanout_request_base(ctx, operation)
}

impl FanoutOperation<MultiRangeRequest, MultiRangeResponse> for MultiRangeFanoutOperation {
    fn name() -> &'static str {
        "mrange"
    }

    fn get_local_response(
        ctx: &Context,
        req: MultiRangeRequest,
    ) -> ValkeyResult<MultiRangeResponse> {
        let options: MRangeOptions = req.try_into()?;
        let series = process_mrange_query(ctx, options, true)?;

        // Convert MRangeSeriesResult to SeriesResponse
        let serialized: Result<Vec<SeriesResponse>, _> =
            series.into_iter().map(|x| x.try_into()).collect();

        Ok(MultiRangeResponse {
            series: serialized?,
        })
    }

    fn generate_request(&mut self) -> MultiRangeRequest {
        serialize_request(&self.options)
    }

    fn on_response(&mut self, resp: MultiRangeResponse, _target: FanoutTarget) {
        self.series.push(resp);
    }

    fn generate_reply(&mut self, ctx: &Context) {
        let series = std::mem::take(&mut self.series);
        let all_series = series
            .into_par()
            .flat_map(deserialize_multi_range_response)
            .reduce(|mut acc, item| {
                acc.extend(item);
                acc
            })
            .unwrap_or_default();

        let series = if let Some(grouping) = &self.options.grouping {
            group_sharded_series(all_series, grouping)
        } else {
            all_series
        };

        // todo: reply directly without intermediate conversion
        let result = ValkeyValue::Array(series.into_iter().map(|x| x.into()).collect());
        ctx.reply(Ok(result));
    }
}

fn deserialize_multi_range_response(
    response: MultiRangeResponse,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    // series from other nodes are mostly encoded, so we parallelize the decoding
    response
        .series
        .into_par()
        .map(|item| item.try_into())
        .into_fallible_result()
        .collect()
}

fn serialize_request(request: &MRangeOptions) -> MultiRangeRequest {
    let range: Option<DateRange> = Some(request.date_range.into());
    let mut selected_labels = Vec::with_capacity(request.selected_labels.len());
    for name in &request.selected_labels {
        selected_labels.push(name.clone());
    }
    let timestamp_filter = match &request.timestamp_filter {
        Some(filter) => filter.clone(),
        None => vec![],
    };

    let aggregation = request.aggregation.map(|agg| agg.into());
    let grouping = request.grouping.as_ref().map(|g| g.into());

    let count = request.count.map(|c| c as u32);

    // Checks for correct values should have been done before in the command parser
    // so we can safely unwrap here
    let filters =
        serialize_matchers_list(&request.filters).expect("Failed to serialize matchers list");

    MultiRangeRequest {
        range,
        with_labels: request.with_labels,
        selected_labels,
        filters,
        value_filter: request.value_filter.map(|x| ValueRange {
            min: x.min,
            max: x.max,
        }),
        timestamp_filter,
        count,
        aggregation,
        grouping,
    }
}

/// Apply GROUPBY/REDUCE to the series coming from remote nodes
fn group_sharded_series(
    metas: Vec<MRangeSeriesResult>,
    grouping: &RangeGroupingOptions,
) -> Vec<MRangeSeriesResult> {
    fn handle_reducer(
        series_results: &[MRangeSeriesResult],
        grouping_opts: &RangeGroupingOptions,
    ) -> Vec<Sample> {
        let iterators: Vec<SampleIter> = series_results
            .iter()
            .map(|meta| SampleIter::Slice(meta.samples.iter()))
            .collect();
        let multi_iter = MultiSeriesSampleIter::new(iterators);
        let aggregation = grouping_opts.aggregation;
        group_reduce(multi_iter, aggregation)
    }

    let mut grouped_by_key: BTreeMap<String, Vec<MRangeSeriesResult>> = BTreeMap::new();

    for meta in metas.into_iter() {
        if let Some(label_value_for_grouping) = &meta.group_label_value {
            let key = format!("{}={}", grouping.group_label, label_value_for_grouping);
            grouped_by_key.entry(key).or_default().push(meta);
        }
    }

    let reducer_name_str = grouping.aggregation.name();
    let group_by_label_name_str = &grouping.group_label;

    grouped_by_key
        .into_iter()
        .iter_into_par()
        .map(|(group_key_str, series_results_in_group)| {
            let group_defining_val_str = group_key_str
                .rsplit_once('=')
                .map(|(_, val)| val)
                .unwrap_or("");

            let samples = handle_reducer(&series_results_in_group, grouping);

            let source_keys: Vec<String> =
                series_results_in_group.into_iter().map(|m| m.key).collect();

            let final_labels = build_mrange_grouped_labels(
                group_by_label_name_str,
                group_defining_val_str,
                reducer_name_str,
                &source_keys,
            );

            MRangeSeriesResult {
                group_label_value: None,
                key: group_key_str,
                samples,
                labels: final_labels,
            }
        })
        .collect::<Vec<_>>()
}
