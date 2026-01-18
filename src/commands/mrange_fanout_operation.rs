use super::fanout::generated::{MultiRangeRequest, MultiRangeResponse, SeriesRangeResponse};
use crate::common::Sample;
use crate::fanout::FanoutOperation;
use crate::fanout::NodeInfo;
use crate::iterators::{MultiSeriesSampleIter, create_sample_iterator_adapter};
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk};
use crate::series::mrange::{
    build_mrange_grouped_labels, process_mrange_query, sort_mrange_results,
};
use crate::series::request_types::{MRangeOptions, MRangeSeriesResult, RangeGroupingOptions};
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use orx_parallel::{IntoParIter, IterIntoParIter};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use valkey_module::{Context, Status, ValkeyResult};

#[derive(Default)]
pub struct MRangeFanoutOperation {
    options: MRangeOptions,
    series: Vec<SeriesRangeResponse>,
}

impl MRangeFanoutOperation {
    pub fn new(options: MRangeOptions) -> Self {
        Self {
            options,
            series: Vec::with_capacity(8),
        }
    }
}

impl FanoutOperation for MRangeFanoutOperation {
    type Request = MultiRangeRequest;
    type Response = MultiRangeResponse;

    fn name() -> &'static str {
        "mrange"
    }

    fn get_local_response(
        ctx: &Context,
        req: MultiRangeRequest,
    ) -> ValkeyResult<MultiRangeResponse> {
        let mut options: MRangeOptions = req.try_into()?;
        // These (along with grouping) will be handled after gathering all results
        options.range.count = None;
        options.range.aggregation = None;
        options.is_reverse = false;

        // Process the MRange query locally
        let series = process_mrange_query(ctx, options, true)?;

        // Convert MRangeSeriesResult to SeriesResponse
        let serialized: Result<Vec<SeriesRangeResponse>, _> =
            series.into_iter().map(|x| x.try_into()).collect();

        Ok(MultiRangeResponse {
            series: serialized?,
        })
    }

    fn generate_request(&self) -> MultiRangeRequest {
        serialize_request(&self.options)
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        let mut resp = resp;
        self.series.append(&mut resp.series);
    }

    fn generate_reply(&mut self, ctx: &Context) -> Status {
        self.options.range.latest = false;
        self.options.range.timestamp_filter = None;
        self.options.range.value_filter = None;

        let options = &self.options;
        let is_grouped = options.grouping.is_some();
        let series = std::mem::take(&mut self.series);

        let result = if is_grouped {
            handle_grouping(series, options)
        } else {
            handle_basic(series, options)
        };

        match result {
            Ok(mut series) => {
                sort_mrange_results(&mut series, is_grouped);
                ctx.reply(Ok(series.into()))
            }
            Err(e) => {
                ctx.log_warning(&format!("Error processing series responses: {e}"));
                ctx.reply_error_string("Internal error processing grouped series");
                Status::Err
            }
        }
    }
}

fn handle_basic(
    series: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    series
        .into_par()
        .map(MRangeSeriesResult::try_from) // Explicit conversion
        .into_fallible_result()
        .map(|series| process_series_samples(series, options))
        .collect()
}

fn serialize_request(request: &MRangeOptions) -> MultiRangeRequest {
    // Note: request should have been validated on construction
    request
        .try_into()
        .expect("Failed to serialize MultiRangeRequest")
}

struct GroupData {
    keys: SmallVec<String, 8>,
    series: Vec<MRangeSeriesResult>,
}

type GroupMap = BTreeMap<String, GroupData>;

/// Apply GROUPBY/REDUCE to the series coming from remote nodes
fn handle_grouping(
    series: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let group_options = options
        .grouping
        .as_ref()
        .expect("Grouping options should be present");
    let results = series
        .into_par()
        .map(MRangeSeriesResult::try_from)
        .into_fallible_result()
        .collect()?;
    let grouped_by_key = construct_group_map(results);

    Ok(grouped_by_key
        .into_iter()
        .iter_into_par()
        .map(|(label, data)| process_group(label, data, options, group_options))
        .collect())
}

fn construct_group_map(series: Vec<MRangeSeriesResult>) -> BTreeMap<String, GroupData> {
    let mut grouped = BTreeMap::new();
    for meta in series {
        if let Some(label) = meta.group_label_value.clone() {
            grouped
                .entry(label)
                .or_insert_with(|| GroupData {
                    keys: SmallVec::new(),
                    series: Vec::new(),
                })
                .series
                .push(meta);
        }
    }
    grouped
}

fn process_group(
    label: String,
    data: GroupData,
    options: &MRangeOptions,
    group_options: &RangeGroupingOptions,
) -> MRangeSeriesResult {
    let samples = process_series_list(&data.series, options);
    let chunk = UncompressedChunk::from_vec(samples);
    let labels = build_mrange_grouped_labels(
        &group_options.group_label,
        &label,
        group_options.aggregation.aggregation_name(),
        &data.keys,
    );

    MRangeSeriesResult {
        key: data.keys.join(","),
        group_label_value: Some(label),
        labels,
        data: TimeSeriesChunk::Uncompressed(chunk),
    }
}

fn process_series_samples(
    mut series: MRangeSeriesResult,
    options: &MRangeOptions,
) -> MRangeSeriesResult {
    let samples = process_series_list(std::slice::from_ref(&series), options);
    series.data = TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(samples));
    series
}

fn process_series_list(series: &[MRangeSeriesResult], options: &MRangeOptions) -> Vec<Sample> {
    let (reverse_iter, reverse_aggr) = validate_reverse(options);

    if reverse_iter {
        let mut samples: Vec<_> = series.iter().flat_map(|s| s.data.iter()).collect();
        samples.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        create_sample_iterator_adapter(
            samples.into_iter(),
            &options.range,
            &options.grouping,
            reverse_aggr,
        )
        .collect()
    } else if series.len() == 1 {
        create_sample_iterator_adapter(
            series[0].data.iter(),
            &options.range,
            &options.grouping,
            reverse_aggr,
        )
        .collect()
    } else {
        let iters = series.iter().map(|s| s.data.iter()).collect::<Vec<_>>();
        create_sample_iterator_adapter(
            MultiSeriesSampleIter::new(iters),
            &options.range,
            &options.grouping,
            reverse_aggr,
        )
        .collect()
    }
}

pub fn validate_reverse(options: &MRangeOptions) -> (bool, bool) {
    let has_aggregation = options.range.aggregation.is_some();

    // Aggregation requires ascending input order from base iterators
    // Without aggregation, base iterators can directly provide the requested order
    let should_reverse_iter = !has_aggregation && options.is_reverse;

    // Apply reversal after aggregation if needed
    let should_reverse_aggr = has_aggregation && options.is_reverse;

    (should_reverse_iter, should_reverse_aggr)
}
