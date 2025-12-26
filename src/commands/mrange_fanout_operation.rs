use super::fanout::generated::{MultiRangeRequest, MultiRangeResponse, SeriesRangeResponse};
use crate::commands::utils::reply_with_mrange_series_results;
use crate::common::Sample;
use crate::fanout::FanoutOperation;
use crate::fanout::NodeInfo;
use crate::iterators::MultiSeriesSampleIter;
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk};
use crate::series::mrange::{
    build_mrange_grouped_labels, collect_samples, create_mrange_iterator_adapter,
    process_mrange_query, sort_mrange_results,
};
use crate::series::request_types::{MRangeOptions, MRangeSeriesResult, RangeGroupingOptions};
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use orx_parallel::{IntoParIter, IterIntoParIter};
use smallvec::{SmallVec, smallvec};
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
        let options = &self.options;
        let is_grouped = options.grouping.is_some();

        let series = std::mem::take(&mut self.series);
        let res = if is_grouped {
            handle_grouping(series, options)
        } else {
            handle_basic(series, options)
        };

        let mut series = match res {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("MRangeFanoutOperation: error processing series responses: {e}");
                ctx.log_warning(&msg);
                // todo: better error
                ctx.reply_error_string("Internal error processing grouped series");
                return Status::Err;
            }
        };

        sort_mrange_results(&mut series, is_grouped);

        if reply_with_mrange_series_results(ctx, &series).is_ok() {
            Status::Ok
        } else {
            Status::Err
        }
    }
}

fn handle_basic(
    series: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let handler = |meta: MRangeSeriesResult, options: &MRangeOptions| {
        let aggr_iter = create_mrange_iterator_adapter(meta.data.iter(), options);
        let count = if options.is_reverse {
            options.range.count
        } else {
            None
        };
        let samples = collect_samples(aggr_iter, options.is_reverse, count);
        let chunk = UncompressedChunk::from_vec(samples);
        let mut meta = meta;
        meta.data = TimeSeriesChunk::Uncompressed(chunk);
        meta
    };

    series
        .into_par()
        .map(MRangeSeriesResult::try_from) // Explicit conversion
        .into_fallible_result()
        .map(|res| handler(res, options))
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
    responses: Vec<SeriesRangeResponse>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let Some(group_options) = &options.grouping else {
        panic!("Grouping options should be present");
    };

    let results = responses
        .into_par() // parallel since chunks are compressed
        .map(MRangeSeriesResult::try_from)
        .into_fallible_result()
        .collect()?;

    let grouped_by_key = construct_group_map(results);

    let result = grouped_by_key
        .into_iter()
        .iter_into_par()
        .map(|(group_label_value, group_data)| {
            process_group(group_label_value, group_data, options, group_options)
        })
        .collect::<Vec<_>>();

    Ok(result)
}

fn construct_group_map(metas: Vec<MRangeSeriesResult>) -> GroupMap {
    let mut grouped_by_key: GroupMap = GroupMap::new();

    for meta in metas.into_iter() {
        let Some(label_value) = meta.group_label_value.clone() else {
            continue;
        };

        let key = meta.key.clone();

        match grouped_by_key.entry(label_value) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let stored = entry.get_mut();
                stored.keys.push(key);
                stored.series.push(meta);
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(GroupData {
                    keys: smallvec![key],
                    series: vec![meta],
                });
            }
        }
    }

    grouped_by_key
}

fn process_group(
    group_label_value: String,
    group_data: GroupData,
    options: &MRangeOptions,
    grouping_options: &RangeGroupingOptions,
) -> MRangeSeriesResult {
    let mut group_data = group_data;
    let samples = process_group_list(&mut group_data.series, options);
    let chunk = UncompressedChunk::from_vec(samples);
    let data = TimeSeriesChunk::Uncompressed(chunk);

    let group_label = &grouping_options.group_label;
    let grouping_aggregation = grouping_options.aggregation;
    let reducer_name = grouping_aggregation.name();

    let labels = build_mrange_grouped_labels(
        group_label,
        &group_label_value,
        reducer_name,
        &group_data.keys,
    );

    MRangeSeriesResult {
        key: group_data.keys.join(","),
        group_label_value: Some(group_label_value),
        labels,
        data,
    }
}

fn process_group_list(items: &mut [MRangeSeriesResult], options: &MRangeOptions) -> Vec<Sample> {
    let iters = items
        .iter_mut()
        .map(|x| {
            let chunk = &x.data;
            chunk.iter()
        })
        .collect::<Vec<_>>();

    let multi_iter = MultiSeriesSampleIter::new(iters);
    let adapter = create_mrange_iterator_adapter(multi_iter, options);
    let count = if options.is_reverse {
        options.range.count
    } else {
        None
    };
    collect_samples(adapter, options.is_reverse, count)
}
