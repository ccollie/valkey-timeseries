use super::fanout::generated::{
    GroupPartialSeries, MultiRangeRequest, MultiRangeResponse, SeriesRangeResponse,
};
use crate::aggregators::{PartialReducer, PartialState};
use crate::commands::utils::reply_with_mrange_series_results;
use crate::common::Sample;
use crate::fanout::{FanoutClientCommand, NodeInfo};
use crate::fanout::{FanoutCommandResult, FanoutContext};
use crate::iterators::{MultiSeriesSampleIter, create_sample_iterator_adapter};
use crate::series::chunks::{TimeSeriesChunk, UncompressedChunk};
use crate::series::mrange::{
    SampleLimit, build_mrange_grouped_labels, process_mrange_group_partials, process_mrange_query,
    sort_mrange_results,
};
use crate::series::request_types::{MRangeOptions, MRangeSeriesResult, RangeGroupingOptions};
use orx_parallel::ParIter;
use orx_parallel::ParIterResult;
use orx_parallel::{IntoParIter, IterIntoParIter};
use smallvec::SmallVec;
use std::collections::{BTreeMap, BTreeSet};
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

#[derive(Default)]
pub struct MRangeFanoutCommand {
    options: MRangeOptions,
    series: Vec<SeriesRangeResponse>,
    group_partials: Vec<GroupPartialSeries>,
    /// Shard-side aggregation push-down: shards return buckets instead of raw
    /// samples. Latched at construction so a concurrent `CONFIG SET` cannot
    /// desynchronize request generation and reply handling within one command.
    pushdown: bool,
    /// GROUPBY/REDUCE push-down: shards pre-reduce their local group members
    /// per bucket and return partial states. Only latched for decomposable
    /// reducers; others fall back to per-series bucket transport.
    pushdown_group: bool,
    /// COUNT push-down: shards apply COUNT as a head/tail pre-filter. The
    /// coordinator always re-applies COUNT, so this only bounds transfer and
    /// carries no version hazard.
    pushdown_count: bool,
}

impl MRangeFanoutCommand {
    pub fn new(options: MRangeOptions) -> Self {
        let config_enabled = crate::config::is_fanout_aggregation_pushdown_enabled();
        let pushdown = config_enabled && options.range.aggregation.is_some();
        let pushdown_group = config_enabled
            && options
                .grouping
                .as_ref()
                .is_some_and(|g| PartialReducer::for_config(&g.aggregation).is_some());
        let pushdown_count = config_enabled && options.range.count.is_some();
        Self {
            options,
            series: Vec::with_capacity(8),
            group_partials: Vec::new(),
            pushdown,
            pushdown_group,
            pushdown_count,
        }
    }
}

impl FanoutClientCommand for MRangeFanoutCommand {
    type Request = MultiRangeRequest;
    type Response = MultiRangeResponse;

    fn name() -> &'static str {
        "mrange"
    }

    fn get_local_response(
        ctx: &Context,
        req: MultiRangeRequest,
    ) -> ValkeyResult<MultiRangeResponse> {
        let apply_aggregation = req.apply_aggregation;
        let apply_group_reduce = req.apply_group_reduce;
        let apply_count = req.apply_count;
        let mut options: MRangeOptions = req.try_into()?;
        // COUNT push-down: capture the requested direction and count before
        // they are reset below. The shard streams ascending, so a reverse
        // query keeps the tail (= first `count` in requested order); the
        // coordinator re-applies COUNT as the final authority either way.
        let limit = if apply_count {
            options.range.count.map(|n| {
                if options.is_reverse {
                    SampleLimit::Tail(n)
                } else {
                    SampleLimit::Head(n)
                }
            })
        } else {
            None
        };
        // These (along with grouping) will be handled after gathering all
        // results. Shards always aggregate/encode ascending; the coordinator
        // reverses bucket order and applies the authoritative COUNT.
        options.range.count = None;
        options.is_reverse = false;

        if apply_group_reduce && options.grouping.is_some() {
            // GROUPBY/REDUCE push-down: pre-reduce local group members per
            // bucket (per-series aggregation included when present) and ship
            // partial states instead of per-series samples.
            let partials = process_mrange_group_partials(ctx, options, limit)?;
            return Ok(MultiRangeResponse {
                series: Vec::new(),
                group_partials: partials.into_iter().map(Into::into).collect(),
            });
        }

        if !apply_aggregation {
            // Legacy flow: return raw samples; the coordinator aggregates.
            options.range.aggregation = None;
        }

        // Process the MRange query locally
        let series = process_mrange_query(ctx, options, true, limit)?;

        // Convert MRangeSeriesResult to SeriesResponse
        let serialized: Result<Vec<SeriesRangeResponse>, _> =
            series.into_iter().map(|x| x.try_into()).collect();

        Ok(MultiRangeResponse {
            series: serialized?,
            group_partials: Vec::new(),
        })
    }

    fn generate_request(&self) -> MultiRangeRequest {
        let mut request = serialize_request(&self.options);
        request.apply_aggregation = self.pushdown;
        request.apply_group_reduce = self.pushdown_group;
        request.apply_count = self.pushdown_count;
        request
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        let mut resp = resp;
        self.series.append(&mut resp.series);
        self.group_partials.append(&mut resp.group_partials);
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        self.options.range.latest = false;
        self.options.range.timestamp_filter = None;
        self.options.range.value_filter = None;
        if self.pushdown {
            // Shards already bucketed each series; do not re-aggregate. The
            // remaining post-processing (GROUPBY reduce, reverse, COUNT)
            // operates on the received buckets.
            self.options.range.aggregation = None;
        }

        let is_grouped = self.options.grouping.is_some();
        let series = std::mem::take(&mut self.series);
        let group_partials = std::mem::take(&mut self.group_partials);
        let options = &self.options;

        let result = if self.pushdown_group {
            handle_group_partials(group_partials, options)
        } else if is_grouped {
            handle_grouping(series, options)
        } else {
            handle_basic(series, options)
        };

        match result {
            Ok(mut series) => {
                sort_mrange_results(&mut series, is_grouped);
                let _ = reply_with_mrange_series_results(ctx, &series);
                Status::Ok
            }
            Err(e) => {
                let warning = format!("Error processing MRange responses: {e:?}");
                ctx.reply(Err(e));
                ctx.log_warning(&warning);
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

/// Merge and finalize the per-(group, shard) partial states returned under
/// GROUPBY/REDUCE push-down: per group, merge states with equal bucket
/// timestamps across shards, finalize each into a sample, then apply
/// reversal and COUNT.
fn handle_group_partials(
    partials: Vec<GroupPartialSeries>,
    options: &MRangeOptions,
) -> ValkeyResult<Vec<MRangeSeriesResult>> {
    let group_options = options
        .grouping
        .as_ref()
        .expect("Grouping options should be present");
    let Some(reducer) = PartialReducer::for_config(&group_options.aggregation) else {
        return Err(ValkeyError::Str(
            "TSDB: internal error: reducer does not support partial reduce",
        ));
    };
    let kind = reducer.kind();

    type MergedGroup = (BTreeMap<i64, PartialState>, BTreeSet<String>);
    let mut groups: BTreeMap<String, MergedGroup> = BTreeMap::new();
    for partial in partials {
        let (buckets, sources) = groups.entry(partial.group_label_value).or_default();
        sources.extend(partial.source_keys);
        for (ts, state) in partial.bucket_timestamps.iter().zip(partial.states.iter()) {
            let state: PartialState = state.into();
            buckets
                .entry(*ts)
                .and_modify(|existing| PartialReducer::merge(kind, existing, &state))
                .or_insert(state);
        }
    }

    Ok(groups
        .into_iter()
        .map(|(label, (buckets, sources))| {
            let mut samples: Vec<Sample> = buckets
                .into_iter()
                .map(|(timestamp, state)| Sample {
                    timestamp,
                    value: PartialReducer::finalize(kind, &state),
                })
                .collect();
            if options.is_reverse {
                samples.reverse();
            }
            if let Some(count) = options.range.count {
                samples.truncate(count);
            }

            let sources: Vec<String> = sources.into_iter().collect(); // sorted via BTreeSet
            let labels = if options.with_labels {
                build_mrange_grouped_labels(
                    &group_options.group_label,
                    &label,
                    group_options.aggregation.aggregation_name(),
                    &sources,
                )
            } else {
                Vec::new()
            };

            MRangeSeriesResult {
                key: format!("{}={}", group_options.group_label, label),
                group_label_value: Some(label),
                labels,
                data: TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(samples)),
            }
        })
        .collect())
}

fn construct_group_map(series: Vec<MRangeSeriesResult>) -> BTreeMap<String, GroupData> {
    let mut grouped = BTreeMap::new();
    for meta in series {
        if let Some(label) = meta.group_label_value.clone() {
            let entry = grouped.entry(label).or_insert_with(|| GroupData {
                keys: SmallVec::new(),
                series: Vec::new(),
            });
            entry.keys.push(meta.key.clone());
            entry.series.push(meta);
        }
    }
    for data in grouped.values_mut() {
        data.keys.sort();
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
    // Grouped replies report the group label, __reducer__ and __source__ only
    // under WITHLABELS; by default the label array is empty (matches the
    // standalone path, see group_series_by_label).
    let labels = if options.with_labels {
        build_mrange_grouped_labels(
            &group_options.group_label,
            &label,
            group_options.aggregation.aggregation_name(),
            &data.keys,
        )
    } else {
        Vec::new()
    };

    MRangeSeriesResult {
        key: format!("{}={}", group_options.group_label, label),
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
        samples.sort_by_key(|b| std::cmp::Reverse(b.timestamp));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregators::AggregationType;
    use crate::common::constants::SOURCE_KEY;
    use crate::series::chunks::UncompressedChunk;
    use crate::series::request_types::{
        AggregationOptions, AggregatorConfig, RangeGroupingOptions, RangeOptions,
    };

    fn samples(pairs: &[(i64, f64)]) -> Vec<Sample> {
        pairs
            .iter()
            .map(|&(timestamp, value)| Sample { timestamp, value })
            .collect()
    }

    fn series_result(key: &str, group: Option<&str>, data: Vec<Sample>) -> MRangeSeriesResult {
        MRangeSeriesResult {
            key: key.into(),
            group_label_value: group.map(String::from),
            labels: Vec::new(),
            data: TimeSeriesChunk::Uncompressed(UncompressedChunk::from_vec(data)),
        }
    }

    fn to_response(result: MRangeSeriesResult) -> SeriesRangeResponse {
        result.try_into().expect("serialize series result")
    }

    fn avg_aggregation(bucket_duration: u64) -> AggregationOptions {
        AggregationOptions {
            aggregation: AggregatorConfig::new(AggregationType::Avg, None).unwrap(),
            bucket_duration,
            timestamp_output: Default::default(),
            alignment: Default::default(),
            report_empty: false,
        }
    }

    fn mrange_options(start: i64, end: i64) -> MRangeOptions {
        MRangeOptions {
            range: RangeOptions::with_range(start, end).unwrap(),
            ..Default::default()
        }
    }

    fn result_samples(result: &MRangeSeriesResult) -> Vec<Sample> {
        result.data.iter().collect()
    }

    /// Simulate what a push-down shard returns: the per-series aggregation
    /// pipeline applied to the raw samples.
    fn shard_aggregate(raw: Vec<Sample>, options: &MRangeOptions) -> Vec<Sample> {
        create_sample_iterator_adapter(raw.into_iter(), &options.range, &None, false).collect()
    }

    /// Push-down equivalence: shard-side bucketing + coordinator post-processing
    /// with aggregation cleared must equal coordinator-side aggregation of the
    /// raw samples.
    #[test]
    fn test_pushdown_basic_equivalence() {
        let raw = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (120, 7.0), (250, 9.0)]);

        for (is_reverse, count) in [
            (false, None),
            (true, None),
            (true, Some(2)),
            (false, Some(1)),
        ] {
            let mut legacy_options = mrange_options(0, 1000);
            legacy_options.range.aggregation = Some(avg_aggregation(100));
            legacy_options.range.count = count;
            legacy_options.is_reverse = is_reverse;

            let legacy = handle_basic(
                vec![to_response(series_result("a", None, raw.clone()))],
                &legacy_options,
            )
            .unwrap();

            // Shard aggregates ascending with COUNT/reverse stripped, exactly
            // as get_local_response does under push-down.
            let mut shard_options = legacy_options.clone();
            shard_options.range.count = None;
            shard_options.is_reverse = false;
            let buckets = shard_aggregate(raw.clone(), &shard_options);

            // Coordinator with aggregation cleared (reply() under push-down)
            let mut coord_options = legacy_options.clone();
            coord_options.range.aggregation = None;
            let pushed = handle_basic(
                vec![to_response(series_result("a", None, buckets))],
                &coord_options,
            )
            .unwrap();

            assert_eq!(legacy.len(), 1);
            assert_eq!(pushed.len(), 1);
            assert_eq!(
                result_samples(&legacy[0]),
                result_samples(&pushed[0]),
                "reverse={is_reverse} count={count:?}"
            );
        }
    }

    /// Grouped push-down: the coordinator reduces per-series buckets column-wise
    /// per timestamp (the standalone "aggregate per series, then reduce" order).
    /// Also covers the GroupData.keys fix: group key and __source__ label.
    #[test]
    fn test_pushdown_grouped_reduce() {
        let make_responses = || {
            // Per-series buckets, as returned by push-down shards
            vec![
                to_response(series_result(
                    "a",
                    Some("us"),
                    samples(&[(0, 1.0), (100, 3.0)]),
                )),
                to_response(series_result(
                    "b",
                    Some("us"),
                    samples(&[(0, 5.0), (100, 1.0)]),
                )),
            ]
        };

        let mut options = mrange_options(0, 1000);
        options.with_labels = true;
        // aggregation already cleared by reply() under push-down
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Max, None).unwrap(),
            group_label: "region".into(),
        });

        let results = handle_grouping(make_responses(), &options).unwrap();

        assert_eq!(results.len(), 1);
        let group = &results[0];
        assert_eq!(group.key, "region=us");
        assert_eq!(group.group_label_value.as_deref(), Some("us"));
        assert_eq!(
            result_samples(group),
            samples(&[(0, 5.0), (100, 3.0)]),
            "max of per-series buckets per timestamp"
        );

        let source = group
            .labels
            .iter()
            .find(|l| l.name == SOURCE_KEY)
            .expect("__source__ label present");
        assert_eq!(source.value, "a,b");

        // Without WITHLABELS the label array must be empty
        options.with_labels = false;
        let results = handle_grouping(make_responses(), &options).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].labels.is_empty());
        assert_eq!(results[0].key, "region=us");
    }

    /// The request flag mirrors the latched push-down decision.
    #[test]
    fn test_generate_request_flag() {
        let mut options = mrange_options(0, 1000);
        options.range.aggregation = Some(avg_aggregation(100));

        let mut command = MRangeFanoutCommand::new(options.clone());
        command.pushdown = true;
        assert!(command.generate_request().apply_aggregation);

        command.pushdown = false;
        assert!(!command.generate_request().apply_aggregation);

        // No aggregation in the query => never pushed down, regardless of config
        options.range.aggregation = None;
        let command = MRangeFanoutCommand::new(options);
        assert!(!command.pushdown);
        assert!(!command.generate_request().apply_aggregation);
    }

    /// Fallback matrix: group-reduce push-down is latched only for
    /// decomposable reducers; others fall back to legacy transport.
    #[test]
    fn test_group_reduce_latch_and_request_flag() {
        let grouping_with = |ty: AggregationType| {
            Some(RangeGroupingOptions {
                aggregation: AggregatorConfig::new(ty, None).unwrap(),
                group_label: "region".into(),
            })
        };

        // Decomposable reducer, no AGGREGATION: Phase 2 over raw timestamps
        let mut options = mrange_options(0, 1000);
        options.grouping = grouping_with(AggregationType::Avg);
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown_group);
        assert!(command.generate_request().apply_group_reduce);

        // Decomposable reducer + AGGREGATION: both flags set
        options.range.aggregation = Some(avg_aggregation(100));
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(command.pushdown_group);
        let request = command.generate_request();
        assert!(request.apply_group_reduce);
        assert!(request.apply_aggregation);

        // Non-decomposable reducer: falls back to per-series buckets (Phase 1)
        options.grouping = grouping_with(AggregationType::Increase);
        let command = MRangeFanoutCommand::new(options.clone());
        assert!(!command.pushdown_group);
        assert!(command.pushdown, "Phase 1 still applies");
        let request = command.generate_request();
        assert!(!request.apply_group_reduce);
        assert!(request.apply_aggregation);

        // No grouping: never group-pushed
        options.grouping = None;
        let command = MRangeFanoutCommand::new(options);
        assert!(!command.pushdown_group);
    }

    /// Coordinator merge/finalize of per-(group, shard) partial states:
    /// states with equal bucket timestamps merge across shards, sources are
    /// the sorted union, reversal and COUNT apply to the finalized buckets.
    #[test]
    fn test_handle_group_partials() {
        use crate::aggregators::PartialSampleReducer;

        let mut options = mrange_options(0, 1000);
        options.with_labels = true;
        options.grouping = Some(RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        });
        let reducer =
            PartialReducer::for_config(&options.grouping.as_ref().unwrap().aggregation).unwrap();

        // Simulate two shards computing partials over their local group members
        let shard_partial = |keys: &[&str], shard_samples: Vec<Sample>| {
            let (bucket_timestamps, states): (Vec<i64>, Vec<PartialState>) =
                PartialSampleReducer::new(shard_samples.into_iter(), reducer.clone()).unzip();
            GroupPartialSeries {
                group_label_value: "us".into(),
                source_keys: keys.iter().map(|s| s.to_string()).collect(),
                bucket_timestamps,
                states: states.into_iter().map(Into::into).collect(),
            }
        };

        // Shard 1 holds series a; shard 2 holds series b and c (merged locally)
        let shard1 = shard_partial(&["a"], samples(&[(0, 1.0), (100, 3.0)]));
        let shard2 = shard_partial(&["b", "c"], samples(&[(0, 4.0), (0, 5.0), (200, 7.0)]));

        let results =
            handle_group_partials(vec![shard1.clone(), shard2.clone()], &options).unwrap();

        assert_eq!(results.len(), 1);
        let group = &results[0];
        assert_eq!(group.key, "region=us");
        assert_eq!(
            result_samples(group),
            samples(&[(0, 10.0), (100, 3.0), (200, 7.0)]),
            "sum merged across shards per bucket"
        );
        let source = group
            .labels
            .iter()
            .find(|l| l.name == SOURCE_KEY)
            .expect("__source__ label present");
        assert_eq!(source.value, "a,b,c");

        // Reverse + COUNT apply to the finalized buckets
        options.is_reverse = true;
        options.range.count = Some(2);
        options.with_labels = false;
        let results = handle_group_partials(vec![shard1, shard2], &options).unwrap();
        assert_eq!(
            result_samples(&results[0]),
            samples(&[(200, 7.0), (100, 3.0)])
        );
        assert!(results[0].labels.is_empty());
    }

    fn limit_for(is_reverse: bool, count: usize) -> SampleLimit {
        if is_reverse {
            SampleLimit::Tail(count)
        } else {
            SampleLimit::Head(count)
        }
    }

    /// COUNT push-down equivalence: shard-side head/tail truncation plus the
    /// coordinator's authoritative COUNT equals the untruncated flow, for raw
    /// and aggregated transport, both directions, count below/at/above the
    /// stream length.
    #[test]
    fn test_count_pushdown_equivalence() {
        let raw = samples(&[(0, 1.0), (10, 3.0), (110, 5.0), (120, 7.0), (250, 9.0)]);

        for aggregated in [false, true] {
            for is_reverse in [false, true] {
                for count in [1usize, 2, 10] {
                    let mut coord_options = mrange_options(0, 1000);
                    coord_options.range.count = Some(count);
                    coord_options.is_reverse = is_reverse;

                    // The stream a shard would ship without COUNT push-down
                    // (raw samples, or buckets under aggregation push-down)
                    let shard_stream = if aggregated {
                        let mut shard_options = mrange_options(0, 1000);
                        shard_options.range.aggregation = Some(avg_aggregation(100));
                        shard_aggregate(raw.clone(), &shard_options)
                    } else {
                        raw.clone()
                    };

                    let reference = handle_basic(
                        vec![to_response(series_result("a", None, shard_stream.clone()))],
                        &coord_options,
                    )
                    .unwrap();

                    // COUNT push-down: shard truncates head/tail before
                    // shipping; the coordinator re-applies COUNT unchanged.
                    let truncated: Vec<Sample> = limit_for(is_reverse, count)
                        .apply(shard_stream.into_iter())
                        .collect();
                    let pushed = handle_basic(
                        vec![to_response(series_result("a", None, truncated))],
                        &coord_options,
                    )
                    .unwrap();

                    assert_eq!(
                        result_samples(&reference[0]),
                        result_samples(&pushed[0]),
                        "aggregated={aggregated} reverse={is_reverse} count={count}"
                    );
                }
            }
        }
    }

    /// Grouped subset property: with timestamps staggered across shards (the
    /// merged k-th timestamp differs from either shard's k-th), per-shard
    /// truncation to COUNT rows must not change the per-group result. Covers
    /// both grouped transports: per-series buckets and group partials.
    #[test]
    fn test_count_pushdown_grouped_subset() {
        // Staggered: merged timestamps {0, 5, 10, 15, 20}
        let series_a = samples(&[(0, 1.0), (10, 2.0), (20, 3.0)]); // shard 1
        let series_b = samples(&[(5, 4.0), (10, 5.0), (15, 6.0)]); // shard 2

        let grouping = RangeGroupingOptions {
            aggregation: AggregatorConfig::new(AggregationType::Sum, None).unwrap(),
            group_label: "region".into(),
        };

        for is_reverse in [false, true] {
            for count in [1usize, 2, 3, 10] {
                let mut options = mrange_options(0, 1000);
                options.grouping = Some(grouping.clone());
                options.range.count = Some(count);
                options.is_reverse = is_reverse;
                let limit = limit_for(is_reverse, count);

                // --- Per-series transport (Phase 1 grouped / legacy) ---
                let full = |key: &str, s: &[Sample]| {
                    to_response(series_result(key, Some("us"), s.to_vec()))
                };
                let cut = |key: &str, s: &[Sample]| {
                    to_response(series_result(
                        key,
                        Some("us"),
                        limit.apply(s.iter().copied()).collect(),
                    ))
                };

                let reference =
                    handle_grouping(vec![full("a", &series_a), full("b", &series_b)], &options)
                        .unwrap();
                let pushed =
                    handle_grouping(vec![cut("a", &series_a), cut("b", &series_b)], &options)
                        .unwrap();
                assert_eq!(
                    result_samples(&reference[0]),
                    result_samples(&pushed[0]),
                    "per-series transport reverse={is_reverse} count={count}"
                );

                // --- Group-partials transport (Phase 2) ---
                let reducer = PartialReducer::for_config(&grouping.aggregation).unwrap();
                let partial = |keys: &[&str], s: &[Sample], limit: Option<SampleLimit>| {
                    let rows = crate::aggregators::PartialSampleReducer::new(
                        s.iter().copied(),
                        reducer.clone(),
                    );
                    let (bucket_timestamps, states): (Vec<i64>, Vec<PartialState>) = match limit {
                        Some(limit) => limit.apply(rows).unzip(),
                        None => rows.unzip(),
                    };
                    GroupPartialSeries {
                        group_label_value: "us".into(),
                        source_keys: keys.iter().map(|s| s.to_string()).collect(),
                        bucket_timestamps,
                        states: states.into_iter().map(Into::into).collect(),
                    }
                };

                let reference = handle_group_partials(
                    vec![
                        partial(&["a"], &series_a, None),
                        partial(&["b"], &series_b, None),
                    ],
                    &options,
                )
                .unwrap();
                let pushed = handle_group_partials(
                    vec![
                        partial(&["a"], &series_a, Some(limit)),
                        partial(&["b"], &series_b, Some(limit)),
                    ],
                    &options,
                )
                .unwrap();
                assert_eq!(
                    result_samples(&reference[0]),
                    result_samples(&pushed[0]),
                    "partials transport reverse={is_reverse} count={count}"
                );
            }
        }
    }

    /// COUNT push-down latch: flag mirrors config && COUNT presence.
    #[test]
    fn test_count_pushdown_latch() {
        let mut options = mrange_options(0, 1000);
        assert!(!MRangeFanoutCommand::new(options.clone()).pushdown_count);
        assert!(
            !MRangeFanoutCommand::new(options.clone())
                .generate_request()
                .apply_count
        );

        options.range.count = Some(5);
        let command = MRangeFanoutCommand::new(options);
        assert!(command.pushdown_count);
        assert!(command.generate_request().apply_count);
    }
}
