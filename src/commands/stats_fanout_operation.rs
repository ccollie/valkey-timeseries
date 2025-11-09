use super::fanout::generated::{PostingStat as MPostingStat, StatsRequest, StatsResponse};
use crate::commands::{DEFAULT_STATS_RESULTS_LIMIT, exec_fanout_request_base};
use crate::fanout::{FanoutOperation, NodeInfo};
use crate::series::index::{PostingStat, PostingsStats, StatsMaxHeap, with_timeseries_index};
use ahash::AHashMap;
use valkey_module::{Context, ValkeyResult, ValkeyValue};

#[derive(Default)]
struct StatsResults {
    num_label_pairs: usize,
    num_labels: usize,
    series_count: usize,
    cardinality_metrics_map: AHashMap<String, PostingStat>,
    cardinality_labels_map: AHashMap<String, PostingStat>,
    label_values_map: AHashMap<String, PostingStat>,
    pairs_map: AHashMap<String, PostingStat>,
}

pub struct StatsFanoutOperation {
    pub limit: usize,
    state: StatsResults,
}

impl StatsFanoutOperation {
    pub fn new(limit: usize) -> Self {
        let limit = if limit == 0 {
            DEFAULT_STATS_RESULTS_LIMIT
        } else {
            limit
        };

        StatsFanoutOperation {
            limit,
            ..Default::default()
        }
    }
}

impl Default for StatsFanoutOperation {
    fn default() -> Self {
        Self::new(DEFAULT_STATS_RESULTS_LIMIT)
    }
}

impl FanoutOperation for StatsFanoutOperation {
    type Request = StatsRequest;
    type Response = StatsResponse;

    fn name() -> &'static str {
        "stats"
    }

    fn get_local_response(ctx: &Context, req: StatsRequest) -> ValkeyResult<StatsResponse> {
        let limit = req.limit as usize;
        let stats = with_timeseries_index(ctx, |index| index.stats("", limit));

        Ok(stats.into())
    }

    fn generate_request(&mut self) -> StatsRequest {
        StatsRequest {
            limit: self.limit as u32,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) {
        // Handle the response from a remote target
        self.state.num_label_pairs += resp.num_label_pairs as usize;
        self.state.num_labels += resp.num_labels as usize;
        self.state.series_count += resp.series_count as usize;

        collate_stats_values(
            &mut self.state.cardinality_metrics_map,
            &resp.cardinality_metric_stats,
        );
        collate_stats_values(
            &mut self.state.cardinality_labels_map,
            &resp.cardinality_label_stats,
        );
        collate_stats_values(&mut self.state.label_values_map, &resp.label_value_stats);
        collate_stats_values(&mut self.state.pairs_map, &resp.label_value_pairs_stats);
    }

    fn generate_reply(&mut self, ctx: &Context) {
        let limit = self.limit;
        let state = std::mem::take(&mut self.state);

        let result = PostingsStats {
            series_count: state.series_count as u64,
            num_labels: state.num_labels,
            num_label_pairs: state.num_label_pairs,
            cardinality_metrics_stats: collect_map_values(state.cardinality_metrics_map, limit),
            cardinality_label_stats: collect_map_values(state.cardinality_labels_map, limit),
            label_value_stats: collect_map_values(state.label_values_map, limit),
            label_value_pairs_stats: collect_map_values(state.pairs_map, limit),
        };

        let result: ValkeyValue = result.into();
        ctx.reply(Ok(result));
    }
}

fn collect_map_values(map: AHashMap<String, PostingStat>, limit: usize) -> Vec<PostingStat> {
    let mut map = map;
    let mut heap = StatsMaxHeap::new(limit);
    for stat in map.drain().map(|(_, v)| v) {
        heap.push(stat);
    }
    heap.into_vec()
}

fn collate_stats_values(map: &mut AHashMap<String, PostingStat>, values: &[MPostingStat]) {
    for result in values.iter() {
        if let Some(stat) = map.get_mut(result.name.as_str()) {
            stat.count += result.count;
        } else {
            map.insert(
                result.name.clone(),
                PostingStat {
                    name: result.name.clone(),
                    count: result.count,
                },
            );
        }
    }
}

pub(super) fn exec_stats_fanout_request(ctx: &Context, limit: usize) -> ValkeyResult<ValkeyValue> {
    let operation = StatsFanoutOperation::new(limit);
    exec_fanout_request_base(ctx, operation)
}
