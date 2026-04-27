use super::fanout::generated::{LabelSearchRequest, LabelSearchResponse};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::fanout::LabelSearchResult;
use crate::commands::label_search_utils::{process_label_search_request, LabelNameSearchArgs};
use crate::common::SortDir;
use crate::fanout::{FanoutClientCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::series::index::{merge_search_results, FuzzyAlgorithm, SearchResult, SearchResultOrdering};
use crate::series::request_types::MatchFilterOptions;
use simd_json::prelude::ArrayTrait;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

#[derive(Default)]
pub struct LabelSearchFanoutCommand {
    pub args: LabelNameSearchArgs,
    ordering: SearchResultOrdering,
    limit: usize,
    results: Vec<SearchResult>,
}

impl LabelSearchFanoutCommand {
    pub fn new(args: LabelNameSearchArgs) -> ValkeyResult<Self> {
        let ordering = args.get_order_by();
        let limit = args.get_limit();
        Ok(Self {
            args,
            ordering,
            limit,
            results: Vec::new(),
        })
    }
}

impl FanoutClientCommand for LabelSearchFanoutCommand {
    type Request = LabelSearchRequest;
    type Response = LabelSearchResponse;

    fn name() -> &'static str {
        "cmd:label_search"
    }

    fn get_local_response(
        ctx: &Context,
        req: LabelSearchRequest,
    ) -> ValkeyResult<LabelSearchResponse> {
        if req.fuzz_threshold > 1.0 {
            return Err(ValkeyError::Str("TSDB: FUZZ_THRESHOLD must be in [0.0..1.0]"));
        }

        let matchers = deserialize_matchers_list(Some(req.filters))?;
        let fuzz_algorithm =
            FuzzyAlgorithm::try_from(req.fuzz_alg.as_str()).map_err(ValkeyError::String)?;

        let label = if req.label.is_empty() {
            None
        } else {
            Some(req.label)
        };

        let parsed = LabelNameSearchArgs {
            search_type: Default::default(),
            label,
            search_terms: req.search_terms,
            fuzz_threshold: req.fuzz_threshold,
            fuzz_algorithm,
            ignore_case: req.ignore_case,
            // include_score is a coordinator-only concern; shards always return plain values.
            include_score: false,
            sort_by: None,
            sort_dir: SortDir::Asc,
            metadata: MatchFilterOptions {
                matchers,
                date_range: req.range.map(Into::into),
                // Do not apply per-node limit; coordinator applies the final limit.
                limit: Some(0),
            },
        };

        process_label_search_request(ctx, &parsed)
            .map(|results| LabelSearchResponse {
                results: results.into_iter().map(|r| LabelSearchResult {
                    value: r.value,
                    score: r.score,
                }).collect(),
            })
    }

    fn generate_request(&self) -> LabelSearchRequest {
        let filters = serialize_matchers_list(self.args.metadata.matchers.as_ref())
            .expect("serialize matchers list");

        let label = self.args.label.as_ref().map_or(String::new(), |x| x.clone());
        LabelSearchRequest {
            request_type: self.args.search_type.into(),
            label,
            search_terms: self.args.search_terms.clone(),
            fuzz_threshold: self.args.fuzz_threshold,
            fuzz_alg: self.args.fuzz_algorithm.to_string(),
            ignore_case: self.args.ignore_case,
            range: self.args.metadata.date_range.map(Into::into),
            filters,
            include_metadata: false,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        // Score and filter the incoming names using the configured ranking args.
        let scored = resp.results
            .into_iter()
            .map(|r| SearchResult {
                value: r.value,
                score: r.score,
            });

        if self.results.is_empty() {
            self.results.extend(scored);
        } else {
            let temp = std::mem::take(&mut self.results);
            self.results = merge_search_results(temp.into_iter(), scored, self.ordering).collect();
        }

        // Bound the collected set to args.limit after each shard response so we
        // never accumulate more entries than the final answer requires.
        if self.limit > 0 && self.results.len() > self.limit {
            self.results.truncate(self.limit);
        }

        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let limit = self.limit;
        if limit > 0 && self.results.len() > limit {
            self.results.truncate(limit);
        }

        if self.args.include_score {
            ctx.reply_with_array(self.results.len());
            for r in self.results.iter() {
                ctx.reply_with_array(2);
                ctx.reply_with_bulk_string(&r.value);
                ctx.reply_with_bulk_string(&r.score.to_string());
            }
        } else {
            ctx.reply_with_array(self.results.len());
            for r in self.results.iter() {
                ctx.reply_with_bulk_string(&r.value);
            }
        }

        Status::Ok
    }
}
