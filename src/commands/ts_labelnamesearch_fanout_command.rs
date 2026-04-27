use super::fanout::generated::{LabelNameSearchRequest, LabelNameSearchResponse};
use crate::commands::fanout::filters::{deserialize_matchers_list, serialize_matchers_list};
use crate::commands::ts_labelnamesearch::{
    LabelNameSearchArgs, apply_label_name_search_ranking, process_label_name_search_request,
};
use crate::common::SortDir;
use crate::fanout::{FanoutClientCommand, FanoutCommandResult, FanoutContext, NodeInfo};
use crate::series::index::{FuzzyAlgorithm, SearchResult};
use crate::series::request_types::MatchFilterOptions;
use ahash::AHashSet;
use valkey_module::{Context, Status, ValkeyError, ValkeyResult};

#[derive(Default)]
pub struct LabelNameSearchFanoutCommand {
    pub args: LabelNameSearchArgs,
    names: AHashSet<String>,
}

impl LabelNameSearchFanoutCommand {
    pub fn new(args: LabelNameSearchArgs) -> Self {
        Self {
            args,
            names: AHashSet::new(),
        }
    }
}

impl FanoutClientCommand for LabelNameSearchFanoutCommand {
    type Request = LabelNameSearchRequest;
    type Response = LabelNameSearchResponse;

    fn name() -> &'static str {
        "label_name_search"
    }

    fn get_local_response(
        ctx: &Context,
        req: LabelNameSearchRequest,
    ) -> ValkeyResult<LabelNameSearchResponse> {
        if req.fuzz_threshold > 1.0 {
            return Err(ValkeyError::Str("TSDB: FUZZ_THRESHOLD must be in [0.0..1.0]"));
        }

        let matchers = deserialize_matchers_list(Some(req.filters))?;
        let fuzz_algorithm =
            FuzzyAlgorithm::try_from(req.fuzz_alg.as_str()).map_err(ValkeyError::String)?;

        let parsed = LabelNameSearchArgs {
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

        process_label_name_search_request(ctx, &parsed)
            .map(|results| LabelNameSearchResponse {
                names: results.into_iter().map(|r| r.value).collect(),
            })
    }

    fn generate_request(&self) -> LabelNameSearchRequest {
        let filters = serialize_matchers_list(self.args.metadata.matchers.as_ref())
            .expect("serialize matchers list");

        LabelNameSearchRequest {
            search_terms: self.args.search_terms.clone(),
            fuzz_threshold: self.args.fuzz_threshold,
            fuzz_alg: self.args.fuzz_algorithm.to_string(),
            ignore_case: self.args.ignore_case,
            range: self.args.metadata.date_range.map(Into::into),
            filters,
        }
    }

    fn on_response(&mut self, resp: Self::Response, _target: &NodeInfo) -> FanoutCommandResult {
        for value in resp.names {
            self.names.insert(value);
        }
        Ok(())
    }

    fn reply(&mut self, ctx: &FanoutContext) -> Status {
        let values = std::mem::take(&mut self.names)
            .into_iter()
            .collect::<Vec<_>>();
        let ranked: Vec<SearchResult> = match apply_label_name_search_ranking(values, &self.args) {
            Ok(results) => results,
            Err(err) => return ctx.reply(Err(err)),
        };

        if self.args.include_score {
            ctx.reply_with_array(ranked.len());
            for r in ranked {
                ctx.reply_with_array(2);
                ctx.reply_with_bulk_string(&r.value);
                ctx.reply_with_bulk_string(&r.score.to_string());
            }
        } else {
            ctx.reply_with_array(ranked.len());
            for r in ranked {
                ctx.reply_with_bulk_string(&r.value);
            }
        }

        Status::Ok
    }
}
