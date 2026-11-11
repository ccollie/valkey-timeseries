use crate::promql::EvalLabels;
use crate::promql::engine::QueryReader;
use crate::promql::engine::label_profile::{LabelProfile, ProfileWindow};
#[cfg(test)]
use crate::promql::engine::memory_series_querier::MemorySeriesQuerier;
use crate::promql::engine::query_reader::{
    AggregationOutcome, AggregationRequest, GridOutcome, GridRequest, raw_grid_read,
};
use crate::promql::engine::selector_batch_executor::SelectorBatchExecutor;
use crate::promql::{InstantSample, QueryOptions, QueryResult, RangeSample};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::VectorSelector;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use valkey_module::Context;

pub static SERIES_SELECTOR: LazyLock<SelectorBatchExecutor> =
    LazyLock::new(SelectorBatchExecutor::new);

/// A concrete implementation of QueryReader that queries the Valkey keyspace.
pub struct ValkeySeriesQuerier {
    /// Identity captured while handling the client command. Selector work runs
    /// later, often on another thread, where Valkey's context no longer carries
    /// the original client user. Shared: every selector read carries it.
    caller_user: Option<Arc<str>>,
    /// Cluster routing scope requested with `HASHTAG`, empty when the command
    /// did not supply one. One expression can contain many selectors and can
    /// retry a push-down as an ordinary selector read, and every one of those
    /// reads must use the same scope, so it is held here — on the reader that
    /// serves the whole command — rather than passed per call. `Arc<[String]>`
    /// so the selectors share the immutable list instead of cloning it.
    hash_tags: Arc<[String]>,
    /// Whether the server runs in cluster mode, read once on the command
    /// thread: the cluster-only pushdown toggles apply to a fanout, not to the
    /// same request answered locally.
    clustered: bool,
    /// Each selector's normalized matchers, built on its first read and shared
    /// by the rest: a range query's per-step reads ask for the same selector
    /// at every step.
    normalized: NormalizedSelectors,
}

/// The normalized matchers of the selectors one command has read, by content.
///
/// By content, not by node address: the instant push-down reads selectors it
/// built for one evaluation and then dropped, and an address can come back as
/// another selector. Linear and capped: a query names a handful of selectors,
/// and past the cap a read normalizes its selector as if there were no cache.
#[derive(Default)]
struct NormalizedSelectors {
    entries: Mutex<Vec<NormalizedSelector>>,
}

/// One selector, as written, and its normalized matchers.
struct NormalizedSelector {
    name: Option<String>,
    matchers: Matchers,
    normalized: Arc<Matchers>,
}

impl NormalizedSelectors {
    /// How many selectors one command keeps normalized.
    const CAPACITY: usize = 32;

    fn get(&self, selector: &VectorSelector) -> Arc<Matchers> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = entries
            .iter()
            .find(|e| e.name == selector.name && e.matchers == selector.matchers)
        {
            return Arc::clone(&entry.normalized);
        }
        let normalized = Arc::new(normalize_selector(selector));
        if entries.len() < Self::CAPACITY {
            entries.push(NormalizedSelector {
                name: selector.name.clone(),
                matchers: selector.matchers.clone(),
                normalized: Arc::clone(&normalized),
            });
        }
        normalized
    }
}

impl QueryReader for ValkeySeriesQuerier {
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<InstantSample<EvalLabels>>> {
        let matchers = self.normalized.get(selector);
        SERIES_SELECTOR.query(
            matchers,
            timestamp,
            options,
            self.caller_user.clone(),
            self.hash_tags.clone(),
        )
    }

    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
        let matchers = self.normalized.get(selector);
        SERIES_SELECTOR.query_range(
            matchers,
            start_ms,
            end_ms,
            options,
            self.caller_user.clone(),
            self.hash_tags.clone(),
        )
    }

    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> QueryResult<AggregationOutcome> {
        // Only operators with a decomposable form can be evaluated at the
        // shards; the rest need the whole input on one node.
        if aggregation.kind.pushdown_strategy().is_none() {
            return Ok(AggregationOutcome::Unsupported);
        }
        let matchers = self.normalized.get(selector);
        SERIES_SELECTOR.query_aggregation(
            matchers,
            timestamp,
            aggregation.clone(),
            options,
            self.caller_user.clone(),
            self.hash_tags.clone(),
        )
    }

    fn query_grid(
        &self,
        selector: &VectorSelector,
        request: &GridRequest,
        options: QueryOptions,
    ) -> QueryResult<GridOutcome> {
        // The toggle governs shipping the grid to *shards*: off, the raw span
        // comes back through the ordinary range fanout and the caller evaluates
        // it with the kernels a shard would have used — the same answer, at the
        // transfer the push-down saves. On a single node the request is
        // answered locally (`GridOutcome::Raw`) either way, which is also the
        // path the in-memory querier gives the PromQL conformance suite.
        if self.clustered && !crate::config::is_fanout_rollup_pushdown_enabled() {
            return raw_grid_read(self, selector, request, options);
        }
        let matchers = self.normalized.get(selector);
        SERIES_SELECTOR.query_grid(
            matchers,
            request.clone(),
            options,
            self.caller_user.clone(),
            self.hash_tags.clone(),
        )
    }

    /// A single node, or a cluster with grid push-down switched off, answers
    /// every grid request with the raw span.
    fn answers_grids_raw(&self) -> bool {
        !(self.clustered && crate::config::is_fanout_rollup_pushdown_enabled())
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        window: Option<ProfileWindow>,
        options: QueryOptions,
    ) -> QueryResult<Option<LabelProfile>> {
        let matchers = self.normalized.get(selector);
        SERIES_SELECTOR.label_profile(
            matchers,
            window,
            options,
            self.caller_user.clone(),
            self.hash_tags.clone(),
        )
    }
}

/// Normalize a parsed [`VectorSelector`] into the [`Matchers`] representation consumed by
/// the selector executor, folding the optional metric name into an explicit `__name__`
/// equality matcher.
///
/// This is the single point at which the query path performs this transformation: the
/// resulting `Matchers` is the shared normalized form that both the local execution path
/// (`Matchers` -> `SeriesSelector`) and the fanout path (`Matchers` -> proto request) build
/// from, so the two stay in sync. `Matchers` is used as the intermediate — rather than the
/// crate-native `SeriesSelector` — because it preserves the original regex source strings
/// needed for lossless fanout serialization.
fn normalize_selector(selector: &VectorSelector) -> Matchers {
    let Some(ref name) = selector.name else {
        return selector.matchers.clone();
    };

    let name_matcher = Matcher {
        op: MatchOp::Equal,
        name: METRIC_NAME.to_string(),
        value: name.clone(),
    };

    let src = &selector.matchers;
    if src.or_matchers.is_empty() {
        // Build in a single pass with the name matcher up front to avoid the extra
        // reallocation a clone-then-push incurs.
        let mut matchers = Vec::with_capacity(src.matchers.len() + 1);
        matchers.push(name_matcher);
        matchers.extend(src.matchers.iter().cloned());
        Matchers {
            matchers,
            or_matchers: Vec::new(),
        }
    } else {
        // A vector selector produced by the parser never carries top-level OR groups, but
        // handle it defensively so the metric name is folded into every group rather than
        // silently dropped by the downstream `matchers`-first conversion.
        let or_matchers = src
            .or_matchers
            .iter()
            .map(|group| {
                let mut g = Vec::with_capacity(group.len() + 1);
                g.push(name_matcher.clone());
                g.extend(group.iter().cloned());
                g
            })
            .collect();
        Matchers {
            matchers: src.matchers.clone(),
            or_matchers,
        }
    }
}

/// The reader that serves one PromQL command, scoped to the shards owning
/// `hash_tags` for a command that parsed a `HASHTAG` clause; an empty list
/// reads the whole keyspace the caller can see. Unit tests have no server, so
/// they get the in-memory querier.
pub(crate) fn series_querier(_ctx: &Context, _hash_tags: Arc<[String]>) -> Arc<dyn QueryReader> {
    cfg_select! {
        test => { Arc::new(MemorySeriesQuerier::new()) }
        _ => {
            let user = _ctx.get_current_user().to_string();
            Arc::new(ValkeySeriesQuerier {
                caller_user: (!user.is_empty()).then(|| Arc::from(user)),
                hash_tags: _hash_tags,
                clustered: crate::fanout::is_clustered(_ctx),
                normalized: NormalizedSelectors::default(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NormalizedSelectors, normalize_selector};
    use promql_parser::parser::{Expr, VectorSelector, parse};
    use std::sync::Arc;

    fn selector(q: &str) -> VectorSelector {
        match parse(q).unwrap() {
            Expr::VectorSelector(vs) => vs,
            other => panic!("not a selector: {other}"),
        }
    }

    #[test]
    fn a_selector_read_again_shares_its_normalized_matchers() {
        let cache = NormalizedSelectors::default();
        let vs = selector(r#"up{job="api"}"#);
        let first = cache.get(&vs);
        assert_eq!(*first, normalize_selector(&vs));
        // A copy is the same selector: equal content, wherever it lives.
        assert!(Arc::ptr_eq(&first, &cache.get(&vs.clone())));
    }

    #[test]
    fn selectors_differing_in_name_or_matchers_are_kept_apart() {
        let cache = NormalizedSelectors::default();
        for q in [
            r#"up{job="api"}"#,
            r#"down{job="api"}"#,
            r#"up{job="web"}"#,
            r#"up{job=~"api"}"#,
            r#"{__name__="up", job="api"}"#,
        ] {
            let vs = selector(q);
            assert_eq!(*cache.get(&vs), normalize_selector(&vs), "{q}");
        }
    }

    #[test]
    fn past_the_capacity_a_selector_is_normalized_uncached() {
        let cache = NormalizedSelectors::default();
        for i in 0..NormalizedSelectors::CAPACITY {
            cache.get(&selector(&format!("m{i}")));
        }
        let vs = selector("overflow");
        let (a, b) = (cache.get(&vs), cache.get(&vs));
        assert_eq!(*a, normalize_selector(&vs));
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
