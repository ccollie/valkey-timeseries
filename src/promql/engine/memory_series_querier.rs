use crate::common::Sample;
use crate::common::hash::IntMap;
use crate::common::threads::IterIntoParRayon;
use crate::labels::filters::SeriesSelector;
use crate::labels::{Labels, SeriesFingerprint};
use crate::promql::EvalLabels;
use crate::promql::engine::QueryReader;
use crate::promql::engine::fanout::check_unique_series;
use crate::promql::engine::label_profile::{
    LabelProfile, LabelProfileBuilder, ProfileCollector, ProfileWindow, profiled_series_cap,
};
use crate::promql::engine::query_limits::{
    copy_snapshots, decode_snapshot, instant_lookback_start_ms, validate_max_series,
};
use crate::promql::engine::query_reader::{AggregationOutcome, AggregationRequest};
use crate::promql::engine::sample_budget::SampleBudget;
use crate::promql::model::InstantSample;
use crate::promql::time::duration_ms;
use crate::promql::{QueryError, QueryOptions, QueryResult, RangeSample};
use crate::series::index::{Postings, PostingsBitmap};
use crate::series::{SeriesRef, TimeSeries};
use ahash::AHashMap;
use orx_parallel::Par;
use orx_parallel::ParResult;
use promql_parser::parser::VectorSelector;
use std::borrow::Cow;
use std::sync::RwLock;
use valkey_module::ValkeyResult;

struct QuerierInner {
    series: IntMap<SeriesRef, TimeSeries>,
    postings: Postings,
    next_id: SeriesRef,
    fingerprint_to_id: AHashMap<SeriesFingerprint, SeriesRef>,
}

impl QuerierInner {
    /// The series `selector` matches. A postings error is the read's error:
    /// iterating the `Result` instead made it read as a selector that matched
    /// nothing.
    fn postings_for(&self, selector: &VectorSelector) -> QueryResult<Cow<'_, PostingsBitmap>> {
        self.postings
            .postings_for_selectors(&[SeriesSelector::from(selector)])
            .map_err(|e| QueryError::Execution(e.to_string()))
    }
}

impl Default for QuerierInner {
    fn default() -> Self {
        QuerierInner {
            series: IntMap::default(),
            postings: Postings::default(),
            next_id: 1, // start IDs at 1 to avoid reserved 0
            fingerprint_to_id: AHashMap::default(),
        }
    }
}

/// A mock QueryReader for testing that holds data in memory.
/// Build one with [`MemorySeriesQuerier::new`] and [`MemorySeriesQuerier::add_sample`].
pub struct MemorySeriesQuerier {
    inner: RwLock<QuerierInner>,
}

impl Default for MemorySeriesQuerier {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySeriesQuerier {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(QuerierInner::default()),
        }
    }

    /// Add a sample with labels. If a series with the same labels already exists globally,
    /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
    pub fn add_sample(&self, labels: &Labels, sample: Sample) {
        let mut inner = self.inner.write().unwrap();
        let fingerprint = labels.get_fingerprint();

        let id = if let Some(&existing) = inner.fingerprint_to_id.get(&fingerprint) {
            existing
        } else {
            let id = inner.next_id;
            inner.next_id += 1;

            let ts = TimeSeries {
                id,
                labels: labels.as_ref().into(),
                ..Default::default()
            };

            inner.series.insert(id, ts);
            inner.fingerprint_to_id.insert(fingerprint, id);
            id
        };

        // To avoid simultaneous mutable borrows of different fields of
        // `inner`, take the TimeSeries out of the map, modify it, update the
        // postings, then put it back.
        if let Some(mut ts) = inner.series.remove(&id) {
            // update the timeseries
            // Last write wins, as when a promqltest `load` lists one series
            // twice: the later line's value is the one Prometheus keeps.
            ts.add(
                sample.timestamp,
                sample.value,
                Some(crate::series::DuplicatePolicy::KeepLast),
            );

            // update postings using the owned `ts` reference
            let key = format!("ts:{id}");
            inner.postings.index_timeseries(&ts, key.as_bytes());

            // put the timeseries back into the map
            inner.series.insert(id, ts);
        }
    }

    fn select_series<F, R>(&self, selector: &VectorSelector, f: F) -> QueryResult<Vec<R>>
    where
        F: Fn(&TimeSeries) -> ValkeyResult<Option<R>> + Send + Sync + Clone,
        R: Send + Sync,
    {
        let inner = self.inner.read().unwrap();
        let postings = inner.postings_for(selector)?;
        let series = postings
            .iter()
            .filter_map(|id| inner.series.get(&id))
            .iter_into_par_rayon()
            .map(&f)
            .into_fallible()
            .flat_map(|opt| opt)
            .collect();

        match series {
            Ok(series) => Ok(series),
            Err(e) => Err(QueryError::Execution(format!(
                "Error processing series: {:?}",
                e
            ))),
        }
    }
}

impl QueryReader for MemorySeriesQuerier {
    /// The latest sample in each series' lookback window, `(timestamp - lookback, timestamp]`,
    /// under the read limits a module read applies: the series limit on what is returned, and
    /// no two series with the same labels. The deadline is not checked.
    fn query(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<InstantSample<EvalLabels>>> {
        let start_inclusive =
            instant_lookback_start_ms(timestamp, duration_ms(options.lookback_delta));

        let samples = self.select_series(selector, |ts| {
            // Fetch samples in the inclusive range [start_inclusive, timestamp].
            let samples = ts.get_range(start_inclusive, timestamp);
            if samples.is_empty() {
                return Ok(None);
            }

            // `get_range` returns samples sorted ascending; take the last (latest)
            // sample that is <= timestamp and > (timestamp - lookback_delta).
            if let Some(s) = samples.last() {
                return Ok(Some(InstantSample {
                    labels: EvalLabels::interned(&ts.labels),
                    value: s.value,
                    timestamp_ms: s.timestamp,
                }));
            }

            Ok(None)
        })?;
        validate_max_series(samples.len(), options.max_series)?;
        check_unique_series(samples.iter().map(|s| &s.labels)).map_err(QueryError::Execution)?;
        Ok(samples)
    }

    /// Each series' samples in `[start_ms, end_ms]`, read as a module read is: chunks copied
    /// under the lock against the query's sample budget, decoded after it under the budget
    /// and the per-series point limit, then the series limit and the duplicate-labels check.
    /// The deadline is not checked.
    fn query_range(
        &self,
        selector: &VectorSelector,
        start_ms: i64,
        end_ms: i64,
        options: QueryOptions,
    ) -> QueryResult<Vec<RangeSample<EvalLabels>>> {
        let budget = SampleBudget::new(options.max_samples);
        let snapshots = {
            let inner = self.inner.read().unwrap();
            let postings = inner.postings_for(selector)?;
            let series = postings.iter().filter_map(|id| inner.series.get(&id));
            copy_snapshots(series, start_ms, end_ms, &budget, |ts| {
                EvalLabels::interned(&ts.labels)
            })?
        };
        let max_points = options.max_points_per_series;
        let series = snapshots
            .into_iter()
            .iter_into_par_rayon()
            .map(|(labels, snapshot)| {
                decode_snapshot(&budget, &snapshot, max_points)
                    .map(|samples| samples.map(|samples| RangeSample { labels, samples }))
            })
            .into_fallible()
            .filter_map(|series| series)
            .collect::<Vec<_>>()?;
        validate_max_series(series.len(), options.max_series)?;
        check_unique_series(series.iter().map(|s| &s.labels)).map_err(QueryError::Execution)?;
        Ok(series)
    }

    /// There is nothing to push down to in memory, but answering `Raw` (rather
    /// than leaving the default `Unsupported`) still routes the evaluator
    /// through the push-down path, so the whole PromQL test suite exercises it.
    fn query_aggregation(
        &self,
        selector: &VectorSelector,
        timestamp: i64,
        _aggregation: &AggregationRequest,
        options: QueryOptions,
    ) -> QueryResult<AggregationOutcome> {
        self.query(selector, timestamp, options)
            .map(AggregationOutcome::Raw)
    }

    fn label_profile(
        &self,
        selector: &VectorSelector,
        window: Option<ProfileWindow>,
        options: QueryOptions,
    ) -> QueryResult<Option<LabelProfile>> {
        let inner = self.inner.read().unwrap();
        let selector: SeriesSelector = SeriesSelector::from(selector);
        let ids = inner
            .postings
            .postings_for_selectors(&[selector])
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        let mut walk = ProfileCollector::new(window, profiled_series_cap(&options));
        for id in ids.iter() {
            let Some(series) = inner.series.get(&id) else {
                continue;
            };
            if walk.visit(series).is_break() {
                return Ok(None);
            }
        }
        let mut builder = LabelProfileBuilder::new();
        builder.add_taken(walk.take());
        Ok(Some(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::label_profile::MAX_PROFILED_KEYS_PER_SERIES;
    use promql_parser::parser::{Expr, parse};

    fn selector(query: &str) -> VectorSelector {
        match parse(query).unwrap() {
            Expr::VectorSelector(vs) => vs,
            other => panic!("not a selector: {other:?}"),
        }
    }

    /// `m{region="eu"}` stopped reporting at 10 ms; `m{region="us"}` reports from 1 s on.
    fn retired_and_live() -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        let retired = Labels::from_pairs(&[("__name__", "m"), ("region", "eu")]);
        let live = Labels::from_pairs(&[("__name__", "m"), ("region", "us")]);
        for t in 0..10 {
            querier.add_sample(&retired, Sample::new(t, 1.0));
            querier.add_sample(&live, Sample::new(1_000 + t, 1.0));
        }
        querier
    }

    fn options(max_series: usize) -> QueryOptions {
        QueryOptions {
            max_series,
            ..QueryOptions::default()
        }
    }

    fn filters(profile: &LabelProfile) -> Vec<String> {
        profile
            .common_filters()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn a_window_profiles_only_the_series_with_a_sample_in_it() {
        let querier = retired_and_live();
        let m = selector("m");

        let index = querier
            .label_profile(&m, None, options(0))
            .unwrap()
            .unwrap();
        assert_eq!(index.series, 2);
        assert_eq!(filters(&index), vec![r#"region=~"eu|us""#]);

        let live = ProfileWindow::new(500, 2_000);
        let windowed = querier
            .label_profile(&m, Some(live), options(0))
            .unwrap()
            .unwrap();
        assert_eq!(windowed.series, 1);
        assert_eq!(filters(&windowed), vec![r#"region="us""#]);

        let none = ProfileWindow::new(100, 200);
        let empty = querier
            .label_profile(&m, Some(none), options(0))
            .unwrap()
            .unwrap();
        assert_eq!(empty.series, 0);
    }

    #[test]
    fn the_cap_counts_only_the_series_in_the_window() {
        let querier = retired_and_live();
        let m = selector("m");
        assert_eq!(querier.label_profile(&m, None, options(1)).unwrap(), None);
        let windowed = querier
            .label_profile(&m, Some(ProfileWindow::new(500, 2_000)), options(1))
            .unwrap()
            .unwrap();
        assert_eq!(windowed.series, 1);
    }

    /// `n` series of `m` retired at 10 ms, then one live from 1 s on.
    fn mostly_retired(n: usize) -> MemorySeriesQuerier {
        let querier = MemorySeriesQuerier::new();
        for i in 0..n {
            let retired = Labels::from_pairs(&[("__name__", "m"), ("gen", &i.to_string())]);
            querier.add_sample(&retired, Sample::new(10, 1.0));
        }
        let live = Labels::from_pairs(&[("__name__", "m"), ("gen", "live")]);
        querier.add_sample(&live, Sample::new(1_000, 1.0));
        querier
    }

    /// A selector the index refuses is the read's error, not a selector that
    /// matched nothing. The parser never builds one, so it is built by hand.
    #[test]
    fn an_index_error_is_the_reads_error() {
        use promql_parser::label::{MatchOp, Matcher, Matchers};
        let querier = retired_and_live();
        // Alone, an empty name matching "" is Prometheus' match-all; beside
        // another matcher the index refuses it.
        let unanswerable = VectorSelector {
            name: None,
            matchers: Matchers::new(vec![
                Matcher::new(MatchOp::Equal, "", ""),
                Matcher::new(MatchOp::Equal, "region", "eu"),
            ]),
            offset: None,
            at: None,
        };
        let err = querier
            .query(&unanswerable, 1_000, QueryOptions::default())
            .expect_err("an index error must not read as no series");
        assert!(err.to_string().contains("at least one matcher"), "{err}");
    }

    /// Reads are refused at the limits a module read applies, so the conformance suite
    /// runs them: the series limit counts only series with a sample in the window, and
    /// the per-series point and per-query sample limits hold for a range read.
    #[test]
    fn reads_apply_the_module_read_limits() {
        let querier = retired_and_live();
        let m = selector("m");
        // Only `us` has a sample in the window, so one series is within a limit of one.
        let short_lookback = QueryOptions {
            lookback_delta: std::time::Duration::from_millis(100),
            ..options(1)
        };
        assert_eq!(querier.query(&m, 1_009, short_lookback).unwrap().len(), 1);
        assert_eq!(
            querier
                .query_range(&m, 1_000, 1_009, options(1))
                .unwrap()
                .len(),
            1
        );
        let both = querier.query_range(&m, 0, 1_009, options(1)).unwrap_err();
        assert!(both.to_string().contains("2 > 1"), "{both}");
        let both = querier
            .query(
                &m,
                1_009,
                QueryOptions {
                    lookback_delta: std::time::Duration::from_secs(10),
                    ..options(1)
                },
            )
            .unwrap_err();
        assert!(both.to_string().contains("2 > 1"), "{both}");

        let points = QueryOptions {
            max_points_per_series: Some(5),
            ..options(0)
        };
        let err = querier.query_range(&m, 1_000, 1_009, points).unwrap_err();
        assert!(err.to_string().contains("max points per series"), "{err}");

        let samples = QueryOptions {
            max_samples: 15,
            ..options(0)
        };
        let err = querier.query_range(&m, 0, 1_009, samples).unwrap_err();
        assert!(err.to_string().contains("max-samples-per-query"), "{err}");
    }

    #[test]
    fn retired_series_count_against_the_keys_a_profile_opens() {
        let m = selector("m");
        let live = Some(ProfileWindow::new(500, 2_000));
        // A cap of one series opens at most four keys: three retired and the
        // live one are profiled, a fourth retired one gives the profile up.
        let fits = mostly_retired(MAX_PROFILED_KEYS_PER_SERIES - 1);
        let profile = fits.label_profile(&m, live, options(1)).unwrap().unwrap();
        assert_eq!(profile.series, 1);
        assert_eq!(filters(&profile), vec![r#"gen="live""#]);

        let past = mostly_retired(MAX_PROFILED_KEYS_PER_SERIES);
        assert_eq!(past.label_profile(&m, live, options(1)).unwrap(), None);
    }
}
