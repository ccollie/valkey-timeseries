use std::ops::Deref;
use metricsql_runtime::{QueryResult, SearchQuery};
use valkey_module::{Context, ValkeyError, ValkeyResult};
use crate::common::{Sample, Timestamp};
use crate::labels::matchers::Matchers;
use crate::series::index::series_by_matchers;
use crate::series::TimeSeries;

pub struct SeriesDataPair<'a> {
    pub(crate) series: &'a TimeSeries,
    pub(crate) samples: Vec<Sample>
}

impl<'a> From<&SeriesDataPair<'a>> for QueryResult {
    fn from(pair: &SeriesDataPair<'a>) -> Self {
        let metric = pair.series.labels.get_metric_name();
        let count = pair.samples.len();
        let mut timestamps = Vec::with_capacity(count);
        let mut values = Vec::with_capacity(count);

        for Sample { timestamp, value } in pair.samples.iter() {
            timestamps.push(*timestamp);
            values.push(*value);
        }

        QueryResult::new(metric, timestamps, values)
    }
}

fn get_series_internal<'a>(
    scope: &mut chili::Scope,
    series: &[&'a TimeSeries],
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> Vec<SeriesDataPair<'a>> {
    match series {
        [] => Vec::new(),
        [series] => {
            collect_query_results(vec![get_range(series, start_ts, end_ts)])
        }
        [s1, s2] => {
            let (r1, r2) = scope.join(
                |_| get_range(s1, start_ts, end_ts),
                |_| get_range(s2, start_ts, end_ts),
            );
            collect_query_results(vec![r1, r2])
        }
        _ => {
            let mid = series.len() / 2;
            let (left, right) = series.split_at(mid);
            let (mut left_results, right_results) = scope.join(
                |s1| get_series_internal(s1, left, start_ts, end_ts),
                |s2| get_series_internal(s2, right, start_ts, end_ts),
            );
            left_results.extend(right_results);
            left_results
        }
    }
}

fn collect_query_results(results: Vec<Option<SeriesDataPair>>) -> Vec<SeriesDataPair> {
    results.into_iter().flatten().collect()
}

fn get_range(series: &TimeSeries, start_ts: Timestamp, end_ts: Timestamp) -> Option<SeriesDataPair> {
    // This function should be implemented based on the VMMetricStorage::get_range logic
    // from the related code
    let samples = series.get_range(start_ts, end_ts);
    if samples.is_empty() {
        return None;
    }
    Some(SeriesDataPair {
        series,
        samples,
    })
}

pub fn get_query_series_data<F, R>(
    ctx: &Context,
    search_query: SearchQuery,
    func: F
) -> ValkeyResult<R> 
where F: Fn(Vec<SeriesDataPair<'_>>) -> ValkeyResult<R>
{
    let matchers: Matchers = search_query.matchers.try_into()
        .map_err(|e| {
            ctx.log_warning(&format!("ERR: {e:?}"));
            ValkeyError::String("Error converting matchers".to_string())
        })?;
    let start_ts = search_query.start;
    let end_ts = search_query.end;

    let guards = series_by_matchers(ctx, &[matchers], None, true, false)?;
    let mut series: Vec<&TimeSeries> = Vec::with_capacity(guards.len());
    for guard in guards.iter() {
        series.push(guard.deref());
    }

    let mut scope = chili::Scope::global();
    let pairs: Vec<SeriesDataPair> = get_series_internal(
        &mut scope,
        &series,
        start_ts,
        end_ts,
    );
    
    func(pairs)
}