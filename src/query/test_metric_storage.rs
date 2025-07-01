use crate::common::{Sample, Timestamp};
use crate::series::index::{series_keys_by_matchers, TimeSeriesIndex};
use crate::series::{TimeSeries, TimeSeriesOptions};
use async_trait::async_trait;
use metricsql_runtime::prelude::{
    Deadline, MetricName, MetricStorage, QueryResult, QueryResults,
    RuntimeResult, RuntimeError, SearchQuery,
};
use std::collections::HashMap;
use std::sync::RwLock;
use valkey_module::{ValkeyError, ValkeyResult};
use crate::labels::matchers::Matchers;
use crate::series::chunks::ChunkEncoding;

type KeyType = Box<[u8]>;

/// Interface between the time series database and the metricsql runtime.
/// Testing only
pub(crate) struct TestMetricStorage {
    index: TimeSeriesIndex,
    series: RwLock<HashMap<KeyType, TimeSeries>>,
}

impl TestMetricStorage {
    pub fn new() -> Self {
        TestMetricStorage {
            index: TimeSeriesIndex::new(),
            series: RwLock::new(HashMap::new()),
        }
    }

    fn add_internal(&mut self, key: KeyType, ts: Timestamp, val: f64) -> ValkeyResult<()> {
        self.with_mutable_series(&key, |series| {
            let res = series.add(ts, val, None);
            if !res.is_ok() {
                return Err(ValkeyError::String("Error adding sample".to_string()));
            }
            Ok(())
        })
    }


    fn key_exists(&self, key: &KeyType) -> bool {
        let map = self.series.read().expect("Failed to acquire read lock on series map");
        map.contains_key(key)
    }

    pub fn add(&mut self, metric: &str, ts: Timestamp, value: f64) -> ValkeyResult<()> {
        let mn = match MetricName::parse(metric) {
            Ok(mn) => mn,
            Err(_) => return Err(ValkeyError::String("Invalid metric name".to_string())),
        };
        let name = mn.to_string();
        let key = string_to_key(&name);
        let mut map = self.series.write().expect("Failed to acquire read lock on series map");
        match map.get_mut(&key) {
            Some(series) => {
                series.add(ts, value, None);
            }
            None => {
                let mut time_series = self.create_series(&mn);
                time_series.add(ts, value, None);
                self.index.index_timeseries(&mut time_series, &key);
                map.insert(key.clone(), time_series);
            }
        }
        Ok(())
    }

    fn insert_series_from_metric_name(&mut self, mn: &MetricName) {
        let mut time_series = self.create_series(mn);
        let mut map = self.series.write()
            .expect("Failed to acquire write lock on series map");
        let key = timeseries_key(&time_series);
        self.index
            .index_timeseries(&mut time_series, &key);
        map.insert(key, time_series);
    }

    fn create_series(&self, mn: &MetricName) -> TimeSeries {
        let chunk_compression = ChunkEncoding::Uncompressed;
        let options = TimeSeriesOptions {
            chunk_size: Some(4096),
            chunk_compression,
            ..Default::default()
        };
        let mut time_series = TimeSeries::with_options(options).unwrap();
        time_series.labels = mn.into();
        time_series.chunk_compression = chunk_compression;
        time_series
    }

    fn with_mutable_series<F>(&mut self, key: &KeyType, mut f: F) -> ValkeyResult<()>
    where
        F: FnMut(&mut TimeSeries) -> ValkeyResult<()>,
    {
        let mut map = self.series.write()
            .expect("Failed to acquire write lock on series map");
        match map.get_mut(key) {
            Some(series) => f(series),
            None => Ok(()),
        }
    }

    fn get_series_range(
        &self,
        key: KeyType,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Option<(MetricName, Vec<Sample>)> {
        let map = self.series.read().unwrap();
        match map.get(&key) {
            Some(series) => {
                let metric = series.labels.get_metric_name();
                let samples = series.get_range(start_ts, end_ts);
                Some((metric, samples))
            }
            None => None,
        }
    }

    fn get_series(
        &self,
        key: KeyType,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> RuntimeResult<Option<QueryResult>> {
        self.get_series_range(key, start_ts, end_ts)
            .map_or(Ok(None), |(metric_name, samples)| {
                let (timestamps, values): (Vec<_>, Vec<_>) = samples
                    .into_iter()
                    .map(|Sample { timestamp, value }| (timestamp, value))
                    .unzip();
                Ok(Some(QueryResult::new(metric_name, timestamps, values)))
            })
    }

    fn get_series_data(&self, search_query: SearchQuery) -> RuntimeResult<Vec<QueryResult>> {
        let matchers: Matchers = search_query.matchers.try_into()
            .map_err(|e| {
                RuntimeError::General(format!("Error converting matchers: {:?}", e))
            })?;
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        let start_ts = search_query.start;
        let end_ts = search_query.end;

        let ctx = &ctx_guard;
        let map = series_keys_by_matchers(ctx, &[matchers], None)
            .map_err(|e| {
                ctx.log_warning(&format!("ERR: {:?}", e));
                // TODO. 1. on the lib side, use a better enum variant
                RuntimeError::General("Error getting series keys".to_string())
            })?;
        let mut results: Vec<QueryResult> = Vec::with_capacity(map.len());

        // use rayon?
        for key in map.into_iter() {
            let k = key.as_slice().to_vec().into_boxed_slice();
            if let Some(result) = self.get_series(k, start_ts, end_ts)? {
                results.push(result);
            }
        }
        Ok(results)
    }
}

fn timeseries_key(ts: &TimeSeries) -> KeyType {
    let key = ts.prometheus_metric_name();
    string_to_key(key.as_str())
}

fn string_to_key(s: &str) -> KeyType {
    s.as_bytes().to_vec().into_boxed_slice()
}

#[async_trait]
impl MetricStorage for TestMetricStorage {
    async fn search(&self, sq: SearchQuery, _deadline: Deadline) -> RuntimeResult<QueryResults> {
        let data = self.get_series_data(sq)?;
        let result = QueryResults::new(data);
        Ok(result)
    }
}
