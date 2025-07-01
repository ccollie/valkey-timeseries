use crate::query::get_query_series_data;
use async_trait::async_trait;
use metricsql_runtime::prelude::{
    Deadline, MetricStorage, QueryResult, QueryResults, RuntimeResult, SearchQuery,
};
use metricsql_runtime::RuntimeError;
use valkey_module::{Context, ValkeyResult};
use crate::common::db::set_current_db;
use crate::fanout::cluster::is_clustered;
use crate::query::cluster::cluster_search_query;

/// Interface between the time series database and the metricsql runtime.
pub struct VMMetricStorage {
    pub db: i32
}

fn get_series_data_standalone(
    ctx: &Context,
    search_query: SearchQuery,
) -> ValkeyResult<Vec<QueryResult>> {
    get_query_series_data(ctx, search_query, |pairs| {
        let results: Vec<QueryResult> = pairs.iter().map(|pair| pair.into()).collect();
        Ok(results)
    })
}

impl VMMetricStorage {
    fn is_clustered(&self) -> bool {
        // see: https://github.com/RedisLabsModules/redismodule-rs/blob/master/examples/call.rs#L144
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        is_clustered(&ctx_guard)
    }
    
    fn get_series_data_standalone(
        &self,
        search_query: SearchQuery,
    ) -> RuntimeResult<QueryResults> {
        let ctx_guard = valkey_module::MODULE_CONTEXT.lock();
        let db = self.db;
        let ctx: &Context = &ctx_guard;
        set_current_db(ctx, db);
        let data = get_series_data_standalone(&ctx_guard, search_query)
            .map_err(|e| RuntimeError::General(e.to_string()))?;
        Ok(QueryResults::new(data))
    }
}

#[async_trait]
impl MetricStorage for VMMetricStorage {
    async fn search(&self, sq: SearchQuery, _deadline: Deadline) -> RuntimeResult<QueryResults> {
        if self.is_clustered() {
            cluster_search_query(sq, self.db).await
        } else {
            self.get_series_data_standalone(sq)
        }
    }
}
