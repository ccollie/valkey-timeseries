use crate::common::Sample;
use crate::query::{InstantQueryResult, RangeQueryResult, QUERY_CONTEXT};
use metricsql_runtime::execution::query::{query, query_range, QueryParams};
use metricsql_runtime::prelude::Context as QueryContext;
use metricsql_runtime::RuntimeError;
use valkey_module::{ValkeyError, ValkeyResult};
use crate::common::async_runtime::block_on;

fn map_error(err: RuntimeError) -> ValkeyError {
    let err_msg = format!("ERR: query execution error: {err:?}");
    // todo: log errors
    ValkeyError::String(err_msg.to_string())
}

// Do not call these synchronous APIs from inside an async context. Use the async API directly if
// you are already within an async runtime.

pub(crate) fn run_instant_query_internal(
    ctx: &QueryContext,
    params: &QueryParams,
) -> ValkeyResult<Vec<InstantQueryResult>> {
    let results = block_on( query(ctx, params) ).map_err(map_error)?;
    Ok(results
        .into_iter()
        .map(|result| InstantQueryResult {
            metric: result.metric,
            sample: Sample {
                timestamp: result.timestamps[0],
                value: result.values[0],
            },
        })
        .collect())
}

pub(crate) fn run_instant_query(params: &QueryParams) -> ValkeyResult<Vec<InstantQueryResult>> {
    run_instant_query_internal(&QUERY_CONTEXT, params)
}

pub(crate) fn run_range_query_internal(
    ctx: &QueryContext,
    params: &QueryParams,
) -> ValkeyResult<impl Iterator<Item = RangeQueryResult>> {
    let results = block_on( query_range(ctx, params) ).map_err(map_error)?;
    Ok(
        results
        .into_iter()
        .map(|result| {
            let samples = result
                .timestamps
                .iter()
                .zip(result.values.iter())
                .map(|(&ts, &value)| Sample {
                    timestamp: ts,
                    value,
                })
                .collect();
            RangeQueryResult {
                metric: result.metric,
                samples,
            }
        })
    )
}

pub(crate) fn run_range_query(params: &QueryParams) -> ValkeyResult<impl Iterator<Item = RangeQueryResult>> {
    run_range_query_internal(&QUERY_CONTEXT, params)
}