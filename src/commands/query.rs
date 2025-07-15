use crate::commands::result::{to_instant_vector_result, to_matrix_result};
use crate::commands::{parse_instant_query_options, parse_range_query_options};
use crate::error_consts;
use crate::query::config::QUERY_DEFAULT_STEP;
use crate::query::{run_instant_query, run_range_query};
use metricsql_runtime::prelude::query::QueryParams;
use metricsql_runtime::Deadline;
use std::thread;
use std::time::Duration;
use valkey_module::{
    Context, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

///
/// TS.QUERY_RANGE <query> start end
///     [STEP duration]
///     [ROUNDING digits]
///     [TIMEOUT duration]
///
pub(crate) fn query_range(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_range_query_options(&mut args)?;
    let round_digits: u8 = options.rounding.unwrap_or(100);

    let (start, end) = options.date_range.get_timestamps(None);
    let step = options.step.unwrap_or_else(|| {
        *QUERY_DEFAULT_STEP
            .lock()
            .expect("query default step lock poisoned")
    });

    // todo: set a cap on timout duration to mitigate DOS
    let deadline = get_deadline(options.timeout)?;

    let query_params: QueryParams = QueryParams {
        query: options.query,
        start,
        end,
        step,
        round_digits,
        deadline,
        ..Default::default()
    };

    let blocked_client = ctx.block_client();
    let _ = thread::spawn(move || match run_range_query(&query_params) {
        Ok(results) => {
            let results = results.collect();
            let valkey_value = to_matrix_result(results);
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Ok(valkey_value));
        }
        Err(e) => {
            let err_msg = format!("PROM: Error: {e:?}");
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::String(err_msg.to_string())));
        }
    });

    Ok(ValkeyValue::NoReply)
}

///
/// TS.QUERY <query> time
///    [STEP duration]
///    [ROUNDING digits]
///    [TIMEOUT duration]
///
pub fn query(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let mut args = args.into_iter().skip(1).peekable();
    let options = parse_instant_query_options(&mut args)?;

    let round_digits: u8 = options.rounding.unwrap_or(100);
    let start = options.timestamp.as_timestamp(None);
    let step = options.step.unwrap_or_else(|| {
        *QUERY_DEFAULT_STEP
            .lock()
            .expect("query default step lock poisoned")
    });

    let deadline = get_deadline(options.timeout)?;

    let query_params: QueryParams = QueryParams {
        query: options.query,
        start,
        step,
        round_digits,
        end: start, // For instant queries, start and end are the same
        deadline,
        ..Default::default()
    };

    let blocked_client = ctx.block_client();
    let _ = thread::spawn(move || match run_instant_query(&query_params) {
        Ok(results) => {
            let valkey_value = to_instant_vector_result(results);
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Ok(valkey_value));
        }
        Err(e) => {
            let err_msg = format!("TSDB(promql): Error: {e:?}");
            let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::String(err_msg.to_string())));
        }
    });

    Ok(ValkeyValue::NoReply)
}

fn get_deadline(timeout: Option<Duration>) -> ValkeyResult<Deadline> {
    if let Some(timeout) = timeout {
        Deadline::new(timeout).map_err(|_| ValkeyError::Str(error_consts::INVALID_DURATION))
    } else {
        Ok(Deadline::default())
    }
}
