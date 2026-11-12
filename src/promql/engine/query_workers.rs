//! The threads that evaluate `TS.QUERY` / `TS.QUERYRANGE`.
//!
//! A fixed number of dedicated threads take queued evaluations in arrival
//! order. Each command used to spawn its own thread, so N concurrent queries
//! were N threads and N sets of intermediate results with no limit on the
//! concurrent work; now at most [`max_concurrent_queries`] evaluate at once
//! and the rest wait their turn, each still bounded by its own deadline
//! (`TIMEOUT`), which the evaluation checks before it starts.
//!
//! A query whose client disconnects is abandoned: skipped if it is still
//! queued, and stopped at its next deadline check if it is running (see
//! [`CancelToken`]). Its reply would go nowhere, and a connection that sends a
//! heavy query and closes would otherwise cost a worker for the whole
//! `TIMEOUT` each time.
//!
//! The wait itself is bounded by [`max_queued_queries`]. A query that arrives
//! to a full backlog is refused at once rather than admitted to sit out its
//! whole `TIMEOUT` holding a blocked client, its parsed statement and its
//! querier, only to be answered with a timeout anyway.
//!
//! Blocking threads, not a pool: an evaluation blocks — on the selector
//! executor's answer, on its own fan-outs — and a pool worker that blocks
//! steals other jobs while it waits (see `SelectorBatchExecutor`'s design
//! notes for what that did). A plain thread waits without stealing.
//!
//! An evaluation's own parallel work runs on the evaluation pool, never the
//! shared pool: see [`run_evaluation`].

use crate::common::Timestamp;
use crate::common::context::is_blocking_denied;
use crate::common::replies::{CancelToken, ReplyContext, ThreadSafeReplyContext, block_client};
use crate::common::threads::{
    BoundedExecutor, Capacity, ExecutorStats, Rejected, run_on_eval_pool,
};
use crate::common::time::current_time_millis;
use crate::config::{max_concurrent_queries, max_queued_queries};
use crate::error_consts;
use crate::promql::engine::{admission, promql_config};
use crate::promql::{QueryError, QueryResult};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyValue};

/// Set by `TS._DEBUG PANIC_NEXT_EVALUATION` (debug mode only): the next evaluation panics.
/// Integration tests use it to check that a failed evaluation still answers its client.
static PANIC_NEXT_EVALUATION: AtomicBool = AtomicBool::new(false);

/// Makes the next PromQL evaluation panic on the evaluation pool. For tests; see
/// [`PANIC_NEXT_EVALUATION`].
pub(crate) fn panic_next_evaluation() {
    PANIC_NEXT_EVALUATION.store(true, Ordering::Relaxed);
}

/// Queries abandoned because their client disconnected, queued or running, since startup.
static CANCELLED: AtomicU64 = AtomicU64::new(0);

/// How many queries were abandoned because their client disconnected (`INFO ts_promql`).
pub(crate) fn cancelled_count() -> u64 {
    CANCELLED.load(Ordering::Relaxed)
}

static QUERY_WORKERS: BoundedExecutor = BoundedExecutor::new(
    "ts-promql-query",
    max_concurrent_queries,
    Capacity::Dynamic(max_queued_queries),
);

/// Run a PromQL evaluation on the evaluation pool.
///
/// Every parallel entry point the evaluation reaches — the `*_rayon` iterators,
/// `threads::join` — resolves to the pool of the pinned worker that calls it, so
/// installing the evaluation there keeps all of its nested work off the shared
/// pool without touching those call sites.
///
/// Wrap only the evaluation: the reply is written from the query worker.
#[track_caller]
pub(crate) fn run_evaluation<R: Send>(evaluate: impl FnOnce() -> R + Send) -> R {
    run_on_eval_pool(evaluate)
}

/// Evaluate a query for the client of `ctx` on a query worker, and answer it from there.
///
/// The client is blocked only once the backlog has room for the query, so a refusal is an
/// ordinary error reply. On the worker, `prepare` runs first — parsing the query text — and an
/// error from it is the reply as it stands; then `evaluate` runs on the evaluation pool
/// ([`run_evaluation`]) over what `prepare` produced, and `reply` writes its result. A query
/// whose `deadline` passed while it waited for a worker is answered with a timeout and neither
/// prepared nor evaluated: the wait counts against its budget. One whose client disconnected
/// meanwhile is dropped the same way.
///
/// `evaluate` gets the [`CancelToken`] for the client's disconnect; it belongs in the
/// query's options, where every deadline check reads it.
///
/// `prepare` is here, not in the command handler, because parsing costs time that grows faster
/// than the query text: a 16 KiB query took 0.76 s to parse, and on the main thread that is a
/// stall for every client, paid even by a query refused afterwards for its depth. On a worker
/// it is bounded by the same pool and queue as evaluation.
pub(crate) fn submit_evaluation<S, R, Q, E, P>(
    ctx: &Context,
    deadline: Option<Timestamp>,
    prepare: Q,
    evaluate: E,
    reply: P,
) -> ValkeyResult
where
    S: Send + 'static,
    R: Send + 'static,
    Q: FnOnce() -> ValkeyResult<S> + Send + 'static,
    E: FnOnce(S, CancelToken) -> QueryResult<R> + Send + 'static,
    P: FnOnce(&ReplyContext, R) + Send + 'static,
{
    // Checked before blocking: the server asserts on a blocked deny-blocking
    // client (a module `RM_Call` without the K flag) and aborts, and inside
    // MULTI or a script the client would get the server's own error while the
    // query still ran with nobody to answer.
    if is_blocking_denied(ctx) {
        return Err(ValkeyError::Str(error_consts::PROMQL_BLOCKING_NOT_ALLOWED));
    }
    // A fresh sample: this is the main thread, and the cron's may be a tick old.
    admission::sample_memory();
    admission::check_memory(promql_config().max_memory_percent)
        .map_err(|err| ValkeyError::String(err.to_string()))?;
    let slot = QUERY_WORKERS
        .try_reserve()
        .map_err(|rejected| ValkeyError::String(format!("TSDB: {}", rejection(rejected))))?;
    let blocked_client = block_client(ctx);
    let cancel = blocked_client.cancel_token();

    slot.spawn(move || {
        let thread_ctx = ThreadSafeReplyContext::with_blocked_client(blocked_client);
        match answer(deadline, current_time_millis(), cancel, prepare, evaluate) {
            Answer::Value(value) => reply(&thread_ctx.get_reply_context(), value),
            Answer::Error(err) => {
                thread_ctx.reply(Err(err));
            }
            Answer::Abandoned => {
                CANCELLED.fetch_add(1, Ordering::Relaxed);
                // Answered all the same: the handle must be, and the server discards it.
                thread_ctx.reply(Err(ValkeyError::String(QueryError::Cancelled.to_string())));
            }
        }
    });

    // Answered later, from a query worker.
    Ok(ValkeyValue::NoReply)
}

/// What a query worker answers a query with.
enum Answer<R> {
    /// The evaluation's result, for the command's `reply` to write.
    Value(R),
    /// An error reply.
    Error(ValkeyError),
    /// The client went away: there is no one to answer.
    Abandoned,
}

/// A query's run on its worker, `now` being when the worker took it up: refused with a
/// timeout, without being prepared, when its `deadline` passed while it waited; abandoned,
/// likewise, when its client disconnected meanwhile; otherwise prepared on this thread, the
/// preparation's error being the answer as it stands, then evaluated on the evaluation pool.
/// An evaluation that ends `Cancelled` was abandoned rather than failed.
fn answer<S, R>(
    deadline: Option<Timestamp>,
    now: Timestamp,
    cancel: CancelToken,
    prepare: impl FnOnce() -> ValkeyResult<S>,
    evaluate: impl FnOnce(S, CancelToken) -> QueryResult<R> + Send,
) -> Answer<R>
where
    S: Send,
    R: Send,
{
    if deadline.is_some_and(|d| now > d) {
        return Answer::Error(ValkeyError::String(QueryError::Timeout.to_string()));
    }
    if cancel.is_cancelled() {
        return Answer::Abandoned;
    }
    // On this thread, not the evaluation pool: it is sequential, and its own thread keeps
    // it from holding a pool worker other evaluations' parallel work could use.
    let prepared = match prepare() {
        Ok(prepared) => prepared,
        Err(err) => return Answer::Error(err),
    };
    let evaluate = move || {
        if PANIC_NEXT_EVALUATION.swap(false, Ordering::Relaxed) {
            panic!("TS._DEBUG PANIC_NEXT_EVALUATION");
        }
        evaluate(prepared, cancel)
    };
    match run_evaluation(evaluate) {
        Ok(value) => Answer::Value(value),
        Err(QueryError::Cancelled) => Answer::Abandoned,
        Err(err) => Answer::Error(ValkeyError::String(err.to_string())),
    }
}

fn rejection(rejected: Rejected) -> String {
    match rejected {
        Rejected::QueueFull { limit } => {
            format!("too many queued PromQL queries (ts-promql-max-queued-queries = {limit})")
        }
        Rejected::NotRunning => "query workers are not running".to_string(),
    }
}

/// The query workers' load, for `INFO`.
pub(crate) fn stats() -> ExecutorStats {
    QUERY_WORKERS.stats()
}

#[cfg(test)]
mod answer_tests {
    use super::{Answer, answer};
    use crate::common::replies::TestClient;
    use crate::promql::QueryError;
    use std::cell::Cell;
    use valkey_module::ValkeyError;

    fn text<R>(answer: Answer<R>) -> String {
        match answer {
            Answer::Error(err) => err.to_string(),
            Answer::Value(_) => "a value".to_string(),
            Answer::Abandoned => "abandoned".to_string(),
        }
    }

    #[test]
    fn a_query_whose_deadline_passed_in_the_queue_times_out_unprepared() {
        let client = TestClient::new();
        let prepared = Cell::new(false);
        let got = answer(
            Some(1_000),
            1_001,
            client.token(),
            || {
                prepared.set(true);
                Ok(())
            },
            |(), _| Ok(1),
        );
        assert_eq!(text(got), QueryError::Timeout.to_string());
        assert!(!prepared.get(), "a query past its deadline is not parsed");

        // At the deadline exactly, it runs.
        let got = answer(Some(1_000), 1_000, client.token(), || Ok(()), |(), _| Ok(7));
        assert!(matches!(got, Answer::Value(7)));
    }

    #[test]
    fn a_query_whose_client_left_in_the_queue_is_abandoned_unprepared() {
        let client = TestClient::new();
        client.disconnect();
        let prepared = Cell::new(false);
        let got = answer(
            None,
            0,
            client.token(),
            || {
                prepared.set(true);
                Ok(())
            },
            |(), _| Ok(1),
        );
        assert!(matches!(got, Answer::Abandoned));
        assert!(!prepared.get());
    }

    #[test]
    fn a_preparation_error_is_the_answer_and_nothing_is_evaluated() {
        let client = TestClient::new();
        let got: Answer<i32> = answer(
            None,
            0,
            client.token(),
            || Err::<(), _>(ValkeyError::Str("TSDB: query too long")),
            |(), _| panic!("evaluated after a failed preparation"),
        );
        assert_eq!(text(got), "TSDB: query too long");
    }

    #[test]
    fn an_evaluation_is_answered_with_its_value_or_its_error() {
        let client = TestClient::new();
        let got = answer(None, 0, client.token(), || Ok(20), |n, _| Ok(n + 1));
        assert!(matches!(got, Answer::Value(21)));

        let got: Answer<i32> = answer(
            None,
            0,
            client.token(),
            || Ok(()),
            |(), _| Err(QueryError::Execution("bad".to_string())),
        );
        assert_eq!(
            text(got),
            QueryError::Execution("bad".to_string()).to_string()
        );
    }

    /// An evaluation the disconnect stopped was abandoned, not failed: no error
    /// reply is due, and the cancellation is what is counted.
    #[test]
    fn an_evaluation_stopped_by_the_disconnect_is_abandoned() {
        let client = TestClient::new();
        let got: Answer<i32> = answer(
            None,
            0,
            client.token(),
            || Ok(()),
            |(), cancel| {
                cancel.disconnect_for_test();
                Err(QueryError::Cancelled)
            },
        );
        assert!(matches!(got, Answer::Abandoned));
    }
}

#[cfg(test)]
mod tests {
    use super::run_evaluation;
    use crate::common::threads::{IntoParRayon, join};
    use crate::config::num_threads;
    use orx_parallel::Par;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;

    /// The freeze the evaluation pool exists to prevent: evaluation jobs parked on
    /// a processor that needs the GIL (a mutex here), while the GIL holder waits on
    /// orx's shared pool for its own parallel work. With the evaluation on the shared
    /// pool, the parked jobs hold every shared worker and the holder's fan-out never
    /// runs.
    #[test]
    fn parked_evaluations_never_starve_a_gil_holder_on_the_shared_pool() {
        let gil = Arc::new(Mutex::new(()));
        let held = gil.lock().unwrap();

        let (task_tx, task_rx) = mpsc::channel::<mpsc::SyncSender<()>>();
        let processor_gil = Arc::clone(&gil);
        std::thread::spawn(move || {
            for responder in task_rx {
                let _gil = processor_gil.lock().unwrap_or_else(|e| e.into_inner());
                let _ = responder.send(());
            }
        });

        // Enough parking jobs to occupy every worker of either pool.
        let eval_workers = num_threads();
        let jobs = 2 * eval_workers.max(orx_parallel::Pool::global().current_num_threads());
        let parked = Arc::new(AtomicUsize::new(0));
        let off_eval_pool = Arc::new(AtomicUsize::new(0));
        let evaluation = {
            let (parked, off_eval_pool) = (Arc::clone(&parked), Arc::clone(&off_eval_pool));
            std::thread::spawn(move || {
                run_evaluation(|| {
                    (0..jobs).into_par_rayon().for_each(|_| {
                        let on_eval_pool = std::thread::current()
                            .name()
                            .is_some_and(|name| name.starts_with("ts-promql-eval-"));
                        if !on_eval_pool {
                            off_eval_pool.fetch_add(1, Ordering::Relaxed);
                        }
                        let (tx, rx) = mpsc::sync_channel(1);
                        task_tx.send(tx).unwrap();
                        parked.fetch_add(1, Ordering::AcqRel);
                        rx.recv().unwrap();
                    })
                })
            })
        };
        let parked_by = std::time::Instant::now() + Duration::from_secs(10);
        while parked.load(Ordering::Acquire) < eval_workers {
            assert!(
                std::time::Instant::now() < parked_by,
                "evaluation jobs never parked"
            );
            std::thread::yield_now();
        }

        // The GIL holder's own fan-out, on the shared pool: a parallel iterator, and a
        // `join` like TS.JOIN's and TS.LABELSTATS'.
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let sum = || (0..10_000usize).into_par_rayon().sum::<usize>();
            let (a, b) = join(sum, sum);
            let _ = done_tx.send(a + b);
        });
        let sum = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("parked evaluation jobs starved the shared pool");
        assert_eq!(sum, 2 * (0..10_000usize).sum::<usize>());

        drop(held);
        evaluation.join().unwrap();
        assert_eq!(off_eval_pool.load(Ordering::Relaxed), 0);
    }
}
