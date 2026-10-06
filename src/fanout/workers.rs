//! Worker threads for fan-out work that takes the module GIL.
//!
//! Each lane is a [`BoundedExecutor`]: a fixed set of blocking threads (the GIL is no job for
//! a pool worker: R1 in `common::threads`) behind a bounded queue, so a burst of fan-out
//! commands or peer requests cannot create a thread per request. The GIL serializes most of this work anyway,
//! so a handful of workers keeps the part that runs outside it (decoding, encoding) parallel.
//!
//! Two lanes, so that neither kind of work can starve the other: a node flooded with its own
//! clients' fan-outs still answers the requests its peers are waiting on, and the reverse.

use crate::common::threads::{BoundedExecutor, Capacity, lane_workers};

/// Queued jobs per lane before submissions are rejected as busy. A client has at most one
/// blocked fan-out at a time, so this is only reached by a burst from that many clients (or
/// peers' coordinators) at once.
const QUEUE_CAPACITY: usize = 1024;

/// Runs requests received from peer coordinators.
pub(super) static PEER_REQUEST_EXECUTOR: BoundedExecutor = BoundedExecutor::new(
    "ts-fanout-request",
    lane_workers,
    Capacity::Fixed(QUEUE_CAPACITY),
);
