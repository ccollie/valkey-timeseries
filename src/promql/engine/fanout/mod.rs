mod aggregation_fanout_command;
mod grid_fanout_command;
mod instant_vector_selector_fanout_command;
mod label_profile_fanout_command;
mod query_utils;
mod range_vector_selector_fanout_command;
mod type_conversions;

pub(in crate::promql) use aggregation_fanout_command::{
    AggregationFanoutCommand, InstantVectorParams,
};
pub(in crate::promql) use grid_fanout_command::GridFanoutCommand;
pub(in crate::promql) use instant_vector_selector_fanout_command::InstantVectorSelectorFanoutCommand;
pub(in crate::promql) use label_profile_fanout_command::LabelProfileFanoutCommand;
pub(in crate::promql) use range_vector_selector_fanout_command::RangeVectorSelectorFanoutCommand;
#[cfg(test)]
pub(in crate::promql) use type_conversions::range_sample_to_proto;
pub(in crate::promql) use type_conversions::{
    WireRangeResponse, check_unique_series, decode_range_series, duplicate_series_error,
};
use valkey_module::{ValkeyError, ValkeyResult};

use crate::error_consts;
use crate::fanout::{ErrorKind, FanoutContext, FanoutError, NodeInfo, register_fanout_operation};
use crate::promql::QueryError;
use crate::promql::deadline::Deadline;

/// The deadline a shard's read and kernels run under: when its requester stops waiting, if
/// the request said, and this node's `ts-promql-max-memory-percent` (`max_memory_percent`),
/// captured once as the read starts. Past the first the requester has answered without this
/// shard; past the second the node is under memory pressure, and the read stops between
/// batches, as at admission, rather than run on.
pub(super) fn shard_deadline(ctx: &FanoutContext, max_memory_percent: u32) -> Deadline {
    Deadline::until(ctx.deadline()).with_memory_limit(max_memory_percent)
}

/// A shard kernel's error as the shard reports it. A timeout can only be
/// [`shard_deadline`]'s, so it says the requester gave up.
pub(super) fn shard_error(err: impl Into<QueryError>) -> ValkeyError {
    match err.into() {
        QueryError::Timeout => ValkeyError::Str(error_consts::FANOUT_REQUESTER_GAVE_UP),
        QueryError::Execution(message) => ValkeyError::String(message),
        other => ValkeyError::String(other.to_string()),
    }
}

/// A field a fan-out request must carry, or the error the shard answers with.
/// A request without it is corrupt, not empty: refusing it keeps a malformed
/// peer from reading as "nothing matched here".
pub(super) fn required<T>(field: Option<T>, what: &str) -> ValkeyResult<T> {
    field.ok_or_else(|| ValkeyError::String(format!("TSDB: fan-out request has no {what}")))
}

/// A peer's answer this node could not use, reported against the peer.
pub(super) fn peer_error(
    target: &NodeInfo,
    what: &str,
    why: impl std::fmt::Display,
) -> FanoutError {
    FanoutError::custom(format!(
        "TSDB: peer {} {what}: {why}",
        target.socket_address
    ))
}

impl From<FanoutError> for QueryError {
    fn from(value: FanoutError) -> Self {
        match value.kind {
            ErrorKind::Timeout => QueryError::Timeout,
            // A shard spent its share of the sample budget. Restored to the variant so the
            // evaluator ends the query at once, as for a local read, instead of falling back
            // to per-step reads that the shards would refuse one by one.
            ErrorKind::QueryLimit => too_many_samples_from_message(&value.message)
                .or_else(|| {
                    // A shard refusing for load ends the query too, with the shard's words.
                    value
                        .message
                        .starts_with(error_consts::PROMQL_OVERLOADED_ERROR)
                        .then(|| QueryError::Overloaded(value.message.clone()))
                })
                .unwrap_or_else(|| QueryError::Execution(value.to_string())),
            _ => QueryError::Execution(value.to_string()),
        }
    }
}

/// The [`QueryError::TooManySamples`] whose message `message` is, as a shard rendered it.
fn too_many_samples_from_message(message: &str) -> Option<QueryError> {
    let rest = message
        .strip_prefix(error_consts::PROMQL_TOO_MANY_SAMPLES_ERROR)?
        .strip_prefix(": ")?;
    let (loaded, rest) = rest.split_once(" > ")?;
    let limit = rest.split_once(' ').map_or(rest, |(limit, _)| limit);
    Some(QueryError::TooManySamples {
        loaded: loaded.parse().ok()?,
        limit: limit.parse().ok()?,
    })
}

pub(crate) fn register_fanout_commands() -> ValkeyResult<()> {
    register_fanout_operation::<InstantVectorSelectorFanoutCommand>()?;
    register_fanout_operation::<RangeVectorSelectorFanoutCommand>()?;
    register_fanout_operation::<AggregationFanoutCommand>()?;
    register_fanout_operation::<GridFanoutCommand>()?;
    register_fanout_operation::<LabelProfileFanoutCommand>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::engine::sample_budget::too_many_samples;

    /// What a shard's refusal becomes on the coordinator, through the string it travels as.
    fn across_the_fanout(err: impl ToString) -> QueryError {
        FanoutError::from(err.to_string().as_str()).into()
    }

    /// A request missing a field it must carry is refused, naming the field,
    /// rather than answered as if it matched nothing.
    #[test]
    fn required_refuses_a_missing_field() {
        let err = required(None::<u8>, "selector").unwrap_err();
        assert!(err.to_string().contains("no selector"), "{err}");
        assert_eq!(required(Some(3u8), "selector").unwrap(), 3);
    }

    #[test]
    fn a_shard_sample_budget_refusal_stays_too_many_samples() {
        let err = across_the_fanout(too_many_samples(12_345, 10_000));
        assert!(
            matches!(
                err,
                QueryError::TooManySamples {
                    loaded: 12_345,
                    limit: 10_000
                }
            ),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            too_many_samples(12_345, 10_000).to_string()
        );
    }

    #[test]
    fn other_shard_limits_keep_their_message() {
        for message in [
            format!("{}: 6 > 4", error_consts::PROMQL_MAX_SERIES_ERROR),
            format!(
                "{}: 9 > 8",
                error_consts::PROMQL_MAX_POINTS_PER_SERIES_ERROR
            ),
        ] {
            let err = across_the_fanout(&message);
            assert!(
                matches!(&err, QueryError::Execution(m) if *m == message),
                "{err:?}"
            );
        }
    }

    #[test]
    fn an_unparsable_sample_refusal_still_keeps_its_message() {
        let message = format!("{}: lots", error_consts::PROMQL_TOO_MANY_SAMPLES_ERROR);
        let err = across_the_fanout(&message);
        assert!(
            matches!(&err, QueryError::Execution(m) if *m == message),
            "{err:?}"
        );
    }
}
