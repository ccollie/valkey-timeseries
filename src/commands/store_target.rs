//! The `STORE` destination shared by the analysis commands that can persist their output
//! (`TS.FORECAST`, `TS.AUTOFORECAST`, `TS.TREND`, `TS.FILLGAPS`, `TS.SANITIZE`).
//!
//! A [`StoreTarget`] is validated on the main thread, while the real client is still attached:
//! the destination must differ from the source, and the caller's ACL user must be allowed to
//! write it. It then carries only owned bytes, so it can cross to the analysis pool, where a
//! worker thread's context has neither the client's user nor anything to replicate verbatim.
//!
//! Replicas must not recompute the analysis — model fits are not guaranteed to be
//! reproducible, and a large job would stall the replication link — so [`StoreTarget::write`]
//! replicates the *effect* instead: `TS._STORE key <samples> [store options...]`, which a
//! replica applies with the same primitive the primary used (see `ts_store.rs`). Replaying a
//! `TS.DEL`/`TS.MADD` pair would not be equivalent: those feed compaction rules, while a STORE
//! write does not. A serialized `TS._RESTORE` would not be either: series ids differ between a
//! primary and its replicas, so a restored blob would break compaction links on the replica.

use crate::commands::command_parser::StoreOptions;
use crate::common::Sample;
use crate::series::acl::check_key_permissions;
use crate::series::{
    DestinationWriteMode, TimeSeriesOptions, create_or_update_series_with_samples,
};
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult};

/// Name of the internal command a STORE write is replicated as.
pub(super) const STORE_REPLICATION_COMMAND: &str = "TS._STORE";

const STORE_SAME_KEY_ERROR: &str = "TSDB: STORE destination must be different from the source key";

/// Bytes per sample in the `TS._STORE` payload: little-endian `i64` timestamp, then `f64` value.
const ENCODED_SAMPLE_LEN: usize = 16;

/// A validated `STORE` destination.
pub(super) struct StoreTarget {
    key: Vec<u8>,
    options: TimeSeriesOptions,
    write_mode: DestinationWriteMode,
    /// The clause's option tokens as the client sent them, replayed to replicas.
    raw_options: Vec<Vec<u8>>,
}

impl StoreTarget {
    /// Validates `store` for a command reading `source_key`. Must run on the main thread,
    /// where `ctx` still carries the calling client's ACL user.
    pub fn new(ctx: &Context, source_key: &[u8], store: StoreOptions) -> ValkeyResult<Self> {
        if store.key.as_slice() == source_key {
            return Err(ValkeyError::Str(STORE_SAME_KEY_ERROR));
        }
        check_key_permissions(
            ctx,
            &store.key,
            &(AclPermissions::UPDATE | AclPermissions::INSERT),
        )?;
        Ok(Self {
            key: store.key.as_slice().to_vec(),
            options: store.options,
            write_mode: store.write_mode,
            raw_options: store.raw_options,
        })
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Writes `samples` to the destination and replicates the result as `TS._STORE`.
    /// Returns the number of samples written. `ctx` must hold the GIL.
    pub fn write(&self, ctx: &Context, samples: &[Sample]) -> ValkeyResult<usize> {
        let key = ctx.create_string(self.key.as_slice());
        let outcome = create_or_update_series_with_samples(
            ctx,
            &key,
            self.options.clone(),
            self.write_mode,
            samples,
        )?;
        if outcome.changed {
            let payload = encode_store_samples(samples);
            let mut args: Vec<&[u8]> = Vec::with_capacity(2 + self.raw_options.len());
            args.push(&self.key);
            args.push(&payload);
            args.extend(self.raw_options.iter().map(Vec::as_slice));
            ctx.replicate(STORE_REPLICATION_COMMAND, args.as_slice());
        }
        Ok(outcome.written)
    }

    /// Writes `samples` without replicating, for a command that replicates itself verbatim
    /// (and so re-runs this same write on the replica).
    pub fn write_unreplicated(&self, ctx: &Context, samples: &[Sample]) -> ValkeyResult<usize> {
        let key = ctx.create_string(self.key.as_slice());
        create_or_update_series_with_samples(
            ctx,
            &key,
            self.options.clone(),
            self.write_mode,
            samples,
        )
        .map(|outcome| outcome.written)
    }
}

pub(super) fn encode_store_samples(samples: &[Sample]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(samples.len() * ENCODED_SAMPLE_LEN);
    for sample in samples {
        buf.extend_from_slice(&sample.timestamp.to_le_bytes());
        buf.extend_from_slice(&sample.value.to_le_bytes());
    }
    buf
}

pub(super) fn decode_store_samples(buf: &[u8]) -> ValkeyResult<Vec<Sample>> {
    if !buf.len().is_multiple_of(ENCODED_SAMPLE_LEN) {
        return Err(ValkeyError::Str("TSDB: invalid TS._STORE sample payload"));
    }
    Ok(buf
        .chunks_exact(ENCODED_SAMPLE_LEN)
        .map(|chunk| {
            let (timestamp, value) = chunk.split_at(8);
            Sample::new(
                i64::from_le_bytes(timestamp.try_into().expect("8-byte half")),
                f64::from_le_bytes(value.try_into().expect("8-byte half")),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_samples_round_trip_bit_exact() {
        let samples = vec![
            Sample::new(i64::MIN, f64::NAN),
            Sample::new(-1, -0.0),
            Sample::new(0, f64::INFINITY),
            Sample::new(1_700_000_000_000, 42.125),
            Sample::new(i64::MAX, f64::MIN_POSITIVE),
        ];
        let decoded = decode_store_samples(&encode_store_samples(&samples)).unwrap();
        assert_eq!(decoded.len(), samples.len());
        for (a, b) in samples.iter().zip(&decoded) {
            assert_eq!(a.timestamp, b.timestamp);
            assert_eq!(a.value.to_bits(), b.value.to_bits());
        }
    }

    #[test]
    fn store_samples_empty_payload_is_empty() {
        assert!(decode_store_samples(&[]).unwrap().is_empty());
    }

    #[test]
    fn store_samples_reject_truncated_payload() {
        let mut payload = encode_store_samples(&[Sample::new(1, 2.0)]);
        payload.pop();
        assert!(decode_store_samples(&payload).is_err());
    }
}
