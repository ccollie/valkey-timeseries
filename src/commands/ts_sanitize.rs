use crate::analysis::forecasting::imputation::{ImputationPolicy, interpolate_series, sanitize};
use crate::analysis::seasonality::dominant_period;
use crate::commands::command_parser::{
    CommandArgToken, parse_command_arg_token, parse_store_clause, parse_timestamp_range,
    reject_extra_args,
};
use crate::commands::store_target::{StoreTarget, report_store_key_positions};
use crate::common::Sample;
use crate::common::replies::reply_with_samples;
use crate::error_consts;
use crate::series::{TimeSeries, get_timeseries_mut};
use valkey_module::{
    AclPermissions, Context, NextArg, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
    ValkeyValue,
};

acl_categories!(TS_SANITIZE, "ts.sanitize", "write timeseries");
/// ```text
/// TS.SANITIZE key fromTimestamp toTimestamp
///     [POLICY <policy> [options]]
///     [STORE destinationKey
///         [MERGE]
///         [RETENTION retentionPeriod]
///         [ENCODING encoding]
///         [CHUNK_SIZE chunkSize]
///         [DUPLICATE_POLICY duplicatePolicy]
///         [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///         [METRIC metric]
///         [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///     ]
///```
/// Sanitizes missing (NaN/infinite) values in a time series within the given
/// timestamp range (inclusive).
///
/// POLICY is optional and defaults to DROP. When specified, POLICY is one of:
///   - DROP                   - Drop all samples with missing values.
///   - FILL value             - Replace missing values with a constant fill value.
///   - FORWARDFILL            - Forward-fill missing values.
///   - BACKWARDFILL           - Backward-fill missing values.
///   - FILLMEAN               - Replace missing with the mean of valid values.
///   - FILLMEDIAN             - Replace missing with the median of valid values.
///   - INTERPOLATE            - Linearly interpolate between valid neighbors.
///   - FORWARDBACKWARDFILL    - Forward-fill then backward-fill.
///   - MOVINGAVERAGE window   - Replace with moving average (window must be odd > 0).
///   - SEASONAL period|<auto> - Replace with seasonal median (period must be > 0).
///
/// The sanitized range always replaces the source range in place. If STORE is specified, the
/// result is also written to the destination key: with MERGE, samples are merged into an
/// existing destination series; without MERGE (overwrite mode), the destination is cleared
/// first. Returns the number of samples written.
///
/// Without STORE, returns the sanitized samples as `[timestamp, value]` pairs.
#[valkey_module_macros::command({
    name: "ts.sanitize",
    flags: [Write, DenyOOM, GetkeysApi],
    summary: "Replace or drop missing values in a time series.",
    complexity: "O(N) where N is the number of samples in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: [
        {
            notes: "The sanitized samples are written back to the source series.",
            flags: [ReadWrite, Update],
            begin_search: Index({ index: 1 }),
            find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
        },
        {
            notes: "Optional destination series written by the STORE clause.",
            flags: [ReadWrite, Update],
            begin_search: Keyword({ keyword: "STORE", startfrom: 4 }),
            find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
        }
    ]
})]
pub fn ts_sanitize_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    if report_store_key_positions(ctx, &args) {
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Parse key and timestamp range
    let key = args.next_arg()?;
    let date_range = parse_timestamp_range(&mut args)?;

    // Get a mutable reference to the series (must exist, need UPDATE permission)
    let mut series = get_timeseries_mut(ctx, &key, Some(AclPermissions::UPDATE))?;
    let (start_ts, end_ts) = date_range.get_series_range(&series, None, false);

    // Get existing samples in the range
    let mut samples = series.get_range(start_ts, end_ts);

    // POLICY is optional; defaults to DROP.
    let policy = if args
        .peek()
        .is_some_and(|s| s.as_slice().eq_ignore_ascii_case(b"policy"))
    {
        args.next(); // consume POLICY
        let policy_token = args.next_str()?.to_uppercase();
        parse_policy(&policy_token, &mut args, &samples)?
    } else {
        ImputationPolicy::Drop
    };

    // STORE destination (optional)
    let destination = if args
        .peek()
        .is_some_and(|s| parse_command_arg_token(s) == Some(CommandArgToken::Store))
    {
        args.next(); // consume STORE
        let store = parse_store_clause(&mut args)?;
        Some(StoreTarget::new(ctx, key.as_slice(), store)?)
    } else {
        None
    };
    reject_extra_args(&mut args)?;

    // Resolved before `policy` moves into sanitize(), for the replicated command below.
    let policy_args = policy_tokens(&policy);

    // Capture policy variant before moving `policy` into sanitize().
    // - MA/Seasonal: samples is NOT modified; `sanitized` is the full imputed result.
    // - All others (including Drop): samples is modified in-place.
    let is_ma_or_seasonal = matches!(
        &policy,
        ImputationPolicy::MovingAverage(_) | ImputationPolicy::Seasonal(_)
    );

    // Keys change from here on and a change can't be undone, so everything a client can cause
    // to fail is checked first. A destination of another type fails even when there is nothing
    // to write; one that can't be created is checked once the result is known.
    if let Some(dest) = &destination {
        dest.check_destination_type(ctx)?;
    }

    // The range as stored, to tell afterwards what sanitizing changed.
    let original = samples.clone();

    // Apply the sanitization policy
    let sanitized = sanitize(&mut samples, policy)
        .map_err(|e| ValkeyError::String(format!("TSDB: sanitize error: {e}")))?;

    // - MovingAverage/Seasonal: sanitized is the full imputed result.
    // - All others (including Drop): samples has been modified in-place.
    let to_return: &Vec<Sample> = if is_ma_or_seasonal {
        &sanitized
    } else {
        &samples
    };

    // A destination that can't be created (a `METRIC` another series already holds) is the one
    // failure a client can still cause, and it only matters when a write follows: an empty
    // result never creates the destination.
    if let Some(dest) = &destination
        && !to_return.is_empty()
    {
        dest.check_destination_writable(ctx)?;
    }

    // The destination is written first: if that fails the source is still as the client left
    // it. `write_unreplicated` because the replica re-runs this whole command, and so this same
    // write, from the replicated command below.
    let stored = destination
        .as_ref()
        .map(|dest| dest.write_unreplicated(ctx, to_return))
        .transpose()?;

    // --- Write sanitized samples back to the source series ---
    write_back(
        &mut series,
        start_ts,
        end_ts,
        to_return,
        &diff_range(&original, to_return),
    )?;
    // --- End write-back ---

    // Replication comes after the last write, so an error reply never follows a replicated
    // command. What remains is a failure inside a write itself (an internal error, not
    // something a client can cause), which can leave the first key written and nothing
    // replicated.
    //
    // Sanitizing runs inline, so the replica re-runs the command rather than replaying its
    // effect. That covers the STORE write above too, which must therefore not replicate itself.
    // What it re-runs is the command with its inputs resolved: the range grammar accepts `*` and
    // relative offsets, and `SEASONAL auto` is detected from the data, so the verbatim command
    // would resolve differently on a replica (or an AOF replay at restart) than it did here.
    let (repl_start, repl_end) = replicable_bounds(start_ts, end_ts);
    let (repl_start, repl_end) = (repl_start.to_string(), repl_end.to_string());
    let mut repl_args: Vec<&[u8]> = vec![
        key.as_slice(),
        repl_start.as_bytes(),
        repl_end.as_bytes(),
        b"POLICY",
    ];
    repl_args.extend(policy_args.iter().map(String::as_bytes));
    if let Some(dest) = &destination {
        repl_args.extend(dest.replication_clause());
    }
    ctx.replicate("TS.SANITIZE", repl_args.as_slice());
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.sanitize", &key);

    if let Some(written) = stored {
        return Ok(ValkeyValue::from(written));
    }

    reply_with_samples(ctx, to_return.iter().cloned());
    Ok(ValkeyValue::NoReply)
}

/// What sanitizing changed within the range it read.
#[derive(Debug, Default, PartialEq)]
struct RangeChanges {
    /// How many stored samples the result no longer contains (only `DROP` removes any).
    removed: usize,
    /// Samples whose value differs from what is stored, ascending by timestamp.
    changed: Vec<Sample>,
}

/// Two values are the same if they are the same bits, or both NaN: a NaN left as it was is not a
/// change, whatever its payload.
fn same_value(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
}

/// Compares the range as stored (`original`) with the sanitized `result`, matching samples by
/// timestamp.
fn diff_range(original: &[Sample], result: &[Sample]) -> RangeChanges {
    let sorted;
    let result = if result.is_sorted_by_key(|s| s.timestamp) {
        result
    } else {
        sorted = {
            let mut copy = result.to_vec();
            copy.sort_by_key(|s| s.timestamp);
            copy
        };
        &sorted
    };

    let mut changes = RangeChanges::default();
    let (mut o, mut r) = (0, 0);
    loop {
        match (original.get(o), result.get(r)) {
            (None, None) => break,
            (Some(stored), Some(new)) if stored.timestamp == new.timestamp => {
                if !same_value(stored.value, new.value) {
                    changes.changed.push(*new);
                }
                o += 1;
                r += 1;
            }
            (Some(stored), Some(new)) if stored.timestamp < new.timestamp => {
                changes.removed += 1;
                o += 1;
            }
            (Some(_), None) => {
                changes.removed += 1;
                o += 1;
            }
            // In the result only: not stored yet.
            (_, Some(new)) => {
                changes.changed.push(*new);
                r += 1;
            }
        }
    }
    changes
}

/// Applies a sanitized `result` of the range `[start_ts, end_ts]` to the source series, writing
/// only what changed.
///
/// A value that was imputed is written over the one stored, so nothing is deleted and nothing
/// else is touched. `DROP` is the one policy that removes samples; clearing the range in one
/// pass and putting the kept samples back costs one chunk rewrite, where deleting each gap
/// separately re-encodes a chunk for every one of them.
///
/// Either way the write bypasses the series' IGNORE filter (see
/// [`TimeSeries::overwrite_samples`]), which would otherwise judge each sample against whatever
/// happens to precede it and drop some of them.
fn write_back(
    series: &mut TimeSeries,
    start_ts: i64,
    end_ts: i64,
    result: &[Sample],
    changes: &RangeChanges,
) -> ValkeyResult<()> {
    if changes.removed == 0 {
        return overwrite(series, &changes.changed);
    }
    series
        .remove_range(start_ts, end_ts)
        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
    let mut kept = result.to_vec();
    kept.sort_by_key(|s| s.timestamp);
    overwrite(series, &kept)
}

/// Writes `samples` (ascending by timestamp) over the series, failing if any is not stored.
fn overwrite(series: &mut TimeSeries, samples: &[Sample]) -> ValkeyResult<()> {
    let outcomes = series
        .overwrite_samples(samples)
        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
    match samples
        .iter()
        .zip(&outcomes)
        .find(|(_, outcome)| !outcome.is_ok())
    {
        None => Ok(()),
        Some((sample, outcome)) => Err(ValkeyError::String(format!(
            "TSDB: could not write the sanitized sample at {}: {outcome}",
            sample.timestamp
        ))),
    }
}

/// The bounds to replicate for a range already resolved to `[start_ts, end_ts]`.
///
/// Plain integers re-parse as absolute timestamps, but only non-negative ones: a negative
/// operand is rejected, and `-3600000` would not read as an offset either. Samples never sit
/// below zero, so a negative start clamps to 0 without changing the window, and a window that
/// ends below zero (a relative end earlier than the start) is replicated as the inverted
/// range `1 0`, which selects nothing just as it did here.
fn replicable_bounds(start_ts: i64, end_ts: i64) -> (i64, i64) {
    if end_ts < 0 {
        (1, 0)
    } else {
        (start_ts.max(0), end_ts)
    }
}

/// The `POLICY` operands that make `parse_policy` rebuild exactly `policy`, with any value
/// the original command left to be inferred (`SEASONAL auto`) already resolved.
fn policy_tokens(policy: &ImputationPolicy) -> Vec<String> {
    match policy {
        ImputationPolicy::Error => vec!["ERROR".into()],
        ImputationPolicy::Drop => vec!["DROP".into()],
        // `{:e}` is the shortest form that parses back to the same bits, and keeps a value like
        // 1e300 from being written out as 301 digits.
        ImputationPolicy::Fill(value) => vec!["FILL".into(), format!("{value:e}")],
        ImputationPolicy::ForwardFill => vec!["FORWARDFILL".into()],
        ImputationPolicy::BackwardFill => vec!["BACKWARDFILL".into()],
        ImputationPolicy::FillMean => vec!["FILLMEAN".into()],
        ImputationPolicy::FillMedian => vec!["FILLMEDIAN".into()],
        ImputationPolicy::Interpolate => vec!["INTERPOLATE".into()],
        ImputationPolicy::ForwardBackwardFill => vec!["FORWARDBACKWARDFILL".into()],
        ImputationPolicy::MovingAverage(window) => vec!["MOVINGAVERAGE".into(), window.to_string()],
        ImputationPolicy::Seasonal(period) => vec!["SEASONAL".into(), period.to_string()],
    }
}

/// Parse the imputation policy and any policy-specific arguments.
fn parse_policy(
    token: &str,
    args: &mut impl Iterator<Item = ValkeyString>,
    samples: &[Sample],
) -> ValkeyResult<ImputationPolicy> {
    let mut policy = ImputationPolicy::Error;
    hashify::fnc_map_ignore_case!(
        token.as_bytes(),
        "Error" => { /* already the default */ },
        "Drop" => { policy = ImputationPolicy::Drop; },
        "Fill" => {
            let value_str = args.next_str()?;
            let value: f64 = value_str
                .parse()
                .map_err(|_| ValkeyError::Str("TSDB: invalid fill value"))?;
            policy = ImputationPolicy::Fill(value);
        },
        "ForwardFill" => { policy = ImputationPolicy::ForwardFill; },
        "BackwardFill" => { policy = ImputationPolicy::BackwardFill; },
        "FillMean" => { policy = ImputationPolicy::FillMean; },
        "FillMedian" => { policy = ImputationPolicy::FillMedian; },
        "Interpolate" => { policy = ImputationPolicy::Interpolate; },
        "ForwardBackwardFill" => { policy = ImputationPolicy::ForwardBackwardFill; },
        "MovingAverage" => {
            let window_str = args.next_str()?;
            let window: usize = window_str
                .parse()
                .map_err(|_| ValkeyError::Str("TSDB: invalid MovingAverage window"))?;
            if window == 0 || window.is_multiple_of(2) {
                return Err(ValkeyError::Str("TSDB: MovingAverage window must be an odd positive integer"));
            }
            policy = ImputationPolicy::MovingAverage(window);
        },
        "Seasonal" => {
            let period_str = args.next_str()?;
            let period = if period_str.eq_ignore_ascii_case("auto") {
                infer_seasonal_period(samples)?
            } else {
                period_str
                    .parse()
                    .map_err(|_| ValkeyError::Str("TSDB: invalid Seasonal period"))?
            };
            if period == 0 {
                return Err(ValkeyError::Str("TSDB: Seasonal period must be a positive integer"));
            }
            policy = ImputationPolicy::Seasonal(period);
        },
        _ => { return Err(ValkeyError::Str(error_consts::INVALID_ARGUMENT)); }
    );
    Ok(policy)
}

fn infer_seasonal_period(samples: &[Sample]) -> ValkeyResult<usize> {
    // The values being imputed are missing by definition, and period detection cannot see
    // through them, so detect on a linearly gap-filled copy.
    let values: Vec<f64> = interpolate_series(samples, true)
        .iter()
        .map(|s| s.value)
        .collect();
    dominant_period(&values).ok_or(ValkeyError::Str(
        "TSDB: unable to detect dominant period for seasonal imputation",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicated_policy_names_every_variant_with_its_resolved_operands() {
        let cases = [
            (ImputationPolicy::Error, vec!["ERROR"]),
            (ImputationPolicy::Drop, vec!["DROP"]),
            (ImputationPolicy::ForwardFill, vec!["FORWARDFILL"]),
            (ImputationPolicy::BackwardFill, vec!["BACKWARDFILL"]),
            (ImputationPolicy::FillMean, vec!["FILLMEAN"]),
            (ImputationPolicy::FillMedian, vec!["FILLMEDIAN"]),
            (ImputationPolicy::Interpolate, vec!["INTERPOLATE"]),
            (
                ImputationPolicy::ForwardBackwardFill,
                vec!["FORWARDBACKWARDFILL"],
            ),
            (
                ImputationPolicy::MovingAverage(5),
                vec!["MOVINGAVERAGE", "5"],
            ),
            // `SEASONAL auto` reaches here already resolved to the detected period.
            (ImputationPolicy::Seasonal(24), vec!["SEASONAL", "24"]),
        ];
        for (policy, expected) in cases {
            assert_eq!(policy_tokens(&policy), expected, "{policy:?}");
        }
    }

    #[test]
    fn replicated_fill_value_parses_back_to_the_same_bits() {
        for value in [
            0.0,
            -0.0,
            0.1,
            -42.5,
            1e300,
            f64::MIN_POSITIVE,
            5e-324,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ] {
            let tokens = policy_tokens(&ImputationPolicy::Fill(value));
            assert_eq!(tokens[0], "FILL");
            let parsed: f64 = tokens[1].parse().unwrap();
            if value.is_nan() {
                assert!(parsed.is_nan());
            } else {
                assert_eq!(
                    parsed.to_bits(),
                    value.to_bits(),
                    "{value} -> {}",
                    tokens[1]
                );
            }
        }
    }

    fn samples(values: &[(i64, f64)]) -> Vec<Sample> {
        values.iter().map(|&(t, v)| Sample::new(t, v)).collect()
    }

    #[test]
    fn diff_reports_only_imputed_values() {
        let original = samples(&[(1, 1.0), (2, f64::NAN), (3, 3.0), (4, f64::INFINITY)]);
        let result = samples(&[(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]);

        let changes = diff_range(&original, &result);

        assert_eq!(changes.removed, 0);
        assert_eq!(changes.changed, samples(&[(2, 2.0), (4, 4.0)]));
    }

    #[test]
    fn diff_counts_dropped_samples() {
        let original = samples(&[
            (1, 1.0),
            (2, f64::NAN),
            (3, f64::NAN),
            (4, 4.0),
            (5, f64::NAN),
        ]);
        let result = samples(&[(1, 1.0), (4, 4.0)]);

        let changes = diff_range(&original, &result);

        assert_eq!(changes.removed, 3);
        assert!(changes.changed.is_empty());
    }

    #[test]
    fn diff_treats_a_nan_left_as_it_was_as_unchanged() {
        // A leading NaN that FORWARDFILL has nothing to fill from stays NaN.
        let original = samples(&[(1, f64::NAN), (2, 2.0)]);
        let result = samples(&[(1, f64::from_bits(0x7ff8_0000_0000_0001)), (2, 2.0)]);

        assert_eq!(diff_range(&original, &result), RangeChanges::default());
    }

    #[test]
    fn diff_of_a_clean_range_is_empty() {
        let original = samples(&[(1, 1.0), (2, -0.0), (3, 3.0)]);

        assert_eq!(diff_range(&original, &original), RangeChanges::default());
        assert_eq!(diff_range(&[], &[]), RangeChanges::default());
    }

    #[test]
    fn diff_tolerates_an_unsorted_result_and_unstored_timestamps() {
        let original = samples(&[(1, 1.0), (2, f64::NAN), (3, 3.0)]);
        let result = samples(&[(3, 3.0), (2, 2.0), (1, 1.0), (9, 9.0)]);

        let changes = diff_range(&original, &result);

        assert_eq!(changes.removed, 0);
        assert_eq!(changes.changed, samples(&[(2, 2.0), (9, 9.0)]));
    }

    #[test]
    fn replicated_bounds_are_absolute_non_negative_integers() {
        // An ordinary window is replicated as resolved.
        assert_eq!(replicable_bounds(1_000, 5_000), (1_000, 5_000));
        // An inverted window stays inverted, and selects nothing on the replica as here.
        assert_eq!(replicable_bounds(5_000, 1_000), (5_000, 1_000));
        // Nothing sits below zero, so clamping the start leaves the window as it was.
        assert_eq!(replicable_bounds(-3_600_000, 5_000), (0, 5_000));
        // A window ending below zero cannot be written as a timestamp; it becomes `1 0`.
        assert_eq!(replicable_bounds(0, -3_600_000), (1, 0));
        assert_eq!(replicable_bounds(i64::MIN, i64::MIN), (1, 0));
        for (start, end) in [(0, i64::MAX), (i64::MIN, i64::MAX), (7, -1)] {
            let (s, e) = replicable_bounds(start, end);
            assert!(s >= 0 && e >= 0);
        }
    }
}
