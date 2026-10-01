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
use crate::series::{DuplicatePolicy, get_timeseries_mut};
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

    // The source is rewritten below before the destination is written; check the destination
    // first so a WRONGTYPE destination fails the command before anything has changed.
    if let Some(dest) = &destination {
        dest.check_destination_type(ctx)?;
    }

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

    // --- Write sanitized samples back to the source series ---
    // Remove the old range, then merge the sanitized result.
    series
        .remove_range(start_ts, end_ts)
        .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;

    if !to_return.is_empty() {
        let mut sorted = to_return.to_vec();
        sorted.sort_by_key(|s| s.timestamp);
        series
            .merge_samples(&sorted, Some(DuplicatePolicy::KeepLast))
            .map_err(|e| ValkeyError::String(format!("TSDB: {e}")))?;
    }

    // Sanitizing runs inline, so the replica re-runs the command rather than replaying its
    // effect. That covers the STORE write below too, which must therefore not replicate itself.
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
    // --- End write-back ---

    if let Some(dest) = destination {
        let written = dest.write_unreplicated(ctx, to_return)?;
        return Ok(ValkeyValue::from(written));
    }

    reply_with_samples(ctx, to_return.iter().cloned());
    Ok(ValkeyValue::NoReply)
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
