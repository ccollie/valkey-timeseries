use crate::commands::analysis_runner::{AnalysisTimeout, parse_timeout, run_analysis};
use crate::commands::command_parser::parse_series_range_samples;
use crate::common::replies::{
    reply_with_array, reply_with_double, reply_with_integer, reply_with_null,
};
use anofox_forecast::detection::period::{PeriodDetectionConfig, detect_periods};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// ```text
/// TS.PERIODS key startTimestamp endTimestamp [MIN_STRENGTH minStrength] [DOMINANT] [TIMEOUT ms]
/// ```
///
/// `TS.PERIODS` detects seasonal periods in a time series using the SAZED algorithm.
///
/// By default, all detected periods are returned as an array of arrays with
/// metadata about each period (period, power, strength, acf, n_cycles).
///
/// If the `DOMINANT` option is specified, only the single dominant period
/// (integer) is returned, or `nil` if no significant period is found.
///
/// `MIN_STRENGTH` sets the minimum seasonal differencing strength (0–1) for a
/// period to be accepted. The default is 0.05. Set to 0 to disable strength filtering.
#[valkey_module_macros::command({
    name: "ts.periods",
    flags: [ReadOnly, DenyOOM],
    summary: "Detect seasonal periods in a time series.",
    complexity: "O(N log N) where N is the number of samples in the range.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_periods_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    if values.len() < 4 {
        return Err(ValkeyError::Str(
            "TSDB: insufficient data for period detection. Need at least 4 samples.",
        ));
    }

    // Parse optional arguments: MIN_STRENGTH, DOMINANT and TIMEOUT
    let mut min_strength: Option<f64> = None;
    let mut dominant = false;
    let mut timeout = AnalysisTimeout::default();

    while let Some(arg) = args.peek() {
        if arg.as_slice().eq_ignore_ascii_case(b"MIN_STRENGTH") {
            args.next(); // consume MIN_STRENGTH
            let val = args.next_arg()?;
            let strength = val
                .parse_float()
                .map_err(|_| ValkeyError::Str("TSDB: invalid MIN_STRENGTH value"))?;
            if !(0.0..=1.0).contains(&strength) {
                return Err(ValkeyError::String(format!(
                    "TSDB: MIN_STRENGTH must be between 0 and 1, got {}",
                    strength
                )));
            }
            min_strength = Some(strength);
        } else if arg.as_slice().eq_ignore_ascii_case(b"DOMINANT") {
            args.next(); // consume DOMINANT
            dominant = true;
        } else if arg.as_slice().eq_ignore_ascii_case(b"TIMEOUT") {
            args.next(); // consume TIMEOUT
            timeout.set(parse_timeout(&mut args)?);
        } else {
            return Err(ValkeyError::String(format!(
                "TSDB: Unknown argument: {}",
                arg
            )));
        }
    }

    args.done()?;

    let config = PeriodDetectionConfig {
        min_strength: min_strength.unwrap_or(0.05),
        max_periods: if dominant { 1 } else { 5 },
        ..Default::default()
    };

    let sample_count = values.len();
    run_analysis(
        ctx,
        sample_count,
        INLINE_MAX_SAMPLES,
        timeout,
        move || Ok(detect_periods(&values, &config)),
        move |actx, periods| {
            let ctx = actx.reply_ctx();
            if dominant {
                match periods.first() {
                    Some(p) => {
                        reply_with_integer(&ctx, p.period as i64);
                    }
                    None => {
                        reply_with_null(&ctx);
                    }
                }
            } else {
                reply_with_array(&ctx, periods.len());
                for p in &periods {
                    // Each period returned as an array: [period, power, strength, acf, n_cycles]
                    reply_with_array(&ctx, 5);
                    reply_with_integer(&ctx, p.period as i64);
                    reply_with_double(&ctx, p.power);
                    reply_with_double(&ctx, p.strength);
                    reply_with_double(&ctx, p.acf);
                    reply_with_integer(&ctx, p.n_cycles as i64);
                }
            }
            Ok(ValkeyValue::NoReply)
        },
    )
}

/// Largest range that runs on the main thread. Period detection is ~7 ms at this
/// size and ~200 ms at 20k (release build), so anything bigger goes to the pool.
const INLINE_MAX_SAMPLES: usize = 5_000;
