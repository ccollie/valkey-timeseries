use crate::analysis::seasonality::Seasonality;
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::{AnalysisTimeout, parse_timeout, run_analysis};
use crate::commands::command_parser::parse_series_range_samples;
use crate::common::replies::{
    IntoRawCtx, reply_with_array, reply_with_double, reply_with_integer, reply_with_str,
};
use anofox_forecast::detection::{PeriodDetectionConfig, detect_periods};
use anofox_forecast::seasonality::{MSTL, MSTLResult, STL, STLResult};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const MAX_SEASONALITY_PERIODS: usize = 4;

/// ```text
/// TS.DECOMPOSE key startTimestamp endTimestamp
///     [SEASONALITY "auto"|period [period...]]
///     [TIMEOUT ms]
/// ```
///
/// `TS.DECOMPOSE` decomposes a time series into its constituent components:
/// trend, seasonality, and residual.
///
/// If SEASONALITY is "auto", the seasonal period(s) are automatically inferred
/// from the data.
///
/// If a single period is specified, STL decomposition is used.
/// If multiple periods are specified, MSTL decomposition is used.
#[valkey_module_macros::command({
    name: "ts.decompose",
    flags: [ReadOnly, DenyOOM],
    summary: "Decompose a time series into trend, seasonal and residual components.",
    complexity: "O(N*P) where N is the number of samples in the range and P is the number of seasonal periods.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_decompose_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    let seasonality = parse_seasonality(&mut args)?;

    let mut timeout = AnalysisTimeout::default();
    while let Some(arg) = args.peek() {
        if arg.as_slice().eq_ignore_ascii_case(b"TIMEOUT") {
            args.next();
            timeout.set(parse_timeout(&mut args)?);
        } else {
            break;
        }
    }

    args.done()?;

    let timestamps: Vec<i64> = samples.iter().map(|s| s.timestamp).collect();
    let sample_count = values.len();

    run_analysis(
        ctx,
        sample_count,
        INLINE_MAX_SAMPLES,
        timeout,
        move || {
            let result = decompose(&values, seasonality)?;
            Ok((values, result))
        },
        move |actx, (values, result)| {
            let ctx = actx.reply_ctx();
            match result {
                Decomposition::Stl(result) => reply_stl_result(&ctx, &timestamps, &values, &result),
                Decomposition::Mstl(result) => {
                    reply_mstl_result(&ctx, &timestamps, &values, &result)
                }
            }
        },
    )
}

/// Largest range that runs on the main thread. STL is ~25 ms here and 1.7–8 s at
/// 200k samples (release build), so anything bigger goes to the pool.
const INLINE_MAX_SAMPLES: usize = 2_000;

enum Decomposition {
    Stl(STLResult),
    Mstl(MSTLResult),
}

/// Resolve the seasonal period(s) and run STL (one period) or MSTL (several).
fn decompose(values: &[f64], seasonality: Seasonality) -> ValkeyResult<Decomposition> {
    let periods = match seasonality {
        Seasonality::Periods(periods) => periods,
        Seasonality::Auto => {
            let config = PeriodDetectionConfig::default();
            let periods = detect_periods(values, &config);
            periods.iter().map(|p| p.period).collect()
        }
    };

    if periods.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: at least one seasonality period is required",
        ));
    }

    let n = values.len();

    if periods.len() == 1 {
        let period = periods[0];
        if n < 2 * period {
            return Err(ValkeyError::String(format!(
                "TSDB: insufficient data for STL decomposition. Need at least {} samples, got {}",
                2 * period,
                n
            )));
        }

        STL::new(period)
            .robust()
            .decompose(values)
            .map(Decomposition::Stl)
            .ok_or(ValkeyError::Str("TSDB: STL decomposition failed"))
    } else {
        let max_period = *periods.iter().max().unwrap_or(&0);
        if n < 2 * max_period {
            return Err(ValkeyError::String(format!(
                "TSDB: insufficient data for MSTL decomposition. Need at least {} samples, got {}",
                2 * max_period,
                n
            )));
        }

        MSTL::new(periods)
            .robust()
            .decompose(values)
            .map(Decomposition::Mstl)
            .ok_or(ValkeyError::Str("TSDB: MSTL decomposition failed"))
    }
}

fn parse_seasonality(args: &mut CommandArgIterator) -> ValkeyResult<Seasonality> {
    // If no more args, default to Auto
    let Some(arg) = args.peek() else {
        return Ok(Seasonality::Auto);
    };

    // Check if the next argument is SEASONALITY
    if !arg.as_slice().eq_ignore_ascii_case(b"SEASONALITY") {
        // No explicit SEASONALITY keyword — default to Auto
        return Ok(Seasonality::Auto);
    }

    args.next(); // consume SEASONALITY

    // Check for "auto"
    if let Some(next_arg) = args.peek()
        && next_arg.as_slice().eq_ignore_ascii_case(b"auto")
    {
        args.next(); // consume auto
        return Ok(Seasonality::Auto);
    }

    let mut periods: Vec<usize> = Vec::with_capacity(4);

    // Loop while the next token is a number
    while let Some(v) = args.peek() {
        if let Ok(value) = v.parse_unsigned_integer() {
            periods.push(value as usize);
            args.next();
            continue;
        }
        break;
    }

    if periods.is_empty() || periods.len() > MAX_SEASONALITY_PERIODS {
        return Err(ValkeyError::Str(
            "TSDB: invalid SEASONALITY periods. Expected 1-4 period values or 'auto'",
        ));
    }

    // Periods should be unique and sorted
    periods.sort_unstable();
    if !periods.windows(2).all(|w| w[0] != w[1]) {
        return Err(ValkeyError::Str("TSDB: SEASONALITY periods must be unique"));
    }

    Ok(Seasonality::Periods(periods))
}

/// Reply with STL decomposition result.
///
/// Response format (array of 4):
///   "original" -> [[ts, val], ...]
///   "trend" -> [[ts, val], ...]
///   "seasonal" -> [[ts, val], ...]
///   "residual" -> [[ts, val], ...]
fn reply_stl_result<C: IntoRawCtx + Copy>(
    ctx: C,
    timestamps: &[i64],
    original: &[f64],
    result: &STLResult,
) -> ValkeyResult {
    reply_with_array(ctx, 8);

    // original
    reply_with_str(ctx, "original");
    reply_sample_array(ctx, timestamps, original);

    // trend
    reply_with_str(ctx, "trend");
    reply_sample_array(ctx, timestamps, &result.trend);

    // seasonal
    reply_with_str(ctx, "seasonal");
    reply_sample_array(ctx, timestamps, &result.seasonal);

    // residual
    reply_with_str(ctx, "residual");
    reply_sample_array(ctx, timestamps, &result.remainder);

    Ok(ValkeyValue::NoReply)
}

/// Reply with MSTL decomposition result.
///
/// Response format (array of 8):
///   "original"              -> [[ts, val], ...]
///   "trend"                 -> [[ts, val], ...]
///   "seasonal_components"   -> [ [period, [[ts, val], ...]], ... ]
///   "residual"              -> [[ts, val], ...]
fn reply_mstl_result<C: IntoRawCtx + Copy>(
    ctx: C,
    timestamps: &[i64],
    original: &[f64],
    result: &MSTLResult,
) -> ValkeyResult {
    reply_with_array(ctx, 8);

    // original
    reply_with_str(ctx, "original");
    reply_sample_array(ctx, timestamps, original);

    // trend
    reply_with_str(ctx, "trend");
    reply_sample_array(ctx, timestamps, &result.trend);

    // seasonal_components (array of [period, [samples]])
    reply_with_str(ctx, "seasonal_components");
    reply_with_array(ctx, result.seasonal_components.len());
    for (idx, seasonal) in result.seasonal_components.iter().enumerate() {
        reply_with_array(ctx, 2);
        reply_with_integer(ctx, result.seasonal_periods[idx] as i64);
        reply_sample_array(ctx, timestamps, seasonal);
    }

    // residual
    reply_with_str(ctx, "residual");
    reply_sample_array(ctx, timestamps, &result.remainder);

    Ok(ValkeyValue::NoReply)
}

/// Reply with an array of [timestamp, value] pairs.
fn reply_sample_array<C: IntoRawCtx + Copy>(ctx: C, timestamps: &[i64], values: &[f64]) {
    reply_with_array(ctx, timestamps.len());
    for (ts, val) in timestamps.iter().zip(values.iter()) {
        reply_with_array(ctx, 2);
        reply_with_integer(ctx, *ts);
        reply_with_double(ctx, *val);
    }
}
