use crate::analysis::MAX_ANALYSIS_LAG;
use crate::commands::analysis_runner::{AnalysisTimeout, WorkLimits, parse_timeout, run_analysis};
use crate::commands::command_parser::parse_series_range_samples;
use crate::commands::command_parser::reject_extra_args;
use anofox_forecast::features::autocorrelation;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

acl_categories!(TS_AUTOCORRELATION, "ts.autocorrelation", "read timeseries");
/// ```text
/// TS.AUTOCORRELATION key startTime endTime lag
/// [PARTIAL | TRA | AGGREGATED <mean|var|std|median>] [TIMEOUT ms]
/// ```
///
/// `TS.AUTOCORRELATION` computes autocorrelation-based statistics on a time series.
///
/// By default, returns the autocorrelation function (ACF) value at the specified lag.
///
/// Options:
/// - `PARTIAL`: Returns the partial autocorrelation (PACF) at the specified lag.
/// - `TRA`: Returns the time reversal asymmetry statistic at the specified lag.
/// - `AGGREGATED <mean|var|std|median>`: Returns aggregated autocorrelation across lags
///   1..=lag, using the specified aggregation function.
#[valkey_module_macros::command({
    name: "ts.autocorrelation",
    flags: [ReadOnly, DenyOOM],
    summary: "Compute autocorrelation statistics for a time series at a given lag.",
    complexity: "O(N*L) where N is the number of samples in the range and L is the lag.",
    since: "1.0.0",
    arity: -5,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_autocorrelation_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Parse lag
    let lag_str = args.next_arg()?;
    let lag: i64 = lag_str
        .parse_integer()
        .map_err(|_| ValkeyError::Str("TSDB: invalid lag value"))?;

    if lag < 0 {
        return Err(ValkeyError::Str("TSDB: lag must be a non-negative integer"));
    }

    let lag = lag as usize;

    let mut kind = Kind::Plain;
    let mut timeout = AnalysisTimeout::default();
    while let Some(arg) = args.peek() {
        let arg = arg.as_slice();
        hashify::fnc_map_ignore_case!(
            arg,
            "PARTIAL" => {
                args.next();
                kind = Kind::Partial;
            },
            "TRA" => {
                args.next();
                kind = Kind::Tra;
            },
            "AGGREGATED" => {
                args.next();
                let agg_str = args.next_str()?;
                let valid =  hashify::set_ignore_case! {
                    agg_str.as_bytes(),
                    "MEAN",
                    "VAR",
                    "STD",
                    "MEDIAN",
                };
                if !valid {
                    return Err(ValkeyError::Str(
                        "TSDB: invalid AGGREGATED function. Expected mean, var, std, or median"
                    ));
                }
                kind = Kind::Aggregated(agg_str.to_ascii_lowercase());
            },
            "TIMEOUT" => {
                args.next();
                timeout.set(parse_timeout(&mut args)?);
            },
            _ => return Err(ValkeyError::String("TSDB: unrecognized option".to_string()))
        )
    }

    reject_extra_args(&mut args)?;

    // Plain and TRA are a single pass whatever the lag; these two grow with it.
    if matches!(kind, Kind::Partial | Kind::Aggregated(_)) && lag > MAX_ANALYSIS_LAG {
        return Err(ValkeyError::String(format!(
            "TSDB: lag must not exceed {MAX_ANALYSIS_LAG} with PARTIAL or AGGREGATED"
        )));
    }

    // Data checks come after the options, so a malformed call reports its syntax error first.
    if values.len() <= lag {
        return Err(ValkeyError::String(format!(
            "TSDB: insufficient data for lag {lag}. Need at least {} samples, got {}",
            lag + 1,
            values.len()
        )));
    }
    if matches!(kind, Kind::Tra) && values.len() <= lag.saturating_mul(2) {
        return Err(ValkeyError::String(format!(
            "TSDB: insufficient data for TRA with lag {lag}. Need at least {} samples, got {}",
            lag.saturating_mul(2).saturating_add(1),
            values.len()
        )));
    }

    // PARTIAL and AGGREGATED revisit the series once per lag.
    let work = match kind {
        Kind::Partial | Kind::Aggregated(_) => values.len().saturating_mul(lag + 1),
        Kind::Plain | Kind::Tra => values.len(),
    };
    run_analysis(
        ctx,
        work,
        LIMITS,
        timeout,
        move || {
            let result = match &kind {
                Kind::Plain => autocorrelation::autocorrelation(&values, lag),
                Kind::Partial => autocorrelation::partial_autocorrelation(&values, lag),
                Kind::Tra => autocorrelation::time_reversal_asymmetry_statistic(&values, lag),
                Kind::Aggregated(agg) => autocorrelation::agg_autocorrelation(&values, lag, agg),
            };
            if result.is_nan() {
                return Err(ValkeyError::Str(
                    "TSDB: autocorrelation computation returned NaN",
                ));
            }
            Ok(result)
        },
        |_actx, result| Ok(ValkeyValue::Float(result)),
    )
}

/// Which statistic to compute.
enum Kind {
    Plain,
    Partial,
    Tra,
    Aggregated(String),
}

/// Work is counted in samples (× (lag + 1) for PARTIAL and AGGREGATED, which revisit the series
/// once per lag). Plain and TRA are linear in the range, and PARTIAL is ~2 ns a sample-lag
/// (~46 ms for 200k samples at lag 100, release build), so the bars are high. Up to `inline_max`
/// it runs on the main thread, anything bigger goes to the pool; where the client cannot be
/// blocked the pool is not available and `unblockable_max` (about 0.25 s) is the most it will
/// take.
const LIMITS: WorkLimits = WorkLimits {
    inline_max: 50_000,
    unblockable_max: 100_000_000,
    unit: "sample-lags",
};
