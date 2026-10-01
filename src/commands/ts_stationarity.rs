use crate::analysis::MAX_ANALYSIS_LAG;
use crate::commands::analysis_runner::{AnalysisTimeout, WorkLimits, parse_timeout, run_analysis};
use crate::commands::command_parser::parse_series_range_samples;
use crate::commands::command_parser::reject_extra_args;
use crate::common::replies::{
    IntoRawCtx, reply_with_integer, reply_with_map, reply_with_statistic, reply_with_str,
};
use anofox_forecast::validation::stationarity::{self, StationarityResult};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

const MIN_SAMPLES: usize = 10;

acl_categories!(TS_STATIONARITY, "ts.stationarity", "read timeseries");
/// ```text
/// TS.STATIONARITY key startTime endTime
///     [TEST adf|kpss|combined]
///     [LAGS n]
///     [TIMEOUT ms]
/// ```
///
/// `TS.STATIONARITY` tests whether a time series is stationary.
///
/// Stationarity is a key property for many forecasting models. A stationary series
/// has constant mean, variance, and autocorrelation over time.
///
/// Options:
/// - `TEST`: Which test to run. Defaults to `combined`.
///   - `adf` — Augmented Dickey-Fuller test (null: series has unit root, i.e., non-stationary)
///   - `kpss` — KPSS test (null: series is stationary)
///   - `combined` — Runs both ADF and KPSS and returns an overall conclusion
/// - `LAGS n`: Number of lags for the test (integer ≥ 0). Only valid with `TEST adf` or `TEST kpss`.
///   If omitted, a sensible default is used automatically.
///
/// Returns a map with test statistics, p-values, critical values, and a conclusion.
#[valkey_module_macros::command({
    name: "ts.stationarity",
    flags: [ReadOnly, DenyOOM],
    summary: "Test whether a time series is stationary.",
    complexity: "O(N*L) where N is the number of samples in the range and L is the number of lags used by the test.",
    since: "1.0.0",
    arity: -4,
    key_spec: [{
        flags: [ReadOnly, Access],
        begin_search: Index({ index: 1 }),
        find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
    }]
})]
pub fn ts_stationarity_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Parse optional TEST, LAGS and TIMEOUT
    let mut test_type = TestType::Combined;
    let mut lags: Option<usize> = None;
    let mut timeout = AnalysisTimeout::default();

    while let Some(arg) = args.peek() {
        let arg_str = arg
            .try_as_str()
            .map_err(|_| ValkeyError::Str("TSDB: invalid argument"))?;

        match arg_str.to_uppercase().as_str() {
            "TEST" => {
                args.next();
                let test_str = args.next_str()?;
                test_type = parse_test_type(test_str)?;
            }
            "LAGS" => {
                args.next();
                let lag_str = args.next_arg()?;
                let lag: i64 = lag_str.parse_integer().map_err(|_| {
                    ValkeyError::Str("TSDB: invalid LAGS value, expected a non-negative integer")
                })?;
                if lag < 0 {
                    return Err(ValkeyError::Str(
                        "TSDB: LAGS must be a non-negative integer",
                    ));
                }
                if lag as u64 > MAX_ANALYSIS_LAG as u64 {
                    return Err(ValkeyError::String(format!(
                        "TSDB: LAGS must not exceed {MAX_ANALYSIS_LAG}"
                    )));
                }
                lags = Some(lag as usize);
            }
            "TIMEOUT" => {
                args.next();
                timeout.set(parse_timeout(&mut args)?);
            }
            _ => break,
        }
    }

    reject_extra_args(&mut args)?;

    // LAGS is incompatible with combined test
    if test_type == TestType::Combined && lags.is_some() {
        return Err(ValkeyError::Str(
            "TSDB: LAGS option is not supported with TEST combined",
        ));
    }

    // Data checks come after the options, so a malformed call reports its syntax error first.
    // The tests are undefined over missing values: a NaN turns every statistic into NaN,
    // which would otherwise be reported as a "non_stationary" conclusion.
    if values.iter().any(|v| !v.is_finite()) {
        return Err(ValkeyError::Str(
            "TSDB: the range contains NaN or infinite values; fill or drop them first (see TS.SANITIZE)",
        ));
    }

    // Minimum data check
    if values.len() < MIN_SAMPLES {
        return Err(ValkeyError::String(format!(
            "TSDB: insufficient data for stationarity test. Need at least {MIN_SAMPLES} samples, got {}",
            values.len()
        )));
    }

    let work = stationarity_work(values.len(), test_type, lags);
    run_analysis(
        ctx,
        work,
        LIMITS,
        timeout,
        move || Ok(run_tests(&values, test_type, lags)),
        move |actx, outcome| {
            let ctx = actx.reply_ctx();
            match outcome {
                Outcome::Combined {
                    adf,
                    kpss,
                    conclusion,
                } => reply_combined(&ctx, &adf, &kpss, conclusion),
                Outcome::Single { result, test_name } => {
                    reply_single_test(&ctx, &result, test_name)
                }
            }
        },
    )
}

/// Work is counted in passes over the samples (see [`stationarity_work`]): ADF and KPSS are
/// linear in the range and in the lags they use, at ~0.9 ns a sample-pass (release build; ADF
/// ~3.5 ns a lag at 50k samples, KPSS ~0.9 ns). Up to `inline_max` (~8 ms; the combined test
/// with default lags up to 50,000 samples) it runs on the main thread, anything bigger goes to
/// the pool; where the client cannot be blocked the pool is not available and `unblockable_max`
/// (about 0.9 s) is the most it will take, e.g. `TEST adf LAGS 1000` over ~250,000 samples.
const LIMITS: WorkLimits = WorkLimits {
    inline_max: 8_500_000,
    unblockable_max: 1_000_000_000,
    unit: "sample-passes",
};

/// Passes over the samples ADF makes per lag: its AIC search computes two means, then the slope
/// sums and then the residuals. KPSS makes one per lag (the autocovariance). Measured at 50k
/// samples, ADF is ~3.5 ns a sample-lag and KPSS ~0.9 ns.
const ADF_PASSES_PER_LAG: usize = 4;

/// Passes either test makes outside its per-lag loop (means, partial sums, the final fit).
const FIXED_PASSES: usize = 4;

/// The lags `adf_test` runs its AIC search up to: `LAGS`, or `(n - 1)^(1/3)` without it, held to
/// `n / 2 - 1` and at least 1 (anofox-forecast 0.15.10).
fn adf_lags(n: usize, lags: Option<usize>) -> usize {
    let default = (n.saturating_sub(1) as f64).powf(1.0 / 3.0).floor() as usize;
    lags.unwrap_or(default)
        .min((n / 2).saturating_sub(1))
        .max(1)
}

/// The lags `kpss_test` uses: `LAGS`, or `4 (n / 100)^(1/4)` without it, held to `n / 2` and at
/// least 1 (anofox-forecast 0.15.10).
fn kpss_lags(n: usize, lags: Option<usize>) -> usize {
    let default = (4.0 * (n as f64 / 100.0).powf(0.25)).floor() as usize;
    lags.unwrap_or(default).min(n / 2).max(1)
}

/// How many passes over the `n` samples the test makes: each lag the test runs through is one
/// more pass over the data (four for ADF), so `LAGS 1000` costs ~1000 times what `LAGS 1` does.
/// The combined test runs both with their default lags (`LAGS` is not allowed with it).
/// Saturating, so an absurd range cannot wrap into looking cheap.
fn stationarity_work(n: usize, test: TestType, lags: Option<usize>) -> usize {
    let adf = n.saturating_mul(ADF_PASSES_PER_LAG * adf_lags(n, lags) + FIXED_PASSES);
    let kpss = n.saturating_mul(kpss_lags(n, lags) + FIXED_PASSES);
    match test {
        TestType::Adf => adf,
        TestType::Kpss => kpss,
        TestType::Combined => adf.saturating_add(kpss),
    }
}

enum Outcome {
    Combined {
        adf: StationarityResult,
        kpss: StationarityResult,
        conclusion: &'static str,
    },
    Single {
        result: StationarityResult,
        test_name: &'static str,
    },
}

fn run_tests(values: &[f64], test_type: TestType, lags: Option<usize>) -> Outcome {
    // Constant series (all values identical) is trivially stationary.
    // The ADF/KPSS regression would fail with zero variance, so handle
    // this edge case by returning a stationary result directly.
    let is_constant = values.len() >= 2
        && values
            .windows(2)
            .all(|w| (w[0] - w[1]).abs() < f64::EPSILON);

    if is_constant {
        let const_result = StationarityResult {
            statistic: 0.0,
            p_value: 1.0,
            lags: 0,
            is_stationary: true,
            critical_values: stationarity::CriticalValues::default(),
        };
        return match test_type {
            TestType::Combined => Outcome::Combined {
                adf: const_result.clone(),
                kpss: const_result,
                conclusion: "stationary",
            },
            TestType::Adf => Outcome::Single {
                result: const_result,
                test_name: "adf",
            },
            TestType::Kpss => Outcome::Single {
                result: const_result,
                test_name: "kpss",
            },
        };
    }

    match test_type {
        TestType::Combined => {
            let (adf, kpss, conclusion) = stationarity::test_stationarity(values);
            Outcome::Combined {
                adf,
                kpss,
                conclusion,
            }
        }
        TestType::Adf => Outcome::Single {
            result: stationarity::adf_test(values, lags),
            test_name: "adf",
        },
        TestType::Kpss => Outcome::Single {
            result: stationarity::kpss_test(values, lags),
            test_name: "kpss",
        },
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestType {
    Adf,
    Kpss,
    Combined,
}

/// Emit the map fields shared by all stationarity test results —
/// `statistic`, `pValue`, `lags`, `isStationary`, and the three
/// critical-value keys.  The caller is responsible for opening the map
/// and writing the `test` / `conclusion` fields before calling this.
fn reply_result_fields<C: IntoRawCtx + Copy>(ctx: C, result: &StationarityResult) {
    reply_with_str(ctx, "statistic");
    reply_with_statistic(ctx, result.statistic);

    reply_with_str(ctx, "pValue");
    reply_with_statistic(ctx, result.p_value);

    reply_with_str(ctx, "lags");
    reply_with_integer(ctx, result.lags as i64);

    reply_with_str(ctx, "isStationary");
    reply_with_integer(ctx, i64::from(result.is_stationary));

    reply_with_str(ctx, "cv1pct");
    reply_with_statistic(ctx, result.critical_values.cv_1pct);

    reply_with_str(ctx, "cv5pct");
    reply_with_statistic(ctx, result.critical_values.cv_5pct);

    reply_with_str(ctx, "cv10pct");
    reply_with_statistic(ctx, result.critical_values.cv_10pct);
}

fn reply_single_test<C: IntoRawCtx + Copy>(
    ctx: C,
    result: &StationarityResult,
    test_name: &str,
) -> ValkeyResult {
    reply_with_map(ctx, 9);

    reply_with_str(ctx, "test");
    reply_with_str(ctx, test_name);

    let conclusion = if result.is_stationary {
        "stationary"
    } else {
        "non_stationary"
    };
    reply_with_str(ctx, "conclusion");
    reply_with_str(ctx, conclusion);

    reply_result_fields(ctx, result);

    Ok(ValkeyValue::NoReply)
}

fn reply_combined<C: IntoRawCtx + Copy>(
    ctx: C,
    adf_result: &StationarityResult,
    kpss_result: &StationarityResult,
    conclusion: &str,
) -> ValkeyResult {
    // Top-level map: 4 keys — test, conclusion, adf (nested), kpss (nested)
    reply_with_map(ctx, 4);

    reply_with_str(ctx, "test");
    reply_with_str(ctx, "combined");

    reply_with_str(ctx, "conclusion");
    reply_with_str(ctx, conclusion);

    // ADF nested map — 7 keys: statistic, pValue, lags, isStationary, cv1pct, cv5pct, cv10pct
    reply_with_str(ctx, "adf");
    reply_with_map(ctx, 7);
    reply_result_fields(ctx, adf_result);

    // KPSS nested map — 7 keys
    reply_with_str(ctx, "kpss");
    reply_with_map(ctx, 7);
    reply_result_fields(ctx, kpss_result);

    Ok(ValkeyValue::NoReply)
}

fn parse_test_type(arg: &str) -> ValkeyResult<TestType> {
    match arg.len() {
        3 if arg.eq_ignore_ascii_case("adf") => Ok(TestType::Adf),
        4 if arg.eq_ignore_ascii_case("kpss") => Ok(TestType::Kpss),
        8 if arg.eq_ignore_ascii_case("combined") => Ok(TestType::Combined),
        _ => Err(ValkeyError::String(format!(
            "TSDB: invalid TEST value '{arg}'. Expected adf, kpss, or combined"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(n: usize) -> Vec<f64> {
        (0..n)
            .map(|i| 10.0 + (i % 24) as f64 + (i as f64 * 0.37) % 5.0 + i as f64 * 0.001)
            .collect()
    }

    #[test]
    fn kpss_lags_match_what_the_crate_uses() {
        for n in [10, 11, 50, 99, 100, 1_000, 5_000] {
            let values = series(n);
            for lags in [None, Some(0), Some(1), Some(7), Some(1_000)] {
                let used = stationarity::kpss_test(&values, lags).lags;
                assert_eq!(kpss_lags(n, lags), used, "n={n} lags={lags:?}");
            }
        }
    }

    #[test]
    fn adf_lags_bound_the_lag_the_crate_selects() {
        for n in [10, 11, 50, 100, 1_000, 5_000] {
            let values = series(n);
            for lags in [None, Some(0), Some(3), Some(1_000)] {
                // The crate reports the lag its AIC search picked, up to the bound we count.
                let picked = stationarity::adf_test(&values, lags).lags;
                assert!(picked <= adf_lags(n, lags), "n={n} lags={lags:?}");
            }
        }
    }

    #[test]
    fn explicit_lags_are_held_to_what_the_range_allows() {
        assert_eq!(adf_lags(100, Some(1_000)), 49);
        assert_eq!(kpss_lags(100, Some(1_000)), 50);
        assert_eq!(adf_lags(100, Some(0)), 1);
        assert_eq!(kpss_lags(100, Some(0)), 1);
        // Degenerate ranges do not underflow.
        assert_eq!(adf_lags(0, None), 1);
        assert_eq!(kpss_lags(0, None), 1);
    }

    #[test]
    fn work_grows_with_the_lags_asked_for() {
        let n = 50_000;
        let few = stationarity_work(n, TestType::Adf, Some(10));
        let many = stationarity_work(n, TestType::Adf, Some(1_000));
        assert!(many > 50 * few, "{few} vs {many}");
        assert_eq!(
            stationarity_work(n, TestType::Adf, Some(1_000)),
            n * (ADF_PASSES_PER_LAG * 1_000 + FIXED_PASSES)
        );
        assert_eq!(
            stationarity_work(n, TestType::Kpss, Some(1_000)),
            n * (1_000 + FIXED_PASSES)
        );
    }

    #[test]
    fn adf_costs_more_a_lag_than_kpss() {
        let n = 20_000;
        assert!(
            stationarity_work(n, TestType::Adf, Some(100))
                > 3 * stationarity_work(n, TestType::Kpss, Some(100))
        );
    }

    #[test]
    fn combined_with_default_lags_stays_inline_up_to_50_000_samples() {
        // The boundary before the work counted lags: 50,000 samples, whatever the test did.
        let work = |n| stationarity_work(n, TestType::Combined, None);
        assert!(work(50_000) <= LIMITS.inline_max, "{}", work(50_000));
        assert!(work(50_001) > LIMITS.inline_max, "{}", work(50_001));
    }

    #[test]
    fn a_lags_request_that_used_to_run_inline_now_goes_to_the_pool() {
        // 50,000 samples was inline whatever LAGS said; ADF at LAGS 1000 is ~170 ms there.
        let work = stationarity_work(50_000, TestType::Adf, Some(1_000));
        assert!(work > LIMITS.inline_max);
        assert!(work <= LIMITS.unblockable_max);
    }

    #[test]
    fn work_of_an_absurd_range_saturates() {
        assert_eq!(
            stationarity_work(usize::MAX, TestType::Combined, None),
            usize::MAX
        );
    }
}
