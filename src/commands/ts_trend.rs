use crate::analysis::forecasting::try_parse_trend_criterion;
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::{AnalysisTimeout, parse_timeout, run_analysis};
use crate::commands::command_parser::{
    StoreOptions, parse_series_range_samples, parse_store_clause,
};
use crate::commands::utils::reply_with_accuracy_metrics;
use crate::common::Sample;
use crate::common::replies::{
    reply_with_array, reply_with_double, reply_with_integer, reply_with_map, reply_with_str,
};
use crate::common::time::compute_median_step_ms;
use crate::series::{
    DestinationWriteMode, TimeSeriesOptions, create_or_update_series_with_samples,
};
use anofox_forecast::seasonality::auto_trend::{AutoTrend, TrendCriterion};
use anofox_forecast::seasonality::traits::{Recency, TrendComponent};
use anofox_forecast::seasonality::{
    AutoRecencyConfig, ExponentialTrend, LogisticTrend, PolynomialTrend, TheilSenTrend,
};
use anofox_forecast::utils::{AccuracyMetrics, calculate_metrics};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// Trend model selection: either Auto (with optional criterion) or a specific model.
#[derive(Debug, Clone)]
enum TrendModel {
    Auto(Option<TrendCriterion>),
    Exponential,
    Logistic,
    Polynomial,
    TheilSen,
}

impl Default for TrendModel {
    fn default() -> Self {
        TrendModel::Auto(None)
    }
}

/// ```text
/// TS.TREND key fromTimestamp toTimestamp
///     [MODEL <Exponential|Logistic|Polynomial|TheilSen|Auto> [AICc|BIC|HOLDOUT]]
///     [RECENCY <FULL|WINDOW n|FRACTION f|AUTO>]
///     [PREDICT <horizon>]
///     [FEATURES]
///     [METRICS]
///     [TIMEOUT ms]
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
/// ```
///
/// `TS.TREND` fits one or more trend components to a time series.
///
/// When `MODEL` is `Auto` (the default), multiple candidate trend components
/// (Linear, Quadratic, Exponential, TheilSen, PiecewiseLinear) are fitted and
/// the best one is selected using an information criterion (AICc by default).
/// When a specific `MODEL` is given, only that trend component is fitted.
///
/// Returns a map. For **Auto** mode:
/// - `model`: name of the selected trend model
/// - `criterion`: criterion used for selection (AICc, BIC, HOLDOUT)
/// - `fitted_trend`: the in-sample fitted trend values
/// - `scores`: array of [name, score] pairs for all candidates, sorted by score
/// - `n_params`: number of free parameters of the fitted model
///
/// For **specific model** mode:
/// - `model`: name of the trend model used
/// - `fitted_trend`: the in-sample fitted trend values
/// - `n_params`: number of free parameters of the fitted model
///
/// Optional response fields:
/// - `predicted_trend`: predicted trend values (when PREDICT is specified)
/// - `features`: map of named features from the fitted component (when FEATURES is specified)
/// - `metrics`: accuracy metrics between observed and fitted values (when METRICS is specified)
#[valkey_module_macros::command({
    name: "ts.trend",
    // Declared `Write` rather than `ReadOnly`: the STORE clause creates/updates the
    // destination series and replicates, so the command must not be routed to replicas
    // or treated as read-only, even though it is a pure read when STORE is omitted.
    flags: [Write, DenyOOM],
    summary: "Fit trend components to a time series, optionally selecting the best model.",
    complexity: "O(N*M) where N is the number of samples in the range and M is the number of candidate trend models.",
    since: "1.0.0",
    arity: -4,
    key_spec: [
        {
            flags: [ReadOnly, Access],
            begin_search: Index({ index: 1 }),
            find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
        },
        {
            notes: "Optional destination series written by the STORE clause.",
            flags: [ReadWrite, Update],
            begin_search: Keyword({ keyword: "STORE", startfrom: 1 }),
            find_keys: Range({ last_key: 0, steps: 1, limit: 0 })
        }
    ]
})]
pub fn ts_trend_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 4 {
        return Err(ValkeyError::WrongArity);
    }

    if ctx.is_keys_position_request() {
        ctx.key_at_pos(1); // key is always at position 1
        if let Some(store_pos) = get_store_key_pos(&args)? {
            ctx.key_at_pos(store_pos as i32);
        }
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();

    // Get the time series and extract sample values
    let samples = parse_series_range_samples(ctx, &mut args)?;
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    if values.len() < 4 {
        return Err(ValkeyError::Str(
            "TSDB: insufficient data for trend fitting. Need at least 4 samples.",
        ));
    }

    let options = parse_trend_args(&mut args)?;

    args.done()?;

    let sample_count = values.len();
    let timeout = options.timeout;
    run_analysis(
        ctx,
        sample_count,
        INLINE_MAX_SAMPLES,
        timeout,
        move || {
            let fit = fit_trend(&options, &values)?;
            Ok((options, samples, fit))
        },
        move |actx, (options, samples, fit)| {
            // If STORE was specified, persist the fitted (and optionally predicted)
            // trend values and reply with the count instead of the fit.
            if let Some(store) = options.store_options {
                // The client has already been told the command failed; do not
                // write behind it.
                if actx.is_timed_out() {
                    return Ok(ValkeyValue::NoReply);
                }
                return actx.with_locked_context(|ctx| store_trend(ctx, store, &samples, &fit));
            }
            reply_with_fit(actx.reply_ctx().context(), &fit)
        },
    )
}

/// Largest range that runs on the main thread. Fitting is ~20 ms here, ~0.5 s at
/// 20k and ~21 s at 200k samples (release build), so anything bigger goes to the pool.
const INLINE_MAX_SAMPLES: usize = 2_000;

fn build_specific_trend(
    model: &TrendModel,
    recency: &Recency,
) -> Option<(&'static str, Box<dyn TrendComponent>)> {
    match model {
        TrendModel::Exponential => Some((
            "Exponential",
            Box::new(ExponentialTrend::new().with_recency(recency.clone())),
        )),
        TrendModel::Logistic => Some((
            "Logistic",
            Box::new(LogisticTrend::new().with_recency(recency.clone())),
        )),
        TrendModel::Polynomial => Some((
            "Polynomial",
            Box::new(PolynomialTrend::new(2).with_recency(recency.clone())),
        )),
        TrendModel::TheilSen => Some((
            "TheilSen",
            Box::new(TheilSenTrend::new().with_recency(recency.clone())),
        )),
        TrendModel::Auto(_) => None,
    }
}

/// Everything a reply or STORE needs, computed off the reply path so the fit can
/// run on the analysis pool.
struct TrendFit {
    model_name: String,
    /// Selection criterion and candidate scores; `Some` only for `MODEL AUTO`.
    selection: Option<(TrendCriterion, Vec<(String, f64)>)>,
    fitted: Vec<f64>,
    predicted: Option<Vec<f64>>,
    features: Option<Vec<(String, f64)>>,
    metrics: Option<AccuracyMetrics>,
    n_params: usize,
}

fn fit_trend(options: &TrendOptions, values: &[f64]) -> ValkeyResult<TrendFit> {
    if let TrendModel::Auto(criterion) = options.model {
        fit_auto_trend(options, criterion, values)
    } else {
        let (model_name, trend) = build_specific_trend(&options.model, &options.recency)
            .ok_or(ValkeyError::Str("TSDB: invalid trend model configuration"))?;
        fit_specific_trend(options, model_name, values, trend)
    }
}

/// Auto-trend: fit all candidates and select the best one.
fn fit_auto_trend(
    options: &TrendOptions,
    criterion: Option<TrendCriterion>,
    values: &[f64],
) -> ValkeyResult<TrendFit> {
    let criterion = criterion.unwrap_or(TrendCriterion::AICc);
    let mut auto_trend = AutoTrend::new()
        .with_recency(options.recency.clone())
        .with_criterion(criterion);

    auto_trend
        .fit_trend(values)
        .map_err(|e| ValkeyError::String(format!("TSDB: trend fitting error: {}", e)))?;

    let fitted = auto_trend.fitted_trend().to_vec();
    let selection = auto_trend.selection_result();
    let model_name = selection
        .map(|result| result.selected.clone())
        .unwrap_or_else(|| auto_trend.trend_name().to_string());
    let scores = selection
        .map(|result| result.scores.clone())
        .unwrap_or_default();

    Ok(TrendFit {
        model_name,
        selection: Some((criterion, scores)),
        predicted: (options.predict > 0).then(|| auto_trend.predict_trend(options.predict)),
        features: options
            .features
            .then(|| owned_features(auto_trend.trend_features())),
        metrics: options
            .metrics
            .then(|| compute_accuracy_metrics(values, &fitted))
            .transpose()?,
        n_params: auto_trend.n_params(),
        fitted,
    })
}

/// A specific trend model (Exponential, Logistic, Polynomial, TheilSen).
fn fit_specific_trend(
    options: &TrendOptions,
    model_name: &str,
    values: &[f64],
    mut trend: Box<dyn TrendComponent>,
) -> ValkeyResult<TrendFit> {
    trend
        .fit_trend(values)
        .map_err(|e| ValkeyError::String(format!("TSDB: trend fitting error: {}", e)))?;

    let fitted = trend.fitted_trend().to_vec();
    Ok(TrendFit {
        model_name: model_name.to_string(),
        selection: None,
        predicted: (options.predict > 0).then(|| trend.predict_trend(options.predict)),
        features: options
            .features
            .then(|| owned_features(trend.trend_features())),
        metrics: options
            .metrics
            .then(|| compute_accuracy_metrics(values, &fitted))
            .transpose()?,
        n_params: trend.n_params(),
        fitted,
    })
}

fn owned_features(features: Vec<(&str, f64)>) -> Vec<(String, f64)> {
    features
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect()
}

fn reply_with_fit(ctx: &Context, fit: &TrendFit) -> ValkeyResult {
    // model [+ criterion + scores for AUTO], fitted_trend, n_params, then the optional tail
    let base_fields = if fit.selection.is_some() { 5 } else { 3 };
    let map_len = base_fields
        + usize::from(fit.predicted.is_some())
        + usize::from(fit.features.is_some())
        + usize::from(fit.metrics.is_some());

    reply_with_map(ctx, map_len);

    reply_with_str(ctx, "model");
    reply_with_str(ctx, &fit.model_name);

    if let Some((criterion, _)) = &fit.selection {
        reply_with_str(ctx, "criterion");
        let criterion_str = match criterion {
            TrendCriterion::AICc => "AICc",
            TrendCriterion::BIC => "BIC",
            TrendCriterion::Holdout => "HOLDOUT",
        };
        reply_with_str(ctx, criterion_str);
    }

    reply_with_str(ctx, "fitted_trend");
    reply_with_double_array(ctx, &fit.fitted);

    if let Some((_, scores)) = &fit.selection {
        reply_with_str(ctx, "scores");
        reply_with_scores(ctx, scores);
    }

    // predicted_trend (optional)
    if let Some(p) = &fit.predicted {
        reply_with_str(ctx, "predicted_trend");
        reply_with_double_array(ctx, p);
    }

    // features (optional)
    if let Some(f) = &fit.features {
        reply_with_str(ctx, "features");
        reply_with_trend_features(ctx, f);
    }

    // metrics (optional)
    if let Some(m) = &fit.metrics {
        reply_with_str(ctx, "accuracy_metrics");
        reply_with_accuracy_metrics(ctx, m);
    }

    // n_params
    reply_with_str(ctx, "n_params");
    reply_with_integer(ctx, fit.n_params as i64);

    Ok(ValkeyValue::NoReply)
}

/// STORE target, held as bytes so the options can cross to the analysis pool
/// (`ValkeyString` is not `Send`).
struct TrendStore {
    key: Vec<u8>,
    options: TimeSeriesOptions,
    write_mode: DestinationWriteMode,
}

impl From<StoreOptions> for TrendStore {
    fn from(store: StoreOptions) -> Self {
        Self {
            key: store.key.into(),
            options: store.options,
            write_mode: store.write_mode,
        }
    }
}

/// Persist fitted (and optionally predicted) trend values to a destination key.
fn store_trend(
    ctx: &Context,
    store: TrendStore,
    samples: &[Sample],
    fit: &TrendFit,
) -> ValkeyResult<ValkeyValue> {
    let destination = ctx.create_string(store.key.as_slice());
    let mut store_samples: Vec<Sample> = fit
        .fitted
        .iter()
        .enumerate()
        .map(|(i, &value)| Sample::new(samples[i].timestamp, value))
        .collect();

    if let Some(predicted_values) = &fit.predicted {
        let timestamps: Vec<i64> = samples.iter().map(|s| s.timestamp).collect();
        if let Some(step) = compute_median_step_ms(&timestamps) {
            let last_ts = samples.last().map(|s| s.timestamp).unwrap_or(0);
            let predicted_samples = predicted_values
                .iter()
                .enumerate()
                .map(|(i, &value)| Sample::new(last_ts + step * (i as i64 + 1), value));
            store_samples.extend(predicted_samples);
        } else {
            ctx.log_warning(
                "TSDB: STORE predicted values skipped — could not determine step from input series",
            );
        }
    }

    let written = create_or_update_series_with_samples(
        ctx,
        &destination,
        Some(store.options),
        store.write_mode,
        &store_samples,
        None,
    )?;
    Ok(ValkeyValue::Integer(written as i64))
}

fn compute_accuracy_metrics(actual: &[f64], predicted: &[f64]) -> ValkeyResult<AccuracyMetrics> {
    calculate_metrics(actual, predicted, None)
        .map_err(|e| ValkeyError::String(format!("TSDB: metrics calculation error: {}", e)))
}

struct TrendOptions {
    model: TrendModel,
    recency: Recency,
    predict: usize,
    features: bool,
    metrics: bool,
    store_options: Option<TrendStore>,
    timeout: AnalysisTimeout,
}

impl Default for TrendOptions {
    fn default() -> Self {
        Self {
            model: TrendModel::default(),
            recency: Recency::Fraction(0.3),
            predict: 0,
            features: false,
            metrics: false,
            store_options: None,
            timeout: AnalysisTimeout::default(),
        }
    }
}

fn get_store_key_pos(args: &[ValkeyString]) -> ValkeyResult<Option<usize>> {
    for (i, arg) in args.iter().enumerate() {
        if arg.eq_ignore_ascii_case(b"store") {
            if i + 1 >= args.len() {
                return Err(ValkeyError::Str("TSDB: Missing value for STORE argument"));
            }
            return Ok(Some(i + 1));
        }
    }
    Ok(None)
}

fn parse_trend_args(args: &mut CommandArgIterator) -> ValkeyResult<TrendOptions> {
    let mut options = TrendOptions::default();

    while let Some(arg) = args.next() {
        let arg_slice = arg.as_slice();
        hashify::fnc_map_ignore_case!(
            arg_slice,
            "MODEL" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for MODEL"))?;
                options.model = parse_trend_model(val, args)?;
            },
            "RECENCY" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for RECENCY"))?;
                options.recency = match val.to_ascii_uppercase().as_str() {
                    "AUTO" => Recency::Auto(AutoRecencyConfig::default()),
                    "FULL" => Recency::Full,
                    "WINDOW" => {
                        let n_str = args.next_str()
                            .map_err(|_| ValkeyError::Str("TSDB: Missing window size for RECENCY WINDOW"))?;
                        let n: usize = n_str.parse().map_err(|_| {
                            ValkeyError::Str("TSDB: invalid window size for RECENCY WINDOW")
                        })?;
                        if n < 4 {
                            return Err(ValkeyError::Str(
                                "TSDB: RECENCY WINDOW must be at least 4"
                            ));
                        }
                        Recency::Window(n)
                    },
                    "FRACTION" => {
                        let f_str = args.next_str()
                            .map_err(|_| ValkeyError::Str("TSDB: Missing fraction value for RECENCY FRACTION"))?;
                        let f: f64 = f_str.parse().map_err(|_| {
                            ValkeyError::Str("TSDB: invalid fraction for RECENCY FRACTION")
                        })?;
                        if f <= 0.0 || f > 1.0 {
                            return Err(ValkeyError::Str(
                                "TSDB: RECENCY FRACTION must be between 0 and 1"
                            ));
                        }
                        Recency::Fraction(f)
                    },
                    other => return Err(ValkeyError::String(format!(
                        "TSDB: invalid RECENCY '{}'. Expected FULL, WINDOW, or FRACTION",
                        other
                    ))),
                };
            },
            "PREDICT" => {
                let val = args.next_str()
                    .map_err(|_| ValkeyError::Str("TSDB: Missing value for PREDICT"))?;
                let n: i64 = val.parse().map_err(|_| {
                    ValkeyError::Str("TSDB: invalid value for PREDICT")
                })?;
                if n <= 0 {
                    return Err(ValkeyError::Str("TSDB: PREDICT must be greater than 0"));
                }
                options.predict = n as usize;
            },
            "FEATURES" => {
                options.features = true;
            },
            "METRICS" => {
                options.metrics = true;
            },
            "STORE" => {
                let opts = parse_store_clause(args)?;
                options.store_options = Some(opts.into());
            },
            "TIMEOUT" => {
                options.timeout.set(parse_timeout(args)?);
            },
            _ => {
                // Unknown argument
                return Err(ValkeyError::String(format!(
                    "TSDB: Unknown argument: {}",
                    arg
                )));
            }
        );
    }

    Ok(options)
}

/// Parse the MODEL argument value and optional criterion.
///
/// Accepted values: Exponential, Logistic, Polynomial, TheilSen, Auto [criterion]
fn parse_trend_model(val: &str, args: &mut CommandArgIterator) -> ValkeyResult<TrendModel> {
    match val.to_ascii_uppercase().as_str() {
        "EXPONENTIAL" => Ok(TrendModel::Exponential),
        "LOGISTIC" => Ok(TrendModel::Logistic),
        "POLYNOMIAL" => Ok(TrendModel::Polynomial),
        "THEILSEN" => Ok(TrendModel::TheilSen),
        "AUTO" => {
            // Optionally parse a criterion after Auto
            if let Some(next) = args.peek() {
                let maybe_criterion = next.try_as_str().map_err(|_| {
                    ValkeyError::Str(
                        "TSDB: Invalid argument after MODEL Auto. Expected AICc, BIC, or HOLDOUT.",
                    )
                })?;
                if let Ok(criterion) = try_parse_trend_criterion(maybe_criterion) {
                    args.next(); // consume the criterion argument
                    Ok(TrendModel::Auto(Some(criterion)))
                } else {
                    Ok(TrendModel::Auto(None))
                }
            } else {
                Ok(TrendModel::Auto(None))
            }
        }
        other => Err(ValkeyError::String(format!(
            "TSDB: Invalid MODEL '{}'. Expected Exponential, Logistic, Polynomial, TheilSen, or Auto.",
            other
        ))),
    }
}

/// Reply with an array of doubles, using the optimized raw API.
fn reply_with_double_array(ctx: &Context, values: &[f64]) {
    reply_with_array(ctx, values.len());
    for &v in values {
        reply_with_double(ctx, v);
    }
}

/// Reply with scores as nested arrays: [[name, score], ...]
fn reply_with_scores(ctx: &Context, scores: &[(String, f64)]) {
    reply_with_array(ctx, scores.len());
    for (name, score) in scores {
        reply_with_array(ctx, 2);
        reply_with_str(ctx, name);
        reply_with_double(ctx, *score);
    }
}

/// Reply with trend features as a flat map (alternating key-value pairs).
fn reply_with_trend_features(ctx: &Context, features: &[(String, f64)]) {
    reply_with_map(ctx, features.len());
    for (name, value) in features {
        reply_with_str(ctx, name);
        reply_with_double(ctx, *value);
    }
}
