use crate::analysis::forecasting::normalize_model_name;
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::{AnalysisTimeout, parse_timeout, run_analysis_job};
use crate::commands::command_parser::{
    parse_forecast_confidence_level, parse_forecast_horizon_value, parse_store_clause,
};
use crate::commands::forecast_utils::{
    StoreAnchor, handle_forecast_key_pos_request, parse_timeseries_for_forecast,
    reply_with_forecast_output, run_forecast, store_anchor, write_forecast_samples,
};
use crate::commands::utils::reply_with_double_array;
use crate::common::replies::{ThreadSafeReplyContext, reply_with_str};
use crate::series::{DestinationWriteMode, TimeSeriesOptions};
use anofox_forecast::core::TimeSeries as ForecastTimeSeries;
use anofox_forecast::detection::detect_dominant_period;
use anofox_forecast::models::auto_forecast::{AutoForecast, AutoForecastConfig};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

struct AutoForecastOptions {
    horizon: usize,
    level: Option<f64>,
    metrics: bool,
    destination_key: Option<Vec<u8>>,
    create_options: Option<TimeSeriesOptions>,
    write_mode: Option<DestinationWriteMode>,
    config: AutoForecastConfig,
    auto_seasonality: bool,
    timeout: AnalysisTimeout,
}

impl Default for AutoForecastOptions {
    fn default() -> Self {
        Self {
            horizon: 5,
            level: None,
            metrics: false,
            destination_key: None,
            create_options: None,
            write_mode: None,
            config: AutoForecastConfig::default(),
            auto_seasonality: false,
            timeout: AnalysisTimeout::default(),
        }
    }
}

/// ```text
/// TS.AUTOFORECAST key fromTimestamp toTimestamp
///     HORIZON <horizon>
///     [SEASONALITY <period>]
///     [MODELS <family1>,<family2> ...]
///     [LEVEL <confidence_level>]
///     [METRICS]
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
/// `TS.AUTOFORECAST` fits all enabled auto models (AutoARIMA, AutoETS, AutoTheta)
/// and selects the best one based on cross-validation error.
#[valkey_module_macros::command({
    name: "ts.autoforecast",
    flags: [Write, DenyOOM],
    summary: "Forecast a time series, automatically selecting the best-fitting model.",
    complexity: "O(N*M) where N is the number of samples in the range and M is the number of candidate models.",
    since: "1.0.0",
    arity: -5,
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
pub(crate) fn ts_autoforecast_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 5 {
        return Err(ValkeyError::WrongArity);
    }

    if handle_forecast_key_pos_request(ctx, &args)? {
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_autoforecast_args(&mut args)?;
    // STORE needs a step to place the forecast samples; reject a range that
    // cannot provide one now rather than after the model search has run.
    let anchor = options
        .destination_key
        .as_ref()
        .map(|_| store_anchor(&series))
        .transpose()?;

    run_analysis_job(ctx, options.timeout, move |thread_ctx| {
        process_forecast(thread_ctx, series, options, anchor);
    });

    // Reply will be sent from the analysis pool
    Ok(ValkeyValue::NoReply)
}

fn parse_autoforecast_args(args: &mut CommandArgIterator) -> ValkeyResult<AutoForecastOptions> {
    let mut options = AutoForecastOptions::default();
    let mut horizon_set = false;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
                arg.as_slice(),
                "HORIZON" => {
                    options.horizon = parse_forecast_horizon_value(args)?;
                    horizon_set = true;
                },
                "SEASONALITY" => {
                    // parse seasonality value
                    if let Some(str) = args.peek()
                        && str.as_slice().eq_ignore_ascii_case(b"AUTO") {
                            options.auto_seasonality = true;
                            args.next(); // consume AUTO
                            continue;
                        }
                    let period = parse_single_value(args, "SEASONALITY")? as usize;
                    options.config.seasonal_period = Some(period);
                },
                "MODELS" => {
                    let models = args.next_str().map_err(|_| ValkeyError::Str("TSDB: Missing value for MODELS"))?;
                    parse_models(models, &mut options.config)?;
                },
                "LEVEL" => {
                    let value = parse_forecast_confidence_level(args)?;
                    options.level = Some(value);
                },
                "METRICS" => {
                    options.metrics = true;
                },
                "TIMEOUT" => {
                    options.timeout.set(parse_timeout(args)?);
                },
                "STORE" => {
                    let store_options = parse_store_clause(args)?;
                    options.destination_key = Some(store_options.key.into());
                    options.create_options = Some(store_options.options);
                    options.write_mode = Some(store_options.write_mode);
                },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: Unknown argument: {}", arg)));
            }
        );
    }

    if !horizon_set {
        return Err(ValkeyError::Str("TSDB: HORIZON is required"));
    }

    Ok(options)
}

fn process_forecast(
    ctx: ThreadSafeReplyContext,
    series: ForecastTimeSeries,
    mut options: AutoForecastOptions,
    anchor: Option<StoreAnchor>,
) {
    if options.config.seasonal_period.is_none() && options.auto_seasonality {
        options.config.seasonal_period = detect_dominant_period(series.primary_values());
    }

    let seasonal_period = options.config.seasonal_period;

    let mut model = AutoForecast::with_config(options.config.clone());

    // { model: "ARIMA", horizon: 5, forecast: [...], lower_interval: [...], upper_interval: [...] }
    let output = run_forecast(
        &series,
        &mut model,
        options.horizon,
        options.level,
        options.metrics,
        seasonal_period,
    );

    let mut output = match output {
        Ok(o) => o,
        Err(err) => {
            let msg = err.to_string();
            ctx.log_warning(&msg);
            ctx.reply(Err(err));
            return;
        }
    };

    // selected_model_name() must be called AFTER fit_predict so the best model is known
    let model_name = model
        .selected_model_name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let selected_model = normalize_model_name(&model_name);
    output.model_name = selected_model.to_string();

    // With STORE the forecast is persisted first; a failed write is the
    // command's failure, since the caller asked for the samples, not the reply.
    if let (Some(dest_key), Some(anchor)) = (options.destination_key.as_ref(), anchor) {
        // The client has already been told the command failed; do not write behind it.
        if ctx.is_timed_out() {
            return;
        }
        if let Err(err) =
            store_forecast(&ctx, dest_key, &options, output.forecast.primary(), anchor)
        {
            ctx.reply(Err(err));
            return;
        }
    }

    reply_with_forecast_output(&ctx, &output);
}

fn store_forecast(
    ctx: &ThreadSafeReplyContext,
    dest_key: &[u8],
    options: &AutoForecastOptions,
    forecast: &[f64],
    anchor: StoreAnchor,
) -> ValkeyResult<()> {
    write_forecast_samples(
        ctx,
        dest_key,
        options.create_options.clone(),
        options.write_mode.unwrap_or_default(),
        forecast,
        anchor,
    )
    .map(|_| ())
}

pub(super) fn reply_with_interval_array(
    ctx: &ThreadSafeReplyContext,
    name: &'static str,
    values: &[f64],
) {
    reply_with_str(ctx, name);
    reply_with_double_array(ctx, values);
}

fn parse_single_value(iter: &mut CommandArgIterator, option_name: &str) -> ValkeyResult<f64> {
    let Ok(value_str) = iter.next_str() else {
        return Err(ValkeyError::String(format!(
            "TSDB: Missing value for {option_name}"
        )));
    };

    let value = value_str.parse().map_err(|_e| {
        ValkeyError::String(format!(
            "TSDB: invalid value for {option_name}: {value_str}"
        ))
    })?;

    Ok(value)
}

fn parse_models(model_str: &str, config: &mut AutoForecastConfig) -> ValkeyResult<()> {
    // remove all models first; we'll add back the ones specified by the user
    config.include_arima = false;
    config.include_ets = false;
    config.include_theta = false;

    for family in model_str.split(',') {
        let model = family.trim().to_uppercase();
        match model.as_str() {
            "ARIMA" | "AUTOARIMA" => {
                config.include_arima = true;
            }
            "ETS" | "AUTOETS" => {
                config.include_ets = true;
            }
            "THETA" | "AUTOTHETA" => {
                config.include_theta = true;
            }
            "TBATS" => {
                config.include_tbats = true;
            }
            "MFLES" => {
                config.include_mfles = true;
            }
            "MSTL" => {
                config.include_mstl = true;
            }
            _ => {
                return Err(ValkeyError::String(format!(
                    "TSDB: unknown auto-forecast model: {}",
                    family
                )));
            }
        }
    }

    if !config.include_arima
        && !config.include_ets
        && !config.include_theta
        && !config.include_tbats
        && !config.include_mfles
        && !config.include_mstl
    {
        return Err(ValkeyError::Str(
            "TSDB: at least one valid model must be specified in MODELS",
        ));
    }
    Ok(())
}
