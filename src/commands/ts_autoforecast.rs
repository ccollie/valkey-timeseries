use crate::analysis::forecasting::normalize_model_name;
use crate::analysis::seasonality::MIN_SEASONAL_PERIOD;
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::{AnalysisTimeout, parse_timeout, run_analysis_job};
use crate::commands::command_parser::{
    parse_forecast_confidence_level, parse_forecast_horizon_value, parse_store_clause,
};
use crate::commands::forecast_utils::{
    StoreAnchor, handle_forecast_key_pos_request, parse_timeseries_for_forecast,
    reply_with_forecast_output, run_forecast, store_anchor, write_forecast_samples,
};
use crate::commands::store_target::StoreTarget;
use crate::commands::utils::reply_with_double_array;
use crate::common::replies::{ThreadSafeReplyContext, reply_with_str};
use anofox_forecast::core::TimeSeries as ForecastTimeSeries;
use anofox_forecast::detection::detect_dominant_period;
use anofox_forecast::models::auto_forecast::{AutoForecast, AutoForecastConfig};
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

struct AutoForecastOptions {
    horizon: usize,
    level: Option<f64>,
    metrics: bool,
    store: Option<StoreTarget>,
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
            store: None,
            config: AutoForecastConfig::default(),
            auto_seasonality: false,
            timeout: AnalysisTimeout::default(),
        }
    }
}

acl_categories!(TS_AUTOFORECAST, "ts.autoforecast", "write timeseries");
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
            begin_search: Keyword({ keyword: "STORE", startfrom: 4 }),
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

    let source_key = args[1].as_slice().to_vec();
    let mut args = args.into_iter().skip(1).peekable();

    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_autoforecast_args(ctx, &source_key, &mut args)?;
    let sample_count = series.primary_values().len();
    if let Some(period) = options.config.seasonal_period
        && period > sample_count
    {
        return Err(ValkeyError::String(format!(
            "TSDB: SEASONALITY period {period} exceeds the {sample_count} samples in the range"
        )));
    }
    // Validate the STORE step and final timestamp before the model search.
    let anchor = options
        .store
        .as_ref()
        .map(|_| store_anchor(&series, options.horizon))
        .transpose()?;

    run_analysis_job(ctx, options.timeout, move |thread_ctx| {
        process_forecast(thread_ctx, series, options, anchor);
    });

    // Reply will be sent from the analysis pool
    Ok(ValkeyValue::NoReply)
}

fn parse_autoforecast_args(
    ctx: &Context,
    source_key: &[u8],
    args: &mut CommandArgIterator,
) -> ValkeyResult<AutoForecastOptions> {
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
                    let period = args.next_i64().map_err(|_| {
                        ValkeyError::Str("TSDB: SEASONALITY must be AUTO or an integer period")
                    })?;
                    if period < MIN_SEASONAL_PERIOD as i64 {
                        return Err(ValkeyError::Str("TSDB: SEASONALITY period must be at least 2"));
                    }
                    options.config.seasonal_period = Some(period as usize);
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
                    let store = parse_store_clause(args)?;
                    options.store = Some(StoreTarget::new(ctx, source_key, store)?);
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
    ctx: &ThreadSafeReplyContext,
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
    if let (Some(target), Some(anchor)) = (options.store.as_ref(), anchor) {
        match write_forecast_samples(ctx, target, output.forecast.primary(), anchor) {
            Ok(Some(_)) => {}
            // Timed out: the client already has its error, and nothing was written.
            Ok(None) => return,
            Err(err) => {
                ctx.reply(Err(err));
                return;
            }
        }
    }

    reply_with_forecast_output(ctx, &output);
}

pub(super) fn reply_with_interval_array(
    ctx: &ThreadSafeReplyContext,
    name: &'static str,
    values: &[f64],
) {
    reply_with_str(ctx, name);
    reply_with_double_array(ctx, values);
}

fn parse_models(model_str: &str, config: &mut AutoForecastConfig) -> ValkeyResult<()> {
    // A repeated MODELS clause replaces the full candidate set.
    config.include_arima = false;
    config.include_ets = false;
    config.include_theta = false;
    config.include_tbats = false;
    config.include_mfles = false;
    config.include_mstl = false;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_models_replaces_every_family_but_preserves_seasonality() {
        let mut config = AutoForecastConfig {
            seasonal_period: Some(12),
            ..AutoForecastConfig::default()
        };

        parse_models("TBATS,MFLES,MSTL", &mut config).unwrap();
        parse_models("ARIMA", &mut config).unwrap();

        assert!(config.include_arima);
        assert!(!config.include_ets);
        assert!(!config.include_theta);
        assert!(!config.include_tbats);
        assert!(!config.include_mfles);
        assert!(!config.include_mstl);
        assert_eq!(config.seasonal_period, Some(12));
    }
}
