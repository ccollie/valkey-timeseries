use crate::analysis::forecasting::DynForecaster;
use crate::analysis::forecasting::{
    PreparedModelSpec, build_transforms_from_specs, prepare_model_specs, wrap_model_with_transforms,
};
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::{AnalysisTimeout, parse_timeout, run_analysis_job};
use crate::commands::command_parser::{
    parse_forecast_confidence_level, parse_forecast_horizon_value,
};
use crate::commands::forecast_utils::{
    ForecastOutput, StoreAnchor, handle_forecast_key_pos_request, parse_timeseries_for_forecast,
    reply_with_forecast_output, run_forecast, store_anchor, write_forecast_samples,
};
use crate::commands::parse_store_clause;
use crate::common::replies::{ThreadSafeReplyContext, reply_with_array};
use crate::series::DestinationWriteMode;
use crate::series::TimeSeriesOptions;
use anofox_forecast::core::TimeSeries as ForecastTimeSeries;
use anofox_forecast::transform::Transform;
use valkey_module::{Context, NextArg, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

#[derive(Default)]
struct ForecastOptions {
    /// Parsed and validated `MODELS`; each entry builds a fresh model on the pool.
    models: Vec<PreparedModelSpec>,
    /// Reversible pre-processing chain applied, in order, ahead of every model
    /// (see `TRANSFORMS`). Built on the main thread so a bad spec is rejected
    /// before the client is blocked; each model gets its own clone.
    transforms: Vec<Box<dyn Transform>>,
    horizon: usize,
    include_metrics: bool,
    level: Option<f64>,
    destination_key: Option<Vec<u8>>,
    series_options: Option<TimeSeriesOptions>,
    write_mode: DestinationWriteMode,
    timeout: AnalysisTimeout,
}

/// Forecasts future values of a time series using a specified model.
///
/// ```text
///  TS.FORECAST key start_timestamp end_timestamp
///   MODELS model spec, ..
///   HORIZON horizon
///   [LEVEL confidenceLevel]
///   [TRANSFORMS transform spec, ..]
///   [METRICS]
///   [TIMEOUT milliseconds]
///   [STORE destinationKey
///     [MERGE]
///     [RETENTION retentionPeriod]
///     [ENCODING <pco|gorilla|uncompressed|compressed>]
///     [CHUNK_SIZE chunkSize]
///     [DUPLICATE_POLICY duplicatePolicy]
///     [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///     [METRIC metric]
///     [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///   ]
/// ```
///
#[valkey_module_macros::command({
    name: "ts.forecast",
    flags: [Write, DenyOOM],
    summary: "Forecast future values of a time series using one or more explicit models.",
    complexity: "O(N*M) where N is the number of samples in the range and M is the number of models.",
    since: "1.0.0",
    arity: -8,
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
pub(crate) fn ts_forecast_cmd(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 8 {
        return Err(ValkeyError::WrongArity);
    }

    if handle_forecast_key_pos_request(ctx, &args)? {
        return Ok(ValkeyValue::NoReply);
    }

    let mut args = args.into_iter().skip(1).peekable();
    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_forecast_args(&mut args)?;
    // STORE needs a step to place the forecast samples; reject a range that
    // cannot provide one now rather than after the models have run.
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

fn process_forecast(
    ctx: ThreadSafeReplyContext,
    series: ForecastTimeSeries,
    options: ForecastOptions,
    anchor: Option<StoreAnchor>,
) {
    let results = match process_models(&series, &options) {
        Ok(results) => results,
        Err(e) => {
            ctx.reply(Err(e));
            return;
        }
    };

    // With STORE the reply is the number of samples written, not the forecast.
    if let (Some(dest_key), Some(anchor)) = (options.destination_key.as_ref(), anchor) {
        // The client has already been told the command failed; do not write behind it.
        if ctx.is_timed_out() {
            return;
        }
        store_forecast(&ctx, dest_key, &options, &results, anchor);
        return;
    }

    reply_with_array(&ctx, results.len());
    for output in results {
        reply_with_forecast_output(&ctx, &output);
    }
}

/// Persist the predicted values into the STORE destination key and reply
/// with the number of samples written, or with an error if the write fails.
fn store_forecast(
    ctx: &ThreadSafeReplyContext,
    dest_key: &[u8],
    options: &ForecastOptions,
    results: &[ForecastOutput],
    anchor: StoreAnchor,
) {
    // STORE is limited to a single model, so the outputs concatenate into one
    // run of consecutive steps after the last observed sample.
    let values: Vec<f64> = results
        .iter()
        .flat_map(|output| output.forecast.primary().iter().copied())
        .collect();
    let reply = write_forecast_samples(
        ctx,
        dest_key,
        options.series_options.clone(),
        options.write_mode,
        &values,
        anchor,
    )
    .map(|written| ValkeyValue::Integer(written as i64));
    ctx.reply(reply);
}

fn process_models(
    series: &ForecastTimeSeries,
    options: &ForecastOptions,
) -> ValkeyResult<Vec<ForecastOutput>> {
    let mut results = Vec::with_capacity(options.models.len());
    for spec in &options.models {
        // Validated at parse time, so this only fails on a bug in the builder.
        let model = spec
            .build()
            .map_err(|e| ValkeyError::String(format!("TSDB: error building model: {e}")))?;
        let model = wrap_model_with_transforms(model, &options.transforms);
        let mut model: DynForecaster = DynForecaster::from(model);
        let mut output = run_forecast(
            series,
            &mut model,
            options.horizon,
            options.level,
            options.include_metrics,
            None, // seasonal_period can be added as an option if needed
        )?;
        output.model_name = spec.display_name().to_string();
        results.push(output);
    }
    Ok(results)
}

fn parse_forecast_args(args: &mut CommandArgIterator) -> ValkeyResult<ForecastOptions> {
    let mut options = ForecastOptions::default();
    let mut horizon_set = false;

    while let Some(arg) = args.next() {
        hashify::fnc_map_ignore_case!(
                arg.as_slice(),
                "HORIZON" => {
                    options.horizon = parse_forecast_horizon_value(args)?;
                    horizon_set = true;
                },
                "MODELS" => {
                    let spec = args.next_string().map_err(|_| ValkeyError::Str("TSDB: missing value for MODELS"))?;
                    options.models = prepare_model_specs(&spec)
                        .map_err(|e| ValkeyError::String(format!("TSDB: error parsing MODELS: {e}")))?;
                },
                "LEVEL" => {
                    let value = parse_forecast_confidence_level(args)?;
                    options.level = Some(value);
                },
                "TRANSFORMS" => {
                    let spec = args.next_string().map_err(|_| ValkeyError::Str("TSDB: missing value for TRANSFORMS"))?;
                    options.transforms = build_transforms_from_specs(&spec)
                        .map_err(|e| ValkeyError::String(format!("TSDB: error parsing TRANSFORMS: {e}")))?;
                    if options.transforms.is_empty() {
                        return Err(ValkeyError::Str("TSDB: TRANSFORMS must contain at least one transform specification"));
                    }
                },
                "METRICS" => {
                    options.include_metrics = true;
                },
                "TIMEOUT" => {
                    options.timeout.set(parse_timeout(args)?);
                },
                "STORE" => {
                    let store_options = parse_store_clause(args)?;
                    options.destination_key = Some(store_options.key.into());
                    options.series_options = Some(store_options.options);
                    options.write_mode = store_options.write_mode;
                },
            _ => {
                return Err(ValkeyError::String(format!("TSDB: Unknown argument: {}", arg)));
            }
        );
    }

    if !horizon_set {
        return Err(ValkeyError::Str("TSDB: HORIZON is required"));
    }

    if options.models.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: MODELS must contain at least one model specification",
        ));
    }

    if options.destination_key.is_some() && options.models.len() > 1 {
        return Err(ValkeyError::Str(
            "TSDB: STORE is only supported with a single model",
        ));
    }

    Ok(options)
}
