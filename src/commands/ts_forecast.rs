use crate::analysis::forecasting::DynForecaster;
use crate::analysis::forecasting::{
    PreparedModelSpec, build_transforms_from_specs, prepare_model_specs, wrap_model_with_transforms,
};
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::{
    AnalysisCtx, AnalysisTimeout, parse_timeout, run_analysis_in_background,
};
use crate::commands::command_parser::{
    parse_forecast_confidence_level, parse_forecast_horizon_value,
};
use crate::commands::forecast_utils::{
    ForecastOutput, StoreAnchor, handle_forecast_key_pos_request, parse_timeseries_for_forecast,
    reply_with_forecast_output, run_forecast, store_anchor, write_forecast_samples,
};
use crate::commands::parse_store_clause;
use crate::commands::store_target::StoreTarget;
use crate::common::replies::reply_with_array;
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
    store: Option<StoreTarget>,
    timeout: AnalysisTimeout,
}

acl_categories!(TS_FORECAST, "ts.forecast", "write timeseries");
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
            begin_search: Keyword({ keyword: "STORE", startfrom: 4 }),
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

    let source_key = args[1].as_slice().to_vec();
    let mut args = args.into_iter().skip(1).peekable();
    let series = parse_timeseries_for_forecast(ctx, &mut args)?;
    let options = parse_forecast_args(ctx, &source_key, &mut args)?;
    // Validate the STORE step and final timestamp before dispatching models.
    let anchor = options
        .store
        .as_ref()
        .map(|_| store_anchor(&series, options.horizon))
        .transpose()?;

    let timeout = options.timeout;
    run_analysis_in_background(
        ctx,
        timeout,
        move || {
            let results = process_models(&series, &options)?;
            Ok((results, options.store))
        },
        move |actx, (results, store)| {
            // With STORE the reply is the number of samples written, not the forecast.
            if let (Some(target), Some(anchor)) = (store.as_ref(), anchor) {
                return store_forecast(actx, target, &results, anchor);
            }
            let reply_ctx = actx.reply_ctx();
            reply_with_array(&reply_ctx, results.len());
            for output in &results {
                reply_with_forecast_output(&reply_ctx, output);
            }
            Ok(ValkeyValue::NoReply)
        },
    )
}

/// Persist the predicted values into the STORE destination key; the reply is the number of
/// samples written.
fn store_forecast(
    actx: &AnalysisCtx<'_>,
    target: &StoreTarget,
    results: &[ForecastOutput],
    anchor: StoreAnchor,
) -> ValkeyResult {
    // STORE is limited to a single model, so the outputs concatenate into one
    // run of consecutive steps after the last observed sample.
    let values: Vec<f64> = results
        .iter()
        .flat_map(|output| output.forecast.primary().iter().copied())
        .collect();
    match write_forecast_samples(actx, target, &values, anchor)? {
        Some(written) => Ok(ValkeyValue::Integer(written as i64)),
        // Timed out: the client already has its error, and nothing was written.
        None => Ok(ValkeyValue::NoReply),
    }
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

fn parse_forecast_args(
    ctx: &Context,
    source_key: &[u8],
    args: &mut CommandArgIterator,
) -> ValkeyResult<ForecastOptions> {
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

    if options.models.is_empty() {
        return Err(ValkeyError::Str(
            "TSDB: MODELS must contain at least one model specification",
        ));
    }

    if options.store.is_some() && options.models.len() > 1 {
        return Err(ValkeyError::Str(
            "TSDB: STORE is only supported with a single model",
        ));
    }

    Ok(options)
}
