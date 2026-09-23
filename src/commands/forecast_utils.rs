use crate::analysis::forecasting::{input_scale_fitted, make_forecast_time_series};
use crate::commands::CommandArgIterator;
use crate::commands::analysis_runner::AnalysisCtx;
use crate::commands::command_parser::parse_series_range_samples;
use crate::commands::store_target::StoreTarget;
use crate::commands::ts_autoforecast::reply_with_interval_array;
use crate::commands::utils::{reply_with_accuracy_metrics, reply_with_double_array};
use crate::common::replies::{
    ReplyContext, reply_with_double, reply_with_map, reply_with_null, reply_with_str,
    reply_with_usize,
};
use crate::common::time::compute_median_step_ms;
use crate::common::{Sample, Timestamp};
use anofox_forecast::core::{Forecast, TimeSeries as ForecastTimeSeries};
use anofox_forecast::models::Forecaster;
use anofox_forecast::prelude::{AccuracyMetrics, calculate_metrics};
use valkey_module::{Context, ValkeyError, ValkeyResult};

pub(super) fn parse_timeseries_for_forecast(
    ctx: &Context,
    args: &mut CommandArgIterator,
) -> ValkeyResult<ForecastTimeSeries> {
    let samples = parse_series_range_samples(ctx, args)?;
    make_forecast_time_series(samples.into_iter())
        .map_err(|_e| ValkeyError::Str("TSDB: Failed to prepare time series for forecasting"))
}

/// Where `STORE` anchors the forecast samples: the last observed timestamp and
/// the step between consecutive forecast points.
#[derive(Clone, Copy, Debug)]
pub(super) struct StoreAnchor {
    pub last_ts: Timestamp,
    pub step_ms: i64,
}

impl StoreAnchor {
    /// Timestamp of the `i`-th (zero-based) forecast point.
    pub fn timestamp_at(&self, i: usize) -> ValkeyResult<Timestamp> {
        let steps = i
            .checked_add(1)
            .and_then(|count| i64::try_from(count).ok())
            .ok_or(ValkeyError::Str(STORE_TIMESTAMP_OVERFLOW_ERROR))?;
        self.step_ms
            .checked_mul(steps)
            .and_then(|delta| self.last_ts.checked_add(delta))
            .ok_or(ValkeyError::Str(STORE_TIMESTAMP_OVERFLOW_ERROR))
    }
}

/// Derive the [`StoreAnchor`] for a series about to be forecast.
///
/// The step is the series' detected frequency, falling back to the median
/// positive gap between samples. Both need at least two distinct timestamps,
/// so a range that cannot yield a step is rejected here — on the main thread,
/// before the client is blocked — rather than silently downgrading `STORE`
/// to a plain reply after the models have run. The final timestamp for the
/// requested horizon is checked here for the same reason.
pub(super) fn store_anchor(
    series: &ForecastTimeSeries,
    horizon: usize,
) -> ValkeyResult<StoreAnchor> {
    let timestamps = series.timestamps();
    let last_ts = timestamps
        .last()
        .map(|dt| dt.timestamp_millis())
        .ok_or(ValkeyError::Str(STORE_STEP_ERROR))?;
    let step_ms = series
        .frequency()
        .map(|d| d.num_milliseconds())
        .filter(|&step| step > 0)
        .or_else(|| {
            let millis: Vec<i64> = timestamps.iter().map(|dt| dt.timestamp_millis()).collect();
            compute_median_step_ms(&millis)
        })
        .ok_or(ValkeyError::Str(STORE_STEP_ERROR))?;
    let anchor = StoreAnchor { last_ts, step_ms };
    if horizon > 0 {
        anchor.timestamp_at(horizon - 1)?;
    }
    Ok(anchor)
}

/// Write forecast `values` as consecutive samples after `anchor` into `target`, under the
/// GIL. Returns the number of samples written, or `None` when the client timed out first and
/// nothing was written.
pub(super) fn write_forecast_samples(
    actx: &AnalysisCtx<'_>,
    target: &StoreTarget,
    values: &[f64],
    anchor: StoreAnchor,
) -> ValkeyResult<Option<usize>> {
    let samples: Vec<Sample> = values
        .iter()
        .enumerate()
        .map(|(i, &value)| anchor.timestamp_at(i).map(|ts| Sample::new(ts, value)))
        .collect::<ValkeyResult<_>>()?;

    actx.with_locked_context(|ctx| {
        // Checked under the lock: the timeout callback runs on the main thread, so it cannot
        // fire between this check and the write. Once it has fired, the client has been told
        // the command failed, so do not write behind it.
        if actx.is_timed_out() {
            return Ok(None);
        }
        target.write(ctx, &samples).map(Some).map_err(|e| {
            let msg = format!(
                "TSDB: failed to store forecast in key '{}': {}",
                String::from_utf8_lossy(target.key()),
                e
            );
            ctx.log_warning(&msg);
            ValkeyError::String(msg)
        })
    })
}

const STORE_STEP_ERROR: &str =
    "TSDB: STORE requires at least two samples in the range to determine the forecast step";
const STORE_TIMESTAMP_OVERFLOW_ERROR: &str =
    "TSDB: STORE forecast timestamps exceed the supported range";

/// The `METRICS` section of a forecast reply.
pub(super) enum ForecastMetrics {
    NotRequested,
    /// Requested, but the model reports no in-sample fit to score (GARCH models volatility,
    /// not the level). Replied as `null` rather than failing the whole command.
    Unavailable,
    Computed(AccuracyMetrics),
}

pub struct ForecastOutput {
    pub(crate) model_name: String,
    horizon: usize,
    level: Option<f64>,
    pub(crate) forecast: Forecast,
    metrics: ForecastMetrics,
}

/// Fits `model` and forecasts `horizon` steps. With `with_metrics`, scores the in-sample fit
/// against the series; `fit_from_residuals` is asked after fitting whether the model's
/// `fitted_values()` are on a differenced scale (an ARIMA-family model picked by an automatic
/// search), in which case the fit is rebuilt from its residuals instead.
pub(super) fn run_forecast<T: Forecaster + ?Sized>(
    series: &ForecastTimeSeries,
    model: &mut T,
    horizon: usize,
    level: Option<f64>,
    with_metrics: bool,
    seasonal_period: Option<usize>,
    fit_from_residuals: impl FnOnce(&T) -> bool,
) -> Result<ForecastOutput, ValkeyError> {
    let res = if let Some(level) = level {
        model.fit_predict_with_intervals(series, horizon, level / 100.0)
    } else {
        model.fit_predict(series, horizon)
    };

    let forecast = match res {
        Ok(prediction) => prediction,
        Err(e) => {
            let msg = format!("TSDB: {}", e);
            return Err(ValkeyError::String(msg));
        }
    };

    let metrics = if with_metrics {
        let actual = series.primary_values();
        let rebuilt;
        let fitted = if fit_from_residuals(model) {
            rebuilt = model
                .residuals()
                .and_then(|residuals| input_scale_fitted(actual, model.fitted_values(), residuals));
            rebuilt.as_deref()
        } else {
            model.fitted_values()
        };
        match fitted {
            None => ForecastMetrics::Unavailable,
            Some(fitted) => {
                let actual = if fitted.len() < actual.len() {
                    &actual[actual.len() - fitted.len()..]
                } else {
                    actual
                };

                // Some models (e.g. ARIMA) report NaN for a leading warm-up period
                // where insufficient lagged terms are available to produce a fitted
                // value. Trim that warm-up prefix from both series before scoring so
                // metrics reflect only the values the model actually fitted.
                let warmup = fitted.iter().take_while(|v| !v.is_finite()).count();
                let (actual, fitted) = (&actual[warmup..], &fitted[warmup..]);

                match calculate_metrics(actual, fitted, seasonal_period) {
                    Ok(m) => ForecastMetrics::Computed(m),
                    Err(e) => {
                        let msg = format!("TSDB: metrics error: {}", e);
                        return Err(ValkeyError::String(msg));
                    }
                }
            }
        }
    } else {
        ForecastMetrics::NotRequested
    };

    let forecast_output = ForecastOutput {
        model_name: model.name().to_string(),
        horizon,
        level,
        forecast,
        metrics,
    };

    Ok(forecast_output)
}

pub(super) fn get_lower_interval(forecast: &Forecast) -> Option<&[f64]> {
    let lower_values = forecast.lower()?;
    if lower_values.is_empty() {
        return None;
    }
    Some(lower_values[0].as_slice())
}

pub(super) fn get_upper_interval(forecast: &Forecast) -> Option<&[f64]> {
    let upper_values = forecast.upper()?;
    if upper_values.is_empty() {
        return None;
    }
    Some(upper_values[0].as_slice())
}

/// Writes the `metrics` entry of a forecast reply: the key, then the metrics map.
pub(super) fn reply_with_metrics_entry(ctx: &ReplyContext, metrics: &AccuracyMetrics) {
    reply_with_str(ctx, "metrics");
    reply_with_accuracy_metrics(ctx, metrics);
}

pub(super) fn reply_with_forecast_output(ctx: &ReplyContext, forecast_output: &ForecastOutput) {
    let mut map_len: usize = 3; // model, horizon, forecast are always included

    let forecast = &forecast_output.forecast;

    let predicted_values = forecast.primary();
    let lower_interval = get_lower_interval(forecast);
    let upper_interval = get_upper_interval(forecast);

    // "level" is only emitted when intervals exist. The same `level` binding gates
    // both the count and the emit below so the map header can never disagree
    // with the number of entries actually written.
    let level = if lower_interval.is_some() || upper_interval.is_some() {
        forecast_output.level
    } else {
        None
    };

    if level.is_some() {
        map_len += 1;
    }
    if lower_interval.is_some() {
        map_len += 1;
    }
    if upper_interval.is_some() {
        map_len += 1;
    }
    if !matches!(forecast_output.metrics, ForecastMetrics::NotRequested) {
        map_len += 1;
    }
    reply_with_map(ctx, map_len);

    reply_with_str(ctx, "model");
    reply_with_str(ctx, &forecast_output.model_name);
    reply_with_str(ctx, "horizon");
    reply_with_usize(ctx, forecast_output.horizon);
    reply_with_str(ctx, "forecast");
    reply_with_double_array(ctx, predicted_values);

    if let Some(level) = level {
        reply_with_str(ctx, "level");
        reply_with_double(ctx, level);
    }

    if let Some(lower_values) = lower_interval {
        reply_with_interval_array(ctx, "lower_interval", lower_values);
    }
    if let Some(upper_values) = upper_interval {
        reply_with_interval_array(ctx, "upper_interval", upper_values);
    }

    match &forecast_output.metrics {
        ForecastMetrics::NotRequested => {}
        ForecastMetrics::Unavailable => {
            reply_with_str(ctx, "metrics");
            reply_with_null(ctx);
        }
        ForecastMetrics::Computed(m) => reply_with_metrics_entry(ctx, m),
    }
}
