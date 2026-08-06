# RFC: Forecasting and Time-Series Analysis Support

- **Status:** Implemented; pending review and release approval
- **Branch:** `feat/forecasting`
- **Date:** 2026-08-06
- **Author:** valkey-tslib team

## 1. Summary

This RFC adds in-module forecasting, backtesting, trend analysis, feature
extraction, decomposition, period detection, stationarity testing,
autocorrelation analysis, and data-preparation utilities to Valkey TimeSeries.
The feature set is exposed through eleven commands:

- `TS.FORECAST` — fit explicitly selected models and forecast future points.
- `TS.AUTOFORECAST` — select a forecasting model automatically and forecast.
- `TS.BACKTEST` — assess one or more models using walk-forward validation.
- `TS.TREND` — fit or select a trend component, optionally predict and store it.
- `TS.FEATURES` — compute statistical and time-series features over a range.
- `TS.AUTOCORRELATION` — compute autocorrelation-derived statistics at a lag.
- `TS.DECOMPOSE` — decompose a series into trend, seasonal, and residual
  components via STL or MSTL.
- `TS.PERIODS` — detect seasonal periods using spectral analysis.
- `TS.STATIONARITY` — test stationarity using ADF, KPSS, or both.
- `TS.FILLGAPS` — compute (and optionally store) samples that would fill
  missing timestamps, without modifying the source series.
- `TS.SANITIZE` — replace or drop missing/infinite values in a series,
  in place, using a selectable imputation policy.

These commands are Valkey TimeSeries extensions; they are not part of the
RedisTimeSeries compatibility surface. They are intended for applications that
want analysis close to their stored series without exporting data to a separate
forecasting service.

## 2. Goals and Non-Goals

### Goals

- Provide explicit and automatic forecasting over a selected series range.
- Support model comparison using held-out, walk-forward backtesting rather than
  only in-sample fit metrics.
- Allow forecast and trend results to be materialized as Valkey time series.
- Provide feature and correlation diagnostics useful for model selection and
  downstream analytics.
- Provide data-preparation utilities (gap filling, missing-value sanitization)
  since most models cannot fit through `NaN`/`Inf` values or irregular grids.
- Keep commands responsive to other clients by performing model fitting and
  feature computation away from Valkey's main thread. Currently achieved for
  `TS.FORECAST`, `TS.AUTOFORECAST`, `TS.BACKTEST`, and `TS.FEATURES` only.

### Non-Goals

- Providing a guarantee that a model will fit every data set or produce a
  statistically useful forecast.
- Distributed or cross-shard forecasting. The commands operate on one source
  series; when a result is stored, normal Valkey cluster key-slot rules apply.
- Replacing a full ML workflow, feature store, or model registry.

## 3. Design

The command layer reads the requested source range and converts it to the
forecasting library's time-series representation. `TS.FORECAST`,
`TS.AUTOFORECAST`, `TS.BACKTEST`, and `TS.FEATURES` then schedule the
expensive work on a blocked-client background thread; replies are sent after
the work completes, so the Valkey main thread is not occupied by fitting,
validation, or feature calculation. `TS.TREND`, `TS.AUTOCORRELATION`,
`TS.DECOMPOSE`, `TS.PERIODS`, `TS.STATIONARITY`, `TS.FILLGAPS`, and
`TS.SANITIZE` currently execute synchronously on the main thread.

Forecasting uses `anofox-forecast` (version 0.15.8 on the branch) with its
parallel, seasonal-detection, distributional, postprocess, and anomaly
features enabled. Supporting analysis code includes work adapted from
MIT-licensed Anofox, SciRS2, and Perfolizer components.

When `STORE` is requested, the implementation derives future timestamps from
the median positive sampling interval of the source series. Therefore storage
of future points requires a source with enough timestamp information to derive
a positive interval.

## 4. Public API

### 4.1 TS.FORECAST

```text
TS.FORECAST key fromTimestamp toTimestamp
  MODELS model_spec[,model_spec ...]
  HORIZON horizon
  [LEVEL confidence_level]
  [WITH_METRICS]
  [STORE destination
    [MERGE]
    [RETENTION retentionPeriod]
    [ENCODING encoding]
    [CHUNK_SIZE chunkSize]
    [DUPLICATE_POLICY duplicatePolicy]
    [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
    [METRIC metric]
    [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  ]
```

`TS.FORECAST` fits every listed model independently and returns one result per
model. A result includes the canonical model name, horizon, and forecast. When
requested and supported, it also includes prediction intervals and in-sample
accuracy metrics (`mae`, `mse`, `rmse`, `mape`, `smape`, `mase`, and
`r_squared`).

`STORE` is permitted only with one model. Without `MERGE`, the destination is
replaced; with `MERGE`, duplicate forecast timestamps use last-value behavior.
The destination may specify normal time-series creation options, including
retention, encoding, chunk size, duplicate policy, rounding, metric name, and
ignore thresholds.

**Example**

```text
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 5
1) 1) "model"
   2) "ARIMA(2,1,0)"
   3) "horizon"
   4) (integer) 5
   5) "forecast"
   6) 1) "104.12"
      2) "105.24"
      3) "106.36"
      4) "107.48"
      5) "108.60"
```

With `STORE`, the forecast is written to a destination key instead and the
reply is the number of samples written:

```text
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 5 STORE forecast:result
(integer) 5
```

### 4.2 TS.AUTOFORECAST

```text
TS.AUTOFORECAST key fromTimestamp toTimestamp HORIZON horizon
  [SEASONALITY period]
  [MODELS family[,family ...]]
  [LEVEL confidence_level]
  [METRICS]
  [STORE destination
    [MERGE]
    [RETENTION retentionPeriod]
    [ENCODING encoding]
    [CHUNK_SIZE chunkSize]
    [DUPLICATE_POLICY duplicatePolicy]
    [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
    [METRIC metric]
    [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  ]
```

`TS.AUTOFORECAST` evaluates enabled automatic model families, including
AutoARIMA, AutoETS, and AutoTheta, and selects the best candidate by
cross-validation error. It can detect seasonality automatically or accept an
explicit seasonal period. Its output follows the explicit-forecast shape but
reports the chosen model. Its `STORE` clause takes the same options as
`TS.FORECAST`'s, including `MERGE` and series-creation options — the branch's
own `docs/commands/ts.autoforcast.md` (note the filename typo: "autoforcast",
missing the "e") documents only `STORE destination`, which is stale relative
to the command's source (`src/commands/ts_autoforecast.rs`, which parses the
full clause via the shared `parse_store_clause`).

**`STORE` does not change the reply shape, unlike `TS.FORECAST`.** For
`TS.FORECAST`, a successful `STORE` short-circuits the reply to an integer
count (`store_forecast_if_necessary` returns a "handled" flag). For
`TS.AUTOFORECAST`, `STORE` is a fire-and-forget side effect
(`store_if_necessary` returns `()`): the client always receives the full
forecast map, and if the write fails — including the "could not determine
forecast step" case that both commands can hit — the failure is only sent to
`ctx.log_warning`, never to the caller. A caller has no way to detect a failed
`STORE` from the reply alone.

**Example**

```text
127.0.0.1:6379> TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5 LEVEL 95
1) "model"
2) "ARIMA"
3) "horizon"
4) "5"
5) "forecast"
6) 1) "105.32"
   2) "105.78"
   3) "106.24"
   4) "106.70"
   5) "107.16"
7) "level"
8) "95"
9) "lower_interval"
10) 1) "103.50"
    2) "103.12"
    3) "102.75"
    4) "102.38"
    5) "102.01"
11) "upper_interval"
12) 1) "107.14"
    2) "108.44"
    3) "109.73"
    4) "111.02"
    5) "112.31"
```

With `STORE`, the reply is unchanged — still the full map above, not a count:

```text
127.0.0.1:6379> TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5 STORE temperature:sensor1:forecast
1) "model"
2) "ARIMA"
3) "horizon"
4) "5"
5) "forecast"
6) 1) "105.32"
   2) "105.78"
   3) "106.24"
   4) "106.70"
   5) "107.16"
```

### 4.3 TS.BACKTEST

```text
TS.BACKTEST key fromTimestamp toTimestamp
  MODELS model_spec[,model_spec ...]
  HORIZON horizon
  [INITIAL_WINDOW size]
  [STRATEGY EXPANDING|ROLLING]
  [STEP stepSize] [N_FOLDS n]
  [GAP gap] [PURGE purge] [EMBARGO embargo]
  [SEASONAL_PERIOD period]
  [WITH_PREDICTIONS]
```

`TS.BACKTEST` performs walk-forward validation. Each fold trains a fresh model
only on its training range, forecasts the held-out horizon, and records
out-of-sample metrics. Folds for a model run in parallel. The response contains
per-fold results and aggregated accuracy metrics; a failure in one model is
reported for that model rather than aborting other requested models.

`EXPANDING` training windows grow over time; `ROLLING` windows retain a fixed
size. `GAP` and `PURGE` both create separation before a test window in the
current implementation, while `EMBARGO` prevents later folds from training on
points immediately following earlier test windows.

### 4.4 TS.TREND

```text
TS.TREND key fromTimestamp toTimestamp
  [MODEL <Exponential|Logistic|Polynomial|TheilSen|Auto> [AICc|BIC|HOLDOUT]]
  [RECENCY <FULL|WINDOW n|FRACTION f|AUTO>]
  [PREDICT horizon] [FEATURES] [METRICS]
  [STORE destination
    [MERGE]
    [RETENTION retentionPeriod]
    [ENCODING encoding]
    [CHUNK_SIZE chunkSize]
    [DUPLICATE_POLICY duplicatePolicy]
    [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
    [METRIC metric]
    [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  ]
```

`TS.TREND` fits a selected trend model or selects one automatically. Auto mode
compares Linear, Quadratic, Exponential, Theil-Sen, and Piecewise Linear
candidates using AICc by default, with BIC and holdout selection available.
It can return fitted values, predictions, model features, and accuracy metrics,
or store the fitted and predicted series. Its `STORE` clause takes the same
options as `TS.FORECAST`'s; without `MERGE` the destination is overwritten,
with `MERGE` fitted (and predicted) samples are merged into it. The branch's
`docs/commands/ts.trend.md` shows a bare `[STORE destination]` in its syntax
block — inconsistent with its own prose two paragraphs later, which describes
`MERGE` — and is stale relative to `src/commands/ts_trend.rs`, which parses
the full clause via the same shared `parse_store_clause` as `TS.FORECAST` and
`TS.AUTOFORECAST`. Unlike `TS.AUTOFORECAST`, `TS.TREND`'s `STORE` is
well-behaved: it short-circuits the reply to an integer count and propagates
a real write failure as a command error, matching `TS.FORECAST`. The one
narrow gap is `PREDICT` combined with `STORE`: if the median step can't be
determined from the input samples, only the predicted extension is silently
dropped (`ctx.log_warning` only) — the fitted values still store normally, so
the returned count can be lower than `PREDICT n` implies.

**Example**

```text
127.0.0.1:6379> TS.TREND temperature - +
1) "model"
2) "Linear"
3) "criterion"
4) "AICc"
5) "fitted_trend"
6) 1) (double) 20.08
   2) (double) 20.31
   3) (double) 20.54
   4) (double) 20.77
   5) (double) 21.0
7) "scores"
8) 1) 1) "Linear"
       2) (double) -45.2
    2) 1) "Quadratic"
       2) (double) -43.1
    3) 1) "TheilSen"
       2) (double) -41.8
9) "n_params"
10) (integer) 2
```

With `STORE`, the reply is the number of samples written (fitted values, plus
predicted values when `PREDICT` is also given and a step could be derived):

```text
127.0.0.1:6379> TS.TREND temperature - + MODEL Auto BIC PREDICT 5 STORE temperature:trend
(integer) 10
```

### 4.5 TS.FEATURES

```text
TS.FEATURES key startTimestamp endTimestamp
  [CATEGORY <basic|distribution|autocorrelation|trend>,...]
  [FEATURE feature[,feature ...]]
```

`TS.FEATURES` returns a map of requested feature values. Categories cover basic,
distribution, autocorrelation, and trend features; individual features include
moments, entropy, change statistics, quantiles, ACF, and PACF. NaN feature
results are represented as null.

### 4.6 TS.AUTOCORRELATION

```text
TS.AUTOCORRELATION key startTimestamp endTimestamp lag
  [PARTIAL | TRA | AGGREGATED <mean|var|std|median>]
```

`TS.AUTOCORRELATION` returns ACF by default, PACF with `PARTIAL`, time-reversal
asymmetry with `TRA`, or an aggregate across lags one through the requested
lag.

### 4.7 TS.DECOMPOSE

```text
TS.DECOMPOSE key fromTimestamp toTimestamp
  [SEASONALITY <AUTO | period [period ...]>]
```

`TS.DECOMPOSE` splits a range into trend, seasonal, and residual components
using STL for a single seasonal period or MSTL for up to four. `SEASONALITY`
defaults to automatic period detection; the returned components satisfy
`original = trend + seasonal (+ seasonal_components) + residual`.

### 4.8 TS.PERIODS

```text
TS.PERIODS key fromTimestamp toTimestamp
  [MIN_STRENGTH minStrength] [DOMINANT]
```

`TS.PERIODS` runs the SAZED ensemble to identify periodic patterns and returns,
per candidate period, its spectral power, seasonal-differencing strength, ACF
at that lag, and cycle count. `MIN_STRENGTH` filters weak candidates (default
`0.05`); `DOMINANT` returns only the single strongest period, or `nil`. At
least 4 samples are required in range.

**Example**

The reply shape (array of `[period, power, strength, acf, n_cycles]` per
candidate) is confirmed against `src/commands/ts_periods.rs`; the illustrative
values below are unverified since this command has no test coverage (see
[§7](#7-testing-and-release-criteria)):

```text
127.0.0.1:6379> TS.PERIODS ts:temperature - +
1) 1) (integer) 2
   2) "0.5"
   3) "0.8"
   4) "0.7"
   5) (integer) 3
```

`DOMINANT` returns just the strongest period as an integer, or `nil` if none
meets `MIN_STRENGTH`:

```text
127.0.0.1:6379> TS.PERIODS ts:temperature - + DOMINANT
(integer) 2
```

### 4.9 TS.STATIONARITY

```text
TS.STATIONARITY key fromTimestamp toTimestamp
  [TEST adf|kpss|combined] [LAGS n]
```

`TS.STATIONARITY` runs ADF, KPSS, or both (`combined`, the default) and
reports a conclusion of `stationary`, `non_stationary`, or — for `combined`,
when the two tests disagree — `inconclusive`. `LAGS` overrides the per-test
default lag selection and is rejected together with `TEST combined`. At least
10 observations are required in the range.

### 4.10 TS.FILLGAPS

```text
TS.FILLGAPS key startTimestamp endTimestamp
  [VALUE value]
  [FREQUENCY duration]
  [ALIGN alignment_timestamp|start|-]
  [STORE destination
    [MERGE]
    [RETENTION retentionPeriod]
    [ENCODING encoding]
    [CHUNK_SIZE chunkSize]
    [DUPLICATE_POLICY duplicatePolicy]
    [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
    [METRIC metric]
    [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  ]
```

`TS.FILLGAPS` computes the timestamps expected on a frequency grid over
`[startTimestamp, endTimestamp]` — inferred from the modal sample interval by
default, or given explicitly via `FREQUENCY` — and returns a `[timestamp,
value]` pair (fill value defaults to `NaN`) for each expected timestamp that
has no existing sample. **It never modifies the source series.** Without
`STORE`, the computed gap samples are returned to the caller and discarded.
With `STORE`, they are written to `destination` instead of being returned,
using the same options as `TS.FORECAST`'s `STORE` clause; if there are no
gaps, the destination is left untouched (and not created if absent).
`ALIGN` anchors the frequency grid to a reference timestamp (`start`/`-` for
`startTimestamp`, or an explicit epoch offset) rather than to the first
sample.

### 4.11 TS.SANITIZE

```text
TS.SANITIZE key fromTimestamp toTimestamp
  [POLICY <ERROR|DROP|FILL value|FORWARDFILL|BACKWARDFILL|FILLMEAN|FILLMEDIAN
          |INTERPOLATE|FORWARDBACKWARDFILL|MOVINGAVERAGE window|SEASONAL period|auto>]
  [STORE destination
    [MERGE] [RETENTION retentionPeriod] [ENCODING encoding] [CHUNK_SIZE chunkSize]
    [DUPLICATE_POLICY duplicatePolicy]
    [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
    [METRIC metric] [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  ]
```

`TS.SANITIZE` finds missing values — `NaN`, `+Inf`, or `-Inf` — within
`[fromTimestamp, toTimestamp]` and applies an imputation policy, defaulting to
`DROP`. **Unlike every other command in this RFC, `TS.SANITIZE` always
rewrites the source series' sample range in place, whether or not `STORE` is
given** — it removes the range and merges back the sanitized result, sends a
`ts.sanitize` keyspace notification, and replicates verbatim. `STORE` writes
the same sanitized samples to an additional destination on top of that
in-place rewrite; it does not make the operation non-destructive. `POLICY
ERROR` returns an error and leaves the series unmodified if any missing value
is found in range. `MOVINGAVERAGE` and `SEASONAL` take a window size (odd,
positive) and a period (positive integer, or `auto` to detect the dominant
period), respectively.

**Example**

The default `DROP` policy removes the missing sample from the series itself —
`TS.RANGE` on the source confirms the mutation, not just the reply:

```text
127.0.0.1:6379> TS.ADD ts:metrics 1000 1.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:metrics 2000 NaN
(integer) 2000
127.0.0.1:6379> TS.ADD ts:metrics 3000 3.0
(integer) 3000
127.0.0.1:6379> TS.SANITIZE ts:metrics 1000 3000
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
127.0.0.1:6379> TS.RANGE ts:metrics - +
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
```

`STORE` adds a second destination write — it does not change what happens to
the source. Continuing from a fresh `ts:raw` with the same NaN sample:

```text
127.0.0.1:6379> TS.SANITIZE ts:raw 1000 3000 POLICY DROP STORE ts:clean
(integer) 2
127.0.0.1:6379> TS.RANGE ts:clean - +
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
127.0.0.1:6379> TS.RANGE ts:raw - +
1) 1) (integer) 1000
   2) 1.0
2) 1) (integer) 3000
   2) 3.0
```

`ts:raw` is left with the same two samples as `ts:clean` — `STORE` did not
make the call non-destructive to the source.

## 5. Models

Explicit model specifications are case-insensitive and support positional and
keyword parameters. Implemented families include:

- ARIMA, SARIMA, AutoARIMA
- ETS, AutoETS, SES, Holt, Holt-Winters, SeasonalES
- Naive, SeasonalNaive, SMA, Theta, RandomWalkWithDrift
- Croston, ADIDA, IMAPA, and TSB for intermittent demand
- TBATS, AutoTBATS, MSTL, and MFLES for seasonal or multi-seasonal data
- GARCH for volatility forecasting

Each `model_spec` is `ModelName(args...)`, with positional or keyword
arguments depending on the family (verified against the `MODELS` argument
table in `docs/commands/ts.forecast.md`):

| Family                | Example spec                                                              |
|------------------------|----------------------------------------------------------------------------|
| `ARIMA`                | `ARIMA(2,1,0)`                                                             |
| `AutoARIMA`            | `AutoARIMA()` or `AutoARIMA(seasonal_period=12)`                           |
| `SARIMA`               | `SARIMA(1,1,1,1,1,1,12)`                                                   |
| `ETS`                  | `ETS(A,N,A)` or `ETS(error=A, trend=N, season=A)`                          |
| `AutoETS`              | `AutoETS()` or `AutoETS(seasonal_period=12)`                               |
| `SES`                  | `SES(0.3)` or `SES(alpha=0.3)`                                             |
| `Holt`                 | `Holt()` or `Holt(alpha=0.3, beta=0.1)`                                    |
| `HoltWinters`          | `HoltWinters(alpha=0.3, beta=0.1, gamma=0.1, seasonal_type="add", seasonal_period=12)` |
| `Naive`                | `Naive()`                                                                  |
| `RandomWalkWithDrift`  | `RandomWalkWithDrift()` or `RandomWalkWithDrift(changepoint=10)`           |
| `SeasonalNaive`        | `SeasonalNaive(12)` or `SeasonalNaive(period=12)`                          |
| `SMA`                  | `SMA(5)` or `SMA(window=5)`                                                |
| `Theta`                | `Theta()` or `Theta(theta_lines=2, decomposition="multiplicative")`        |
| `Croston`              | `Croston()` or `Croston(alpha=0.3)`                                       |
| `ADIDA`                | `ADIDA()` or `ADIDA(size=5)`                                               |
| `IMAPA`                | `IMAPA()`                                                                  |
| `TSB`                  | `TSB(alpha_d=0.3, alpha_p=0.2)`                                            |
| `SeasonalES`           | `SeasonalES(12)` or `SeasonalES(seasonal_period=12)`                       |
| `TBATS`                | `TBATS(12, 24)` or `TBATS(use_boxcox=false, seasonal_periods=[12,24])`     |
| `AutoTBATS`            | `AutoTBATS(12)` or `AutoTBATS(12, 24)`                                     |
| `MSTL`                 | `MSTL(12)` or `MSTL(12, 24, iterations=5)`                                 |
| `MFLES`                | `MFLES(12)` or `MFLES(12, 24, robust=true)`                                |
| `GARCH`                | `GARCH(1, 1)`                                                              |

`MODELS` takes a comma-separated list of specs, so `TS.FORECAST` and
`TS.BACKTEST` can compare several in one call:

```text
MODELS "ARIMA(2,1,0), SES(alpha=0.3), Naive()"
```

Model suitability, minimum data requirements, and prediction-interval support
vary by model. Model-specific failures, insufficient history, and numerical
instability are returned as command or per-model errors rather than silently
falling back to a different explicit model.

## 6. Command Semantics and Safety

`TS.FEATURES`, `TS.BACKTEST`, `TS.AUTOCORRELATION`, `TS.DECOMPOSE`,
`TS.PERIODS`, and `TS.STATIONARITY` are read-only.

`TS.FORECAST`, `TS.AUTOFORECAST`, `TS.TREND`, and `TS.FILLGAPS` are registered
as write commands solely because their optional `STORE` clauses can create or
update a destination series; callers must treat them as write-capable even
when they omit `STORE`, but the source series itself is never modified by any
of these four.

The source key is checked as a time series. `STORE` destination keys are
identified through command key specifications, and cross-slot source/destination
operations are rejected in cluster mode. Range bounds use the standard `-` and
`+` time-series sentinels.

## 7. Testing and Release Criteria

Release requires coverage of argument validation, model parsing, stored-result
behavior, timestamps derived for stored forecasts, model failure isolation in
backtesting, cluster cross-slot rejection, and — before `TS.DECOMPOSE` and
`TS.PERIODS` can ship — basic correctness tests for both. Forecast-quality
examples should be treated as regression tests for API behavior, not as
guarantees that a particular model is universally best.

## 8. References
- [anofox-forecast](https://github.com/anofox/forecast)
