# TS.FORECAST

Fit user-specified forecasting models to a time series and return predicted future values.

`TS.FORECAST` allows precise control over which forecasting models to use, including their
hyperparameters. Unlike `TS.AUTOFORECAST`, which automatically selects the best model, this command
lets you specify exactly one or more model specifications (e.g., `ARIMA(2,1,0)`, `SES(alpha=0.3)`)
and returns their individual forecasts. Each model is fit independently and produces its own set
of predicted values, plus prediction intervals with `LEVEL` and accuracy metrics with `METRICS`.

## Syntax

```
TS.FORECAST key fromTimestamp toTimestamp
  MODELS model_spec[,model_spec ...]
  HORIZON horizon
  [LEVEL confidence_level]
  [TRANSFORMS transform_spec[,transform_spec ...]]
  [METRICS]
  [TIMEOUT milliseconds]
  [STORE destinationKey
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

[Examples](#examples)

## Required Arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to forecast.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data used to fit the model (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data used to fit the model (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

<details open>
<summary><code>MODELS model_spec[,model_spec ...]</code></summary>

Comma-separated list of model specifications to fit, optionally wrapped in `[...]`. Each model
specification is formatted as `ModelName(args...)` with optional positional arguments followed by
optional `key=value` keyword arguments; a model that takes no arguments can be written bare
(`AutoARIMA` is the same as `AutoARIMA()`). Supported model families are listed below.

At least one model must be specified. Model names and keyword names are case-insensitive.
Keyword *values* that name an option (`additive`, `linear`, `Naive`, ...) are case-sensitive and
must be spelled exactly as shown below. Flags are `true` or `false`; numbers may not be quoted.

Arguments are checked before any model runs:

- A keyword the model does not support is an error, so a misspelling cannot silently fall back
  to a default.
- Integer arguments (periods, windows, orders) must be whole numbers from 0 to 1,000,000.
- ARIMA, SARIMA and GARCH orders are at most 20. Iteration counts (`iterations`, `max_rounds`,
  `max_iterations`) are at most 10,000. Seasonal periods must be positive.
- Lists such as `seasonal_period=[7, 365]` are flat; nested lists are rejected. Only MFLES accepts
  more than one period in `seasonal_period`; the other models take a single period.
- Where a value can be given positionally or by keyword, the positional value wins if both are
  present (MFLES and HoltWinters reject a seasonal period given both ways instead).

#### Available Models

| Model                 | Positional arguments                           | Keyword arguments                                                                                                  |
|-----------------------|------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `ARIMA`               | `p, d, q` (all three required)                 | —                                                                                                                  |
| `SARIMA`              | `p, d, q` or `p, d, q, P, D, Q, seasonal_period` | —                                                                                                                |
| `AutoARIMA`           | —                                              | `seasonal_period` (int)                                                                                            |
| `ETS`                 | optional notation and/or seasonal period       | `seasonal_period` (int; only with a notation-only spec such as `ETS(AAA, seasonal_period=12)`)                    |
| `AutoETS`             | —                                              | `seasonal_period` (int)                                                                                            |
| `SES`                 | `alpha`                                        | `alpha` (float)                                                                                                    |
| `Holt`                | `alpha, beta[, phi]`                           | `alpha`, `beta`, `phi` (float); `damped` (flag)                                                                    |
| `HoltWinters`         | `seasonal_period[, alpha, beta, gamma]`        | `seasonal_period` (int); `seasonal_type` (`additive` (default) or `multiplicative`); `alpha`, `beta`, `gamma` (float) |
| `SeasonalES`          | `period` (required, positional or keyword)     | `period` (int); `alpha` (float); `optimized` (flag)                                                                |
| `Naive`               | —                                              | —                                                                                                                  |
| `RandomWalkWithDrift` | `changepoint`                                  | `changepoint` (int)                                                                                                |
| `SeasonalNaive`       | `period` (default 12)                          | `period` (int)                                                                                                     |
| `SMA`                 | `window` (default 0 = mean of the whole range) | `window` (int); `changepoint` (int)                                                                                |
| `Theta`               | —                                              | `seasonal_period` (int); `decomposition_type` (`additive` or `multiplicative`); `optimized` (flag); `theta` (float) |
| `Croston`             | —                                              | `alpha` (float); `sba`, `sba_optimized`, `optimized` (flags)                                                       |
| `ADIDA`               | —                                              | `alpha` (float); `aggregation_level` (int)                                                                         |
| `IMAPA`               | —                                              | `max_aggregation` (int)                                                                                            |
| `TSB`                 | `alpha_d, alpha_p`                             | `alpha_d`, `alpha_p` (float)                                                                                       |
| `TBATS`               | one or more seasonal periods (required)        | `use_boxcox` (flag or number); `damped_trend` (float)                                                              |
| `AutoTBATS`           | one or more seasonal periods (required)        | `use_boxcox_search`, `use_damped_trend_search`, `use_no_trend_search` (flags, default `true`)                      |
| `MSTL`                | one or more seasonal periods (required)        | `iterations` (int); `robust` (flag); `trend_forecast_method`; `seasonal_forecast_method`                           |
| `MFLES`               | zero or more seasonal periods (default `[12]`) | `seasonal_period` (int or list); `max_rounds` (int); `seasonal_lr`, `trend_lr` (float); `robust`, `multiplicative` (flags) |
| `GARCH`               | `p[, q]`                                       | `p`, `q` (int, default 1); `omega` (float); `max_iterations` (int); `tolerance` (float)                            |

See [Model Details](#model-details) for how the arguments combine.
</details>

<details open>
<summary><code>HORIZON horizon</code></summary>

Number of future data points to predict. Must be a positive integer no larger than the
`ts-forecast-max-horizon` configuration parameter (default 10000).
</details>

## Optional Arguments

<details open>
<summary><code>LEVEL confidence_level</code></summary>

Confidence level for prediction intervals, as a percentage between `0` and `100` (exclusive).
For example, `LEVEL 95` returns 95% prediction intervals.

When specified, each model's response includes:

- `level` — the confidence level
- `lower_interval` — array of lower bounds for each forecast point
- `upper_interval` — array of upper bounds for each forecast point

For each point `i`, `lower_interval[i] <= forecast[i] <= upper_interval[i]`.

`level` is emitted only together with the intervals: if a model returns no intervals, all three
fields are omitted from that model's entry.
</details>

<details open>
<summary><code>TRANSFORMS transform_spec[,transform_spec ...]</code></summary>

A comma-separated chain of reversible pre-processing transforms applied to the series, in
order, before every model in `MODELS` is fit. Each model gets its own independently fitted
copy of the chain. Forecasts, prediction intervals and in-sample fitted values (and therefore
`METRICS`) are all inverse-transformed back into the original units, so the response
shape is identical with or without `TRANSFORMS`.

Use this to hand a stationary or variance-stabilised series to models that assume one
(for example `Difference(1)` ahead of `SES`, or `Log` ahead of `ARIMA` on multiplicative data).

Specs use the same `Name(arg, ..., key=value)` syntax as `MODELS`; names are case-insensitive.

| Transform | Arguments | Description |
|-----------|-----------|-------------|
| `Difference(d)` | `d` — order (integer from 0 to 20) | Ordinary differencing; consumes the first `d` observations. |
| `SeasonalDifference(period)` | `period` — season length (positive) | Seasonal differencing; consumes the first `period` observations. |
| `Log` | — | Natural log. The series must be strictly positive. |
| `BoxCox` / `BoxCox(lambda)` / `BoxCox(lambda=λ)` | optional `lambda` | Box-Cox power transform. With no lambda it is estimated from the data. |
| `YeoJohnson` | — | Yeo-Johnson power transform (handles zero and negative values). |
| `Scale(method)` | `Standardize`, `Normalize` or `RobustScale` (alias `Robust`), case-insensitive | Rescale to zero-mean/unit-variance, `[0, 1]`, or median/IQR. |

Differencing shortens the series handed to the model by the transform's offset, so the range
must contain enough samples for the model *after* the chain is applied.
</details>

<details open>
<summary><code>METRICS</code></summary>

When specified, each model's response includes a `metrics` map with accuracy metrics using in-sample observed values and fitted values from the model.

Returned fields per model:

- `mae` — Mean Absolute Error
- `mse` — Mean Squared Error
- `rmse` — Root Mean Squared Error
- `mape` — Mean Absolute Percentage Error (may be `null` when actual contains zeros)
- `smape` — Symmetric Mean Absolute Percentage Error
- `mase` — Mean Absolute Scaled Error (may be `null` when insufficient scaling history)
- `r_squared` — Coefficient of determination

For ARIMA, SARIMA and AutoARIMA the in-sample fit is rebuilt on the scale of the series from
the model's one-step residuals (the model itself reports it on its differenced scale), with the
warm-up period excluded. `GARCH` models volatility and has no in-sample fit of the level, so its
`metrics` entry is null. If the metrics cannot be computed for another reason, the whole command
fails with a `TSDB: metrics error: ...` error; no forecast is returned for any model.
</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds, counted from when the request is accepted (so time
spent queued behind other forecasting work counts). When it elapses the client receives
`TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` and
the request is abandoned: its result is discarded and a `STORE` that has not yet happened is
skipped. `0` disables the deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).

Forecasting commands run on a dedicated pool of worker threads sized by `ts-num-threads`, so
they do not block the server's main thread; requests beyond the worker count wait in a queue.
The exception is a client that cannot be blocked — inside `MULTI`/`EXEC`, a Lua script or a
module call — where the command runs inline on the main thread and no deadline applies. There
the range is held to 20,000 samples × models (the heaviest families cost about 75 µs a sample);
a larger request fails with
`TSDB: range too large to run inside MULTI, a script or a module call: …; run the command outside of it`.
</details>

<details open>
<summary><code>STORE destinationKey</code></summary>

Persist the forecast values into a time series key. The predicted values are stored as samples
with timestamps continuing, one forecast step apart, from the last timestamp in the fitted range
(see the step rule below).

The destination must be a different key from the source; naming the source fails with
`TSDB: STORE destination must be different from the source key`. Only the primary runs the
analysis: replicas and the AOF receive the stored samples, not the command.

> **Important:** `STORE` is only supported when a **single model** is specified. If multiple
> models are provided with `STORE`, the command returns an error.

#### STORE Options

| Option                           | Description                                                                        |
|----------------------------------|------------------------------------------------------------------------------------|
| `MERGE`                          | Merge forecast samples into an existing destination key (default: overwrite)       |
| `RETENTION retentionPeriod`      | Retention period (milliseconds or a duration such as `1d`) for the destination     |
| `ENCODING encoding`              | Chunk encoding: `COMPRESSED` (Chimp), `CHIMP`, `GORILLA` or `UNCOMPRESSED` (case-insensitive); anything else fails with `TSDB: unknown ENCODING parameter` |
| `CHUNK_SIZE chunkSize`           | Chunk size of the destination, in bytes                                            |
| `DUPLICATE_POLICY policy`        | Duplicate sample policy: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM`             |
| `SIGNIFICANT_DIGITS digits`      | Round to this many significant digits in the destination                           |
| `DECIMAL_DIGITS digits`          | Round to this many decimal digits in the destination                               |
| `METRIC metric`                  | Metric name, with optional labels (`name{label="value",...}`), for the destination |
| `IGNORE maxTimeDiff maxValDiff`  | The destination's `ignoreMaxTimeDiff`/`ignoreMaxValDiff`, as in `TS.CREATE`        |

The creation options (everything except `MERGE`) apply only when the destination does not exist
yet; omitted ones come from the module configuration. As with `TS.ADD`, an existing destination
keeps its own settings and the options are ignored.

Without `MERGE` (overwrite mode), the destination's samples are cleared before writing.
With `MERGE`, forecast samples use `KeepLast` semantics for duplicate timestamps.

The forecast step is the series' detected sampling frequency, falling back to the median
positive gap between samples. That needs at least two samples in the range; with fewer the
command fails with an error rather than returning the forecast without storing it.
</details>

## Return Value

The response is an **array of maps**, one entry per model specified in `MODELS`, in the order
given. Under RESP3 each entry is a map; under RESP2 it is a flat array of alternating keys and
values (doubles are sent as bulk strings). Each entry has the following fields:

| Field            | Type            | Always Present | Description                                                                                              |
|------------------|-----------------|----------------|----------------------------------------------------------------------------------------------------------|
| `model`          | string          | Yes            | Normalised model spec (see below)                                                                        |
| `horizon`        | integer         | Yes            | Number of forecast points                                                                                |
| `forecast`       | array of double | Yes            | Predicted values in order                                                                                |
| `level`          | double          | No             | Confidence level (only when `LEVEL` is specified and the model returned intervals)                      |
| `lower_interval` | array of double | No             | Lower prediction interval bounds (only when `LEVEL` is specified and the model returned intervals)       |
| `upper_interval` | array of double | No             | Upper prediction interval bounds (only when `LEVEL` is specified and the model returned intervals)       |
| `metrics`        | map             | No             | Accuracy metrics map (only when `METRICS` is specified)                                                  |

`model` echoes the spec in a normalised form rather than verbatim: the model name in its canonical
spelling, empty parentheses added, whitespace removed, keyword names lower-cased and numbers
reformatted. For example `Naive` comes back as `Naive()`, `arima(2, 1, 0)` as `ARIMA(2,1,0)` and
`ses(ALPHA=0.30)` as `SES(alpha=0.3)`.

**With `STORE`:** The response is a single integer: the number of samples written to the
destination key. With `MERGE` this can be less than `HORIZON` if the destination drops some of
the samples.

### Example Response (without STORE)

```
1) 1) model
   2) ARIMA(2,1,0)
   3) horizon
   4) (integer) 5
   5) forecast
   6) 1) "101"
      2) "102"
      3) "103"
      4) "104"
      5) "105"
2) 1) model
   2) SES(alpha=0.3)
   3) horizon
   4) (integer) 5
   5) forecast
   6) 1) "97.66666666666666"
      2) "97.66666666666666"
      3) "97.66666666666666"
      4) "97.66666666666666"
      5) "97.66666666666666"
```

### Example Response (with STORE)

```
(integer) 5
```

## Model Details

### ARIMA / SARIMA

`ARIMA(p, d, q)` fits a non-seasonal ARIMA model with the specified autoregressive order `p`,
differencing order `d`, and moving average order `q`. All three are required and positional.

`SARIMA(p, d, q, P, D, Q, seasonal_period)` fits a seasonal ARIMA model. You must provide
either 3 arguments (non-seasonal) or 7 arguments (seasonal). `seasonal_period` may be `0` only
when `P`, `D` and `Q` are all `0`.

`AutoARIMA()` automatically searches for the best ARIMA model; `AutoARIMA(seasonal_period=N)`
searches seasonal models with period `N`. It takes no positional arguments.

### ETS / AutoETS

`ETS` takes an optional model notation and an optional seasonal period, in either order:
`ETS()`, `ETS(12)`, `ETS(AAA)`, `ETS(AAA, 12)`, `ETS(12, AAA)` or
`ETS(AAA, seasonal_period=12)`. The notation is one identifier naming the error, trend and
season components: error `A` or `M`, trend `N`, `A` or `Ad` (damped), season `N`, `A` or `M` —
for example `ANN`, `AAN` (Holt's linear trend), `AAdN`, `MAM`. The defaults are `ANN` and a
period of 1. Components cannot be passed as separate arguments (`ETS(A,N,A)` is rejected), and
there is no automatic (`Z`) component; use `AutoETS` for that.

`AutoETS()` automatically selects the best ETS model; `AutoETS(seasonal_period=N)` includes
seasonal candidates with period `N`. It takes no positional arguments.

### Theta

`Theta()` implements the Theta method (theta = 2). It takes no positional arguments and accepts
one of these keyword combinations:

- `seasonal_period=N` — seasonal Theta with multiplicative decomposition, optionally with
  either `decomposition_type=additive|multiplicative` or `optimized=true|false`
- `theta=x` — a fixed theta coefficient
- `optimized=true` — optimise the smoothing parameter

Any other combination (for example `theta` together with `optimized`, or `decomposition_type`
without `seasonal_period`) fails with `Invalid options for Theta forecaster`.

### TBATS / AutoTBATS

`TBATS(period1, period2, ...)` handles complex multiple seasonalities. At least one period is
required, given positionally. Keyword arguments:

- `use_boxcox` — `true` applies a log transform (λ = 0), `false` none (λ = 1), and a number
  sets λ directly (clamped to `[0, 1]`). Without it, λ is estimated from the data when every
  value is positive, and no transform is applied otherwise.
- `damped_trend` — enables a damped trend with this damping factor φ (clamped to `[0.8, 0.99]`).

`AutoTBATS(period1, period2, ...)` automatically selects the best TBATS configuration. Its
`use_boxcox_search`, `use_damped_trend_search` and `use_no_trend_search` flags (all default
`true`) can switch individual parts of the search off.

### MSTL

`MSTL(period1, period2, ...)` decomposes the series into trend and multiple seasonal
components using LOESS, then forecasts each separately. At least one period is required, given
positionally.

Keyword arguments: `iterations` (default 2), `robust=true`, `trend_forecast_method` (`linear`,
`AutoETS` (default), `SES` or `Naive`; an unquoted identifier) and `seasonal_forecast_method`
(`Naive` (default) or `Average`).

### MFLES

`MFLES(period1, period2, ...)` uses Fourier basis functions for seasonal decomposition with a
learned trend. Periods can be given positionally or as `seasonal_period=N` /
`seasonal_period=[N, M]`, but not both; with neither, the period is 12.

Keyword arguments: `max_rounds`, `seasonal_lr`, `trend_lr`, `robust=true`, `multiplicative`.

### Baseline Models

- `Naive()` — forecasts all future values as the last observed value. Takes no arguments.
- `RandomWalkWithDrift([changepoint])` — last value plus the average drift, estimated from the
  first differences (from `changepoint` onward when given).
- `SeasonalNaive([period])` — forecasts using the value from the same seasonal position in
  the previous cycle (period default 12).
- `SMA([window])` — forecasts the mean of the last `window` observations; `window` 0 (the
  default) uses the whole range. `changepoint` limits the window to observations after it.

### Exponential Smoothing Variants

- `SES([alpha])` — Simple Exponential Smoothing; `alpha` is optimised when omitted.
- `Holt([alpha, beta[, phi]])` — Holt's linear trend method. Give `alpha` and `beta` together,
  or neither to optimise them. A `phi`, or `damped=true` (φ = 0.98 when `alpha`/`beta` are
  given), makes the trend damped.
- `HoltWinters(seasonal_period[, alpha, beta, gamma])` — Holt-Winters seasonal method. The
  period is required (positionally or as `seasonal_period=N`); the smoothing parameters are all
  three or none (optimised), positionally or as keywords. `seasonal_type` is `additive`
  (default) or `multiplicative`.
- `SeasonalES(period)` — Seasonal Exponential Smoothing. The period is required (positionally
  or as `period=N`); `alpha=x` fixes the smoothing parameter and `optimized=true` optimises it.

### Intermittent Demand Models

- `Croston()` — Croston's method; `alpha=x` sets the smoothing parameter, and the flags
  `sba_optimized`, `sba` and `optimized` select a variant (checked in that order).
- `ADIDA()` — Aggregate-Disaggregate Intermittent Demand Approach; keywords `alpha` and
  `aggregation_level`.
- `IMAPA()` — Intermittent Multiple Aggregation Prediction Algorithm; keyword `max_aggregation`.
- `TSB([alpha_d, alpha_p])` — Teunter-Syntetos-Babai method; give both smoothing parameters
  (positionally or as keywords) or neither.

### GARCH

`GARCH(p, q)` fits a GARCH(p,q) model for volatility forecasting. `p` and `q` default to 1 and
can also be given as keywords, alongside `omega`, `max_iterations` and `tolerance`.

## Errors

- `TSDB: the key does not exist` — the source key does not exist.
- `WRONGTYPE Operation against a key holding the wrong kind of value` — the source key is not a
  time series.
- `TSDB: HORIZON is required` — the `HORIZON` argument is missing.
- `TSDB: missing forecast horizon value` — `HORIZON` has no value.
- `TSDB: forecast horizon must be greater than 0` — `HORIZON` is zero or negative.
- `TSDB: forecast horizon must not exceed N (ts-forecast-max-horizon)` — `HORIZON` is above the
  configured cap.
- `TSDB: MODELS must contain at least one model specification` — `MODELS` is missing, empty or
  `[]`.
- `TSDB: missing value for MODELS` / `TSDB: missing value for TRANSFORMS` — the keyword has no
  value.
- `TSDB: error parsing MODELS: <reason>` — a model spec could not be parsed or validated, e.g.
  `TSDB: error parsing MODELS: Unsupported model name Foo` or
  `TSDB: error parsing MODELS: Unsupported keyword argument(s) for model SES: alhpa`.
- `TSDB: error parsing TRANSFORMS: <reason>` — a transform name is unknown or its arguments are
  invalid.
- `TSDB: TRANSFORMS must contain at least one transform specification` — `TRANSFORMS` was given
  an empty string.
- `TSDB: STORE is only supported with a single model` — `STORE` was specified with multiple models.
- `TSDB: STORE destination must be different from the source key` — `STORE` names the source key.
- `TSDB: STORE requires at least two samples in the range to determine the forecast step` — the
  range holds too few samples to infer where the stored forecast samples should be placed.
- `TSDB: STORE forecast timestamps exceed the supported range` — the last timestamp and forecast
  step would overflow the timestamp type at the requested `HORIZON`; rejected before model work.
- `TSDB: unknown ENCODING parameter` — the `STORE` `ENCODING` value is not one listed above.
- `TSDB: LEVEL must be between 0 and 100` — `LEVEL` is out of the valid range.
- `TSDB: missing forecast confidence level` — `LEVEL` has no value, or it is not a number.
- `TSDB: Unknown argument: <arg>` — an unrecognized argument was provided.
- `TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` —
  the `TIMEOUT` (or `ts-analysis-timeout`) deadline elapsed before the result was available.
- `TSDB: missing value for TIMEOUT` — `TIMEOUT` has no value, or it is not an integer.
- `TSDB: TIMEOUT must be zero or positive` — a negative `TIMEOUT` was given.
- `TSDB: metrics error: <reason>` — `METRICS` was requested and the metrics could not be
  computed from the model's in-sample fit.
- `TSDB: failed to store forecast in key '<key>': <reason>` — an error occurred while writing
  `STORE` samples.
- `TSDB: Failed to prepare time series for forecasting` — the series data could not be
  converted to the format required by the forecasting library.
- Model errors from the forecasting library, prefixed with `TSDB: `, for example
  `TSDB: empty input data` (no samples in the range) or
  `TSDB: insufficient data: need at least 24, got 5 (...)`.

## Examples

The examples use a series holding the values 1 to 100 at timestamps 1000 to 100000, one second
apart.

### Basic forecast with a single ARIMA model

```
127.0.0.1:6379> TS.CREATE ts:metrics
OK
127.0.0.1:6379> TS.ADD ts:metrics 1000 1
(integer) 1000
127.0.0.1:6379> TS.ADD ts:metrics 2000 2
(integer) 2000
... (continue up to TS.ADD ts:metrics 100000 100)
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 5
1) 1) model
   2) ARIMA(2,1,0)
   3) horizon
   4) (integer) 5
   5) forecast
   6) 1) "101"
      2) "102"
      3) "103"
      4) "104"
      5) "105"
```

### Compare multiple models

`model` echoes each spec in normalised form, so `Naive` would also come back as `Naive()`.

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0), SES(alpha=0.3), Naive()" HORIZON 5
1) 1) model
   2) ARIMA(2,1,0)
   3) horizon
   4) (integer) 5
   5) forecast
   6) 1) "101"
      2) "102"
      3) "103"
      4) "104"
      5) "105"
2) 1) model
   2) SES(alpha=0.3)
   3) horizon
   4) (integer) 5
   5) forecast
   6) 1) "97.66666666666666"
      2) "97.66666666666666"
      3) "97.66666666666666"
      4) "97.66666666666666"
      5) "97.66666666666666"
3) 1) model
   2) Naive()
   3) horizon
   4) (integer) 5
   5) forecast
   6) 1) "100"
      2) "100"
      3) "100"
      4) "100"
      5) "100"
```

### Forecast with prediction intervals and metrics

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "SES(alpha=0.3)" HORIZON 5 LEVEL 95 METRICS
1)  1) model
    2) SES(alpha=0.3)
    3) horizon
    4) (integer) 5
    5) forecast
    6) 1) "97.66666666666666"
       2) "97.66666666666666"
       3) "97.66666666666666"
       4) "97.66666666666666"
       5) "97.66666666666666"
    7) level
    8) "95"
    9) lower_interval
   10) 1) "91.25548912787424"
       2) "89.84082714770543"
       3) "89.23383547658472"
       4) "88.9518292066034"
       5) "88.81692602120003"
   11) upper_interval
   12) 1) "104.07784420545907"
       2) "105.49250618562789"
       3) "106.0994978567486"
       4) "106.38150412672991"
       5) "106.51640731213328"
   13) metrics
   14)  1) mae
        2) "3.222222222222233"
        3) mse
        4) "10.588235294117702"
        5) rmse
        6) "3.253956867279851"
        7) mape
        8) "11.558054562008587"
        9) smape
       10) "13.31459878225548"
       11) mase
       12) "3.222222222222233"
       13) r_squared
       14) "0.9872928469317519"
```

### Difference a trending series before a level-only model

`Naive` repeats the last observation, so on a trend it flat-lines. Differencing first makes the
trend the thing being forecast, and the result is re-integrated back into the original units.
Here `temperature` rises by 2 every second, ending at 201.

```
127.0.0.1:6379> TS.FORECAST temperature - + MODELS Naive HORIZON 3 TRANSFORMS Difference(1)
1) 1) model
   2) Naive()
   3) horizon
   4) (integer) 3
   5) forecast
   6) 1) "203"
      2) "205"
      3) "207"
```

### Store forecast into a destination key

The stored samples continue one step (1000 ms here) after the last sample in the range.

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 5 STORE forecast:result
(integer) 5
127.0.0.1:6379> TS.RANGE forecast:result - +
1) 1) (integer) 101000
   2) "101"
2) 1) (integer) 102000
   2) "102"
3) 1) (integer) 103000
   2) "103"
4) 1) (integer) 104000
   2) "104"
5) 1) (integer) 105000
   2) "105"
```

### Store with custom creation options

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "SES(alpha=0.3)" HORIZON 10 \
  STORE forecast:daily RETENTION 86400000 CHUNK_SIZE 256 ENCODING UNCOMPRESSED
(integer) 10
```
