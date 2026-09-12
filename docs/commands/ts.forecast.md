# TS.FORECAST

Fit user-specified forecasting models to a time series and return predicted future values.

`TS.FORECAST` allows precise control over which forecasting models to use, including their
hyperparameters. Unlike `TS.AUTOFORECAST`, which automatically selects the best model, this command
lets you specify exactly one or more model specifications (e.g., `ARIMA(2,1,0)`, `SES(alpha=0.3)`)
and returns their individual forecasts. Each model is fit independently and produces its own set
of predicted values, prediction intervals, and accuracy metrics.

## Syntax

```
TS.FORECAST key fromTimestamp toTimestamp
  MODELS model_spec[,model_spec ...]
  HORIZON horizon
  [LEVEL confidence_level]
  [TRANSFORMS transform_spec[,transform_spec ...]]
  [WITH_METRICS]
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

Comma-separated list of model specifications to fit. Each model specification is formatted as
`ModelName(args...)` with optional positional and keyword arguments. Supported model families are
listed below.

At least one model must be specified. All models are case-insensitive.

#### Available Models

| Model            | Syntax                                                  | Description                                                                    |
|------------------|---------------------------------------------------------|--------------------------------------------------------------------------------|
| `ARIMA`          | `ARIMA(p, d, q)`                                        | ARIMA model with specified order                                               |
| `AutoARIMA`      | `AutoARIMA()` or `AutoARIMA(seasonal_period=N)`         | Automatically selects the best ARIMA/SARIMA parameters                         |
| `SARIMA`         | `SARIMA(p, d, q, P, D, Q, seasonal_period)`             | Seasonal ARIMA with explicit parameters (3 or 7 positional args)               |
| `ETS`            | `ETS(error=M, trend=N, season=S)` or `ETS(A,N,A)`       | Explicit ETS model with known components                                       |
| `AutoETS`        | `AutoETS()` or `AutoETS(seasonal_period=N)`              | Automatically selects the best ETS model                                       |
| `SES`            | `SES(alpha=0.3)` or `SES(0.3)`                          | Simple Exponential Smoothing                                                   |
| `Holt`           | `Holt()` or `Holt(alpha=0.3, beta=0.1, ...)`            | Holt linear trend model                                                        |
| `HoltWinters`    | `HoltWinters()` or `HoltWinters(alpha=0.3, beta=0.1, gamma=0.1, seasonal_type="add", seasonal_period=12)` | Holt-Winters seasonal model |
| `Naive`          | `Naive()`                                               | Naive (last value) forecaster                                                   |
| `RandomWalkWithDrift` | `RandomWalkWithDrift()` or `RandomWalkWithDrift(changepoint=N)` | Random walk with drift forecaster; drift estimated from first differences |
| `SeasonalNaive`  | `SeasonalNaive(12)` or `SeasonalNaive(period=12)`       | Seasonal naive forecaster                                                      |
| `SMA`            | `SMA(5)` or `SMA(window=5)`                             | Simple Moving Average                                                          |
| `Theta`          | `Theta()` or `Theta(theta_lines=2, decomposition="multiplicative")` | Theta method for forecasting                                        |
| `Croston`        | `Croston()` or `Croston(alpha=0.3)`                     | Croston's intermittent demand forecasting                                      |
| `ADIDA`          | `ADIDA()` or `ADIDA(size=5)`                            | Aggregate-Disaggregate Intermittent Demand Approach                            |
| `IMAPA`          | `IMAPA()`                                               | Intermittent Multiple Aggregation Prediction Algorithm                         |
| `TSB`            | `TSB(alpha_d=0.3, alpha_p=0.2)`                         | Teunter-Syntetos-Babai method                                                  |
| `SeasonalES`     | `SeasonalES(12)` or `SeasonalES(seasonal_period=12)`    | Seasonal Exponential Smoothing                                                 |
| `TBATS`          | `TBATS(12, 24)` or `TBATS(use_boxcox=false, seasonal_periods=[12,24])` | TBATS model with explicit configuration |
| `AutoTBATS`      | `AutoTBATS(12)` or `AutoTBATS(12, 24)`                  | Auto-configured TBATS with given seasonal periods                              |
| `MSTL`           | `MSTL(12)` or `MSTL(12, 24, iterations=5)`              | Multiple Seasonal-Trend decomposition using LOESS                              |
| `MFLES`          | `MFLES(12)` or `MFLES(12, 24, robust=true)`             | Multiplicative-Fourier Least-squares Ensemble with Shrinkage                    |
| `GARCH`          | `GARCH(1, 1)`                                           | Generalized AutoRegressive Conditional Heteroskedasticity model                 |

**Common keyword arguments:**

| Argument            | Type            | Description                                                  |
|---------------------|-----------------|--------------------------------------------------------------|
| `alpha`             | float           | Smoothing parameter for the level component                  |
| `beta`              | float           | Smoothing parameter for the trend component                  |
| `gamma`             | float           | Smoothing parameter for the seasonal component               |
| `seasonal_period`   | integer         | Length of the seasonal cycle                                 |
| `seasonal_periods`  | list of int     | Multiple seasonal periods (TBATS, MSTL, MFLES)               |
| `seasonal_type`     | string          | `"add"` or `"multiplicative"`                                |
| `theta_lines`       | integer         | Number of theta lines (Theta model, default 2)               |
| `decomposition`     | string          | `"additive"` or `"multiplicative"` (Theta model)             |
| `window`            | integer         | Window size (SMA)                                            |
| `interval_size`     | integer         | Aggregation interval (ADIDA)                                 |
| `iterations`        | integer         | Number of STL iterations (MSTL)                              |
| `max_rounds`        | integer         | Maximum optimization rounds (MFLES)                          |
| `robust`            | boolean         | Enable robust mode (MSTL, MFLES, TBATS)                      |
| `trend_method`      | string          | Trend forecasting method (`"auto"`, `"arima"`, `"linear"`, etc.) |
| `seasonal_method`   | string          | Seasonal forecasting method (`"auto"`, `"arima"`, `"naive"`, etc.) |
| `multiplicative`    | boolean         | Use multiplicative seasonality (MFLES)                       |
| `changepoint`       | integer         | Start index for drift estimation (RandomWalkWithDrift)      |
</details>

<details open>
<summary><code>HORIZON horizon</code></summary>

Number of future data points to predict. Must be a positive integer no larger than the `ts-forecast-max-horizon` configuration
parameter (default 10000).
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

Not all models support prediction intervals. Models that do not support intervals will omit
the `lower_interval` and `upper_interval` fields, but `level` will still be included.
</details>

<details open>
<summary><code>TRANSFORMS transform_spec[,transform_spec ...]</code></summary>

A comma-separated chain of reversible pre-processing transforms applied to the series, in
order, before every model in `MODELS` is fit. Each model gets its own independently fitted
copy of the chain. Forecasts, prediction intervals and in-sample fitted values (and therefore
`WITH_METRICS`) are all inverse-transformed back into the original units, so the response
shape is identical with or without `TRANSFORMS`.

Use this to hand a stationary or variance-stabilised series to models that assume one
(for example `Difference(1)` ahead of `SES`, or `Log` ahead of `ARIMA` on multiplicative data).

Specs use the same `Name(arg, ..., key=value)` syntax as `MODELS`; names are case-insensitive.

| Transform | Arguments | Description |
|-----------|-----------|-------------|
| `Difference(d)` | `d` — order (non-negative integer) | Ordinary differencing; consumes the first `d` observations. |
| `SeasonalDifference(period)` | `period` — season length | Seasonal differencing; consumes the first `period` observations. |
| `Log` | — | Natural log. The series must be strictly positive. |
| `BoxCox` / `BoxCox(lambda)` / `BoxCox(lambda=λ)` | optional `lambda` | Box-Cox power transform. With no lambda it is estimated from the data. |
| `YeoJohnson` | — | Yeo-Johnson power transform (handles zero and negative values). |
| `Scale(method)` | `Standardize`, `Normalize` or `RobustScale` | Rescale to zero-mean/unit-variance, `[0, 1]`, or median/IQR. |

Differencing shortens the series handed to the model by the transform's offset, so the range
must contain enough samples for the model *after* the chain is applied.
</details>

<details open>
<summary><code>WITH_METRICS</code></summary>

When specified, each model's response includes a `metrics` map with accuracy metrics using in-sample observed values and fitted values from the model.

Returned fields per model:

- `mae` — Mean Absolute Error
- `mse` — Mean Squared Error
- `rmse` — Root Mean Squared Error
- `mape` — Mean Absolute Percentage Error (may be `null` when actual contains zeros)
- `smape` — Symmetric Mean Absolute Percentage Error
- `mase` — Mean Absolute Scaled Error (may be `null` when insufficient scaling history)
- `r_squared` — Coefficient of determination

</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds, counted from when the request is accepted (so time
spent queued behind other forecasting work counts). When it elapses the client receives
`TSDB: forecast timed out before the result was ready` and the request is abandoned: its result
is discarded and a `STORE` that has not yet happened is skipped. `0` disables the deadline for this call.

When omitted, the `ts-forecast-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).

Forecasting commands run on a dedicated pool of worker threads sized by `ts-num-threads`, so
they never block the server's main thread; requests beyond the worker count wait in a queue.
</details>

<details open>
<summary><code>STORE destinationKey</code></summary>

Persist the forecast values into a time series key. The predicted values are stored as samples
with timestamps continuing from the last observed timestamp using the series' median sampling
interval.

> **Important:** `STORE` is only supported when a **single model** is specified. If multiple
> models are provided with `STORE`, the command returns an error.

#### STORE Options

| Option                           | Description                                                                        |
|----------------------------------|------------------------------------------------------------------------------------|
| `MERGE`                          | Merge forecast samples into an existing destination key (default: overwrite)       |
| `RETENTION retentionPeriod`      | Maximum retention period (in milliseconds) for the destination series              |
| `ENCODING encoding`              | Chunk encoding: `COMPRESSED` (default), `UNCOMPRESSED`, `PCO`, or `GORILLA`       |
| `CHUNK_SIZE chunkSize`           | Number of samples per memory chunk in the destination                              |
| `DUPLICATE_POLICY policy`        | Duplicate sample policy: `BLOCK`, `FIRST`, `LAST`, `MIN`, `MAX`, `SUM`             |
| `SIGNIFICANT_DIGITS digits`      | Round to this many significant digits in the destination                           |
| `DECIMAL_DIGITS digits`          | Round to this many decimal digits in the destination                               |
| `METRIC metric`                  | Metric name label for the destination series                                       |
| `IGNORE maxTimeDiff maxValDiff`  | Ignore samples differing by more than these thresholds when merging                |

Without `MERGE` (overwrite mode), the destination is cleared before writing.
With `MERGE`, forecast samples use `KeepLast` semantics for duplicate timestamps.

The forecast step is the series' detected sampling frequency, falling back to the median
positive gap between samples. That needs at least two samples in the range; with fewer the
command fails with an error rather than returning the forecast without storing it.
</details>

## Return Value

The response is an **array of flat key-value maps**, one entry per model specified in `MODELS`.
Each map contains alternating keys and values with the following fields:

| Field            | Type            | Always Present | Description                                                                                              |
|------------------|-----------------|----------------|----------------------------------------------------------------------------------------------------------|
| `model`          | string          | Yes            | Name of the model as specified (e.g., `ARIMA(2,1,0)`, `SES(alpha=0.3)`)                                 |
| `horizon`        | integer         | Yes            | Number of forecast points                                                                                |
| `forecast`       | array of double | Yes            | Predicted values in order                                                                                |
| `level`          | double          | No             | Confidence level (only when `LEVEL` is specified)                                                        |
| `lower_interval` | array of double | No             | Lower prediction interval bounds (only when supported by the model and `LEVEL` is specified)             |
| `upper_interval` | array of double | No             | Upper prediction interval bounds (only when supported by the model and `LEVEL` is specified)             |
| `metrics`        | map             | No             | Accuracy metrics map (only when `WITH_METRICS` is specified and the model supports fitted values)        |

**With `STORE`:** The response is a single integer representing the number of samples written
to the destination key.

### Example Response (without STORE)

```
1) 1) "model"
   2) "ARIMA(2,1,0)"
   3) "horizon"
   4) (integer) 5
   5) "forecast"
   6) 1) "105.32"
      2) "105.78"
      3) "106.24"
      4) "106.70"
      5) "107.16"
2) 1) "model"
   2) "SES(alpha=0.3)"
   3) "horizon"
   4) (integer) 5
   5) "forecast"
   6) 1) "104.50"
      2) "104.75"
      3) "105.00"
      4) "105.25"
      5) "105.50"
```

### Example Response (with STORE)

```
(integer) 5
```

## Model Details

### ARIMA / SARIMA

`ARIMA(p, d, q)` fits a non-seasonal ARIMA model with the specified autoregressive order `p`,
differencing order `d`, and moving average order `q`.

`SARIMA(p, d, q, P, D, Q, seasonal_period)` fits a seasonal ARIMA model. You must provide
either 3 arguments (non-seasonal) or 7 arguments (seasonal).

`AutoARIMA()` automatically searches for the best (S)ARIMA model using information criteria.

### ETS / AutoETS

`ETS(error, trend, season)` requires 3 positional arguments specifying the error, trend,
and seasonal components. Each argument is a character: `A` (additive), `M` (multiplicative),
`N` (none), or `Z`/`auto` (automatic selection). Example: `ETS(A,A,N)` is Holt's linear trend.

Keyword form: `ETS(error=auto, trend=A, season=N)` is also supported.

`AutoETS()` automatically selects the best ETS model.

### Theta

`Theta()` implements the Theta method for forecasting. By default, it uses 2 theta lines
with multiplicative decomposition. Keyword options include `theta_lines` and `decomposition`
(`"additive"` or `"multiplicative"`).

### TBATS / AutoTBATS

`TBATS(seasonal_period1, seasonal_period2, ...)` handles complex multiple seasonalities.
Periods must be provided as positional arguments. Supports keyword arguments:

- `use_boxcox` (boolean, default: `true`) — whether to search for a Box-Cox transformation
- `use_damped_trend` (boolean, default: `true`) — whether to search for damped trend
- `use_no_trend` (boolean, default: `true`) — whether to consider models without a trend
- `seasonal_periods` — list of seasonal periods as a keyword alternative

`AutoTBATS(period1, period2, ...)` automatically selects the best TBATS configuration.

### MSTL

`MSTL(seasonal_period1, seasonal_period2, ...)` decomposes the series into trend and multiple
seasonal components using LOESS, then forecasts each separately.

Keyword arguments: `iterations`, `robust`, `trend_method`, `seasonal_method`.

### MFLES

`MFLES(seasonal_period1, seasonal_period2, ...)` uses Fourier basis functions for seasonal
decomposition with learned trend.

Keyword arguments: `max_rounds`, `seasonal_lr`, `trend_lr`, `robust`, `multiplicative`.

### Baseline Models

- `Naive()` — forecasts all future values as the last observed value (random walk with drift)
- `SeasonalNaive(period)` — forecasts using the value from the same seasonal position in
  the previous cycle
- `SMA(window)` — forecasts using the simple moving average of the last `window` observations

### Exponential Smoothing Variants

- `SES(alpha)` — Simple Exponential Smoothing
- `Holt(alpha, beta)` — Holt's linear trend method
- `HoltWinters(alpha, beta, gamma, seasonal_type, seasonal_period)` — Holt-Winters seasonal
- `SeasonalES(seasonal_period)` — Seasonal Exponential Smoothing

### Intermittent Demand Models

- `Croston(alpha)` — Croston's method for intermittent demand
- `ADIDA(size)` — Aggregate-Disaggregate Intermittent Demand Approach
- `IMAPA()` — Intermittent Multiple Aggregation Prediction Algorithm
- `TSB(alpha_d, alpha_p)` — Teunter-Syntetos-Babai method

### GARCH

`GARCH(p, q)` fits a GARCH(p,q) model for volatility forecasting.

## Errors

- `TSDB: the key does not exist` — the specified key does not hold a time series.
- `TSDB: HORIZON is required` — the `HORIZON` argument is missing.
- `TSDB: forecast horizon must be greater than 0` — `HORIZON` is zero or negative.
- `TSDB: forecast horizon must not exceed N (ts-forecast-max-horizon)` — `HORIZON` is above the
  configured cap.
- `TSDB: MODELS must contain at least one model specification` — no models were provided.
- `TSDB: error parsing MODELS` — the model specification string could not be parsed.
- `TSDB: error parsing TRANSFORMS` — a transform name is unknown or its arguments are invalid.
- `TSDB: TRANSFORMS must contain at least one transform specification` — `TRANSFORMS` was given
  an empty string.
- `TSDB: STORE is only supported with a single model` — `STORE` was specified with multiple models.
- `TSDB: STORE requires at least two samples in the range to determine the forecast step` — the
  range holds too few samples to infer where the stored forecast samples should be placed.
- `TSDB: LEVEL must be between 0 and 100` — `LEVEL` is out of the valid range.
- `TSDB: Unknown argument` — an unrecognized argument was provided.
- `TSDB: forecast timed out before the result was ready` — the `TIMEOUT` (or `ts-forecast-timeout`)
  deadline elapsed before the result was available.
- `TSDB: TIMEOUT must be zero or positive` — a negative `TIMEOUT` was given.
- `TSDB: failed to store forecast in key` — an error occurred while writing STORE samples.
- `TSDB: Failed to prepare time series for forecasting` — the series data could not be
  converted to the format required by the forecasting library.
- Model-specific errors from `anofox-forecast` (e.g., insufficient data, numerical issues).

## Examples

### Basic forecast with a single ARIMA model

```
127.0.0.1:6379> TS.CREATE ts:metrics
OK
127.0.0.1:6379> TS.ADD ts:metrics 1000 1.0
(integer) 1000
127.0.0.1:6379> TS.ADD ts:metrics 2000 2.0
(integer) 2000
127.0.0.1:6379> TS.ADD ts:metrics 3000 3.0
... (add 100 linear data points)
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

### Compare multiple models

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0), SES(alpha=0.3), Naive()" HORIZON 5
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
2) 1) "model"
   2) "SES(alpha=0.3)"
   3) "horizon"
   4) (integer) 5
   5) "forecast"
   6) 1) "102.80"
      2) "103.40"
      3) "104.00"
      4) "104.60"
      5) "105.20"
3) 1) "model"
   2) "Naive()"
   3) "horizon"
   4) (integer) 5
   5) "forecast"
   6) 1) "100.00"
      2) "100.00"
      3) "100.00"
      4) "100.00"
      5) "100.00"
```

### Forecast with prediction intervals and metrics

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 5 LEVEL 95 WITH_METRICS
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
   7) "level"
   8) "95"
   9) "lower_interval"
   10) 1) "102.50"
       2) "102.80"
       3) "103.10"
       4) "103.40"
       5) "103.70"
   11) "upper_interval"
   12) 1) "105.74"
       2) "107.68"
       3) "109.62"
       4) "111.56"
       5) "113.50"
   13) "metrics"
   14) 1) "mae"
       2) "0.15"
       3) "mse"
       4) "0.03"
       5) "rmse"
       6) "0.17"
       7) "mape"
       8) "1.23"
       9) "smape"
       10) "1.22"
       11) "mase"
       12) "1.05"
       13) "r_squared"
       14) "0.99"
```

### Difference a trending series before a level-only model

`Naive` repeats the last observation, so on a trend it flat-lines. Differencing first makes the
trend the thing being forecast, and the result is re-integrated back into the original units.

```
127.0.0.1:6379> TS.FORECAST temperature - + MODELS Naive HORIZON 3 TRANSFORMS Difference(1)
1) 1) "model"
   2) "Naive"
   3) "horizon"
   4) (integer) 3
   5) "forecast"
   6) 1) "203"
      2) "205"
      3) "207"
```

### Store forecast into a destination key

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 5 STORE forecast:result
(integer) 5
127.0.0.1:6379> TS.RANGE forecast:result - +
1) 1) (integer) 101000
   2) 104.12
2) 1) (integer) 102000
   2) 105.24
3) 1) (integer) 103000
   2) 106.36
4) 1) (integer) 104000
   2) 107.48
5) 1) (integer) 105000
   2) 108.60
```

### Store with custom creation options

```
127.0.0.1:6379> TS.FORECAST ts:metrics - + MODELS "SES(alpha=0.3)" HORIZON 10 \
  STORE forecast:daily RETENTION 86400000 CHUNK_SIZE 256 ENCODING UNCOMPRESSED
(integer) 10
```
