# TS.AUTOFORECAST

Automatically select and fit the best forecasting model for a time series, returning predicted future values.

`TS.AUTOFORECAST` evaluates all enabled auto-forecasting model families and selects the best model based on
cross-validation error. By default, AutoARIMA, AutoETS, and AutoTheta are enabled; TBATS, MFLES, and
MSTL can be enabled via the `MODELS` argument. The command returns the predicted values and the selected
model, optionally with prediction intervals and in-sample accuracy metrics, and can persist the forecast into a
new or existing time series key.

## Syntax

```
TS.AUTOFORECAST key fromTimestamp toTimestamp
  HORIZON horizon
  [SEASONALITY period | AUTO]
  [MODELS family1[,family2 ...]]
  [LEVEL confidence_level]
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

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to forecast.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data used to fit the model (inclusive).

Use `-` to denote the earliest timestamp in the series, `*` for the current time, or a signed duration
such as `-30d` for a time relative to now.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data used to fit the model (inclusive).

Use `+` to denote the latest timestamp in the series; `*` and relative durations work as for `fromTimestamp`.
</details>

The range must hold at least 10 samples; model comparison fails with an `insufficient data` error below that.

<details open>
<summary><code>HORIZON horizon</code></summary>

Number of future data points to predict. Must be a positive integer no larger than the `ts-forecast-max-horizon` configuration
parameter (default 10000).

</details>

## Optional arguments

<details open>
<summary><code>SEASONALITY period | AUTO</code></summary>

The seasonal period of the data (number of observations per seasonal cycle). For example:

- `24` for hourly data with daily seasonality
- `7` for daily data with weekly seasonality

When provided, the forecasting models will account for seasonal patterns in the data. The period
must be an integer of at least 2 and no larger than the number of samples in the range.

`AUTO` detects the dominant period from the data instead; if none is detected the models run
non-seasonally. Without `SEASONALITY`, all models are fitted non-seasonally.
</details>

<details open>
<summary><code>MODELS family1[,family2 ...]</code></summary>

Comma-separated list of model families to evaluate. Supported values (case-insensitive):

| Value   | Aliases     | Default | Description                                                                 |
|---------|-------------|---------|-----------------------------------------------------------------------------|
| `ARIMA` | `AUTOARIMA` | Yes     | Auto-selected ARIMA/SARIMA model                                            |
| `ETS`   | `AUTOETS`   | Yes     | Automatic exponential smoothing                                             |
| `THETA` | `AUTOTHETA` | Yes     | Theta method for forecasting                                                |
| `TBATS` | —           | No      | Automatically configured TBATS; needs a seasonal period                     |
| `MFLES` | —           | No      | Fourier seasonal decomposition with trend learning                          |
| `MSTL`  | —           | No      | Seasonal-trend decomposition with LOESS; needs a seasonal period            |

If omitted, ARIMA, ETS, and THETA are enabled by default. Every entry must be one of the values above;
an unknown or empty entry fails with `TSDB: unknown auto-forecast model: <entry>`.
If `MODELS` appears more than once, the last clause replaces the entire earlier model list.

`TBATS` and `MSTL` are silently skipped when no seasonal period is set (no `SEASONALITY`, or `AUTO`
detected none). A family whose cross-validation fails is also dropped from the comparison; if no
family remains, the command fails with
`TSDB: convergence failure: No candidate model produced valid cross-validation results`.
</details>

<details open>
<summary><code>LEVEL confidence_level</code></summary>

Confidence level for prediction intervals, as a percentage between `0` and `100` (exclusive). For example, `LEVEL 95`
returns 95% prediction intervals.

When specified, and the selected model produces intervals, the response includes:

- `level` — the confidence level
- `lower_interval` — array of lower bounds for each forecast point
- `upper_interval` — array of upper bounds for each forecast point

For each point `i`, `lower_interval[i] <= forecast[i] <= upper_interval[i]`.
</details>

<details open>
<summary><code>METRICS</code></summary>

When specified, the response includes a `metrics` map computed with
`anofox-forecast`'s `calculate_metrics` using in-sample observed values and
fitted values from the selected model (a leading run of non-finite fitted values, such as an
ARIMA warm-up period, is excluded). When the search picks ARIMA or SARIMA, whose fitted values
are on their differenced scale, the fit is rebuilt on the scale of the series from the model's
one-step residuals. The seasonal period (given, or detected by `AUTO`), when set, is
used as the MASE seasonal period.

Returned fields, in this order:

- `mae`
- `mse`
- `rmse`
- `mape` (may be `null` when actual contains zeros)
- `smape`
- `mase` (may be `null` when insufficient scaling history)
- `r_squared`

</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds, counted from when the request is accepted (so time
spent queued behind other forecasting work counts). When it elapses the client receives
`TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` and the
request is abandoned: its result is discarded and a `STORE` that has not yet happened is skipped. `0`
disables the deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).

Forecasting commands run on a dedicated pool of worker threads sized by `ts-num-threads`, so
they do not block the server's main thread; requests beyond the worker count wait in a queue.
The exception is a call that cannot block — inside `MULTI`/`EXEC`, a Lua script, or a module's
`RM_Call` — which runs inline on the main thread, where no deadline applies.
</details>

<details open>
<summary><code>STORE destinationKey</code></summary>

Persist the forecast values into a time series key. The predicted values are stored as samples with
timestamps continuing from the last timestamp in the range, spaced by the series' detected sampling
frequency, falling back to the median positive gap between samples. That needs at least two samples in
the range; with fewer the command fails with an error, checked before any model runs.

The destination must be a different key from the source; naming the source fails with
`TSDB: STORE destination must be different from the source key`. In cluster mode the destination must
hash to the same slot as the source (use a hash tag), or the command is rejected with `CROSSSLOT`.
Only the primary runs the analysis: replicas and the AOF receive the stored samples, not the command.

#### STORE Options

| Option                                  | Description                                                                            |
|-----------------------------------------|----------------------------------------------------------------------------------------|
| `MERGE`                                 | Merge the forecast into an existing destination instead of overwriting it              |
| `RETENTION retentionPeriod`             | Retention period of a newly created destination                                        |
| `ENCODING encoding`                     | Chunk encoding: `COMPRESSED`, `UNCOMPRESSED`, `GORILLA`, or `CHIMP`                     |
| `CHUNK_SIZE chunkSize`                  | Chunk size in bytes (a multiple of 8), as in `TS.CREATE`                                |
| `DUPLICATE_POLICY policy`               | Duplicate sample policy of a newly created destination                                 |
| `SIGNIFICANT_DIGITS digits`             | Round stored values to this many significant digits                                    |
| `DECIMAL_DIGITS digits`                 | Round stored values to this many decimal digits (mutually exclusive with the above)    |
| `METRIC metric`                         | Metric name / labels of a newly created destination                                    |
| `IGNORE maxTimeDiff maxValDiff`         | `IGNORE` thresholds of a newly created destination, as in `TS.CREATE`                   |

The series options (all but `MERGE`) apply only when the destination is created. As with
`TS.ADD`, an existing destination keeps its own settings and the options are ignored.

- If the destination key does not exist, a new time series is created.
- Without `MERGE` (the default), an existing destination is cleared before the forecast is written.
- With `MERGE`, the forecast samples are merged into the existing series; a forecast sample replaces an
  existing sample at the same timestamp.
- If the samples cannot be written (for example the destination holds a value of another type), the
  command fails with an error rather than returning the forecast without storing it.
</details>

## Return Value

The response is a map (a flat array of alternating keys and values in RESP2) with the following fields.
With `STORE` the reply is still the forecast map, sent after the samples are written, with a
`stored` field added. It differs from the other `STORE` commands, which reply with the count only,
because the map names the model the search picked.

| Field            | Type            | Always Present | Description                                                                                                    |
|------------------|-----------------|----------------|----------------------------------------------------------------------------------------------------------------|
| `model`          | string          | Yes            | Name of the best model selected (`ARIMA`, `SARIMA`, `ETS`, `Theta`, `AutoTBATS`, `MFLES`, or `MSTLForecaster`) |
| `horizon`        | integer         | Yes            | Number of forecast points                                                                                      |
| `forecast`       | array of double | Yes            | Predicted values in order                                                                                      |
| `level`          | double          | No             | Confidence level (only when `LEVEL` is specified and intervals were produced)                                  |
| `lower_interval` | array of double | No             | Lower prediction interval bounds                                                                               |
| `upper_interval` | array of double | No             | Upper prediction interval bounds                                                                               |
| `metrics`        | map             | No             | Accuracy metrics map (only when `METRICS` is specified)                                                        |
| `stored`         | integer         | No             | Samples written to the destination (only with `STORE`)                                                         |

### Example Response

```
1) "model"
2) "ARIMA"
3) "horizon"
4) (integer) 5
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

## Model Selection

`TS.AUTOFORECAST` scores each enabled model family with expanding-window cross-validation over the
range and selects the family with the lowest cross-validation RMSE; that model, refit on the whole
range, produces the final forecast.

- **AutoARIMA**: Automatically determines the optimal ARIMA (p,d,q) or SARIMA (P,D,Q,m) parameters.
- **AutoETS**: Automatically selects the best ETS (Error-Trend-Seasonality) model.
- **AutoTheta**: Fits the Theta method, which decomposes the series into short-term and long-term components.
- **TBATS**: Automatically configures TBATS (Trigonometric seasonality, Box-Cox transformation, ARMA errors,
  Trend, and Seasonal components) for the seasonal period. Skipped without a seasonal period.
- **MFLES**: Multiplicative-Fourier Least-squares Ensemble with Shrinkage. Uses Fourier basis functions for
  seasonal decomposition with a learned trend component.
- **MSTL**: Seasonal-Trend decomposition using LOESS. Decomposes the series into trend and seasonal
  components, then forecasts each separately. Skipped without a seasonal period.

The returned model name reflects the concrete model variant:

| Family          | Returned Name    |
|-----------------|------------------|
| ARIMA           | `ARIMA`          |
| ARIMA, seasonal | `SARIMA`         |
| ETS             | `ETS`            |
| THETA           | `Theta`          |
| TBATS           | `AutoTBATS`      |
| MFLES           | `MFLES`          |
| MSTL            | `MSTLForecaster` |

## Examples

### Basic Forecast

Predict the next 5 data points:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5
```

### With Prediction Intervals

Predict 10 points with 95% confidence intervals:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 10 LEVEL 95
```

### With Accuracy Metrics

Include in-sample accuracy metrics for the selected model:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 10 METRICS
```

### With Specific Model

Use only the ARIMA model family:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5 MODELS ARIMA
```

### With Seasonality

Specify hourly data with daily seasonality (period=24):

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 24 SEASONALITY 24 MODELS ARIMA,ETS
```

Or let the command detect the period:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 24 SEASONALITY AUTO MODELS ARIMA,ETS,MSTL
```

### Store Forecast to a Key

Predict 5 points and persist them to a destination key (replacing its contents), creating it with a
one-day retention if it does not exist. The reply is the usual forecast map plus `stored`, the
number of samples written:

```
TS.AUTOFORECAST temperature:sensor1 - + HORIZON 5 STORE {temperature:sensor1}:forecast RETENTION 86400000
```

Add `MERGE` to keep the destination's existing samples and merge the forecast into them.

### Forecast on a Subset of Data

Use only the last 30 days of data (relative to the current time):

```
TS.AUTOFORECAST temperature:sensor1 -30d + HORIZON 7
```

## Error Responses

- `ERR wrong number of arguments for 'ts.autoforecast' command` — fewer than four arguments after the
  command name.
- `TSDB: HORIZON is required` — The `HORIZON` argument was not provided.
- `TSDB: missing forecast horizon value` — `HORIZON` was given without a value.
- `TSDB: forecast horizon must be greater than 0` — `HORIZON` value is zero or negative.
- `TSDB: forecast horizon must not exceed N (ts-forecast-max-horizon)` — `HORIZON` is above the
  configured cap.
- `TSDB: the key does not exist` — The specified time series key was not found.
- `TSDB: wrong fromTimestamp` / `TSDB: wrong toTimestamp` — a range bound could not be parsed.
- `TSDB: SEASONALITY must be AUTO or an integer period` — `SEASONALITY` is missing its value or it is
  not an integer or `AUTO`.
- `TSDB: SEASONALITY period must be at least 2` — the period is below 2.
- `TSDB: SEASONALITY period P exceeds the N samples in the range` — the period is larger than the range.
- `TSDB: missing forecast confidence level` — `LEVEL` was given without a numeric value.
- `TSDB: LEVEL must be between 0 and 100` — The confidence level is out of range.
- `TSDB: unknown auto-forecast model: <entry>` — An unrecognized (or empty) model family was specified in `MODELS`.
- `TSDB: Missing value for MODELS` — The `MODELS` argument was given without a value.
- `TSDB: missing key` — `STORE` was given without a key name.
- `TSDB: STORE destination must be different from the source key` — `STORE` names the source key.
- `TSDB: rounding already set` — both `SIGNIFICANT_DIGITS` and `DECIMAL_DIGITS` were given in `STORE`.
- `TSDB: STORE requires at least two samples in the range to determine the forecast step` — the range
  holds too few samples to infer where the stored forecast samples should be placed.
- `TSDB: STORE forecast timestamps exceed the supported range` — the last timestamp and forecast
  step would overflow the timestamp type at the requested `HORIZON`; rejected before model work.
- `TSDB: failed to store forecast in key '<key>': <reason>` — the forecast samples could not be written
  to the destination.
- `TSDB: Unknown argument: <arg>` — An unrecognized optional argument was provided.
- `TSDB: missing value for TIMEOUT` / `TSDB: TIMEOUT must be zero or positive` — `TIMEOUT` is missing
  its value or is negative.
- `TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` — the
  `TIMEOUT` (or `ts-analysis-timeout`) deadline elapsed before the result was available.
- `TSDB: Failed to prepare time series for forecasting` — Internal error converting series data.
- `TSDB: insufficient data: need at least 10, got N (...)` — the range holds fewer than 10 samples.
- `TSDB: convergence failure: No candidate model produced valid cross-validation results` — no enabled
  family could be scored (see `MODELS`).
- `TSDB: <reason>` — the selected model failed to fit or predict, or (`TSDB: metrics error: <reason>`)
  its metrics could not be computed.

## Complexity

Depends on the number of enabled model families and the size of the input time series. Each model family performs a
hyperparameter search proportional to the number of candidate models evaluated.

Forecasting is executed on a background thread to avoid blocking the server.

## ACL Categories

`write timeseries`
