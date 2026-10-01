# TS.TREND

Fit trend components to a time series, with optional automatic model selection.

`TS.TREND` can operate in two modes:

- **Auto mode** (default): Fits multiple candidate trend models (Linear, Quadratic,
  Exponential, Logistic, TheilSen, PiecewiseLinear) and selects the best one using an information
  criterion (AICc by default).
- **Specific model mode**: Fits a single specified trend model (Exponential, Logistic,
  Polynomial, or TheilSen).

The fitted trend values, optional predictions, model features, and optional accuracy metrics are returned.
The range must contain at least 4 samples.

## Syntax

```
TS.TREND key fromTimestamp toTimestamp
  [MODEL <Exponential|Logistic|Polynomial|TheilSen|Auto> [AICc|BIC|HOLDOUT]]
  [RECENCY <FULL|WINDOW n|FRACTION f|AUTO>]
  [PREDICT <horizon>]
  [FEATURES]
  [METRICS]
  [TIMEOUT milliseconds]
  [STORE destinationKey
    [MERGE]
    [RETENTION retentionPeriod]
    [ENCODING <compressed|uncompressed|gorilla|chimp>]
    [CHUNK_SIZE chunkSize]
    [DUPLICATE_POLICY duplicatePolicy]
    [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
    [METRIC metric]
    [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
  ]
```

Options may appear in any order. Model names and keywords are case-insensitive.

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to analyze.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to analyze (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to analyze (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>MODEL</code></summary>

Trend model to fit. If omitted, `Auto` is assumed.

* `Auto` (default) — Fit multiple candidate trend components and select the best one
  using an information criterion. Optionally followed by a criterion name:
  `AICc` (default), `BIC`, or `HOLDOUT`.
* `Exponential` — Fit an exponential trend: `y = exp(a + b*t)`.
* `Logistic` — Fit a logistic (S-curve) trend: `y = K / (1 + exp(-steepness * (t - midpoint)))`.
* `Polynomial` — Fit a quadratic polynomial trend (degree 2).
* `TheilSen` — Fit a robust linear trend using the Theil-Sen estimator (median of pairwise slopes).

When `MODEL` is `Auto`, the response includes `model`, `criterion`, and `scores`.
When a specific model is given, the response includes `model` but omits `criterion` and `scores`.

</details>

<details open>
<summary><code>RECENCY</code></summary>

Controls which portion of the data is used for fitting the trend. For forecasting,
the most recent trend is usually what matters.

* `FULL` — Use all data.
* `WINDOW n` — Use only the last `n` observations (minimum 4).
* `FRACTION f` (default: `0.3`) — Use the last fraction of data (e.g., `0.3` = last 30%);
  `f` must be greater than 0 and at most 1.
* `AUTO` — Automatically detect the recency window via changepoint analysis (PELT).

The fitted values still cover the full series (the portion before the recency window
is filled by evaluating the fitted model at earlier indices — backwards extrapolation).

Examples:
- `RECENCY FULL` — fit on all data
- `RECENCY WINDOW 50` — fit on last 50 observations
- `RECENCY FRACTION 0.5` — fit on last 50% of data
- `RECENCY AUTO` — automatically detect recency window

</details>

<details open>
<summary><code>PREDICT</code></summary>

Number of steps ahead to predict the trend: a positive integer no larger than the
`ts-forecast-max-horizon` configuration parameter.

When specified, the response includes a `predicted_trend` array with `horizon` values.

Example: `PREDICT 10` predicts the trend for the next 10 observations.

</details>

<details open>
<summary><code>FEATURES</code></summary>

When specified, the response includes a `features` map with named attributes of the
fitted trend component. The exact features depend on the selected model.

</details>

<details open>
<summary><code>METRICS</code></summary>

When specified, the response includes an `accuracy_metrics` map, which measures the deviation of the fitted trend values from the observed values.

Returned metrics:

- `mae` (Mean Absolute Error): Average of absolute differences.
- `mse` (Mean Squared Error): Average of squared differences.
- `rmse` (Root Mean Squared Error): Square root of the mean squared error.
- `mape` (Mean Absolute Percentage Error): Average of absolute percentage differences (may be `null` when actual contains zeros).
- `smape` (Symmetric Mean Absolute Percentage Error): Average of symmetric absolute percentage differences.
- `mase` (Mean Absolute Scaled Error): Average of absolute errors scaled by the in-sample mean absolute error (may be `null` when insufficient data for scaling).
- `r_squared` (Coefficient of Determination): Proportion of variance explained by the model.

</details>

<details open>
<summary><code>STORE destinationKey</code></summary>

Persist the fitted trend values into a time series key. The fitted values are stored with their
original timestamps from the input series.

The destination must be a different key from the source; naming the source fails with
`TSDB: STORE destination must be different from the source key`. Only the primary runs the analysis: replicas and the AOF receive the stored samples, not the
command.

- If the destination key does not exist, a new time series is created.
- If the destination key already exists, it is overwritten by default. Pass `MERGE` to merge
  the fitted (and optionally predicted) samples into the existing series instead.
- The other clause options (`RETENTION`, `ENCODING`, `CHUNK_SIZE`, `DUPLICATE_POLICY`,
  `SIGNIFICANT_DIGITS`/`DECIMAL_DIGITS`, `METRIC`, `IGNORE`) configure a newly created destination,
  as in the other `STORE` clauses. As with `TS.ADD`, an existing destination keeps its own
  settings and the options are ignored.

With `STORE`, the reply is the number of samples written (an integer) instead of the fit.

When `STORE` is combined with `PREDICT`, the predicted trend values are appended after the fitted
values with timestamps continuing from the last observed timestamp using the series' median
sampling interval. If the series has fewer than two timestamps or no positive time intervals,
predicted values are skipped with a warning (fitted values are still stored).

</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds. Ranges of up to 2,000 samples are computed
inline, as is any call inside `MULTI`, a Lua script or a module call (up to 40,000 samples there;
a larger range is refused, see the [overview](../overview.md#running-the-analysis-commands));
larger ranges run on a
dedicated pool of analysis worker threads (sized by `ts-num-threads`) so they never stall the
server, and the deadline applies only to them. It is
counted from when the request is accepted, so time spent queued behind other analysis work
counts. When it elapses the client receives `TSDB: command timed out before the result was
ready (see TIMEOUT / ts-analysis-timeout)` and the request is abandoned; a `STORE` that has not
yet happened is skipped. `0` disables the deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).
</details>

## Return

`TS.TREND` returns a map (key-value pairs; a flat array of alternating names and values under
RESP2, as shown below). The fields depend on whether `MODEL Auto` (or default) or a specific model
was used. Fields appear in this order: `model`, `criterion`, `fitted_trend`, `scores`,
`predicted_trend`, `features`, `accuracy_metrics`, `n_params`. With `STORE`, the reply is instead
the number of samples written.

### Auto mode response

- `model` — Name of the selected trend model: "Linear", "Quadratic", "Exponential", "Logistic", "TheilSen" or "PiecewiseLinear".
- `criterion` — The criterion used for selection: `AICc`, `BIC`, or `HOLDOUT`.
- `fitted_trend` — Array of in-sample fitted trend values (same length as the input data).
- `scores` — Array of `[name, score]` pairs for all candidate models, sorted from best to worst. Lower scores are better.
- `n_params` — Number of free parameters in the selected model.

### Specific model response

- `model` — Name of the trend model used: "Exponential", "Logistic", "Polynomial" or "TheilSen".
- `fitted_trend` — Array of in-sample fitted trend values (same length as the input data).
- `n_params` — Number of free parameters in the fitted model.

### Optional response fields (both modes)

If `PREDICT` is specified:
- `predicted_trend` — Array of predicted trend values (length = `horizon`).

If `FEATURES` is specified:
- `features` — Map of named feature values for the fitted component (e.g. `theilsen_slope`,
  `theilsen_intercept`, `theilsen_r_squared` for TheilSen).

If `METRICS` is specified:
- `accuracy_metrics` — Map of accuracy metrics (`mae`, `mse`, `rmse`, `mape`, `smape`, `mase`, `r_squared`) computed from observed vs fitted values.

### Example response (Auto mode)

```
 1) model
 2) Linear
 3) criterion
 4) AICc
 5) fitted_trend
 6) 1) "20.1"
    2) "20.3"
    ...
 7) scores
 8) 1) 1) Linear
       2) "-45.2"
    2) 1) Quadratic
       2) "-43.1"
    ...
 9) n_params
10) (integer) 2
```

### Example response (specific model)

```
1) model
2) Exponential
3) fitted_trend
4) 1) "20.1"
   2) "20.3"
   ...
5) n_params
6) (integer) 2
```

## Examples

Explore the following examples to learn how to get started.

## Select the best trend with AICc (default)

Get the best trend model for a time series using the default AICc criterion.

```
127.0.0.1:6379> TS.CREATE temperature
OK
127.0.0.1:6379> TS.ADD temperature 1000 20.1
(integer) 1000
127.0.0.1:6379> TS.ADD temperature 2000 20.3
(integer) 2000
127.0.0.1:6379> TS.ADD temperature 3000 20.5
(integer) 3000
127.0.0.1:6379> TS.ADD temperature 4000 20.8
(integer) 4000
127.0.0.1:6379> TS.ADD temperature 5000 21.0
(integer) 5000
127.0.0.1:6379> TS.TREND temperature - +
 1) model
 2) Exponential
 3) criterion
 4) AICc
 5) fitted_trend
 6) 1) "20.028450268323255"
    2) "20.271228226550807"
    3) "20.51694905535553"
    4) "20.76564842719838"
    5) "21.017362446949566"
 7) scores
 8) 1) 1) Exponential
       2) "-8.180142499729044"
    2) 1) Linear
       2) "-7.024509544897857"
    3) 1) PiecewiseLinear
       2) "-7.024509544892798"
    4) 1) TheilSen
       2) "-4.856329619523269"
    5) 1) Logistic
       2) "13.954717646827778"
    6) 1) Quadratic
       2) "29.952823989871185"
 9) n_params
10) (integer) 2
```

## Select trend with BIC and predict ahead

Use BIC for selection (via MODEL Auto) and predict the next 5 trend values.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL Auto BIC PREDICT 5
 1) model
 2) Exponential
 3) criterion
 4) BIC
 5) fitted_trend
 6) 1) "20.028450268323255"
    2) "20.271228226550807"
    3) "20.51694905535553"
    4) "20.76564842719838"
    5) "21.017362446949566"
 7) scores
 8) 1) 1) Exponential
       2) "-14.961266674860843"
    2) 1) Linear
       2) "-13.805633720029656"
    3) 1) PiecewiseLinear
       2) "-13.805633720024597"
    4) 1) TheilSen
       2) "-11.637453794655068"
    5) 1) Logistic
       2) "-11.216968615869922"
    6) 1) Quadratic
       2) "4.781137727173485"
 9) predicted_trend
10) 1) "21.272127657130053"
    2) "21.529981043216626"
    3) "21.790960039011264"
    4) "22.055102532075566"
    5) "22.322446869231058"
11) n_params
12) (integer) 2
```

## Use MODEL Auto with holdout criterion

Use auto model selection with the holdout criterion, specified inline after MODEL Auto.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL Auto HOLDOUT
 1) model
 2) Quadratic
 3) criterion
 4) HOLDOUT
 5) fitted_trend
 6) 1) "19.59999999999985"
    2) "20.09999999999994"
    3) "20.499999999999996"
    4) "20.800000000000015"
    5) "20.999999999999996"
 7) scores
 8) 1) 1) Quadratic
       2) "0.009999999999988206"
    2) 1) TheilSen
       2) "0.0625"
    3) 1) Logistic
       2) "0.06839470527575342"
    4) 1) Linear
       2) "0.0711111111111125"
    5) 1) PiecewiseLinear
       2) "0.07111111111112577"
    6) 1) Exponential
       2) "0.07405346177509159"
 9) n_params
10) (integer) 3
```

## Fit a specific exponential trend

Fit only an exponential trend model (no auto-selection).

```
127.0.0.1:6379> TS.TREND temperature - + MODEL Exponential
1) model
2) Exponential
3) fitted_trend
4) 1) "20.028450268323255"
   2) "20.271228226550807"
   3) "20.51694905535553"
   4) "20.76564842719838"
   5) "21.017362446949566"
5) n_params
6) (integer) 2
```

## Fit a specific Theil-Sen trend with prediction

Fit a robust Theil-Sen trend and predict ahead.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL TheilSen PREDICT 5
1) model
2) TheilSen
3) fitted_trend
4) 1) "20"
   2) "20.25"
   3) "20.5"
   4) "20.75"
   5) "21"
5) predicted_trend
6) 1) "21.25"
   2) "21.5"
   3) "21.75"
   4) "22"
   5) "22.25"
7) n_params
8) (integer) 2
```

## Fit on recent data only

Use only the last window of observations for trend fitting.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL TheilSen RECENCY WINDOW 4
1) model
2) TheilSen
3) fitted_trend
4) 1) "20.045833333333334"
   2) "20.2875"
   3) "20.52916666666667"
   4) "20.770833333333336"
   5) "21.0125"
5) n_params
6) (integer) 2
```

## Include feature details and accuracy metrics

Request the fitted component's features and accuracy metrics computed from observed vs fitted values.

```
127.0.0.1:6379> TS.TREND temperature - + MODEL TheilSen FEATURES METRICS
 1) model
 2) TheilSen
 3) fitted_trend
 4) 1) "20"
    2) "20.25"
    3) "20.5"
    4) "20.75"
    5) "21"
 5) features
 6) 1) theilsen_slope
    2) "0.25"
    3) theilsen_intercept
    4) "20"
    5) theilsen_r_squared
    6) "0.9802631578947363"
 7) accuracy_metrics
 8)  1) mae
     2) "0.04000000000000057"
     3) mse
     4) "0.0030000000000000855"
     5) rmse
     6) "0.05477225575051739"
     7) mape
     8) "0.19684049438295728"
     9) smape
    10) "0.19720722572557553"
    11) mase
    12) "0.1777777777777806"
    13) r_squared
    14) "0.9718045112781947"
 9) n_params
10) (integer) 2
```

## Store the fitted and predicted trend

```
127.0.0.1:6379> TS.TREND temperature - + MODEL TheilSen PREDICT 2 STORE temperature:trend
(integer) 7
127.0.0.1:6379> TS.RANGE temperature:trend - +
1) 1) (integer) 1000
   2) "20"
2) 1) (integer) 2000
   2) "20.25"
3) 1) (integer) 3000
   2) "20.5"
4) 1) (integer) 4000
   2) "20.75"
5) 1) (integer) 5000
   2) "21"
6) 1) (integer) 6000
   2) "21.25"
7) 1) (integer) 7000
   2) "21.5"
```

The two predicted samples are stored at 6000 and 7000, continuing the series' 1000 ms step.

## See also

- [`TS.AUTOFORECAST`](ts.autoforecast.md) — Automatic forecasting with model selection.
- [`TS.DECOMPOSE`](ts.decompose.md) — Decompose a series into trend, seasonal, and residual components.
- [`TS.PERIODS`](ts.periods.md) — Detect seasonal periods in a time series.
- [`TS.AUTOCORRELATION`](ts.autocorrelation.md) — Compute autocorrelation statistics.
