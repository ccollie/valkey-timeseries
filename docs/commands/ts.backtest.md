# TS.BACKTEST

Evaluate one or more forecasting models against historical data using walk-forward validation,
returning out-of-sample accuracy metrics for each model.

`TS.BACKTEST` answers the question "how would this model have performed if I'd used it to
forecast this series over time?" It repeatedly trains a model on a window of historical data,
forecasts forward `HORIZON` points, compares the forecast against the actual observed values,
and records the error. This is repeated across multiple folds that slide across the requested
range. Unlike `TS.FORECAST METRICS`, which scores a model against its own in-sample fitted
values, `TS.BACKTEST` scores models against **held-out data the model never saw during
training** — a realistic estimate of production forecast accuracy.

Use `TS.BACKTEST` to compare candidate models or hyperparameters before choosing one for
`TS.FORECAST` or `TS.AUTOFORECAST`.

## Syntax

```
TS.BACKTEST key fromTimestamp toTimestamp
  MODELS model_spec[,model_spec ...]
  HORIZON horizon
  [INITIAL_WINDOW size]
  [STRATEGY EXPANDING|ROLLING]
  [STEP stepSize]
  [N_FOLDS n]
  [GAP gap]
  [PURGE purge]
  [EMBARGO embargo]
  [SEASONAL_PERIOD period]
  [WITH_PREDICTIONS]
  [TIMEOUT milliseconds]
```

[Examples](#examples)

## Required Arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to backtest against.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of historical data used for backtesting (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of historical data used for backtesting (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

<details open>
<summary><code>MODELS model_spec[,model_spec ...]</code></summary>

Comma-separated list of model specifications to evaluate. Uses the same grammar as
[`TS.FORECAST`](ts.forecast.md#models-model_specmodel_spec-): `ModelName(args...)` with optional
positional and keyword arguments (e.g. `ARIMA(2,1,0)`, `SES(alpha=0.3)`, `MSTL(12, iterations=5)`).
See the `TS.FORECAST` documentation for the full list of supported models and their parameters.

At least one model must be specified. The specs are parsed and validated before any work is scheduled,
so a malformed or unsupported spec fails immediately with `TSDB: error parsing MODELS: <reason>`. If
`MODELS` appears more than once, the last clause wins.

Each model is re-fit independently for every fold; folds run in parallel (see
[Performance](#performance-folds-run-in-parallel)), but the total CPU work for slower models (`ARIMA`,
`TBATS`, `AutoARIMA`, `AutoTBATS`) still scales with `N_FOLDS`.
</details>

<details open>
<summary><code>HORIZON horizon</code></summary>

Number of points to forecast forward at each fold, and the number of points each fold's
predictions are compared against. Must be a positive integer no larger than the `ts-forecast-max-horizon` configuration
parameter (default 10000).
</details>

## Optional Arguments

<details open>
<summary><code>INITIAL_WINDOW size</code></summary>

Minimum number of observations required in a fold's training window. The earliest fold to
backtest will have at least this many points to train on.

Default: `max(3 * horizon, 10)`.
</details>

<details open>
<summary><code>STRATEGY EXPANDING|ROLLING</code></summary>

Controls how the training window moves between folds:

- `EXPANDING` (default) — the training window always starts at the beginning of the requested
  range and grows with each successive fold. Later folds train on more data than earlier ones.
- `ROLLING` — the training window has a fixed size (`INITIAL_WINDOW` points) and slides forward,
  always training on the same amount of data. Use this to evaluate how a model performs with a
  consistent amount of history (e.g. to simulate a fixed retention window in production).
</details>

<details open>
<summary><code>STEP stepSize</code></summary>

Number of points to advance between consecutive fold origins.

Default: equal to `HORIZON`, which produces non-overlapping test windows (each point in the
range is scored by at most one fold). Set `STEP` smaller than `HORIZON` for overlapping test
windows — more folds from the same amount of data, at the cost of correlated fold errors. Set
it larger than `HORIZON` to sample sparsely across a long history.
</details>

<details open>
<summary><code>N_FOLDS n</code></summary>

Target number of folds to evaluate per model. Folds are placed backwards from the end of the
requested range, and the `n` most recent are kept, so the most recent data is included (unless
`EMBARGO` later drops those folds — see below).

If the data is too short to produce `n` folds while satisfying `INITIAL_WINDOW`, the number of
folds is silently reduced to whatever is feasible — this is not an error. If not even one fold
is feasible, `TS.BACKTEST` returns an error.

Default: `5`.
</details>

<details open>
<summary><code>GAP gap</code></summary>

Number of points skipped between the end of a fold's training window and the start of its test
window. Use a non-zero `GAP` to simulate production latency (e.g. "the model is trained on data
up to yesterday but must forecast starting two days from now") or to prevent leakage from
features with lag effects.

Default: `0`.
</details>

<details open>
<summary><code>PURGE purge</code></summary>

Number of points dropped from the **end** of each fold's training window before the test window
begins. Use `PURGE` to guard against lookahead leakage from features or labels whose influence
spans the train/test boundary (autocorrelated engineered features, overlapping label windows).

> **Note:** In the current forecasting engine, `PURGE` and `GAP` have the same effect on the
> reported fold boundaries — both carve a discard band between training and test, and the total
> separation between `train_end` and `test_start` is `GAP + PURGE`. They differ only in intent:
> `GAP` models forecast/deployment latency, `PURGE` models feature-leakage protection. Use whichever
> name documents your reason; use both if you want to record both reasons independently.

Default: `0`.
</details>

<details open>
<summary><code>EMBARGO embargo</code></summary>

Number of points excluded from **later** folds' training windows immediately following each test
window. Unlike `GAP`/`PURGE` (which only affect the boundary before a single fold's test window),
`EMBARGO` acts across folds: walking the folds oldest first, each fold's training **start** is moved
forward to `EMBARGO` points past the end of the latest earlier test window, whenever that is later
than its own start. This keeps a test window, and the `EMBARGO` points after it, out of every later
fold's training data.

A fold whose training window is fully consumed by this is dropped (after the `N_FOLDS` cap is
applied, so the effective fold count may fall below the requested value; this is not an error). In
walk-forward validation a later fold's training window normally covers the earlier test windows, so
unless `STEP` exceeds `HORIZON + GAP + PURGE + EMBARGO`, **any non-zero `EMBARGO` leaves only the
earliest fold**. With a large enough `STEP`, later folds survive with training windows of at most
`STEP - HORIZON - GAP - PURGE - EMBARGO` points, so `STRATEGY EXPANDING` behaves like a delayed-start
window.

Default: `0`.
</details>

<details open>
<summary><code>SEASONAL_PERIOD period</code></summary>

Seasonal period used only for computing the MASE (Mean Absolute Scaled Error) component of each
fold's accuracy metrics. Does not affect model fitting — pass model-specific seasonality via the
relevant `MODELS` model spec (e.g. `MSTL(12)`, `SeasonalNaive(period=12)`).

MASE's scaling denominator is derived from the fold's **test window**, not its training window.
Consequently, if `SEASONAL_PERIOD` is greater than or equal to `HORIZON` (the number of points in
each test window), the seasonal scaling factor cannot be computed and `mase` is reported as `null`
for that fold. When `SEASONAL_PERIOD` is omitted, MASE falls back to non-seasonal (lag-1) scaling,
which is defined whenever `HORIZON >= 2`. In short: to get a non-null seasonal `mase`, use a
`HORIZON` larger than `SEASONAL_PERIOD`; otherwise omit `SEASONAL_PERIOD` and rely on the lag-1
scaling. This mirrors how `TS.FORECAST METRICS` computes MASE.
</details>

<details open>
<summary><code>WITH_PREDICTIONS</code></summary>

When specified, each fold's entry in the response includes `predictions` and `actuals` arrays —
the raw forecasted and observed values for that fold's test window. Useful for plotting forecast
accuracy over time. Omitted by default, since `N_FOLDS × HORIZON` values per model can add up for
large backtests.
</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds, counted from when the request is accepted (so time
spent queued behind other forecasting work counts). When it elapses the client receives
`TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` and the
request is abandoned: its result is discarded. `0` disables the deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).

Forecasting commands run on the analysis lane (2–8 worker threads, from `ts-num-threads`), so
they do not block the server's main thread; requests beyond the worker count wait in a queue
of up to 256, and a request that finds it full is refused at once with
`TSDB: analysis: too many queued jobs (limit 256)`.
The exception is a call that cannot block — inside `MULTI`/`EXEC`, a Lua script, or a module's
`RM_Call` — which runs inline on the main thread (folds sequentially), where no deadline applies.
There the work is held to 60,000 samples × models × folds; a larger request fails with
`TSDB: range too large to run inside MULTI, a script or a module call: …; run the command outside of it`.
</details>

## How Backtesting Works

`TS.BACKTEST`:

1. Computes fold boundaries once, shared by every model, across the samples in the requested
   `[fromTimestamp, toTimestamp]` range based on `STRATEGY`, `INITIAL_WINDOW`, `HORIZON`, `STEP`,
   `N_FOLDS`, `GAP`, `PURGE`, and `EMBARGO`. All sizes count samples, not time. Folds are placed
   backwards from the end of the range, so the most recent `HORIZON` points belong to the last
   fold's test window (unless `EMBARGO` drops the later folds).
2. For each model and fold, fits a **fresh instance** of the model on the fold's training window
   only (the model never sees the test window's data) and forecasts `HORIZON` points forward.
3. Compares the forecast against the actual observed values in the test window, computing
   per-fold accuracy metrics (`mae`, `mse`, `rmse`, `mape`, `smape`, `mase`, `r_squared`).
4. Aggregates per-fold metrics into an overall summary (`mae`, `rmse`, `smape`, `mape`, plus
   `mae_std`/`rmse_std` — the standard deviation of `mae`/`rmse` across folds, which indicates how
   consistent the model's accuracy is over time).

If a model fails on any fold (for example, a model requiring more data than the earliest fold's
training window provides), that model's response entry contains only `model` and `error`. Other
models in the same `MODELS` list are unaffected and still return results.

### Performance: folds run in parallel

Within a single model, all of its folds are fit and scored **in parallel** across the module's
analysis worker pool — the dominant cost (one full model fit per fold) is not paid serially.
Models in `MODELS` are still dispatched one at a time, so that one model's fold failure can't
abort another model's still-in-progress results; with multiple folds per model this still means
most of the total work happens concurrently. The whole command additionally runs off the main
Valkey thread (like `TS.FORECAST`/`TS.AUTOFORECAST`), so it does not block other clients while
backtesting runs — except where the client cannot be blocked (see `TIMEOUT`).

`STRATEGY EXPANDING`'s later folds still cost more per-fold than earlier ones (larger training
window), so total wall-clock time for a backtest isn't perfectly flat across `N_FOLDS`, but it no
longer scales linearly with fold count the way naive sequential evaluation would.

## Return Value

The response is an **array of maps** (flat key-value arrays in RESP2), one entry per model specified
in `MODELS`, in the same order.

| Field       | Type            | Always Present | Description                                                                 |
|-------------|-----------------|-----------------|-------------------------------------------------------------------------------|
| `model`     | string          | Yes             | The model spec in canonical form (e.g. `ARIMA(2,1,0)`; `Naive` becomes `Naive()`) |
| `horizon`   | integer         | No              | Forecast horizon used for each fold (absent if `error` is present)           |
| `strategy`  | string          | No              | `"expanding"` or `"rolling"` (absent if `error` is present)                  |
| `n_folds`   | integer         | No              | Number of folds actually evaluated, may be less than `N_FOLDS` (absent if `error` is present) |
| `metrics`   | map             | No              | Aggregated accuracy metrics across all folds (absent if `error` is present)  |
| `folds`     | array of maps   | No              | Per-fold results (absent if `error` is present)                              |
| `error`     | string          | No              | Present only if this model failed on one or more folds; the entry then holds only `model` and `error` |

**`metrics` map** (aggregated across folds):

| Field       | Type    | Description                                                        |
|-------------|---------|----------------------------------------------------------------------|
| `mae`       | double  | Mean of per-fold Mean Absolute Error                                 |
| `rmse`      | double  | Mean of per-fold Root Mean Squared Error                             |
| `smape`     | double  | Mean of per-fold Symmetric Mean Absolute Percentage Error            |
| `mape`      | double  | Mean of per-fold Mean Absolute Percentage Error (`null` if any fold's `mape` is `null`, e.g. its actuals contain zeros) |
| `mae_std`   | double  | Population standard deviation of `mae` across folds (`0` for a single fold) |
| `rmse_std`  | double  | Population standard deviation of `rmse` across folds (`0` for a single fold) |

**Each entry in `folds`:**

| Field         | Type            | Always Present | Description                                              |
|---------------|-----------------|-----------------|------------------------------------------------------------|
| `train_start` | integer         | Yes             | Timestamp of the first training sample in this fold        |
| `train_end`   | integer         | Yes             | Timestamp of the last training sample in this fold         |
| `test_start`  | integer         | Yes             | Timestamp of the first sample in this fold's test window   |
| `test_end`    | integer         | Yes             | Timestamp of the last sample in this fold's test window    |
| `metrics`     | map             | Yes             | `mae`, `mse`, `rmse`, `mape`, `smape`, `mase`, `r_squared` for this fold (same shape as `TS.FORECAST METRICS`) |
| `predictions` | array of double | No              | Forecasted values (only with `WITH_PREDICTIONS`)           |
| `actuals`     | array of double | No              | Observed values (only with `WITH_PREDICTIONS`)              |

### Example Response

```
1) 1) "model"
   2) "ARIMA(2,1,0)"
   3) "horizon"
   4) (integer) 7
   5) "strategy"
   6) "expanding"
   7) "n_folds"
   8) (integer) 5
   9) "metrics"
   10) 1) "mae"
       2) "1.23"
       3) "rmse"
       4) "1.58"
       5) "smape"
       6) "2.05"
       7) "mape"
       8) "2.11"
       9) "mae_std"
       10) "0.31"
       11) "rmse_std"
       12) "0.42"
   11) "folds"
   12) 1) 1) "train_start"
          2) (integer) 1000
          3) "train_end"
          4) (integer) 71000
          5) "test_start"
          6) (integer) 72000
          7) "test_end"
          8) (integer) 78000
          9) "metrics"
          10) 1) "mae"
              2) "1.05"
              3) "mse"
              4) "1.42"
              5) "rmse"
              6) "1.19"
              7) "mape"
              8) "1.98"
              9) "smape"
              10) "1.95"
              11) "mase"
              12) "0.87"
              13) "r_squared"
              14) "0.98"
       2) ... (remaining folds)
2) 1) "model"
   2) "SES(alpha=0.3)"
   3) "horizon"
   4) (integer) 7
   5) "strategy"
   6) "expanding"
   7) "n_folds"
   8) (integer) 5
   9) "metrics"
   10) 1) "mae"
       2) "1.87"
       3) "rmse"
       4) "2.34"
       5) "smape"
       6) "3.10"
       7) "mape"
       8) "3.22"
       9) "mae_std"
       10) "0.18"
       11) "rmse_std"
       12) "0.25"
   11) "folds"
   12) ...
```

### Example Response (model failure isolated to one entry)

On a 40-sample series, the earliest fold trains on only 25 samples:

```
127.0.0.1:6379> TS.BACKTEST ts:short - + MODELS "SARIMA(1,1,1,1,1,1,12), Naive" HORIZON 3 INITIAL_WINDOW 10
1) 1) "model"
   2) "SARIMA(1,1,1,1,1,1,12)"
   3) "error"
   4) "TSDB: insufficient data: need at least 27, got 25 (SARIMA(1,1,1)(1,1,1)_12 requires at least 27 observations)"
2) 1) "model"
   2) "Naive()"
   3) "horizon"
   4) (integer) 3
   5) "strategy"
   6) "expanding"
   7) "n_folds"
   8) (integer) 5
   9) "metrics"
   10) ...
   11) "folds"
   12) ...
```

## Errors

- `ERR wrong number of arguments for 'ts.backtest' command` — fewer than seven arguments after the
  command name.
- `TSDB: the key does not exist` — the specified key does not exist (a key of another type gives
  `WRONGTYPE`).
- `TSDB: wrong fromTimestamp` / `TSDB: wrong toTimestamp` — a range bound could not be parsed.
- `TSDB: HORIZON is required` — the `HORIZON` argument is missing.
- `TSDB: missing forecast horizon value` — `HORIZON` was given without a value.
- `TSDB: invalid forecast horizon, expected an integer` — the `HORIZON` value is not an integer.
- `TSDB: forecast horizon must be greater than 0` — `HORIZON` is zero or negative.
- `TSDB: forecast horizon must not exceed N (ts-forecast-max-horizon)` — `HORIZON` is above the
  configured cap.
- `TSDB: MODELS must contain at least one model specification` — `MODELS` is missing or empty.
- `TSDB: missing value for MODELS` — `MODELS` was given without a value.
- `TSDB: error parsing MODELS: <reason>` — a model specification could not be parsed, names an
  unsupported model, or has invalid parameters.
- `TSDB: missing value for STRATEGY` / `TSDB: STRATEGY must be EXPANDING or ROLLING` — `STRATEGY` is
  missing its value or the value is invalid.
- `TSDB: missing value for <OPTION>` / `TSDB: invalid value for <OPTION>, expected an integer` —
  `INITIAL_WINDOW`, `STEP`, `N_FOLDS`, `GAP`, `PURGE`, `EMBARGO`, or `SEASONAL_PERIOD` is missing
  its value, or the value is not an integer.
- `TSDB: <OPTION> must be greater than 0` — `N_FOLDS`, `STEP`, `INITIAL_WINDOW`, or
  `SEASONAL_PERIOD` was zero or negative.
- `TSDB: <OPTION> must be non-negative` — `GAP`, `PURGE`, or `EMBARGO` was negative.
- `TSDB: not enough data for backtest with the given HORIZON/INITIAL_WINDOW` — the requested
  range is too short to produce even one fold. The minimum length for a single fold is
  `INITIAL_WINDOW + PURGE + GAP + HORIZON` observations.
- `TSDB: Unknown argument: <arg>` — an unrecognized argument was provided.
- `TSDB: missing value for TIMEOUT` / `TSDB: TIMEOUT must be zero or positive` — `TIMEOUT` is missing
  its value or is negative.
- `TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` — the
  `TIMEOUT` (or `ts-analysis-timeout`) deadline elapsed before the result was available.
- `TSDB: Failed to prepare time series for forecasting` — the series data could not be converted
  to the format required by the forecasting library.
- Per-model, per-fold failures do **not** abort the whole command — see
  [How Backtesting Works](#how-backtesting-works). They surface as an `error` field on that
  model's response entry instead.

## Examples

### Compare two models with default backtest settings

```
127.0.0.1:6379> TS.BACKTEST ts:metrics - + MODELS "ARIMA(2,1,0), Naive()" HORIZON 7
1) 1) "model"
   2) "ARIMA(2,1,0)"
   3) "horizon"
   4) (integer) 7
   5) "strategy"
   6) "expanding"
   7) "n_folds"
   8) (integer) 5
   9) "metrics"
   10) 1) "mae"  2) "1.23"  3) "rmse" 4) "1.58" 5) "smape" 6) "2.05" 7) "mape" 8) "2.11" 9) "mae_std" 10) "0.31" 11) "rmse_std" 12) "0.42"
   11) "folds"
   12) ...
2) 1) "model"
   2) "Naive()"
   3) "horizon"
   4) (integer) 7
   5) "strategy"
   6) "expanding"
   7) "n_folds"
   8) (integer) 5
   9) "metrics"
   10) 1) "mae" 2) "3.40" 3) "rmse" 4) "4.12" 5) "smape" 6) "5.80" 7) "mape" 8) "6.01" 9) "mae_std" 10) "0.55" 11) "rmse_std" 12) "0.70"
   11) "folds"
   12) ...
```

`ARIMA(2,1,0)` shows lower average error than `Naive()` over the 5 folds — a reasonable signal to
prefer it in `TS.FORECAST`.

### Rolling window with explicit step and gap

```
127.0.0.1:6379> TS.BACKTEST ts:metrics - + MODELS "SeasonalNaive(12)" HORIZON 12 \
  INITIAL_WINDOW 60 STRATEGY ROLLING STEP 6 GAP 1 N_FOLDS 10
1) 1) "model"
   2) "SeasonalNaive(12)"
   3) "horizon"
   4) (integer) 12
   5) "strategy"
   6) "rolling"
   7) "n_folds"
   8) (integer) 10
   9) "metrics"
   10) ...
   11) "folds"
   12) ...
```

Each fold trains on a fixed window of 60 points, skips 1 point after training before the test
window starts (`GAP 1`), and successive folds start 6 points apart (`STEP 6`), giving 10
overlapping folds.

### Leakage-safe evaluation with PURGE and EMBARGO

```
127.0.0.1:6379> TS.BACKTEST ts:metrics - + MODELS "ARIMA(2,1,0)" HORIZON 12 \
  STEP 60 PURGE 5 EMBARGO 10 N_FOLDS 3
1) 1) "model"
   2) "ARIMA(2,1,0)"
   3) "horizon"
   4) (integer) 12
   5) "strategy"
   6) "expanding"
   7) "n_folds"
   8) (integer) 3
   9) "metrics"
   10) ...
   11) "folds"
   12) ...
```

`PURGE 5` drops the 5 points immediately before each test window from training (guarding against
boundary leakage from engineered features), and `EMBARGO 10` excludes each earlier test window and
the 10 points after it from later folds' training. With `STEP 60`, each later fold keeps a training
window of `60 - 12 - 5 - 10 = 33` points. With the default `STEP` (= `HORIZON`) the embargo would
consume every later fold's training window, leaving `n_folds` at 1 (the earliest fold).

### Inspect raw predictions vs actuals per fold

```
127.0.0.1:6379> TS.BACKTEST ts:metrics - + MODELS "ETS()" HORIZON 5 N_FOLDS 3 WITH_PREDICTIONS
1) 1) "model"
   2) "ETS()"
   ...
   11) "folds"
   12) 1) 1) "train_start"
          2) (integer) 1000
          ...
          9) "metrics"
          10) ...
          11) "predictions"
          12) 1) "101.20" 2) "102.05" 3) "102.90" 4) "103.75" 5) "104.60"
          13) "actuals"
          14) 1) "101.00" 2) "102.30" 3) "102.60" 4) "104.10" 5) "104.20"
       2) ... (remaining folds)
```

## ACL Categories

`read timeseries`
