# TS.FEATURES

Compute a set of statistical features on a time series.

`TS.FEATURES` extracts named statistical features from a time series using the
`anofox-forecast` feature engineering library (tsfresh-compatible). Features can
be selected by category or specified individually.

## Syntax

```
TS.FEATURES key startTimestamp endTimestamp
    [CATEGORY <basic|distribution|autocorrelation|trend>,..]
    [FEATURE feature1,feature2,feature3..]
    [TIMEOUT milliseconds]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to analyze.
</details>

<details open>
<summary><code>startTimestamp</code></summary>

Start timestamp for the range of data to analyze (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>endTimestamp</code></summary>

End timestamp for the range of data to analyze (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>CATEGORY</code></summary>

A comma-separated list of feature categories to compute (case-insensitive). Duplicate categories
within the list are rejected. If `CATEGORY` is given more than once, only the last list is used.

Available categories and the features they include:

| Category          | Included features                                                 |
|-------------------|-------------------------------------------------------------------|
| `basic`           | mean, median, variance, variance_sample, minimum, maximum, length |
| `distribution`    | skewness, kurtosis, quantiles at 0.25, 0.5, 0.75, 0.9, 0.95, 0.99 |
| `autocorrelation` | autocorrelation at lags 1, 2, 3                                   |
| `trend`           | linear trend intercept, slope, p-value, r-squared                 |

Example: `CATEGORY basic,trend`
</details>

<details open>
<summary><code>FEATURE</code></summary>

A comma-separated list of individual feature names to compute. Feature names are
case-insensitive; a feature listed twice is computed once. If `FEATURE` is given more than once,
only the last list is used.

**Simple features** (no parameters):

*Basic:* `mean`, `median`, `variance`, `variance_sample`, `standard_deviation`,
`minimum`, `maximum`, `abs_energy`, `absolute_maximum`, `absolute_sum_of_changes`,
`length`, `mean_abs_change`, `mean_change`, `mean_second_derivative_central`,
`root_mean_square`, `sum_values`

*Distribution:* `skewness`, `kurtosis`, `variance_larger_than_std`,
`variation_coefficient`

*Autocorrelation:* `time_reversal_asymmetry` (at lag 1)

*Counting:* `count_above_mean`, `count_below_mean`, `number_crossing_mean`,
`longest_strike_above_mean`, `longest_strike_below_mean`, `first_location_of_maximum`,
`first_location_of_minimum`, `last_location_of_maximum`, `last_location_of_minimum`,
`has_duplicate`, `has_duplicate_max`, `has_duplicate_min`

*Entropy:* `fourier_entropy`

*Trend:* `linear_trend_slope`, `linear_trend_intercept`, `linear_trend_r_squared`,
`linear_trend_p_value`, `augmented_dickey_fuller`

*Change:* `percentage_reoccurring_datapoints`, `percentage_reoccurring_values`,
`ratio_value_number_to_length`, `sum_of_reoccurring_data_points`,
`sum_of_reoccurring_values`

**Parameterized features** use the format `name:value`:

| Feature                   | Syntax                                          | Parameter            | Constraints               |
|---------------------------|-------------------------------------------------|----------------------|---------------------------|
| `quantile`                | `quantile:<q>`                                  | `q` — quantile value | Float between 0.0 and 1.0 |
| `autocorrelation`         | `autocorrelation:<lag>`                         | `lag` — lag value    | Positive integer          |
| `partial_autocorrelation` | `partial_autocorrelation:<lag>` or `pacf:<lag>` | `lag` — lag value    | Integer from 1 to 1000    |

An `autocorrelation` lag at or beyond the number of samples yields null.

Example: `FEATURE mean,median,quantile:0.5,autocorrelation:3`
</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds; defaults to `ts-analysis-timeout`. Features are
computed on a dedicated pool of analysis worker threads (sized by `ts-num-threads`), so they
never stall the server. The deadline counts from when the request is accepted, including time
spent queued behind other analysis work. When it elapses the client receives `TSDB: command
timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` and the request is
abandoned. `0` disables the deadline for this call. Inside `MULTI` or a script, where a client
cannot be blocked, the command runs inline instead.
</details>

## Return

`TS.FEATURES` returns a map of `{feature_name: value}` pairs, sorted by feature name: a map in
RESP3, a flat array of alternating names and values in RESP2. Each key is the canonical feature
name (e.g., `"mean"`, `"quantile_0.5"`, `"autocorrelation_3"`, `"partial_autocorrelation_2"`,
`"time_reversal_asymmetry_1"`). Every value is a double (a bulk string in RESP2), including
counts and flags such as `length` or `has_duplicate`. Features that produce `NaN` — for example
`kurtosis` with fewer than 4 samples — are returned as null.

Several features — including `mean`, `variance`, `variance_sample`, `standard_deviation`,
`sum_values` and `abs_energy` — are computed in single precision (about 7 significant digits).
`skewness` and `kurtosis` use the same formulas as [`TS.STATS`](ts.stats.md#return).

The final feature list is the union of features from `CATEGORY` and `FEATURE`,
with duplicates removed.

Returns an error if:
* The key does not exist or is not a time series
* No samples exist in the specified time range (`TSDB: no samples in the specified time range`)
* Neither `CATEGORY` nor `FEATURE` is specified
  (`TSDB: at least one of CATEGORY or FEATURE must be specified`)
* A category is repeated within the list (`TSDB: duplicate category '<name>'`)
* A category or feature name is unrecognized, or a parameterized feature has an invalid
  parameter. These messages carry a doubled prefix, e.g.
  `TSDB: TSDB forecast error: Unknown feature 'foo'`
* A list is empty (`TSDB: empty category list`, `TSDB: empty feature list`)
* An unknown argument is given (`TSDB: unrecognized argument '<ARG>'`, upper-cased)

## Complexity

`TS.FEATURES` reads the samples in the specified time range and computes each
requested feature. Computation always runs on the analysis pool (see `TIMEOUT`). Most features
are linear in the number of samples; `pacf:<lag>` is O(n × lag).

## Examples

### Compute basic features

```valkey
127.0.0.1:6379> TS.CREATE temp:readings
OK
127.0.0.1:6379> TS.ADD temp:readings 1000 23.5
(integer) 1000
127.0.0.1:6379> TS.ADD temp:readings 2000 24.1
(integer) 2000
127.0.0.1:6379> TS.ADD temp:readings 3000 22.8
(integer) 3000
127.0.0.1:6379> TS.ADD temp:readings 4000 25.0
(integer) 4000
127.0.0.1:6379> TS.ADD temp:readings 5000 23.9
(integer) 5000
127.0.0.1:6379> TS.FEATURES temp:readings - + CATEGORY basic
 1) "length"
 2) "5"
 3) "maximum"
 4) "25"
 5) "mean"
 6) "23.85999870300293"
 7) "median"
 8) "23.9"
 9) "minimum"
10) "22.8"
11) "variance"
12) "0.52239990234375"
13) "variance_sample"
14) "0.6529998779296875"
```

### Compute specific parameterized features

```valkey
127.0.0.1:6379> TS.FEATURES temp:readings - + FEATURE quantile:0.5,autocorrelation:1,skewness
1) "autocorrelation_1"
2) "-0.7195633542062451"
3) "quantile_0.5"
4) "23.9"
5) "skewness"
6) "0.28445709746627007"
```

### Combine categories and features

```valkey
127.0.0.1:6379> TS.FEATURES temp:readings 1000 5000 CATEGORY basic,trent FEATURE quantile:0.95
(error) TSDB: TSDB forecast error: Unknown feature category: trent
127.0.0.1:6379> TS.FEATURES temp:readings 1000 5000 CATEGORY basic,trend FEATURE kurtosis,pacf:2
 1) "kurtosis"
 2) "5.610923800352969"
 3) "length"
 4) "5"
 5) "linear_trend_intercept"
 6) "23.52000000000001"
 7) "linear_trend_p_value"
 8) "0.5412518806380207"
 9) "linear_trend_r_squared"
10) "0.1106431852986195"
11) "linear_trend_slope"
12) "0.16999999999999602"
13) "maximum"
14) "25"
15) "mean"
16) "23.85999870300293"
17) "median"
18) "23.9"
19) "minimum"
20) "22.8"
21) "partial_autocorrelation_2"
22) "-0.26285558136513704"
23) "variance"
24) "0.52239990234375"
25) "variance_sample"
26) "0.6529998779296875"
```

### A feature with no value

```valkey
127.0.0.1:6379> TS.FEATURES temp:readings - + FEATURE mean,autocorrelation:10
1) "autocorrelation_10"
2) (nil)
3) "mean"
4) "23.85999870300293"
```

### Error cases

```valkey
# Non-existent key
127.0.0.1:6379> TS.FEATURES nonexistent - + CATEGORY basic
(error) TSDB: the key does not exist

# Duplicate categories
127.0.0.1:6379> TS.FEATURES temp:readings - + CATEGORY basic,basic
(error) TSDB: duplicate category 'basic'

# Missing CATEGORY and FEATURE
127.0.0.1:6379> TS.FEATURES temp:readings - +
(error) TSDB: at least one of CATEGORY or FEATURE must be specified
```

## ACL Categories

`@read`, `@timeseries`
