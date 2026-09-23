# TS.STATIONARITY

Test whether a time series is stationary using statistical hypothesis tests.

Stationarity is a key property for many forecasting and analysis methods. A stationary
series has constant mean, variance, and autocorrelation over time — its statistical
properties do not depend on when you observe it. Non-stationary series (those with
trends, seasonality, or changing variance) often require differencing or transformation
before they can be modeled reliably.

`TS.STATIONARITY` uses the [anofox-forecast](https://docs.rs/anofox-forecast) crate to
run up to two complementary tests:

- **ADF (Augmented Dickey-Fuller)**: Tests the null hypothesis that the series has a
  unit root (i.e., is non-stationary). Rejection of the null implies stationarity.
- **KPSS (Kwiatkowski-Phillips-Schmidt-Shin)**: Tests the null hypothesis that the
  series is stationary. Rejection of the null implies non-stationarity.

Using both tests together (`TEST combined`, the default) provides a more robust
conclusion than either test alone.

## Syntax

```
TS.STATIONARITY key fromTimestamp toTimestamp
    [TEST adf|kpss|combined]
    [LAGS n]
    [TIMEOUT milliseconds]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to test.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to test (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to test (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>TEST</code></summary>

Which test to run (case-insensitive). If omitted, defaults to `combined`. Any other value fails
with `TSDB: invalid TEST value '<value>'. Expected adf, kpss, or combined`.

- `adf` — Augmented Dickey-Fuller test only.
- `kpss` — KPSS test only.
- `combined` — Runs both ADF and KPSS and returns an overall conclusion.

When a single test is specified, the response includes the test statistic, p-value,
number of lags used, whether the series appears stationary according to that test, and
critical values at 1%, 5%, and 10% significance levels.

When `combined` is used, the response includes nested maps for both the ADF and KPSS
results, plus an overall conclusion:

| Conclusion | Meaning |
|---|---|
| `stationary` | ADF rejects the null (non-stationary) AND KPSS fails to reject the null (stationary) |
| `non_stationary` | ADF fails to reject the null (non-stationary) AND KPSS rejects the null (stationary) |
| `inconclusive` | The two tests disagree (both reject or both fail to reject) |

</details>

<details open>
<summary><code>LAGS n</code></summary>

Number of lags to use in the test (integer from 0 to 1000). Only valid when `TEST` is
`adf` or `kpss`. `TEST` and `LAGS` may appear in either order.

- For `adf`: The maximum lag considered; the lag actually used is chosen by AIC from 1 up to
  this value (capped at `n/2 - 1`). If omitted, the maximum is `(n-1)^(1/3)`, where `n` is the
  number of observations.
- For `kpss`: The lags parameter for the HAC (heteroskedasticity and autocorrelation
  consistent) variance estimator, clamped to `1..n/2`. If omitted, `4*(n/100)^0.25` is used.

The `lags` field of the reply reports the value actually used, so it can differ from `LAGS`.

Returns an error if `LAGS` is specified with `TEST combined`, since the combined test
function uses its own internal defaults.
</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds. Ranges of up to 50,000 samples are computed
inline; larger ranges run on a dedicated pool of analysis worker threads (sized by
`ts-num-threads`) so they never stall the server, and the deadline applies to them. It is
counted from when the request is accepted, so time spent queued behind other analysis work
counts. When it elapses the client receives `TSDB: command timed out before the result was
ready (see TIMEOUT / ts-analysis-timeout)` and the request is abandoned. `0` disables the
deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).
</details>

## Return

`TS.STATIONARITY` returns a map in RESP3. In RESP2 each map (including the nested `adf` and
`kpss` maps) is a flat array of alternating field names and values, in the order shown below.
Floats are doubles in RESP3 and bulk strings in RESP2.

### Single test response (`TEST adf` or `TEST kpss`)

A flat map with 9 fields:

| Key            | Type    | Description                          |
|----------------|---------|--------------------------------------|
| `test`         | string  | `"adf"` or `"kpss"`                  |
| `conclusion`   | string  | `"stationary"` or `"non_stationary"` |
| `statistic`    | float   | Test statistic value                 |
| `pValue`       | float   | Approximate p-value                  |
| `lags`         | integer | Number of lags used                  |
| `isStationary` | integer | `1` if stationary, `0` otherwise     |
| `cv1pct`       | float   | Critical value at 1% significance    |
| `cv5pct`       | float   | Critical value at 5% significance    |
| `cv10pct`      | float   | Critical value at 10% significance   |

### Combined test response (`TEST combined`, or default)

A map with 4 top-level fields:

| Key          | Type   | Description                                                                                                     |
|--------------|--------|-----------------------------------------------------------------------------------------------------------------|
| `test`       | string | `"combined"`                                                                                                    |
| `conclusion` | string | `"stationary"`, `"non_stationary"`, or `"inconclusive"`                                                         |
| `adf`        | map    | Nested map with the 7 ADF fields: `statistic`, `pValue`, `lags`, `isStationary`, `cv1pct`, `cv5pct`, `cv10pct`  |
| `kpss`       | map    | Nested map with the 7 KPSS fields: `statistic`, `pValue`, `lags`, `isStationary`, `cv1pct`, `cv5pct`, `cv10pct` |

## Complexity

`TS.STATIONARITY` reads the samples in the range and runs the selected statistical test(s),
O(n × lags) in the number of observations. At least 10 observations are required
(`TSDB: insufficient data for stationarity test. Need at least 10 samples, got <n>`).

A constant series (all values equal) is reported as stationary without running the tests:
`statistic` 0, `pValue` 1, `lags` 0 and all critical values 0. A range containing a NaN or
infinite sample is rejected (`TSDB: the range contains NaN or infinite values; fill or drop them
first (see TS.SANITIZE)`), since the tests are undefined over missing values.

Other errors: the key does not exist or is not a time series; `LAGS` is not an integer, is
negative or exceeds 1000; `LAGS` is combined with `TEST combined`; an unknown argument
(`TSDB: unknown argument '<arg>'`).

## Examples

### Basic combined test

Test whether a time series is stationary using both ADF and KPSS. Here the tests disagree —
ADF cannot reject a unit root while KPSS cannot reject stationarity — so the result is
`inconclusive`:

```valkey
127.0.0.1:6379> TS.CREATE sensor:readings
OK
127.0.0.1:6379> TS.ADD sensor:readings 1000 1.2
(integer) 1000
127.0.0.1:6379> TS.ADD sensor:readings 2000 1.5
(integer) 2000
127.0.0.1:6379> TS.ADD sensor:readings 3000 1.3
(integer) 3000
127.0.0.1:6379> TS.ADD sensor:readings 4000 1.6
(integer) 4000
127.0.0.1:6379> TS.ADD sensor:readings 5000 1.4
(integer) 5000
127.0.0.1:6379> TS.ADD sensor:readings 6000 1.7
(integer) 6000
127.0.0.1:6379> TS.ADD sensor:readings 7000 1.5
(integer) 7000
127.0.0.1:6379> TS.ADD sensor:readings 8000 1.8
(integer) 8000
127.0.0.1:6379> TS.ADD sensor:readings 9000 1.6
(integer) 9000
127.0.0.1:6379> TS.ADD sensor:readings 10000 1.9
(integer) 10000
127.0.0.1:6379> TS.STATIONARITY sensor:readings - +
1) "test"
2) "combined"
3) "conclusion"
4) "inconclusive"
5) "adf"
6)  1) "statistic"
    2) "-2.1908902300206643"
    3) "pValue"
    4) "0.2"
    5) "lags"
    6) (integer) 1
    7) "isStationary"
    8) (integer) 0
    9) "cv1pct"
   10) "-3.43"
   11) "cv5pct"
   12) "-2.86"
   13) "cv10pct"
   14) "-2.57"
7) "kpss"
8)  1) "statistic"
    2) "0.4525210084033617"
    3) "pValue"
    4) "0.054516806722688944"
    5) "lags"
    6) (integer) 2
    7) "isStationary"
    8) (integer) 1
    9) "cv1pct"
   10) "0.739"
   11) "cv5pct"
   12) "0.463"
   13) "cv10pct"
   14) "0.347"
```

### ADF test only

Run only the Augmented Dickey-Fuller test and inspect its critical values (RESP2 output):

```valkey
127.0.0.1:6379> TS.STATIONARITY sensor:readings - + TEST adf
 1) "test"
 2) "adf"
 3) "conclusion"
 4) "non_stationary"
 5) "statistic"
 6) "-2.1908902300206643"
 7) "pValue"
 8) "0.2"
 9) "lags"
10) (integer) 1
11) "isStationary"
12) (integer) 0
13) "cv1pct"
14) "-3.43"
15) "cv5pct"
16) "-2.86"
17) "cv10pct"
18) "-2.57"
```

### KPSS test with custom lags

Override the default lag selection for the KPSS test:

```valkey
127.0.0.1:6379> TS.STATIONARITY sensor:readings - + TEST kpss LAGS 3
 1) "test"
 2) "kpss"
 3) "conclusion"
 4) "stationary"
 5) "statistic"
 6) "0.42569169960474346"
 7) "pValue"
 8) "0.06608116396347266"
 9) "lags"
10) (integer) 3
11) "isStationary"
12) (integer) 1
13) "cv1pct"
14) "0.739"
15) "cv5pct"
16) "0.463"
17) "cv10pct"
18) "0.347"
```

## ACL Categories

`@read`, `@timeseries`
