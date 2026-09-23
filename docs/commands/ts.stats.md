# TS.STATS

Compute per-series statistical metrics for exploratory data analysis.

`TS.STATS` calculates descriptive statistics over a time series to help profile
data before modeling. It returns metrics including length, central tendency (mean,
median), dispersion (standard deviation, min, max, range), value distribution
(zeros, positive/negative counts, nulls, unique values, constancy), plateau
characteristics, and data-quality indicators (skewness, kurtosis,
leading/trailing zeros).

This command is inspired by the `anofox_fcst_ts_stats` function from the
[AnoFox forecast extension](https://anofox.com/docs/forecast/eda).

## Syntax

```
TS.STATS key [fromTimestamp toTimestamp]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to analyze.
</details>

## Optional arguments

<details open>
<summary><code>fromTimestamp toTimestamp</code></summary>

Start and end timestamps for the range of data to analyze (inclusive).

If omitted, stats are computed over the entire series. If only `fromTimestamp` is given,
`toTimestamp` defaults to the latest sample (`+`).

Use `-` to denote the earliest timestamp, and `+` to denote the latest.
</details>

## Return

`TS.STATS` returns a map in RESP3. In RESP2 it is a flat array of alternating field names and
values, sorted by field name.
Floats are doubles in RESP3 and bulk strings in RESP2.

NaN and ±Inf samples are counted in `length` and `n_nans` and otherwise ignored: every other
statistic is computed over the finite values only.

| Field                  | Type    | Description                                                              |
|------------------------|---------|--------------------------------------------------------------------------|
| `length`               | Integer | Number of samples in the range, including NaN/Inf                        |
| `start_timestamp`      | Integer | First timestamp in the range                                             |
| `end_timestamp`        | Integer | Last timestamp in the range                                              |
| `mean`                 | Float   | Average value                                                            |
| `std`                  | Float   | Population standard deviation                                            |
| `min`                  | Float   | Minimum value                                                            |
| `max`                  | Float   | Maximum value                                                            |
| `range`                | Float   | Difference between max and min                                           |
| `median`               | Float   | Median value                                                             |
| `n_nans`               | Integer | Count of NaN/Inf values                                                  |
| `n_zeros`              | Integer | Count of exactly-zero values                                             |
| `n_positive`           | Integer | Count of positive values                                                 |
| `n_negative`           | Integer | Count of negative values                                                 |
| `n_unique_values`      | Integer | Count of distinct finite values                                          |
| `is_constant`          | Integer | 1 if there is exactly one distinct finite value, else 0                  |
| `plateau_size`         | Integer | Longest run of consecutive identical values                              |
| `plateau_size_non_zero`| Integer | Longest run of consecutive identical non-zero values                     |
| `n_zeros_start`        | Integer | Count of leading zeros                                                   |
| `n_zeros_end`          | Integer | Count of trailing zeros                                                  |
| `skewness`             | Float   | Adjusted skewness (see below)                                            |
| `kurtosis`             | Float   | Adjusted excess kurtosis (see below)                                     |

`skewness` is the adjusted Fisher–Pearson coefficient `n/((n−1)(n−2)) · Σ((x−mean)/s)³` and
`kurtosis` the bias-adjusted excess kurtosis `n(n+1)/((n−1)(n−2)(n−3)) · Σ((x−mean)/s)⁴ −
3(n−1)²/((n−2)(n−3))`, where `s` is the sample standard deviation — the estimators Excel's
`SKEW`/`KURT` and pandas report. (`std` in the reply is the population standard deviation.)
`skewness` is null with fewer than 3 values and 0 for a constant series; `kurtosis` is null with
fewer than 4 values or for a constant series. Like every analysis command, `TS.STATS` returns an
undefined statistic as null rather than NaN.

An empty range (or empty series) is not an error: `length` is 0 and every other field is 0.
`TS.STATS` runs inline and takes no `TIMEOUT`. It returns an error if the key does not exist or
is not a time series, or a timestamp cannot be parsed.

## Examples

<details open>
<summary><code>TS.STATS</code> on a time series</summary>

Create a time series and compute its statistics (RESP2 output):

```
127.0.0.1:6379> TS.CREATE ts:temperature
OK
127.0.0.1:6379> TS.ADD ts:temperature 1000 22.5
(integer) 1000
127.0.0.1:6379> TS.ADD ts:temperature 2000 23.1
(integer) 2000
127.0.0.1:6379> TS.ADD ts:temperature 3000 22.8
(integer) 3000
127.0.0.1:6379> TS.ADD ts:temperature 4000 0
(integer) 4000
127.0.0.1:6379> TS.ADD ts:temperature 5000 22.9
(integer) 5000
127.0.0.1:6379> TS.STATS ts:temperature
 1) "end_timestamp"
 2) (integer) 5000
 3) "is_constant"
 4) (integer) 0
 5) "kurtosis"
 6) "4.9909823820233346"
 7) "length"
 8) (integer) 5
 9) "max"
10) "23.1"
11) "mean"
12) "18.26"
13) "median"
14) "22.8"
15) "min"
16) "0"
17) "n_nans"
18) (integer) 0
19) "n_negative"
20) (integer) 0
21) "n_positive"
22) (integer) 4
23) "n_unique_values"
24) (integer) 5
25) "n_zeros"
26) (integer) 1
27) "n_zeros_end"
28) (integer) 0
29) "n_zeros_start"
30) (integer) 0
31) "plateau_size"
32) (integer) 1
33) "plateau_size_non_zero"
34) (integer) 1
35) "range"
36) "23.1"
37) "skewness"
38) "-2.233559777030494"
39) "start_timestamp"
40) (integer) 1000
41) "std"
42) "9.132053438301815"
```

</details>

<details open>
<summary><code>TS.STATS</code> with a timestamp range</summary>

Compute statistics over a specific time window. With only three values, `kurtosis` is null:

```
127.0.0.1:6379> TS.STATS ts:temperature 2000 4000
 1) "end_timestamp"
 2) (integer) 4000
 3) "is_constant"
 4) (integer) 0
 5) "kurtosis"
 6) (nil)
 7) "length"
 8) (integer) 3
 9) "max"
10) "23.1"
11) "mean"
12) "15.300000000000002"
13) "median"
14) "22.8"
15) "min"
16) "0"
17) "n_nans"
18) (integer) 0
19) "n_negative"
20) (integer) 0
21) "n_positive"
22) (integer) 2
23) "n_unique_values"
24) (integer) 3
25) "n_zeros"
26) (integer) 1
27) "n_zeros_end"
28) (integer) 1
29) "n_zeros_start"
30) (integer) 0
31) "plateau_size"
32) (integer) 1
33) "plateau_size_non_zero"
34) (integer) 1
35) "range"
36) "23.1"
37) "skewness"
38) "-1.7310521129921668"
39) "start_timestamp"
40) (integer) 2000
41) "std"
42) "10.819426971887191"
```

</details>

## See also

- `TS.RANGE` – Query raw values from a time series
- `TS.PERIODS` – Detect seasonal periods in a time series
- `TS.TREND` – Fit trend components to a time series

## ACL Categories

`@read`, `@timeseries`
