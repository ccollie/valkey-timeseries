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
values; **the field order is not fixed** and can differ between calls, so look fields up by name.
Floats are doubles in RESP3 and bulk strings in RESP2.

NaN and ±Inf samples are counted in `length` and `n_nans` and otherwise ignored: every other
statistic is computed over the finite values only.

| Field                  | Type    | Description                                                              |
|------------------------|---------|--------------------------------------------------------------------------|
| `length`               | Integer | Number of samples in the range, including NaN/Inf                        |
| `start_timestamp`      | Integer | First timestamp in the range                                             |
| `end_timestamp`        | Integer | Last timestamp in the range                                              |
| `mean`                 | Float   | Average value, computed in single precision (about 7 significant digits) |
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

`skewness` is `n/((n−1)(n−2)) · Σ((x−mean)/σ)³` and `kurtosis` is
`n(n+1)/((n−1)(n−2)(n−3)) · Σ((x−mean)/σ)⁴ − 3(n−1)²/((n−2)(n−3))`, where `σ` is the population
standard deviation (`std`). `skewness` is NaN with fewer than 3 values and 0 for a constant
series; `kurtosis` is NaN with fewer than 4 values or for a constant series. This differs from
the usual sample estimators, which use the sample standard deviation, so the values are larger
in magnitude, markedly so for short ranges.

An empty range (or empty series) is not an error: `length` is 0 and every other field is 0.
`TS.STATS` runs inline and takes no `TIMEOUT`. It returns an error if the key does not exist or
is not a time series, or a timestamp cannot be parsed.

## Examples

<details open>
<summary><code>TS.STATS</code> on a time series</summary>

Create a time series and compute its statistics (RESP2 output; field order varies):

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
 1) "n_zeros"
 2) (integer) 1
 3) "plateau_size_non_zero"
 4) (integer) 1
 5) "start_timestamp"
 6) (integer) 1000
 7) "n_negative"
 8) (integer) 0
 9) "n_zeros_end"
10) (integer) 0
11) "min"
12) "0"
13) "length"
14) (integer) 5
15) "range"
16) "23.1"
17) "plateau_size"
18) (integer) 1
19) "median"
20) "22.8"
21) "mean"
22) "18.259998321533203"
23) "n_positive"
24) (integer) 4
25) "n_unique_values"
26) (integer) 5
27) "n_zeros_start"
28) (integer) 0
29) "is_constant"
30) (integer) 0
31) "std"
32) "9.13205728271805"
33) "skewness"
34) "-3.12148959227386"
35) "kurtosis"
36) "12.298368906273137"
37) "end_timestamp"
38) (integer) 5000
39) "n_nans"
40) (integer) 0
41) "max"
42) "23.1"
```

</details>

<details open>
<summary><code>TS.STATS</code> with a timestamp range</summary>

Compute statistics over a specific time window. With only three values, `kurtosis` is NaN:

```
127.0.0.1:6379> TS.STATS ts:temperature 2000 4000
 1) "mean"
 2) "15.300000190734863"
 3) "kurtosis"
 4) "nan"
 5) "n_positive"
 6) (integer) 2
 7) "n_unique_values"
 8) (integer) 3
 9) "plateau_size"
10) (integer) 1
11) "n_zeros_start"
12) (integer) 0
13) "skewness"
14) "-3.1801467555253087"
15) "start_timestamp"
16) (integer) 2000
17) "end_timestamp"
18) (integer) 4000
19) "max"
20) "23.1"
21) "is_constant"
22) (integer) 0
23) "std"
24) "10.819426153905052"
25) "plateau_size_non_zero"
26) (integer) 1
27) "n_zeros_end"
28) (integer) 1
29) "length"
30) (integer) 3
31) "min"
32) "0"
33) "n_zeros"
34) (integer) 1
35) "n_negative"
36) (integer) 0
37) "n_nans"
38) (integer) 0
39) "range"
40) "23.1"
41) "median"
42) "22.8"
```

</details>

## See also

- `TS.RANGE` – Query raw values from a time series
- `TS.PERIODS` – Detect seasonal periods in a time series
- `TS.TREND` – Fit trend components to a time series

## ACL Categories

`@read`, `@timeseries`
