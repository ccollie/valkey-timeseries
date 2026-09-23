# TS.XCORR

Compute the cross-correlation function (CCF) between two time series.

`TS.XCORR` time-aligns two series via an inner join on matching timestamps (the
same alignment `TS.JOIN` performs by default), then computes the Pearson
correlation coefficient between the aligned value sequences at every lag in
`-maxLag..=maxLag`.

## Syntax

```
TS.XCORR key1 key2 fromTimestamp toTimestamp maxLag [TIMEOUT milliseconds]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key1</code></summary>

Key name for the first time series.
</details>

<details open>
<summary><code>key2</code></summary>

Key name for the second time series. Must differ from `key1`.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to analyze (inclusive).

Use `-` to denote the earliest timestamp.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to analyze (inclusive).

Use `+` to denote the latest timestamp.
</details>

<details open>
<summary><code>maxLag</code></summary>

Maximum lag (integer from 0 to 1000, in samples) to test in either direction.
The command computes correlation at every integer lag in `-maxLag..=maxLag`.
</details>

## Optional arguments

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds; defaults to `ts-analysis-timeout`. When the number
of aligned pairs times `2 × maxLag + 1` exceeds 10,000,000, the correlations are computed on a
dedicated pool of analysis worker threads (sized by `ts-num-threads`) so they never stall the
server, and the deadline applies to them; the timestamp alignment itself always runs inline.
When it elapses the client receives `TSDB: command timed out before the result was ready (see
TIMEOUT / ts-analysis-timeout)`. `0` disables the deadline for this call. Any argument after
`maxLag` other than `TIMEOUT` is rejected with `TSDB: invalid argument`.
</details>

## Lag convention

For lag `h`, `key1[i]` is compared against `key2[i + h]` over the timestamp-aligned
sample sequence:

* `h > 0` — `key1` at time `t` is compared to `key2` at time `t + h`. A strong
  positive correlation here means **`key1` leads `key2`** by `h` steps.
* `h < 0` — `key2` at time `t` is compared to `key1` at time `t + |h|`. A strong
  positive correlation here means **`key2` leads `key1`** by `|h|` steps.
* `h == 0` — contemporaneous correlation, no lead/lag.

## Alignment

Only samples with **exactly matching timestamps** in both series contribute to
the computation — the same semantics as `TS.JOIN`'s default `INNER` join. Series
sampled on different or irregular grids will have few or no aligned points; use
`TS.FILLGAPS` / resampling via `TS.RANGE ... AGGREGATION` beforehand to put both
series on a common grid if needed.

## Return

`TS.XCORR` returns a map in RESP3; in RESP2 the same fields arrive as a flat array of
alternating names and values, in the order below. Doubles are bulk strings in RESP2.

| Key                | Type             | Description                                                         |
|--------------------|------------------|---------------------------------------------------------------------|
| `lags`             | array of integer | The tested lags, `-maxLag..=maxLag`, in order                       |
| `values`           | array of double  | Correlation coefficient at each corresponding lag, range `[-1, 1]`  |
| `peak_lag`         | integer          | The lag with the largest absolute correlation                       |
| `peak_correlation` | double           | The (signed) correlation value at `peak_lag`                        |
| `n`                | integer          | Number of timestamp-aligned sample pairs used                       |

When several lags tie for the largest absolute correlation, `peak_lag` is the lowest of them. A
lag whose correlation is undefined (a NaN sample in the overlap) is reported as null and is
not chosen as the peak; if every lag is undefined, `peak_correlation` is null. A constant series correlates as `0` at
every lag.

Returns an error if:
* Either key does not exist or is not a time series
* `key1` and `key2` are identical (`TSDB: duplicate join keys`)
* `maxLag` is not an integer, is negative, or exceeds 1000 (`TSDB: MAXLAG must not exceed 1000`)
* Fewer than `maxLag + 2` timestamp-aligned sample pairs exist in the range

## Complexity

`TS.XCORR` is O(n × maxLag), where `n` is the number of timestamp-aligned sample
pairs in the range.

## ACL Categories

`@read`, `@timeseries`

## Examples

### Detect a 2-step lead of one sensor over another

Two series of 480 samples on the same timestamps, where `sensor:downstream` repeats
`sensor:upstream` (plus noise) two samples later:

```valkey
127.0.0.1:6379> TS.XCORR sensor:upstream sensor:downstream - + 5
 1) "lags"
 2)  1) (integer) -5
     2) (integer) -4
     3) (integer) -3
     4) (integer) -2
     5) (integer) -1
     6) (integer) 0
     7) (integer) 1
     8) (integer) 2
     9) (integer) 3
    10) (integer) 4
    11) (integer) 5
 3) "values"
 4)  1) "0.0319810268535779"
     2) "0.009782531825730863"
     3) "-0.008677073186086012"
     4) "0.060952581814017644"
     5) "0.058564697222020325"
     6) "-0.04799861471542607"
     7) "0.023702292969867603"
     8) "0.9604875608215362"
     9) "0.012303861512756436"
    10) "-0.042095504258574404"
    11) "0.06686795186145182"
 5) "peak_lag"
 6) (integer) 2
 7) "peak_correlation"
 8) "0.9604875608215362"
 9) "n"
10) (integer) 480
```

Here `peak_lag = 2` with a strong positive correlation means `sensor:upstream`
leads `sensor:downstream` by 2 samples.

### Contemporaneous correlation only

With `sensor:a` holding `0, 1, …, 11` and `sensor:b` their squares, on the same 12 timestamps:

```valkey
127.0.0.1:6379> TS.XCORR sensor:a sensor:b - + 0
 1) "lags"
 2) 1) (integer) 0
 3) "values"
 4) 1) "0.9635292999382342"
 5) "peak_lag"
 6) (integer) 0
 7) "peak_correlation"
 8) "0.9635292999382342"
 9) "n"
10) (integer) 12
```

### Error: identical keys

```valkey
127.0.0.1:6379> TS.XCORR sensor:a sensor:a - + 10
(error) TSDB: duplicate join keys
```

### Error: insufficient aligned data

```valkey
127.0.0.1:6379> TS.XCORR sensor:a sensor:b - + 100
(error) TSDB: insufficient aligned samples for MAXLAG 100. Need at least 102 timestamp-aligned samples between the two series, got 12
```

## See also

`TS.JOIN` | `TS.AUTOCORRELATION` | `TS.PERIODS`
