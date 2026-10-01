# TS.AUTOCORRELATION

Compute autocorrelation-based statistics on a time series.

## Syntax

```
TS.AUTOCORRELATION key startTime endTime lag 
    [PARTIAL | TRA | AGGREGATED mean|var|std|median]
    [TIMEOUT milliseconds]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series.
</details>

<details open>
<summary><code>startTime</code></summary>

Start timestamp for the range query. Use `-` for the earliest sample.
</details>

<details open>
<summary><code>endTime</code></summary>

End timestamp for the range query. Use `+` for the latest sample.
</details>

<details open>
<summary><code>lag</code></summary>

Lag value (non-negative integer) for the autocorrelation computation. The range must hold more
than `lag` samples (more than `2 × lag` with `TRA`). With `PARTIAL` or `AGGREGATED`, whose cost
grows with the lag, it must not exceed 1000.
</details>

## Optional arguments

`PARTIAL`, `TRA` and `AGGREGATED` select the statistic; at most one takes effect — if several are
given, the last one wins.

<details open>
<summary><code>PARTIAL</code></summary>

Returns the partial autocorrelation function (PACF) at the specified lag,
computed using the Durbin-Levinson algorithm.

PACF measures the correlation between observations at lag `k` after removing
the effects of correlations at smaller lags.
</details>

<details open>
<summary><code>TRA</code></summary>

Returns the time reversal asymmetry statistic at the specified lag.

Measures whether the time series looks the same when reversed in time.
A value close to zero indicates time-reversible dynamics. Positive or
negative values suggest directional asymmetry, which is often a signature
of non-linear processes (e.g., skewness in financial returns).
</details>

<details open>
<summary><code>AGGREGATED</code></summary>

Returns an aggregated autocorrelation statistic across lags 1..=lag.

Requires an aggregation function:

* `mean` - Mean of the autocorrelation values across lags 1..=lag
* `var` - Variance of the autocorrelation values across lags 1..=lag
* `std` - Standard deviation of the autocorrelation values across lags 1..=lag
* `median` - Median of the autocorrelation values across lags 1..=lag

The function name is case-insensitive. With `lag` 0 there are no lags to aggregate and the
command returns the NaN error below.
</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds. Ranges of up to 50,000 samples (for `PARTIAL` and
`AGGREGATED`, samples × `(lag + 1)` up to 50,000) are computed inline (inside `MULTI`, a script
or a module call, where the client cannot be blocked, up to 100,000,000 of that measure; a larger
range is refused, see the [overview](../overview.md#running-the-analysis-commands)); anything
larger runs on
a dedicated pool of analysis worker threads (sized by `ts-num-threads`) so it never stalls the
server, and the deadline applies to it. It is counted from when the request is accepted, so
time spent queued behind other analysis work counts. When it elapses the client receives
`TSDB: command timed out before the result was ready (see TIMEOUT / ts-analysis-timeout)` and
the request is abandoned. `0` disables the deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).
</details>

## Return

`TS.AUTOCORRELATION` returns the computed statistic as a double (a bulk string in RESP2).
Returns an error if:

* The key does not exist or is not a time series
* `lag` is not an integer (`TSDB: invalid lag value`) or is negative
  (`TSDB: lag must be a non-negative integer`)
* There is insufficient data for the requested lag (`TSDB: insufficient data for lag <lag>. Need
  at least <lag + 1> samples, got <n>`; for `TRA`, `TSDB: insufficient data for TRA with lag
  <lag>. Need at least <2 × lag + 1> samples, got <n>`)
* `lag` exceeds 1000 with `PARTIAL` or `AGGREGATED`
  (`TSDB: lag must not exceed 1000 with PARTIAL or AGGREGATED`)
* The `AGGREGATED` function is not one of the four above (`TSDB: invalid AGGREGATED function.
  Expected mean, var, std, or median`)
* An option is unknown (`TSDB: unrecognized option`)
* The computation results in NaN, for example when the range contains a NaN sample
  (`TSDB: autocorrelation computation returned NaN`). A constant series is not an error: its
  autocorrelation is reported as `0`.

## Complexity

`TS.AUTOCORRELATION` reads the samples in the range and computes the statistic. The plain ACF
and `TRA` are linear in the number of samples; `PARTIAL` and `AGGREGATED` compute the ACF at
every lag up to `lag`, O(n × lag), and `PARTIAL` adds O(lag²) for the Durbin-Levinson
recursion.

## Examples

### Basic ACF

```valkey
127.0.0.1:6379> TS.CREATE temp:readings
OK
127.0.0.1:6379> TS.ADD temp:readings 1000 20.0
(integer) 1000
127.0.0.1:6379> TS.ADD temp:readings 2000 21.0
(integer) 2000
127.0.0.1:6379> TS.ADD temp:readings 3000 22.0
(integer) 3000
127.0.0.1:6379> TS.ADD temp:readings 4000 23.0
(integer) 4000
127.0.0.1:6379> TS.ADD temp:readings 5000 24.0
(integer) 5000
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 1
"0.5"
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 2
"-0.16666666666666666"
```

### Partial autocorrelation

```valkey
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 1 PARTIAL
"0.5"
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 2 PARTIAL
"-0.5555555555555555"
```

### Time reversal asymmetry

```valkey
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 1 TRA
"1938.6666666666667"
```

### Aggregated autocorrelation

```valkey
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED mean
"-0.2222222222222222"
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED var
"0.5648148148148149"
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED std
"0.7515416254704824"
127.0.0.1:6379> TS.AUTOCORRELATION temp:readings - + 3 AGGREGATED median
"-0.16666666666666666"
```

## ACL Categories

`@read`, `@timeseries`
