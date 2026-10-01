# TS.DECOMPOSE

Decompose a time series into its constituent components: trend, seasonality, and residual.

`TS.DECOMPOSE` uses STL (Seasonal-Trend decomposition using LOESS) for single seasonal periods
or MSTL (Multiple Seasonal-Trend decomposition using LOESS) for multiple seasonal periods.
The seasonal period can be explicitly specified or automatically detected from the data.

## Syntax

```
TS.DECOMPOSE key fromTimestamp toTimestamp
  [SEASONALITY <AUTO | period [period ...]>]
  [TIMEOUT milliseconds]
```

[Examples](#examples)

## Required arguments

<details open>
<summary><code>key</code></summary>

Key name for the time series to decompose.
</details>

<details open>
<summary><code>fromTimestamp</code></summary>

Start timestamp for the range of data to decompose (inclusive).

Use `-` to denote the earliest timestamp in the series.
</details>

<details open>
<summary><code>toTimestamp</code></summary>

End timestamp for the range of data to decompose (inclusive).

Use `+` to denote the latest timestamp in the series.
</details>

## Optional arguments

<details open>
<summary><code>SEASONALITY</code></summary>

Controls how seasonal periods are determined. One of:

* `AUTO` (the default, also used when `SEASONALITY` is omitted) — Detect seasonal periods from
  the data. If none are found the command fails with
  `TSDB: at least one seasonality period is required`.
* `<period> [period ...]` — One to four distinct seasonal periods, each an integer of at least 2,
  in samples. The range must hold at least two full cycles of the largest period
  (`samples >= 2 × period`).
  - A single period uses STL decomposition.
  - Multiple periods use MSTL decomposition; the components are reported in ascending period
    order, whatever order they were given in.

Examples:
- `SEASONALITY 24` — daily seasonality for hourly data
- `SEASONALITY 24 168` — daily and weekly seasonality for hourly data
- `SEASONALITY auto` — automatic detection

</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds. Ranges of up to 2,000 samples are computed
inline (inside `MULTI`, a script or a module call, where the client cannot be blocked, up to
100,000 samples; a larger range is refused, see the
[overview](../overview.md#running-the-analysis-commands)); larger ranges run on the analysis lane (2–8 worker threads, from `ts-num-threads`) so they never stall the server, and the deadline applies to them. It is
counted from when the request is accepted, so time spent queued behind other analysis work
counts. When it elapses the client receives `TSDB: command timed out before the result was
ready (see TIMEOUT / ts-analysis-timeout)` and the request is abandoned. `0` disables the
deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).
</details>

## Return

`TS.DECOMPOSE` returns a map of component names to sample arrays in RESP3, and a flat array of
alternating names and sample arrays in RESP2. Each sample is a `[timestamp, value]` pair; values are
doubles in RESP3 and bulk strings in RESP2. Every component has one sample per sample in the
range, with the original timestamps.

### STL response (single period)

```
1) "original"
2) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
3) "trend"
4) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
5) "seasonal"
6) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
7) "residual"
8) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
```

### MSTL response (multiple periods)

```
1) "original"
2) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
3) "trend"
4) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
5) "seasonal_components"
6) 1) 1) (integer) <period>
      2) 1) 1) (integer) <timestamp>
            2) (double) <value>
         2) ...
   2) ...
7) "residual"
8) 1) 1) (integer) <timestamp>
      2) (double) <value>
   2) ...
```

In the MSTL response each `seasonal_components` entry is a `[period, samples]` pair, in ascending
period order.

The components satisfy the identity `original = trend + seasonal + residual`, where for MSTL
`seasonal` is the sum of the `seasonal_components`.

Returns an error if:

* The key does not exist or is not a time series
* A period is below 2, repeated, or more than four are given (`TSDB: SEASONALITY periods must be
  at least 2`, `TSDB: SEASONALITY periods must be unique`, `TSDB: invalid SEASONALITY periods.
  Expected 1-4 period values or 'auto'`)
* The range is shorter than two cycles of the largest period (`TSDB: insufficient data for STL
  decomposition. Need at least <2 × period> samples, got <n>`, or the same with `MSTL`)
* `AUTO` detects no period — including for an empty or very short range

## Examples

### Decompose with automatic seasonality detection

```
127.0.0.1:6379> TS.DECOMPOSE ts:sensor1 - + SEASONALITY auto
```

### Decompose with a known daily period

```
127.0.0.1:6379> TS.DECOMPOSE ts:sensor1 - + SEASONALITY 24
```

### Decompose with daily and weekly periods (MSTL)

```
127.0.0.1:6379> TS.DECOMPOSE ts:sensor1 - + SEASONALITY 24 168
```

## See also

- `TS.OUTLIERS` — Detect outliers in a time series
- `TS.AUTOFORECAST` — Automatically select and fit the best forecasting model
- `TS.RANGE` — Query a range of samples from a time series

## Complexity

`TS.DECOMPOSE` is O(n × p × i) where n is the number of samples, p is the number of seasonal periods,
and i is the number of inner/outer LOESS iterations.

Ranges of more than 2,000 samples are computed on the analysis lane (see `TIMEOUT`), so the
server is never stalled; the calling client waits for the result. Narrow the time range to bound
the cost on large series.

## ACL Categories

`@read`, `@timeseries`
