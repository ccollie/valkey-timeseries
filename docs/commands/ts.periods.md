# TS.PERIODS

Detect seasonal periods in a time series.

`TS.PERIODS` uses the SAZED ensemble to identify periodic patterns in the data. Each detected
period is returned with metadata about its strength and reliability. At most five periods are
returned, strongest first; a period must fit at least two full cycles in the range.

If the `DOMINANT` option is specified, only the single most significant period
is returned as an integer, or `nil` if no significant period is found.

## Syntax

```
TS.PERIODS key fromTimestamp toTimestamp [MIN_STRENGTH minStrength] [DOMINANT] [TIMEOUT milliseconds]
```

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
<summary><code>MIN_STRENGTH</code></summary>

Minimum seasonal differencing strength (0–1) for a period to be accepted.

* Default: `0.05`.
* Must be between 0 and 1 (`TSDB: MIN_STRENGTH must be between 0 and 1, got <value>`).
* Values > 0.6 indicate strong seasonality; < 0.3 is weak.
* Set to `0` to disable strength filtering entirely.

</details>

<details open>
<summary><code>DOMINANT</code></summary>

When specified, only the dominant period is returned, as an integer: the first entry of the
list `TS.PERIODS` returns without `DOMINANT`. It is also the period that `TS.AUTOFORECAST ...
SEASONALITY AUTO` and `TS.SANITIZE ... POLICY SEASONAL auto` use. Returns `nil` if no
significant period is detected.

Without this option, all detected periods are returned as an array of arrays,
each containing: `[period, power, strength, acf, n_cycles]`.

* `period` — The detected period (integer number of observations per cycle).
* `power` — The detector's confidence in this period.
* `strength` — Seasonal differencing strength (0–1). Values > 0.6 indicate strong seasonality.
* `acf` — Autocorrelation at the detected lag. Positive values confirm a repeating pattern.
* `n_cycles` — Number of complete cycles of this period in the signal.

</details>

<details open>
<summary><code>TIMEOUT milliseconds</code></summary>

Deadline for the command, in milliseconds. Ranges of up to 5,000 samples are computed
inline (inside `MULTI`, a script or a module call, where the client cannot be blocked, up to
1,000,000 samples; a larger range is refused, see the
[overview](../overview.md#running-the-analysis-commands)); larger ranges run on a dedicated pool of analysis worker threads (sized by
`ts-num-threads`) so they never stall the server, and the deadline applies to them. It is
counted from when the request is accepted, so time spent queued behind other analysis work
counts. When it elapses the client receives `TSDB: command timed out before the result was
ready (see TIMEOUT / ts-analysis-timeout)` and the request is abandoned. `0` disables the
deadline for this call.

When omitted, the `ts-analysis-timeout` configuration parameter applies (default 60000 ms;
`0` there means no default deadline).
</details>

## Return

By default, `TS.PERIODS` returns an array where each element is itself an array of
5 elements representing a detected period:

```
[period, power, strength, acf, n_cycles]
```

`period` and `n_cycles` are integers; `power`, `strength` and `acf` are doubles in RESP3 and bulk
strings in RESP2. The array is empty when no period is detected.

When the `DOMINANT` option is used, returns an integer (the dominant period) or `nil`.

Returns an error if:

* The key does not exist or is not a time series
* The range holds fewer than 4 samples (`TSDB: insufficient data for period detection. Need at
  least 4 samples.`)
* `MIN_STRENGTH` is not a number or is outside 0–1
* An unknown argument is given (`TSDB: Unknown argument: <arg>`)

## Examples

### Detect all periods

A series of 56 samples, one per second, repeating the 7-sample pattern
`10 14 16 12 8 6 7`:

```
127.0.0.1:6379> TS.CREATE ts:temperature
OK
127.0.0.1:6379> TS.ADD ts:temperature 1000 10
(integer) 1000
127.0.0.1:6379> TS.ADD ts:temperature 2000 14
(integer) 2000
...
127.0.0.1:6379> TS.ADD ts:temperature 56000 7
(integer) 56000
127.0.0.1:6379> TS.PERIODS ts:temperature - +
1) 1) (integer) 7
   2) "1"
   3) "1"
   4) "1"
   5) (integer) 8
```

### Detect the dominant period only

```
127.0.0.1:6379> TS.PERIODS ts:temperature - + DOMINANT
(integer) 7
```

### No periodic pattern

```
127.0.0.1:6379> TS.PERIODS ts:noise - +
(empty array)
127.0.0.1:6379> TS.PERIODS ts:noise - + DOMINANT
(nil)
```

## ACL Categories

`@read`, `@timeseries`
