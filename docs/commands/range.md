# TS.RANGE

Return a range of samples from a time-series.

---

## Syntax

```bash
TS.RANGE key fromTimestamp toTimestamp
  [LATEST]
  [FILTER_BY_TS ts...]
  [FILTER_BY_VALUE min max]
  [COUNT count]
  [
      [ALIGN align] AGGREGATION aggregator bucketDuration [CONDITION operator value] [BUCKETTIMESTAMP bt] [EMPTY]
  ]
```

---

## Required Arguments

<details open><summary><code>key</code></summary> 

is the key name of the time series.

</details>


<details open><summary><code>fromTimestamp</code></summary>

`fromTimestamp` is the first timestamp or relative delta from the current time of the request range.

</details>

<details open><summary><code>toTimestamp</code></summary>

`toTimestamp` is the last timestamp of the requested range, or a relative delta from `fromTimestamp`

</details>

### Timestamp formats

The `fromTimestamp` and `toTimestamp` arguments accept the following formats:

#### Absolute Timestamps

**Numeric timestamp** (milliseconds since Unix epoch)

```bash
TS.RANGE temperature:office 1700000000000 1700003600000
```

#### Relative Timestamps

| Symbol | Meaning                                                                  | Example                             |
|--------|--------------------------------------------------------------------------|-------------------------------------|
| `-`    | **Earliest** — start of the time series                                  | `TS.RANGE temperature:office - +`   |
| `+`    | **Latest** — most recent data point                                      | `TS.RANGE temperature:office -1h +` |
| `*`    | **Current time** — equivalent to the current server time in milliseconds | `TS.RANGE temperature:office - *`   |

#### Relative Time Offsets

Time offsets relative to the current time.

Supported formats:

- **Integer milliseconds:** `60000`, `3600000`
- **Duration strings:** `5s`, `1m`, `2h`, `1d`, `1w`, etc.
  - `s` = seconds
  - `m` = minutes
  - `h` = hours
  - `d` = days
  - `w` = weeks, specified as 7 days

Examples:

```bash
# Last hour of data
TS.RANGE temperature:office -1h *

# Last 5 minutes
TS.RANGE temperature:office -5m *

# From earliest to 2 hours ago
TS.RANGE temperature:office - -2h
```

---

### Optional Arguments

#### Range shaping & limits

| Option   | Arguments | Description                                                                                      |
|----------|-----------|--------------------------------------------------------------------------------------------------|
| `LATEST` | (none)    | Return the current value of the latest "unclosed" bucket, if it exists.                          |
| `COUNT`  | `count`   | Maximum number of returned samples (or buckets when aggregated). Must be a non-negative integer. |

#### Filtering

| Option            | Arguments | Description                                                                                                                                                                      |
|-------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FILTER_BY_TS`    | `ts...`   | Only return samples whose timestamps match one of the provided timestamps. Supports up to **128** timestamps;                                                                    |
| `FILTER_BY_VALUE` | `min max` | Only return samples with values in `[min, max]`. `min`/`max` support numeric values (and number-with-unit parsing, if enabled elsewhere in your parser); `max` must be `>= min`. |

#### Aggregation / downsampling

| Option            | Arguments                   | Description                                                                                                       |
|-------------------|-----------------------------|-------------------------------------------------------------------------------------------------------------------|
| `AGGREGATION`     | `aggregator bucketDuration` | Downsample into fixed time buckets of size `bucketDuration` and apply `aggregator` per bucket.                    |
| `ALIGN`           | `align`                     | Bucket alignment anchor.                                                                                          |
| `BUCKETTIMESTAMP` | `bt`                        | Controls the timestamp emitted for each bucket. Default: `start`.                                                 |
| `EMPTY`           | (none)                      | Include empty buckets (buckets with no samples).                                                                  |
| `CONDITION`       | `operator value`            | Provides a comparison filter used by conditional aggregators (e.g., `countif`, `sumif`, `share`, `all/any/none`). |

##### `bucketDuration` format

`bucketDuration` is a **duration**. It can be:

- An integer (milliseconds), e.g. `60000`
- A duration string (e.g., `5s`, `1m`, etc.)

##### Alignment restrictions

When aggregation is used:

- If `fromTimestamp` is `-` (earliest), you **cannot** use `ALIGN start`.
- If `toTimestamp` is `+` (latest), you **cannot** use `ALIGN end`.

---

## Available aggregators

These are the supported `aggregator` values for `AGGREGATION`:

| Aggregator | Description                                                                                            |
|------------|--------------------------------------------------------------------------------------------------------|
| `avg`      | Average of values in the bucket.                                                                       |
| `sum`      | Sum of values in the bucket. If `EMPTY` is enabled, empty buckets yield `0`.                           |
| `count`    | Number of samples in the bucket. If `EMPTY` is enabled, empty buckets yield `0`.                       |
| `min`      | Minimum value in the bucket.                                                                           |
| `max`      | Maximum value in the bucket.                                                                           |
| `range`    | `max - min` within the bucket.                                                                         |
| `first`    | First value encountered in the bucket.                                                                 |
| `last`     | Last value encountered in the bucket.                                                                  |
| `var.p`    | Population variance.                                                                                   |
| `var.s`    | Sample variance.                                                                                       |
| `std.p`    | Population standard deviation.                                                                         |
| `std.s`    | Sample standard deviation.                                                                             |
| `increase` | Counter increase over the bucket (handles counter resets/backward jumps).                              |
| `rate`     | Counter rate per second over the bucket window (`increase / window_seconds`).                          |
| `irate`    | Instantaneous per-second rate from the last two samples in the bucket/window (handles counter resets). |
| `countif`  | Count of samples matching `CONDITION operator value`.                                                  |
| `sumif`    | Sum of sample values matching `CONDITION operator value`.                                              |
| `share`    | Fraction of samples matching `CONDITION` (range `[0..1]`), or empty when no samples.                   |
| `all`      | `1.0` if all samples match `CONDITION`, else `0.0`.                                                    |
| `any`      | `1.0` if any sample matches `CONDITION`, else `0.0`.                                                   |
| `none`     | `1.0` if no samples match `CONDITION`, else `0.0`.                                                     |

---

## Return value

An array of samples:

```plain text
[
  [timestamp, value],
  [timestamp, value],
  ...
]
```

- Without `AGGREGATION`: each entry is a raw sample.
- With `AGGREGATION`: each entry represents a bucket; the emitted timestamp depends on `BUCKETTIMESTAMP` (default
  `start`).

---

## Examples

### Raw range query

```bash
TS.RANGE temperature:office 1700000000000 1700003600000
```

### Limit results

```bash
TS.RANGE temperature:office 1700000000000 1700003600000 COUNT 100
```

### Filter by value

```bash
TS.RANGE temperature:office 1700000000000 1700003600000 FILTER_BY_VALUE 20 25
```

### Downsample to 1-minute average buckets

```bash
TS.RANGE temperature:office 1700000000000 1700003600000 AGGREGATION avg 60000
```

### Aggregation with alignment + empty buckets

```bash
TS.RANGE temperature:office 1700000000000 1700003600000 ALIGN 1700000000000 AGGREGATION sum 60000 EMPTY
```

### Conditional aggregation (`countif`) with `CONDITION`

Count how many times per 5-minute interval that cpu-utilization exceeded 90%:

```bash
TS.RANGE cpu:utilization 1700000000000 1700003600000 AGGREGATION countif 300000 CONDITION > 0.90
```

### Share of samples matching a condition

Over the past hour, what percentage of per minute error rates exceeded 5%:

```bash
TS.RANGE errors:rate -1h * AGGREGATION share 60000 CONDITION > 0.05
```
