```
TS.QUERY query [time]
    [STEP step]
    [TIMEOUT timeout]
```

Executes the `query` expression at the given `time`:

Params:

* `query` - [MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/) expression.
* `time` - optional, [timestamp](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats)
  in millisecond precision to evaluate the `query` at. If omitted, `time` is set to `now()` (current timestamp).
  The `time` param can be specified in [multiple allowed formats](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats).
* `step` - optional [interval](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations)
  for searching for raw samples in the past when executing the `query` (used when a sample is missing at the specified `time`).
  For example, the request `/api/v1/query?query=up&step=1m` looks for the last written raw sample for the metric `up`
  in the `(now()-1m, now()]` interval (the first millisecond is not included). If omitted, `step` is set to `5m` (5 minutes)
  by default.
* `timeout` - optional query timeout. For example, `5s`. The query is canceled when the timeout is reached.
  By default, the timeout is set to the value of `ts-max-query-duration` configuration flag.

The result of an Instant query is a list of [time series](https://docs.victoriametrics.com/victoriametrics/keyconcepts/#time-series)
matching the filter in `query` expression. Each returned series contains exactly one `(timestamp, value)` entry,
where `timestamp` equals to the `time` query arg, while the `value` contains `query` result at the requested `time`.

To understand how instant queries work, let's begin with a data sample:

```
foo_bar 1.00 1652169600000 # 2022-05-10T08:00:00Z
foo_bar 2.00 1652169660000 # 2022-05-10T08:01:00Z
foo_bar 3.00 1652169720000 # 2022-05-10T08:02:00Z
foo_bar 5.00 1652169840000 # 2022-05-10T08:04:00Z, one point missed
foo_bar 5.50 1652169960000 # 2022-05-10T08:06:00Z, one point missed
foo_bar 5.50 1652170020000 # 2022-05-10T08:07:00Z
foo_bar 4.00 1652170080000 # 2022-05-10T08:08:00Z
foo_bar 3.50 1652170260000 # 2022-05-10T08:11:00Z, two points missed
foo_bar 3.25 1652170320000 # 2022-05-10T08:12:00Z
foo_bar 3.00 1652170380000 # 2022-05-10T08:13:00Z
foo_bar 2.00 1652170440000 # 2022-05-10T08:14:00Z
foo_bar 1.00 1652170500000 # 2022-05-10T08:15:00Z
foo_bar 4.00 1652170560000 # 2022-05-10T08:16:00Z
```

The data above contains a list of samples for the `foo_bar` time series with time intervals between samples
ranging from 1m to 3m. If we plot this data sample on the graph, it will have the following form:

![data samples](data_samples.webp)
{width="500"}

To get the value of the `foo_bar` series at some specific moment of time, for example `2022-05-10T08:03:00Z`, we need to issue an **instant query**:

```sh
TS.QUERY foo_bar 2022-05-10T08:03:00.000Z
```

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "foo_bar"
        },
        "value": [
          1652169780, // 2022-05-10T08:03:00Z
          "3"
        ]
      }
    ]
  }
}
```

In response, `valkey-timeseries` returns a single sample-timestamp pair with a value of `3` for the series
`foo_bar` at the given moment in time `2022-05-10T08:03:00Z`. But if we take a look at the original data sample again,
we'll see that there is no raw sample at `2022-05-10T08:03:00Z`. When there is no raw sample at the
requested timestamp, `valkey-timeseries` will try to locate the closest sample before the requested timestamp:

![instant query](instant_query.webp)
{width="500"}

The time range in which it will try to locate a replacement for a missing data sample is equal to `5m`
by default and can be overridden via the `step` parameter.

Instant queries can return multiple time series, but always only one data sample per series. Instant queries are used in
the following scenarios:

* Getting the last recorded value;
* For [rollup functions](https://docs.victoriametrics.com/victoriametrics/metricsql/#rollup-functions) such as `count_over_time`;
* Plotting Stat or Table panels in Grafana.