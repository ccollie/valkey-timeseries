
```
TS.QUERY_RANGE query start end 
    [STEP step]
    [TIMEOUT timeout]
```

Executes the `query` expression at the given [`start`...`end`] time range with the given `step`:

Params:
* `query` - [MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/) expression.
* `start` - the starting [timestamp](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats) of the time range for `query` evaluation.
* `end` - the ending [timestamp](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#timestamp-formats) of the time range for `query` evaluation.
  If the `end` isn't set, then the `end` is automatically set to the current time.
* `step` - the [interval](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations) between data points, which must be returned from the range query.
  The `query` is executed at `start`, `start+step`, `start+2*step`, ..., `start+N*step` timestamps,
  where `N` is the whole number of steps that fit between `start` and `end`.
  `end` is included only when it equals to `start+N*step`.
  If the `step` isn't set, then it defaults to `5m` (5 minutes).
* `timeout` - optional query timeout. For example, `timeout=5s`. Query is canceled when the timeout is reached.
  By default, the timeout is set to the value of the `ts-max-query-duration` module config flag.

The result of a Range query is a list of [time series](https://docs.victoriametrics.com/victoriametrics/keyconcepts/#time-series) matching the filter in `query` expression. Each returned series 
contains `(timestamp, value)` results for the `query` executed  at `start`, `start+step`, `start+2*step`, ..., `start+N*step` timestamps. 
In other words, Range query is an `Instant Query` executed independently at `start`, `start+step`, ..., `start+N*step` timestamps with 
the only difference that an instant query does not return `ephemeral` samples (see below). Instead, if the database does not contain any 
samples for the requested time and step, it simply returns an empty result.

Assume the following list of samples for the `foo_bar` time series with time intervals between samples
ranging from 1m to 3m.
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

If we plot this data sample on the graph, it will have the following form:

![data samples](data_samples.webp){width="500"}

To get the values of `foo_bar` during the time range from `2022-05-10T07:59:00Z` to `2022-05-10T08:17:00Z`,
we need to issue a range query:

```
TS.QUERY_RANGE foo_bar 2022-05-10T07:59:00.000Z 2022-05-10T08:17:00.000Z STEP 1m
```

```json
{
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "foo_bar"
        },
        "values": [
          [1652169600, "1"],
          [1652169660, "2"],
          [1652169720, "3"],
          [1652169780, "3"],
          [1652169840, "5"],
          [1652169900, "5"],
          [1652169960, "5.5"],
          [1652170020, "5.5"],
          [1652170080, "4"],
          [1652170140, "4"],
          [1652170260, "3.5"],
          [1652170320, "3.25"],
          [1652170380, "3"],
          [1652170440, "2"],
          [1652170500, "1"],
          [1652170560, "4"],
          [1652170620, "4"]
        ]
      }
    ]
  }
}
```

In response, `valkey-timeseries` returns `17` sample-timestamp pairs for the series `foo_bar` at the given time range
from `2022-05-10T07:59:00Z` to `2022-05-10T08:17:00Z`. But if we take a look at the original data sample again, we'll
see that it contains only 13 raw samples. What happens here is that the range query is actually
an [instant query](#instant-query) executed `1 + (start-end)/step` times on the time range from `start` to `end`. If we plot
this request, the graph will look something like the following:

![range query](range_query.webp)

The blue dotted lines in the figure are the moments when the instant query was executed. Since the instant query retains the
ability to return replacements for missing points, the graph contains two types of data points: `real` and `ephemeral`.
`ephemeral` data points always repeat the closest raw sample that occurred before (see red arrow on the pic above).

This behavior of adding ephemeral data points comes from the specifics of the [pull model](#pull-model):

* Metrics are scraped at fixed intervals.
* Scrape may be skipped if the monitoring system is overloaded.
* Scrape may fail due to network issues.

According to these specifics, the range query assumes that if there is a missing raw sample, then it is likely a missed
scrape, so it fills it with the previous raw sample. The same will work for cases when `step` is lower than the actual
interval between samples. In fact, if we set `step=1s` for the same request, we'll get about 1 thousand data points in
response, where most of them are `ephemeral`.

Sometimes, the lookbehind window for locating the datapoint isn't big enough, and the graph will contain a gap. For range
queries, a lookbehind window isn't equal to the `step` parameter. It is calculated as the median of the intervals between
the first 20 raw samples in the requested time range. In this way, `valkey-timeseries` automatically adjusts the lookbehind
window to fill gaps and detect stale series at the same time.

Range queries are mostly used for plotting time series data over specified time ranges. These queries are extremely
useful in the following scenarios:

* Track the state of a metric on the given time interval;
* Correlate changes between multiple metrics on the time interval;
* Observe trends and dynamics of the metric change.
