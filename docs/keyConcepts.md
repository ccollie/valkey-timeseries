## Data model

### What is a metric

Simply put, a `metric` is a numeric measure or observation of something.

The most common use-cases for metrics are:

- check how the system behaves at the particular time period;
- correlate behavior changes to other measurements;
- observe or forecast trends;
- trigger events (alerts) if the metric exceeds a threshold.

### Structure of a metric

Let's start with an example. To track how many requests our application serves, we'll define a metric with the
name `requests_total`.

You can be more specific here by saying `requests_success_total` (for only successful requests)
or `request_errors_total` (for requests which failed). Choosing a metric name is crucial and supposed to clarify
what is actually measured to every person who reads it, just like **variable names** in programming.

#### Labels

Every metric can contain additional meta-information in the form of label-value pairs:

```
requests_total{path="/", code="200"} 
requests_total{path="/", code="403"} 
```

The meta-information - a set of `labels` in curly braces - gives us a context for which `path` and with what `code`
the `request` was served. Label-value pairs are always of a `string` type. VictoriaMetrics data model is schemaless,
which means there is no need to define metric names or their labels in advance. The user is free to add or change ingested
metrics anytime.

Actually, the metric name is also a label with a special name `__name__`. 
The `__name__` key could be omitted {{% available_from "v1.111.0" %}} for simplicity. So the following series are identical:

```
requests_total{path="/", code="200"} 
{__name__="requests_total", path="/", code="200"} 
{"requests_total", path="/", code="200"} 
```

`valkey-timeseries` supports enforcing of label filters for [query API](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#prometheus-querying-api-enhancements) to emulate data isolation. 

#### Time series

A combination of a metric name and its labels defines a `time series`. For example,
`requests_total{path="/", code="200"}` and `requests_total{path="/", code="403"}`
are two different time series because they have different values for `code` label.

The number of unique time series has an impact on database resource usage.
See [what is an active time series](https://docs.victoriametrics.com/victoriametrics/faq/#what-is-an-active-time-series) and [what is high churn rate](https://docs.victoriametrics.com/victoriametrics/faq/#what-is-high-churn-rate) docs for details.

#### Cardinality

The number of unique [time series](#time-series) is named `cardinality`. Too big number of unique time series is named `high cardinality`.
High cardinality may result in increased resource usage at VictoriaMetrics.
See [these docs](https://docs.victoriametrics.com/victoriametrics/faq/#what-is-high-cardinality) for more details.

#### Raw samples

Every unique time series may consist of an arbitrary number of `(value, timestamp)` data points (aka `raw samples`) sorted by `timestamp`.
`valkey-timeseries` stores all the `values` as [float64](https://en.wikipedia.org/wiki/Double-precision_floating-point_format) with compression applied.

The `timestamp` is a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) with millisecond precision.

Below is an example of a single raw sample in [Prometheus text exposition format](https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#text-based-format):

```
requests_total{path="/", code="200"} 123 4567890
```

- The `requests_total{path="/", code="200"}` identifies the associated time series for the given sample.
- The `123` is a sample value.
- The `4567890` is the timestamp for the sample.

#### Time series resolution

Resolution is the minimum interval between [raw samples](https://docs.victoriametrics.com/victoriametrics/keyconcepts/#raw-samples)
of the [time series](https://docs.victoriametrics.com/victoriametrics/keyconcepts/#time-series). Consider the following example:
```
----------------------------------------------------------------------
|              <time series>                 | <value> | <timestamp> |
| requests_total{path="/health", code="200"} |    1    |  1676297640 |
| requests_total{path="/health", code="200"} |    2    |  1676297670 |
| requests_total{path="/health", code="200"} |    3    |  1676297700 |
| requests_total{path="/health", code="200"} |    4    |  1676297730 |
....
```
Here we have a time series `requests_total{path="/health", code="200"}` which has a value update each `30s`.
This means, its resolution is also a `30s`.

Generally, resolution is an interval between samples timestamps and is controlled by a client (metrics collector).

Try to keep time series resolution consistent, since some [MetricsQL](#metricsql) functions may expect it to be so.


### Types of metrics

Internally, `valkey-timeseries` does not have the notion of a metric type. The concept of a metric
type exists specifically to help users to understand how the metric was measured. There are four common metric types.

#### Counter

A Counter is a metric that counts some events. Its value increases or stays the same over time.
It cannot decrease in the general case. The only exception is e.g. `counter reset`,
when the metric resets to zero. A `counter reset` can occur when the service, which exposes the counter, restarts.
So, the `counter` metric shows the number of observed events since the service start.

In programming, a `counter` is a variable that you **increment** each time something happens.

![counter](counter.webp)

`vm_http_requests_total` is a typical example of a counter. The interpretation of the graph
above is that time series `vm_http_requests_total{instance="localhost:8428", job="victoriametrics", path="api/v1/query_range"}`
was rapidly changing from 1:38 pm to 1:39 pm, then there were no changes until 1:41 pm.

A Counter is used for measuring the number of events, like the number of requests, errors, logs, messages, etc.
The most common [MetricsQL](#metricsql) functions used with counters are:

* [rate](https://docs.victoriametrics.com/victoriametrics/metricsql/#rate)—calculates the average per-second speed of metric change.
  For example, `rate(requests_total)` shows how many requests are served per second on average;
* [increase](https://docs.victoriametrics.com/victoriametrics/metricsql/#increase)—calculates the growth of a metric on the given time period specified in square brackets.
  For example, `increase(requests_total[1h])` shows the number of requests served over the last hour.

It is OK to have fractional counters. For example, `request_duration_seconds_sum` counter may sum the durations of all the requests.
Every duration may have a fractional value in seconds, e.g. `0.5` of a second. So the cumulative sum of all the request durations
may be fractional as well.

It is recommended to put `_total`, `_sum` or `_count` suffix to `counter` metric names, so such metrics can be easily differentiated
by humans from other metric types.

#### Gauge

A Gauge is used for measuring a value that can go up and down:

![gauge](gauge.webp)

The metric `process_resident_memory_anon_bytes` on the graph shows the memory usage of the application at every given time.
It is changing frequently, going up and down showing how the process allocates and frees the memory.
In programming, a `gauge` is a variable to which you **set** a specific value as it changes.

A Gauge is used in the following scenarios:

* measuring temperature, memory usage, disk usage etc;
* storing the state of some process. For example, gauge `config_reloaded_successful` can be set to `1` if everything is
  good, and to `0` if configuration failed to reload;
* storing the timestamp when the event happened. For example, `config_last_reload_success_timestamp_seconds`
  can store the timestamp of the last successful configuration reload.

The most common functions used with gauges are [aggregation functions](#aggregation-and-grouping-functions)
and [rollup functions](https://docs.victoriametrics.com/victoriametrics/metricsql/#rollup-functions).

#### Histogram

A Histogram is a set of [counter](#counter) metrics with different `vmrange` or `le` labels.
The `vmrange` or `le` labels define measurement boundaries of a particular bucket.
When the observed measurement hits a particular bucket, then the corresponding counter is incremented.

Histogram buckets usually have `_bucket` suffix in their names.
For example, VictoriaMetrics tracks the distribution of rows processed per query with the `vm_rows_read_per_query` histogram.
The exposition format for this histogram has the following form:

```
vm_rows_read_per_query_bucket{vmrange="4.084e+02...4.642e+02"} 2
vm_rows_read_per_query_bucket{vmrange="5.275e+02...5.995e+02"} 1
vm_rows_read_per_query_bucket{vmrange="8.799e+02...1.000e+03"} 1
vm_rows_read_per_query_bucket{vmrange="1.468e+03...1.668e+03"} 3
vm_rows_read_per_query_bucket{vmrange="1.896e+03...2.154e+03"} 4
vm_rows_read_per_query_sum 15582
vm_rows_read_per_query_count 11
```

The `vm_rows_read_per_query_bucket{vmrange="4.084e+02...4.642e+02"} 2` line means
that there were 2 queries with the number of rows in the range `(408.4 - 464.2]`
since the last VictoriaMetrics start.

The counters ending with `_bucket` suffix allow estimating arbitrary percentile
for the observed measurement with the help of [histogram_quantile](https://docs.victoriametrics.com/victoriametrics/metricsql/#histogram_quantile)
function. For example, the following query returns the estimated 99th percentile
on the number of rows read per each query during the last hour (see `1h` in square brackets):

```metricsql
histogram_quantile(0.99, sum(increase(vm_rows_read_per_query_bucket[1h])) by (vmrange))
```

This query works in the following way:

1. The `increase(vm_rows_read_per_query_bucket[1h])` calculates per-bucket per-instance
   number of events over the last hour.
1. The `sum(...) by (vmrange)` calculates per-bucket events by summing per-instance buckets
   with the same `vmrange` values.
1. The `histogram_quantile(0.99, ...)` calculates 99th percentile over `vmrange` buckets returned at step 2.

Histogram metric type exposes two additional counters ending with `_sum` and `_count` suffixes:

- the `vm_rows_read_per_query_sum` is a sum of all the observed measurements,
  e.g., the sum of rows served by all the queries since the last VictoriaMetrics start.

- the `vm_rows_read_per_query_count` is the total number of observed events,
  e.g., the total number of observed queries since the last VictoriaMetrics start.

These counters allow calculating the average measurement value on a particular lookbehind window.
For example, the following query calculates the average number of rows read per query
during the last 5 minutes (see `5m` in square brackets):

```metricsql
increase(vm_rows_read_per_query_sum[5m]) / increase(vm_rows_read_per_query_count[5m])
```

![histogram](histogram.webp)

Grafana doesn't understand buckets with `vmrange` labels, so the [prometheus_buckets](https://docs.victoriametrics.com/victoriametrics/metricsql/#prometheus_buckets)
function must be used for converting buckets with `vmrange` labels to buckets with `le` labels before building heatmaps in Grafana.

Histograms are usually used for measuring the distribution of latency, sizes of elements (batch size, for example) etc. There are two
implementations of a histogram supported by `valkey-timeseries`:

1. [Prometheus histogram](https://prometheus.io/docs/practices/histograms/). The canonical histogram implementation is supported by most of
   the [client libraries for metrics instrumentation](https://prometheus.io/docs/instrumenting/clientlibs/). Prometheus histogram requires a user to define ranges (`buckets`) statically.
1. [VictoriaMetrics histogram](https://valyala.medium.com/improving-histogram-usability-for-prometheus-and-grafana-bc7e5df0e350) .
   valkey-timeseries histograms automatically handle bucket boundaries, so users don't need to think about them.

We recommend reading the following articles before you start using histograms:

1. [Prometheus histogram](https://prometheus.io/docs/concepts/metric_types/#histogram)
1. [Histograms and summaries](https://prometheus.io/docs/practices/histograms/)
1. [How does a Prometheus Histogram work?](https://www.robustperception.io/how-does-a-prometheus-histogram-work)
1. [Improving histogram usability for Prometheus and Grafana](https://valyala.medium.com/improving-histogram-usability-for-prometheus-and-grafana-bc7e5df0e350)

#### Summary

Summary metric type is quite similar to [histogram](#histogram) and is used for
[quantiles](https://prometheus.io/docs/practices/histograms/#quantiles) calculations. The main difference
is that calculations are made on the client-side, so the metrics exposition format already contains pre-defined
quantiles:

```
go_gc_duration_seconds{quantile="0"} 0
go_gc_duration_seconds{quantile="0.25"} 0
go_gc_duration_seconds{quantile="0.5"} 0
go_gc_duration_seconds{quantile="0.75"} 8.0696e-05
go_gc_duration_seconds{quantile="1"} 0.001222168
go_gc_duration_seconds_sum 0.015077078
go_gc_duration_seconds_count 83
```

The visualization of summaries is pretty straightforward:

![summary](summary.webp)

Such an approach makes summaries easier to use but also puts significant limitations compared to [histograms](#histogram):

- It is impossible to calculate quantile over multiple summary metrics, e.g. `sum(go_gc_duration_seconds{quantile="0.75"})`,
  `avg(go_gc_duration_seconds{quantile="0.75"})` or `max(go_gc_duration_seconds{quantile="0.75"})`
  won't return the expected 75th percentile over `go_gc_duration_seconds` metrics collected from multiple instances
  of the application. See [this article](https://latencytipoftheday.blogspot.de/2014/06/latencytipoftheday-you-cant-average.html) for details.

- It is impossible to calculate quantiles other than the already pre-calculated quantiles.

- It is impossible to calculate quantiles for measurements collected over an arbitrary time range. Usually, `summary`
  quantiles are calculated over a fixed time range such as the last 5 minutes.

Summaries are usually used for tracking the pre-defined percentiles for latency, sizes of elements (batch size, for example) etc.

#### Naming

We recommend following [Prometheus naming convention for metrics](https://prometheus.io/docs/practices/naming/). There
are no strict restrictions, so any metric name and labels are accepted by `valkey-timeseries`.
But the convention helps to keep names meaningful, descriptive, and clear to other people.
Following convention is a good practice.

#### Labels

Every measurement can contain an arbitrary number of `key="value"` labels. A good practice is to keep this number limited.
Otherwise, it would be challenging to deal with measurements containing a large number of labels.

Every label can contain an arbitrary string value. It is best to use short and meaningful label values to
describe the attribute of the metric, not to tell the story about it. For example, label-value pair
`environment="prod"` is ok, but `log_message="long log message with a lot of details..."` is not. By default,
label value sizes are capped at 4KiB. This limit can be changed via `-maxLabelValueLen` command-line flag.

It is crucial to keep under control the number of unique label values, since every unique label value
leads to a new [time series](#time-series). Try to avoid using volatile label values such as session ID or query ID to
avoid excessive resource usage and database slowdown.

## Adding Data
todo

## Query data

VictoriaMetrics provides an [HTTP API](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#prometheus-querying-api-usage) for serving read queries. 

The API consists of two main handlers for serving [instant queries](#instant-query) and [range queries](#range-query).

### MetricsQL

`valkey-timeseries` provides a special query language for executing read queries - [MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/).
It is a [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics)-like query language with a powerful set of functions and features for working specifically with 
time series data. MetricsQL is backward-compatible with PromQL, so it shares most of the query concepts. The basic concepts 
for PromQL and MetricsQL are described [here](https://valyala.medium.com/promql-tutorial-for-beginners-9ab455142085).

#### Filtering

In sections [instant query](#instant-query) and [range query](#range-query) we've already used MetricsQL to get data for
metric `foo_bar`. It is as simple as just writing a metric name in the query:

```metricsql
foo_bar
```

A single metric name may correspond to multiple time series with distinct label sets. For example:

```metricsql
requests_total{path="/", code="200"} 
requests_total{path="/", code="403"} 
```

To select only time series with a specific label value, specify the matching filter in curly braces:

```metricsql
requests_total{code="200"} 
```

The query above returns all time series with the name `requests_total` and label `code="200"`. We use the operator `=` to
match label value. For negative match use `!=` operator. Filters also support positive regex matching via `=~`
and negative regex matching via `!~`:

```metricsql
requests_total{code=~"2.*"}
```

Filters can also be combined:

```metricsql
requests_total{code=~"200", path="/home"}
```

The query above returns all time series with `requests_total` name, which simultaneously have labels `code="200"` and `path="/home"`.

#### Filtering by name

Sometimes it is required to return all the time series for multiple metric names. As was mentioned in
the [data model section](#data-model), the metric name is just an ordinary label with a special name - `__name__`. So
filtering by multiple metric names may be performed by applying regexps on metric names:

```metricsql
{__name__=~"requests_(error|success)_total"}
```

The query above returns series for two metrics: `requests_error_total` and `requests_success_total`.

#### Filtering by multiple "or" filters

[MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/) supports selecting time series, which match at least one of multiple "or" filters.
Such filters must be delimited by `or` inside curly braces. For example, the following query selects time series with
`{job="app1",env="prod"}` or `{job="app2",env="dev"}` labels:

```metricsql
{job="app1",env="prod" or job="app2",env="dev"}
```

The number of `or` groups can be arbitrary. The number of `,`-delimited label filters per each `or` group can be arbitrary.
Per-group filters are applied with `and` operation, e.g., they select series simultaneously matching all the filters in the group.

This functionality allows passing the selected series to [rollup functions](https://docs.victoriametrics.com/victoriametrics/metricsql/#rollup-functions) such as [rate()](https://docs.victoriametrics.com/victoriametrics/metricsql/#rate) without the need to use [subqueries](https://docs.victoriametrics.com/victoriametrics/metricsql/#subqueries):

```metricsql
rate({job="app1",env="prod" or job="app2",env="dev"}[5m])
```

If you need to select series matching multiple filters for the same label, then it is better from a performance PoV
to use regexp filter `{label=~"value1|...|valueN"}` instead of `{label="value1" or ... or label="valueN"}`.


#### Arithmetic operations

MetricsQL supports all the basic arithmetic operations:

* addition - `+`
* subtraction - `-`
* multiplication - `*`
* division - `/`
* modulo - `%`
* power - `^`

This allows performing various calculations across multiple metrics.
For example, the following query calculates the percentage of error requests:

```metricsql
(requests_error_total / (requests_error_total + requests_success_total)) * 100
```

#### Combining multiple series

Combining multiple time series with arithmetic operations requires an understanding of matching rules. Otherwise, the
query may break or may lead to incorrect results. The basics of the matching rules are simple:

* The query engine strips metric names from all the time series on the left and right side of the arithmetic operation
  without touching labels.
* For each time series on the left side, the engine searches for the corresponding time series on the right
  with the same set of labels, applies the operation for each data point, and returns the resulting time series with the
  same set of labels. If there are no matches, then the time series is dropped from the result.
* The matching rules may be augmented with `ignoring`, `on`, `group_left` and `group_right` modifiers.
  See [these docs](https://prometheus.io/docs/prometheus/latest/querying/operators/#vector-matching) for details.

#### Comparison operations

MetricsQL supports the following comparison operators:

* equal - `==`
* not equal - `!=`
* greater - `>`
* greater-or-equal - `>=`
* less - `<`
* less-or-equal - `<=`

These operators may be applied to arbitrary MetricsQL expressions as with arithmetic operators. The result of the
comparison operation is a time series with only matching data points. For instance, the following query would return
series only for processes where memory usage exceeds `100MB`:

```metricsql
process_resident_memory_bytes > 100*1024*1024
```

#### Aggregation and grouping functions

MetricsQL allows aggregating and grouping of time series. Time series are grouped by the given set of labels, and then the
given aggregation function is applied individually per each group. For instance, the following query returns
summary memory usage for each `job`:

```metricsql
sum(process_resident_memory_bytes) by (job)
```

See [docs for aggregate functions in MetricsQL](https://docs.victoriametrics.com/victoriametrics/metricsql/#aggregate-functions).

#### Calculating rates

One of the most widely used functions for [counters](#counter)
is [rate](https://docs.victoriametrics.com/victoriametrics/metricsql/#rate). It calculates the average per-second increase rate individually
per each matching time series. For example, the following query shows the average per-second data receive speed
per each monitored `node_exporter` instance, which exposes the `node_network_receive_bytes_total` metric:

```metricsql
rate(node_network_receive_bytes_total)
```

By default, `valkey-timeseries` calculates the `rate` over [raw samples](#raw-samples) on the lookbehind window specified in the `step` param
passed either to [instant query](#instant-query) or to [range query](#range-query).
The interval on which `rate` needs to be calculated can be specified explicitly as [duration](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations) in square brackets:

```metricsql
 rate(node_network_receive_bytes_total[5m])
```

In this case VictoriaMetrics uses the specified lookbehind window - `5m` (5 minutes) - for calculating the average per-second increase rate.
A bigger lookbehind window usually leads to smoother graphs.

`rate` strips metric name while leaving all the labels for the inner time series. If you need to keep the metric name,
then add [keep_metric_names](https://docs.victoriametrics.com/victoriametrics/metricsql/#keep_metric_names) modifier after the `rate(..)`. For example, the following query leaves metric names after calculating the `rate()`:

```metricsql
rate(node_network_receive_bytes_total) keep_metric_names
```

`rate()` must be applied only to [counters](#counter). The result of applying the `rate()` to [gauge](#gauge) is undefined.
