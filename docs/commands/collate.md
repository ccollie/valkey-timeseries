```
VM.COLLATE fromTimestamp toTimestamp FILTER filter...
    [COUNT count]
    [WITHLABELS]
    [SELECTED_LABELS label...]
    [AGGREGATION aggregator]
```

Return multiple series matched by timestamp. Similar to `MRANGE`, but outer joined by timestamp

### Required Arguments

<details open><summary><code>fromTimestamp</code></summary>
the start of the range to query from (inclusive)
</details>

<details open><summary><code>endTimestamp</code></summary>
the end of the range to query from (inclusive)
</details>

<details open><summary><code>filter</code></summary>
one or more series selectors to specify the series being fetched
</details>

### Details

It is a common practice to monitor `measurements` across timeseries annotated by tags. For instance
to handle api latencies, we segregate series data across various axes into different db keys.

We can as an example have the following time series:

| metric                                                      | key                             |
|-------------------------------------------------------------|---------------------------------|
| `latencies{service="auth", region="us-east-1", code="200"}` | `latencies:auth:us-east-1:200`  |
| `latencies{service="auth", region="us-east-1", code="404"}` | `latencies:auth:us-east-1:404`  |
| `latencies{service="auth", region="us-east-2", code="200"}` | `latencies:auth:us-east-2:200`  |
| `latencies{service="auth", region="us-west-1", code="200"}` | `latencies:auth:us-west-1:200`  |
| `latencies{service="auth", region="us-west-2", code="200"}` | `latencies:auth:us-west-2:200`  |


Now suppose we want to gather all latencies for `http 200` response in the east `aws` regions, over the last 6 hours. Further,
we want to collate the results such that for each timestamp recorded (across series), we return the value for each series, 
or a NULL if it does not have a sample at the given timestamp.

We can use the following

```aiignore
VM.COLLATE -6hr * FILTER latencies{service="auth", region~="us-east-*" code="200"} WITHLABELS
```

This would pull back data for `us-east-1`, `us-east-2` etc. 

As an example, if we have the following data

`latencies:auth:us-east-1:200`

| Timestamp | Value |
|-----------|-------|
| 1000      | 10    |
| 2000      | 20    |
| 3000      | 35    |
|           |       |

`latencies:auth:us-east-2:200`

| Timestamp | Value |
|-----------|-------|
| 1000      | 10    |
| 3000      | 15    |
| 4000      | 40    |


This would result in 

| Timestamp | `latencies:auth:us-east-1:200` | `latencies:auth:us-east-2:200` |
|-----------|--------------------------------|--------------------------------|
| 1000      | 10                             | 10                             |
| 2000      | 20                             | NULL                           |
| 3000      | 30                             | 15                             |
| 4000      | NULL                           | 40                             |

Now suppose that instead of raw values, we want to get the average latencies per timestamp. We can use the `aggregation` argument.

```aiignore
TS.COLLATE -6hr * FILTER latencies{service="auth", region~="us-east-*" code="200"} WITHLABELS AGGREGATION avg
```

This would result in

| Timestamp | Avg | 
|-----------|-----|
| 1000      | 10  |
| 2000      | 20  |
| 3000      | 25  |
| 4000      | 40  |