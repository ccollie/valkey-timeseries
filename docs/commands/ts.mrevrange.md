# TS.MREVRANGE

Reverse range query for multiple time series.

```
TS.MREVRANGE fromTimestamp toTimestamp
    [LATEST]
    [FILTER_BY_TS ts...]
    [FILTER_BY_VALUE min max]
    [WITHLABELS | SELECTED_LABELS label...]
    [COUNT count]
    [[ALIGN align] AGGREGATION aggregator[(op value)][,aggregator[(op value)]...] bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
    [GROUPBY label REDUCE reducer[(op value)]]
    FILTER selector...
```