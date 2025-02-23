# ValkeyTimeSeries

ValkeyTimeSeries is a module providing a TimeSeries data type for [Valkey](https:://valkey.io).

## Features
- In-memory storage for time series data
- Configurable data retention period
- Configurable encoding
- Single sample and range queries
- Supports [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) like queries
- Basic compatibility with the [RedisTimeSeries](https://oss.redislabs.com/redistimeseries/) API.

## Caveats
- Is highly experimental and not yet ready for production use


## Tests

The module includes a basic set of unit tests and integration tests.

**Unit tests**

To run all unit tests, follow these steps:

    $ cargo test


**Integration tests**

```bash
RLTest --module ./target/debug/libredis_promql.so --module-args
```

#### MacOSX
```bash
RLTest --module ./target/debug/libredis_promql.dylib --module-args
```

## Commands

Command names and option names are case-insensitive.

#### A note about keys
Since the general use-case for this module is querying across timeseries, it is a best practice to group related timeseries
using "hash tags" in the key. This allows for more efficient querying across related timeseries. For example, if your
metrics are generally grouped by environment, you could use a key like
`latency:api:{dev}` and `latency:frontend:{staging}`. If you are more likely to group by service, you could use
`latency:{api}:dev` and `latency:{frontend}:staging`.


https://tech.loveholidays.com/redis-cluster-multi-key-command-optimisation-with-hash-tags-8a2bd7ce12de

The following commands are supported

```aiignore
TS.CREATE
TS.ALTER
TS.ADD
TS.MADD
TS.DEL
TS.LABELS
TS.JOIN
TS.GET
TS.MGET
TS.DELETE-SERIES
TS.MRANGE
TS.RANGE
TS.QUERYINDEX
TS.CARDINALITY
TS.LABEL-NAMES
TS.LABEL-VALUES
TS.STATS
```

## License
ValkeyTimeSeries is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).