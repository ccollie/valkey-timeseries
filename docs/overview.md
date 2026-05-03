# Valkey TimeSeries Overview

Valkey TimeSeries is a module for [Valkey](https://valkey.io) that adds a high-performance, in-memory time series data
type. It is designed to handle high-velocity ingestion and real-time querying of metric data, leveraging the speed of
memory and the
versatility of the Valkey ecosystem. The API is a superset of RedisTimeSeries, offering compatibility with existing
tools while
providing optimizations for Valkey.

## Use Cases

The module is architected for scenarios requiring low-latency storage and retrieval of time-stamped numerical data:

* **Application Monitoring:** Storage of metrics from servers, containers, or distributed applications (CPU, memory,
  latency).
* **IoT Telemetry:** Ingesting sensor data from edge devices with high throughput.
* **Real-time Analytics:** Calculation of moving averages, max/min values, and aggregating data streams for dashboards.
* **Financial Data:** Storing tick data, price history, or trading volume for rapid analysis.

## Supported Commands

The command set generally follows the `TS.<COMMAND>` pattern.

### Management

* `TS.CREATE`: Create a new time series with specific retention, encoding, and chunk size policies.
* `TS.ALTER`: Modify the configuration of an existing series.
* `TS.DEL`: Remove an entire time series from the database.
* `TS.MDEL`: Remove multiple time series matching a filter from the database.

### Ingestion

* `TS.ADD`: Append a new sample (timestamp, value) to a series.
* `TS.MADD`: Append samples to multiple series atomically.
* `TS.ADDBULK`: Append multiple samples to a single series in one command.
* `TS.INCRBY` / `TS.DECRBY`: Increment or decrement the value of the latest sample.
* `TS.DEL`: Delete samples within a specific time range.

### Querying

* `TS.GET`: Retrieve the last sample of a series.
* `TS.MGET`: Retrieve the last sample from multiple series matching a filter.
* `TS.RANGE`: Query a range of samples from a single series.
* `TS.MRANGE`: Query ranges across multiple series based on filters.

### Compaction & Rules

* `TS.CREATERULE`: Create a downsampling rule to aggregate data from a source key to a destination key over fixed time
  buckets.
* `TS.DELETERULE`: Remove an existing compaction rule.

### Metadata & Indexing

* `TS.INFO`: Retrieve detailed information and statistics about a specific time series.
* `TS.QUERYINDEX`: Retrieve all series keys matching a label filter.
* `TS.CARD`: Get the cardinality of the index for a specific label filter.
* `TS.LABELNAMES`: Get all label names used in the index.
* `TS.METRICNAMES`: Search metric names with substring and optional fuzzy matching.
* `TS.LABELVALUES`: Get all values for a specific label name in the index.
* `TS.LABELSTATS`: Get statistics about label usage in the index.

### Anomaly Detection

* `TS.OUTLIERS`: Identify outliers in a series based on a specified algorithm and parameters.

## Indexes

Valkey TimeSeries uses a label-based indexing system separate from the key space.

* **Labels:** Every time series can be associated with a set of `field=value` labels (e.g., `region=us-east`,
  `env=prod`).
* **Secondary Indexing:** These labels form a secondary index that allows for efficient discovery of keys. Unlike
  standard Valkey keys which are accessed by the exact name, time series can be grouped and queried dynamically.
* **Cardinality:** Commands like `TS.CARD` provide insights into the cardinality of the index to help monitor memory
  usage and query efficiency.

`valkey-timeseries` maintains a per-node, per-database inverted index to map labels to series.
The indexes themselves exist separate from the Valkey database itself. Applications don't directly modify an index,
rather
mutation operations on keys within the declared keyspace of an index automatically update the index with the labels of
that key.

# Index Replication

Indexes are node-local. Each node, regardless of whether it's a primary or a replica, maintains its own index
independently. Indexes on replicas are updated by key mutations transmitted on the replication channel and thus are
subject to replication lag just like the Valkey database itself.

## Querying

Data retrieval is highly flexible, supporting filtering, aggregation, and arithmetic operations.

* **Filtering:** Queries use label matchers (e.g., `TS.MRANGE ... FILTER region=us-east metric!=cpu`) to select the
  target series.
* **Aggregation:** Raw samples can be aggregated at query time using functions such as `avg`, `sum`, `min`, `max`,
  `count`, `first`, `last`, `std.p`, `std.s`, `var.p`, and `var.s`.
* **Alignment:** Time buckets can be aligned to specific intervals to ensure consistent reporting across different
  series.

# Cluster Mode

Timeseries fully supports cluster mode and uses Valkey's cluster bus and protobuf for intra-cluster communication.

In cluster mode, Valkey distributes keys according to the hash algorithm of the keyname. This placement of data is not
affected by the presence of the timeseries module or any timeseries indexes. Since timeseries commands operate at the
index level -- not the key level -- valkey-timeseries is responsible for the distribution of data, performing
intra-cluster RPC
to execute commands as needed. Thus, the application interface to valkey-timeseries operates the same in cluster and
non-cluster mode.

Timeseries uses a simple architecture where index definitions are replicated on every node, but the corresponding index
only contains the data which is co-resident on that node. Index update operations remain wholly local to a node and will
scale horizontally (save/restore operations also wholly node local). Vertical scaling is also effective because of the
multithreaded architecture.

Query operations are performed by one node of each shard on its local index, and the results are transparently merged to
form a full command response. Query operations are subject to increasing overhead as the cluster shard count increases,
meaning that query operations may scale sub-linearly with increasing shard count.