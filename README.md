# valkey-timeseries

ValkeyTimeSeries is a Rust based module providing a TimeSeries data type for [Valkey](https:://valkey.io).

### WARNING !!!!
This is a work in progress and a moving target. It is not yet ready for production or even casual use. It probably doesn't work. 
On any day of the week it may not even build :-)

## Features
- In-memory storage for time series data
- Configurable data retention period
- Configurable encoding
- Single sample and range queries
- Supports [Metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metadata) like queries
- Basic compatibility with the [RedisTimeSeries](https://oss.redislabs.com/redistimeseries/) API.

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
TS.DECRBY
TS.DEL
TS.JOIN
TS.GET
TS.INCRBY
TS.MGET
TS.MRANGE
TS.MREVRANGE
TS.RANGE
TS.REVRANGE
TS.QUERYINDEX
TS.CARD
TS.LABELNAMES
TS.LABELVALUES
TS.STATS
```


## Build instructions
```
curl https://sh.rustup.rs -sSf | sh
sudo yum install clang
git clone https://github.com/ccollie/valkey-timeseries.git
cd valkey-timeseries
# Building for Valkey 8.1 and above:
cargo build --release
# Building for Valkey 8.0 specifically:
cargo build --release --features valkey_8_0
valkey-server --loadmodule ./target/release/libvalkey_timeseries.so
```
#### Running Unit tests

To run all unit tests, follow these steps:

    $ cargo test

#### Local development script to build, run format checks, run unit / integration tests, and for cargo release:
```
# Builds the valkey-server (unstable) for integration testing.
SERVER_VERSION=unstable
./build.sh
# Same as above, but uses valkey-server (8.0.0) for integration testing.
SERVER_VERSION=8.0.0
./build.sh
# Build with asan, you may need to remove the old valkey binary if you have used ./build.sh before. You can do this by deleting the `.build` folder in the `tests` folder 
ASAN_BUILD=true
./build.sh
```

## Load the Module
To test the module with a Valkey, you can load the module in the following ways:

#### Using valkey.conf:
```
1. Add the following to valkey.conf:
    loadmodule /path/to/libvalkey_timeseries.so
2. Start valkey-server:
    valkey-server /path/to/valkey.conf
```

#### Starting Valkey with the `--loadmodule` option:
```text
valkey-server --loadmodule /path/to/libvalkey_timeseries.so
```

#### Using the Valkey command `MODULE LOAD`:
```
1. Connect to a running Valkey instance using valkey-cli
2. Execute Valkey command:
    MODULE LOAD /path/to/libvalkey_timeseries.so
```
## Feature Flags

* valkey_8_0: valkey-timeseries is intended to be loaded on server versions >= Valkey 8.1 and by default it is built this way (unless this flag is provided). It is however compatible with Valkey version 8.0 if the user explicitly provides this feature flag in their cargo build command.
```
cargo build --release --features valkey_8_0
```

This can also be done by specifying SERVER_VERSION=8.0.0 and then running `./build.sh`

## License
ValkeyTimeSeries is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).