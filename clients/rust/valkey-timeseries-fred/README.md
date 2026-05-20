# valkey-timeseries-fred

`valkey-timeseries-fred` is an async Rust client for the `valkey-timeseries` module built on top of [`fred`](https://crates.io/crates/fred).

It wraps `fred::RedisClient` and exposes one method per `ts_*` command exported in `src/commands/mod.rs`, including wrappers for the command families (`TS.RANGE/TS.REVRANGE`, `TS.MRANGE/TS.MREVRANGE`, and `TS._DEBUG LIST_CONFIGS`).

## Example

```rust,no_run
use fred::{interfaces::ClientLike, prelude::RedisConfig};
use valkey_timeseries_fred::{AddOptions, CreateOptions, RangeOptions, ValkeyTimeseriesClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisConfig::from_url("redis://127.0.0.1/")?;
    let client = ValkeyTimeseriesClient::from_config(config);

    let _task = client.inner().connect();
    client.wait_connected().await?;

    client
        .ts_create(
            "sensor:temp",
            CreateOptions::default().retention(60_000).label("sensor", "temp"),
        )
        .await?;

    let ts = client
        .ts_add("sensor:temp", "*", 20.5, AddOptions::default())
        .await?;
    println!("inserted timestamp: {ts}");

    let samples = client
        .ts_range("sensor:temp", "-", "+", RangeOptions::default().count(100))
        .await?;
    println!("samples returned: {}", samples.len());

    Ok(())
}
```
