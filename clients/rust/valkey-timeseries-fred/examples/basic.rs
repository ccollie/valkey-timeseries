use fred::{interfaces::ClientLike, prelude::RedisConfig};
use valkey_timeseries_fred::{
    AddOptions, CreateOptions, MGetOptions, RangeOptions, ValkeyTimeseriesClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisConfig::from_url("redis://127.0.0.1/")?;
    let client = ValkeyTimeseriesClient::from_config(config);

    let _connect_task = client.inner().connect();
    client.wait_connected().await?;

    client
        .ts_create(
            "weather:sf",
            CreateOptions::default()
                .retention(300_000)
                .label("city", "sf"),
        )
        .await?;

    client
        .ts_add("weather:sf", "*", 14.2, AddOptions::default())
        .await?;

    let _latest = client.ts_get("weather:sf", Default::default()).await?;

    let _series = client
        .ts_range("weather:sf", "-", "+", RangeOptions::default().count(10))
        .await?;

    let _mget = client
        .ts_mget(
            &["city=sf".to_string()],
            MGetOptions::default().with_labels(true),
        )
        .await?;

    Ok(())
}
