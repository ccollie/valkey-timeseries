//! Async Rust client for `valkey-timeseries` built on top of `fred` custom commands.

mod options;
mod types;

pub use options::*;
pub use types::*;

use fred::{
    interfaces::ClientLike,
    prelude::{RedisClient, RedisConfig, RedisError, RedisValue},
    types::{ClusterHash, CustomCommand},
};

#[derive(Clone)]
pub struct ValkeyTimeseriesClient {
    inner: RedisClient,
}

#[derive(Debug, Clone)]
pub struct MaddSample {
    pub key: RedisValue,
    pub timestamp: RedisValue,
    pub value: RedisValue,
}

impl MaddSample {
    pub fn new(
        key: impl Into<RedisValue>,
        timestamp: impl Into<RedisValue>,
        value: impl Into<RedisValue>,
    ) -> Self {
        Self {
            key: key.into(),
            timestamp: timestamp.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AddBulkSample {
    pub timestamp: RedisValue,
    pub value: RedisValue,
}

impl AddBulkSample {
    pub fn new(timestamp: impl Into<RedisValue>, value: impl Into<RedisValue>) -> Self {
        Self {
            timestamp: timestamp.into(),
            value: value.into(),
        }
    }
}

impl ValkeyTimeseriesClient {
    pub fn new(inner: RedisClient) -> Self {
        Self { inner }
    }

    pub fn from_config(config: RedisConfig) -> Self {
        Self::new(RedisClient::new(config, None, None, None))
    }

    pub fn inner(&self) -> &RedisClient {
        &self.inner
    }

    pub async fn wait_connected(&self) -> Result<(), RedisError> {
        self.inner.wait_for_connect().await
    }

    async fn custom(
        &self,
        command: &'static str,
        args: Vec<RedisValue>,
        cluster_hash: ClusterHash,
    ) -> Result<RedisValue, RedisError> {
        let command = CustomCommand::new_static(command, cluster_hash, false);
        self.inner.custom(command, args).await
    }

    pub async fn ts_create(
        &self,
        key: impl Into<RedisValue>,
        options: CreateOptions,
    ) -> Result<(), RedisError> {
        let mut args = vec![key.into()];
        options.write_args(&mut args);
        parse_status_ok(
            self.custom("TS.CREATE", args, ClusterHash::FirstKey)
                .await?,
        )
    }

    pub async fn ts_alter(
        &self,
        key: impl Into<RedisValue>,
        options: AlterOptions,
    ) -> Result<(), RedisError> {
        let mut args = vec![key.into()];
        options.write_args(&mut args);
        parse_status_ok(self.custom("TS.ALTER", args, ClusterHash::FirstKey).await?)
    }

    pub async fn ts_add(
        &self,
        key: impl Into<RedisValue>,
        timestamp: impl Into<RedisValue>,
        value: impl Into<RedisValue>,
        options: AddOptions,
    ) -> Result<i64, RedisError> {
        let mut args = vec![key.into(), timestamp.into(), value.into()];
        options.write_args(&mut args);
        parse_i64(self.custom("TS.ADD", args, ClusterHash::FirstKey).await?)
    }

    /// `TS.ADDBULK` returns a module-defined response shape, so this method
    /// exposes the raw `RedisValue` for caller-directed parsing.
    pub async fn ts_addbulk(
        &self,
        key: impl Into<RedisValue>,
        samples: &[AddBulkSample],
        options: AddOptions,
    ) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(1 + samples.len() * 2 + 8);
        args.push(key.into());
        for sample in samples {
            args.push(sample.timestamp.clone());
            args.push(sample.value.clone());
        }
        options.write_args(&mut args);
        self.custom("TS.ADDBULK", args, ClusterHash::FirstKey).await
    }

    pub async fn ts_card(&self) -> Result<i64, RedisError> {
        parse_i64(
            self.custom("TS.CARD", Vec::new(), ClusterHash::Random)
                .await?,
        )
    }

    pub async fn ts_createrule(
        &self,
        source_key: impl Into<RedisValue>,
        dest_key: impl Into<RedisValue>,
        aggregation: impl Into<RedisValue>,
        bucket_duration_ms: i64,
    ) -> Result<(), RedisError> {
        let args = vec![
            source_key.into(),
            dest_key.into(),
            "AGGREGATION".into(),
            aggregation.into(),
            bucket_duration_ms.into(),
        ];
        parse_status_ok(
            self.custom("TS.CREATERULE", args, ClusterHash::FirstKey)
                .await?,
        )
    }

    /// Generic wrapper over `TS._DEBUG`.
    ///
    /// Current module subcommands include `HELP`, `STRINGPOOLSTATS [TOPK]`,
    /// and `LIST_CONFIGS [VERBOSE]`.
    pub async fn ts_debug(
        &self,
        subcommand: impl Into<RedisValue>,
        args: impl IntoIterator<Item = RedisValue>,
    ) -> Result<RedisValue, RedisError> {
        let mut cmd_args = vec![subcommand.into()];
        cmd_args.extend(args);
        self.custom("TS._DEBUG", cmd_args, ClusterHash::Random)
            .await
    }

    pub async fn ts_debug_configs(&self, verbose: bool) -> Result<RedisValue, RedisError> {
        let mut args = vec!["LIST_CONFIGS".into()];
        if verbose {
            args.push("VERBOSE".into());
        }
        self.custom("TS._DEBUG", args, ClusterHash::Random).await
    }

    pub async fn ts_del(
        &self,
        key: impl Into<RedisValue>,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
    ) -> Result<i64, RedisError> {
        let args = vec![key.into(), from_timestamp.into(), to_timestamp.into()];
        parse_i64(self.custom("TS.DEL", args, ClusterHash::FirstKey).await?)
    }

    pub async fn ts_deleterule(
        &self,
        source_key: impl Into<RedisValue>,
        dest_key: impl Into<RedisValue>,
    ) -> Result<(), RedisError> {
        let args = vec![source_key.into(), dest_key.into()];
        parse_status_ok(
            self.custom("TS.DELETERULE", args, ClusterHash::FirstKey)
                .await?,
        )
    }

    pub async fn ts_get(
        &self,
        key: impl Into<RedisValue>,
        options: GetOptions,
    ) -> Result<Option<Sample>, RedisError> {
        let mut args = vec![key.into()];
        options.write_args(&mut args);
        parse_sample(self.custom("TS.GET", args, ClusterHash::FirstKey).await?)
    }

    pub async fn ts_incrby(
        &self,
        key: impl Into<RedisValue>,
        value: impl Into<RedisValue>,
        options: IncrDecrOptions,
    ) -> Result<i64, RedisError> {
        let mut args = vec![key.into(), value.into()];
        options.write_args(&mut args);
        parse_i64(
            self.custom("TS.INCRBY", args, ClusterHash::FirstKey)
                .await?,
        )
    }

    pub async fn ts_decrby(
        &self,
        key: impl Into<RedisValue>,
        value: impl Into<RedisValue>,
        options: IncrDecrOptions,
    ) -> Result<i64, RedisError> {
        let mut args = vec![key.into(), value.into()];
        options.write_args(&mut args);
        parse_i64(
            self.custom("TS.DECRBY", args, ClusterHash::FirstKey)
                .await?,
        )
    }

    pub async fn ts_info(
        &self,
        key: impl Into<RedisValue>,
        options: InfoOptions,
    ) -> Result<RedisValue, RedisError> {
        let mut args = vec![key.into()];
        options.write_args(&mut args);
        self.custom("TS.INFO", args, ClusterHash::FirstKey).await
    }

    /// `join_args` should contain the trailing `TS.JOIN` options after
    /// destination/source keys (for example join strategy and aggregations).
    pub async fn ts_join(
        &self,
        destination_key: impl Into<RedisValue>,
        source_keys: impl IntoIterator<Item = RedisValue>,
        join_args: impl IntoIterator<Item = RedisValue>,
    ) -> Result<RedisValue, RedisError> {
        let mut args = vec![destination_key.into()];
        args.extend(source_keys);
        args.extend(join_args);
        self.custom("TS.JOIN", args, ClusterHash::FirstKey).await
    }

    pub async fn ts_labelnames(&self, filters: &[String]) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(filters.len() + 1);
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }
        self.custom("TS.LABELNAMES", args, ClusterHash::Random)
            .await
    }

    pub async fn ts_labelstats(&self, filters: &[String]) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(filters.len() + 1);
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }
        self.custom("TS.LABELSTATS", args, ClusterHash::Random)
            .await
    }

    pub async fn ts_labelvalues(
        &self,
        label: impl Into<RedisValue>,
        filters: &[String],
    ) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(filters.len() + 2);
        args.push(label.into());
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }
        self.custom("TS.LABELVALUES", args, ClusterHash::Random)
            .await
    }

    pub async fn ts_madd(&self, samples: &[MaddSample]) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(samples.len() * 3);
        for sample in samples {
            args.push(sample.key.clone());
            args.push(sample.timestamp.clone());
            args.push(sample.value.clone());
        }
        self.custom("TS.MADD", args, ClusterHash::FirstKey).await
    }

    pub async fn ts_mdel(&self, filters: &[String]) -> Result<i64, RedisError> {
        let mut args = Vec::with_capacity(filters.len() + 1);
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }
        parse_i64(self.custom("TS.MDEL", args, ClusterHash::Random).await?)
    }

    pub async fn ts_metricnames(&self, filters: &[String]) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(filters.len() + 1);
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }
        self.custom("TS.METRICNAMES", args, ClusterHash::Random)
            .await
    }

    pub async fn ts_mget(
        &self,
        filters: &[String],
        options: MGetOptions,
    ) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(filters.len() + 8);
        options.write_args(&mut args);
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }
        self.custom("TS.MGET", args, ClusterHash::Random).await
    }

    pub async fn ts_mrange(
        &self,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        filters: &[String],
        options: MRangeOptions,
    ) -> Result<RedisValue, RedisError> {
        self.ts_mrange_inner("TS.MRANGE", from_timestamp, to_timestamp, filters, options)
            .await
    }

    pub async fn ts_mrevrange(
        &self,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        filters: &[String],
        options: MRangeOptions,
    ) -> Result<RedisValue, RedisError> {
        self.ts_mrange_inner(
            "TS.MREVRANGE",
            from_timestamp,
            to_timestamp,
            filters,
            options,
        )
        .await
    }

    async fn ts_mrange_inner(
        &self,
        command: &'static str,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        filters: &[String],
        options: MRangeOptions,
    ) -> Result<RedisValue, RedisError> {
        let mut args = vec![from_timestamp.into(), to_timestamp.into()];
        options.write_args(&mut args);
        args.push("FILTER".into());
        for filter in filters {
            args.push(filter.clone().into());
        }

        self.custom(command, args, ClusterHash::Random).await
    }

    pub async fn ts_outliers(
        &self,
        key: impl Into<RedisValue>,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        options: OutliersOptions,
    ) -> Result<RedisValue, RedisError> {
        let mut args = vec![key.into(), from_timestamp.into(), to_timestamp.into()];
        options.write_args(&mut args);
        self.custom("TS.OUTLIERS", args, ClusterHash::FirstKey)
            .await
    }

    pub async fn ts_queryindex(&self, filters: &[String]) -> Result<RedisValue, RedisError> {
        let mut args = Vec::with_capacity(filters.len());
        for filter in filters {
            args.push(filter.clone().into());
        }
        self.custom("TS.QUERYINDEX", args, ClusterHash::Random)
            .await
    }

    pub async fn ts_range(
        &self,
        key: impl Into<RedisValue>,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        options: RangeOptions,
    ) -> Result<Vec<Sample>, RedisError> {
        self.ts_range_inner("TS.RANGE", key, from_timestamp, to_timestamp, options)
            .await
    }

    pub async fn ts_revrange(
        &self,
        key: impl Into<RedisValue>,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        options: RangeOptions,
    ) -> Result<Vec<Sample>, RedisError> {
        self.ts_range_inner("TS.REVRANGE", key, from_timestamp, to_timestamp, options)
            .await
    }

    async fn ts_range_inner(
        &self,
        command: &'static str,
        key: impl Into<RedisValue>,
        from_timestamp: impl Into<RedisValue>,
        to_timestamp: impl Into<RedisValue>,
        options: RangeOptions,
    ) -> Result<Vec<Sample>, RedisError> {
        let mut args = vec![key.into(), from_timestamp.into(), to_timestamp.into()];
        options.write_args(&mut args);
        parse_samples(self.custom(command, args, ClusterHash::FirstKey).await?)
    }
}
