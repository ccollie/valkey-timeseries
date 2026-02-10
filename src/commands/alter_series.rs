use crate::commands::command_args::CommandArgToken;
use crate::commands::parse_series_options;
use crate::labels::MetricName;
use crate::series::index::get_timeseries_index;
use crate::series::{SampleDuplicatePolicy, TimeSeries, TimeSeriesOptions, with_timeseries_mut};
use std::ops::Deref;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, VALKEY_OK, ValkeyError, ValkeyResult, ValkeyString,
};

/// Alter a time series
///
/// TS.ALTER key
///   [RETENTION retentionPeriod]
///   [DUPLICATE_POLICY duplicatePolicy]
///   [SIGNIFICANT_DIGITS significantDigits | DECIMAL_DIGITS decimalDigits]
///   [IGNORE ignoreMaxTimediff ignoreMaxValDiff]
///   [LABELS label1=value1 label2=value2 ...]
pub fn alter_series(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args;
    let key = args.remove(1);

    with_timeseries_mut(ctx, &key, Some(AclPermissions::UPDATE), |series| {
        let options = parse_series_options(
            args,
            1,
            &[CommandArgToken::Encoding, CommandArgToken::OnDuplicate],
        )?;

        let changed = update_series(ctx, series, options, &key);

        ctx.replicate_verbatim();
        if changed {
            ctx.notify_keyspace_event(NotifyEvent::MODULE, "ts.alter", &key);
        }
        VALKEY_OK
    })
}

fn options_from_series(series: &TimeSeries) -> TimeSeriesOptions {
    let policy_default = SampleDuplicatePolicy::default();
    let sample_duplicates = if series.sample_duplicates == policy_default {
        None
    } else {
        Some(series.sample_duplicates)
    };
    let labels = if series.labels.is_empty() {
        None
    } else {
        Some(series.labels.to_label_vec())
    };
    TimeSeriesOptions {
        retention: Some(series.retention),
        chunk_size: Some(series.chunk_size_bytes),
        labels,
        sample_duplicate_policy: sample_duplicates,
        chunk_compression: series.chunk_compression,
        rounding: series.rounding,
        ..Default::default()
    }
}

fn update_series(
    ctx: &Context,
    series: &mut TimeSeries,
    options: TimeSeriesOptions,
    key: &ValkeyString,
) -> bool {
    let mut has_changed = false;

    if let Some(chunk_size) = options.chunk_size
        && chunk_size != series.chunk_size_bytes
    {
        // todo: recompress the chunks
        series.chunk_size_bytes = chunk_size;
        has_changed = true;
    }

    if let Some(labels) = options.labels {
        has_changed = true;

        // reindex the series
        let guard = get_timeseries_index(ctx);
        let ts_index = guard.deref();

        ts_index.remove_timeseries(series);
        // update labels in series
        series.labels = if labels.is_empty() {
            MetricName::default()
        } else {
            MetricName::new(&labels)
        };

        ts_index.index_timeseries(series, key.as_slice());
    }

    if let Some(retention) = options.retention
        && retention != series.retention
    {
        series.retention = retention;
        has_changed = true;
    }

    if let Some(duplicate_policy) = options.sample_duplicate_policy
        && duplicate_policy != series.sample_duplicates
    {
        series.sample_duplicates = duplicate_policy;
        has_changed = true;
    }

    has_changed
}
