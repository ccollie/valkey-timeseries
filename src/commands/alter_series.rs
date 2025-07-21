use crate::commands::arg_parse::CommandArgToken;
use crate::commands::parse_series_options;
use crate::series::index::with_timeseries_index;
use crate::series::{with_timeseries_mut, TimeSeries, TimeSeriesOptions};
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString, VALKEY_OK,
};

pub fn alter_series(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let mut args = args.into_iter().skip(1).peekable();

    let key = args
        .next()
        .ok_or(ValkeyError::Str("Err missing key argument"))?;

    with_timeseries_mut(ctx, &key, Some(AclPermissions::UPDATE), |series| {
        let opts = options_from_series(series);
        let options = parse_series_options(
            &mut args,
            opts,
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
    TimeSeriesOptions {
        retention: Some(series.retention),
        chunk_size: Some(series.chunk_size_bytes),
        labels: series.labels.to_label_vec(),
        sample_duplicate_policy: series.sample_duplicates,
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
    if let Some(retention) = options.retention {
        if retention != series.retention {
            series.retention = retention;
            has_changed = true;
        }
    }

    // TODO: !!!!!
    if let Some(chunk_size) = options.chunk_size {
        if chunk_size != series.chunk_size_bytes {
            // todo: recompress the chunks
            series.chunk_size_bytes = chunk_size;
            has_changed = true;
        }
    }

    if options.sample_duplicate_policy != series.sample_duplicates {
        series.sample_duplicates = options.sample_duplicate_policy;
        has_changed = true;
    }

    let mut labels_changed = false;
    // see if labels have changed
    if series.labels.len() != options.labels.len() {
        labels_changed = true;
    } else {
        for label in options.labels.iter() {
            if let Some(value) = series.labels.get_value(&label.name) {
                if value != label.value {
                    labels_changed = true;
                    break;
                }
            } else {
                labels_changed = true;
                break;
            }
        }
    }

    if labels_changed {
        has_changed = true;

        // reindex the series
        with_timeseries_index(ctx, |ts_index| {
            ts_index.remove_timeseries(series);
            // update labels in series
            series.labels.clear();
            for label in options.labels {
                series.labels.add_label(&label.name, &label.value);
            }
            ts_index.index_timeseries(series, key.as_slice());
        });
    }

    has_changed
}
