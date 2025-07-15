use crate::common::constants::METRIC_NAME_LABEL;
use crate::common::db::get_current_db;
use crate::error_consts;
use crate::labels::{InternedMetricName, SeriesLabel};
use crate::series::acl::check_key_permissions;
use crate::series::index::{next_timeseries_id, with_timeseries_index, TimeSeriesIndex};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesGuard, SeriesGuardMut, TimeSeries, TimeSeriesOptions};
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
};

pub fn with_timeseries<R>(
    ctx: &Context,
    key: &ValkeyString,
    check_acl: bool,
    f: impl FnOnce(&TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let redis_key = ctx.open_key(key);
    if let Some(series) = redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)? {
        if check_acl {
            check_key_permissions(ctx, key, &AclPermissions::ACCESS)?;
        }
        f(series)
    } else {
        Err(invalid_series_key_error())
    }
}

pub fn with_timeseries_mut<R>(
    ctx: &Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
    f: impl FnOnce(&mut TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    let perms = permissions.unwrap_or(AclPermissions::UPDATE);
    // expect should not panic, since must_exist will cause an error if the key is non-existent, and `?` will ensure it propagates
    let mut series =
        get_timeseries_mut(ctx, key, true, Some(perms))?.expect("expected key to exist");
    f(&mut series)
}

pub fn get_timeseries(
    ctx: &Context,
    key: ValkeyString,
    permissions: Option<AclPermissions>,
    must_exist: bool,
) -> ValkeyResult<Option<SeriesGuard>> {
    if let Some(permissions) = permissions {
        check_key_permissions(ctx, &key, &permissions)?;
    }

    let redis_key = ctx.open_key(&key);
    match redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Err(_) => Err(ValkeyError::WrongType),
        Ok(Some(_)) => Ok(Some(SeriesGuard {
            key: redis_key,
            key_inner: key,
        })),
        Ok(None) => {
            if must_exist {
                return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
            }
            Ok(None)
        }
    }
}

pub fn get_timeseries_by_id(
    ctx: &Context,
    id: u64,
    permissions: Option<AclPermissions>,
    must_exist: bool,
) -> ValkeyResult<Option<SeriesGuard>> {
    with_timeseries_index(ctx, |index| {
        // ugly. Simplify this later
        let mut state = ();
        index.with_postings(&mut state, |posting, _| {
            let Some(key) = posting.get_valkey_string_key_by_id(ctx, id) else {
                return Ok(None);
            };
            get_timeseries(ctx, key, permissions, must_exist)
        })
    })
}

pub fn get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    must_exist: bool,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<Option<SeriesGuardMut<'a>>> {
    let value_key = ctx.open_key_writable(key);
    match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Ok(Some(series)) => {
            if let Some(permissions) = permissions {
                check_key_permissions(ctx, key, &permissions)?;
            }
            Ok(Some(SeriesGuardMut { series }))
        }
        Ok(None) => {
            if must_exist {
                return Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND));
            }
            Ok(None)
        }
        Err(_e) => Err(ValkeyError::WrongType),
    }
}

pub(crate) fn invalid_series_key_error() -> ValkeyError {
    ValkeyError::Str(error_consts::KEY_NOT_FOUND)
}

pub fn validate_unique_series_by_labels(
    ctx: &Context,
    index: &TimeSeriesIndex,
    labels: &InternedMetricName,
) -> ValkeyResult<()> {
    fn compare_labels(
        existing_labels: &InternedMetricName,
        new_labels: &InternedMetricName,
    ) -> bool {
        if existing_labels.len() != new_labels.len() {
            return false;
        }
        for label in new_labels.iter() {
            let Some(value) = new_labels.get_value(label.name()) else {
                return false;
            };
            if value != label.value() {
                return false;
            }
        }
        true
    }

    let Some(id) = index.unique_series_id_by_labels(labels)? else {
        // No unique series found by labels, so we can proceed with the creation.
        return Ok(());
    };

    // Edge case: we may have a single series like `memory_usage{bar=bar, bar=baz}` that is a superset of the
    // labels we are trying to create - g.g, labels = `memory_usage{bar=baz}`
    // See comments in `TimeSeriesIndex::unique_series_id_by_labels` for more details.

    // At this point, we have a unique series, or a series with a superset of `labels`.
    // Grab the series by id and check if it matches the labels.
    if let Some(existing_series) = get_timeseries_by_id(ctx, id, None, false)? {
        let existing_series = existing_series.get_series();
        let existing_labels = &existing_series.labels;
        if compare_labels(existing_labels, labels) {
            // The series already exists with the same labels.
            return Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES));
        }
    }
    Ok(())
}

pub fn create_series(
    key: &ValkeyString,
    options: TimeSeriesOptions,
    ctx: &Context,
) -> ValkeyResult<TimeSeries> {
    let mut ts = TimeSeries::with_options(options)?;
    if ts.id == 0 {
        ts.id = next_timeseries_id();
    }

    ts._db = get_current_db(ctx);

    with_timeseries_index(ctx, |index| {
        // Check if this refers to an existing series (a pre-existing series with the same label-value pairs)
        // We do this only in the case where we have a __name__ label, signaling that the user is
        // opting in to Prometheus semantics, meaning a metric name is unique to a series.
        if ts.labels.get_value(METRIC_NAME_LABEL).is_some() {
            validate_unique_series_by_labels(ctx, index, &ts.labels)?;
        }

        index.index_timeseries(&ts, key.iter().as_slice());
        Ok(ts)
    })
}

pub fn create_and_store_series(
    ctx: &Context,
    key: &ValkeyString,
    options: TimeSeriesOptions,
) -> ValkeyResult<()> {
    let _key = ValkeyKeyWritable::open(ctx.ctx, key);
    // check if this refers to an existing series
    if !_key.is_empty() {
        return Err(ValkeyError::Str(error_consts::DUPLICATE_KEY));
    }

    let ts = create_series(key, options, ctx)?;
    _key.set_value(&VK_TIME_SERIES_TYPE, ts)?;

    ctx.replicate_verbatim();
    ctx.notify_keyspace_event(NotifyEvent::MODULE, "TS.CREATE", key);
    ctx.log_verbose("series created");

    Ok(())
}
