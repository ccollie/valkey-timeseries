use crate::common::constants::METRIC_NAME_LABEL;
use crate::error_consts;
use crate::series::index::{next_timeseries_id, with_timeseries_index};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesGuard, SeriesGuardMut, TimeSeries, TimeSeriesOptions};
use valkey_module::key::ValkeyKeyWritable;
use valkey_module::{
    AclPermissions, Context, NotifyEvent, ValkeyError, ValkeyResult, ValkeyString,
};


#[inline]
fn has_key_permissions(ctx: &Context, key: &ValkeyString, permissions: AclPermissions) -> bool {
    let user = ctx.get_current_user();
    ctx.acl_check_key_permission(&user, key, &permissions)
        .is_ok()
}

pub fn check_key_read_permission(ctx: &Context, key: &ValkeyString) -> bool {
    has_key_permissions(ctx, key, AclPermissions::ACCESS)
}

#[inline]
pub fn check_key_permissions(
    ctx: &Context,
    key: &ValkeyString,
    permissions: &AclPermissions,
) -> ValkeyResult<()> {
    let user = ctx.get_current_user();
    if ctx
        .acl_check_key_permission(&user, key, permissions)
        .is_ok()
    {
        Ok(())
    } else {
        if permissions.contains(AclPermissions::DELETE) {
            return Err(ValkeyError::Str(error_consts::KEY_DELETE_PERMISSION_ERROR));
        }
        if permissions.contains(AclPermissions::UPDATE) {
            return Err(ValkeyError::Str(error_consts::KEY_WRITE_PERMISSION_ERROR));
        }
        Err(ValkeyError::Str(error_consts::KEY_READ_PERMISSION_ERROR))
    }
}

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

#[allow(dead_code)]
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
        Err(_) => Err(ValkeyError::Str(error_consts::INVALID_TIMESERIES_KEY)),
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
        Err(_e) => Err(ValkeyError::Str(error_consts::INVALID_TIMESERIES_KEY)),
    }
}

pub(crate) fn invalid_series_key_error() -> ValkeyError {
    ValkeyError::Str(error_consts::KEY_NOT_FOUND)
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
    with_timeseries_index(ctx, |index| {
        // Check if this refers to an existing series (a pre-existing series with the same label-value pairs)
        // We do this only in the case where we have a __name__ label, signalling that the user is
        // opting in to Prometheus semantics, meaning a metric name is unique to a series.
        if ts.labels.get_value(METRIC_NAME_LABEL).is_some() {
            let labels = ts.labels.to_label_vec();
            // will return an error if the series already exists
            let existing_id = index.posting_by_labels(&labels)?;
            if let Some(_id) = existing_id {
                return Err(ValkeyError::Str(error_consts::DUPLICATE_SERIES));
            }
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
