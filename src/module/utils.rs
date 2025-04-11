use crate::error_consts;
use crate::module::VK_TIME_SERIES_TYPE;
use crate::series::TimeSeries;
use std::ops::{Deref, DerefMut};
use valkey_module::key::ValkeyKey;
use valkey_module::{AclPermissions, Context, ValkeyError, ValkeyResult, ValkeyString};

pub struct SeriesGuard {
    key: ValkeyKey,
}

impl SeriesGuard {
    pub fn get_series(&self) -> &TimeSeries {
        self.key
            .get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)
            .expect("Key existence should be checked before deref")
            .unwrap()
    }
}

impl Deref for SeriesGuard {
    type Target = TimeSeries;

    fn deref(&self) -> &Self::Target {
        self.get_series()
    }
}

impl AsRef<TimeSeries> for SeriesGuard {
    fn as_ref(&self) -> &TimeSeries {
        self.get_series()
    }
}

pub struct SeriesGuardMut<'a> {
    series: &'a mut TimeSeries,
}

impl SeriesGuardMut<'_> {
    pub fn get_series_mut(&mut self) -> &mut TimeSeries {
        self.deref_mut()
    }
}

impl Deref for SeriesGuardMut<'_> {
    type Target = TimeSeries;

    fn deref(&self) -> &Self::Target {
        self.series
    }
}

impl DerefMut for SeriesGuardMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.series
    }
}

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
    permissions: AclPermissions,
) -> ValkeyResult<()> {
    let user = ctx.get_current_user();
    if ctx
        .acl_check_key_permission(&user, key, &permissions)
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
            check_key_permissions(ctx, key, AclPermissions::ACCESS)?;
        }
        f(series)
    } else {
        Err(invalid_series_key_error())
    }
}

pub fn with_timeseries_mut<R>(
    ctx: &Context,
    key: &ValkeyString,
    f: impl FnOnce(&mut TimeSeries) -> ValkeyResult<R>,
) -> ValkeyResult<R> {
    // expect should not panic, since must_exist will cause an error if the key is non-existent, and `?` will ensure it propagates
    let mut series = get_timeseries_mut(ctx, key, true)?.expect("expected key to exist");
    f(&mut series)
}

#[allow(dead_code)]
pub fn get_timeseries(
    ctx: &Context,
    key: &ValkeyString,
    permissions: Option<AclPermissions>,
) -> ValkeyResult<SeriesGuard> {
    if let Some(permissions) = permissions {
        check_key_permissions(ctx, key, permissions)?;
    }

    let redis_key = ctx.open_key(key);
    match redis_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Err(_) => Err(ValkeyError::Str(error_consts::INVALID_TIMESERIES_KEY)),
        Ok(Some(_)) => Ok(SeriesGuard { key: redis_key }),
        Ok(None) => Err(ValkeyError::Str(error_consts::KEY_NOT_FOUND)),
    }
}

pub fn get_timeseries_mut<'a>(
    ctx: &'a Context,
    key: &ValkeyString,
    must_exist: bool,
) -> ValkeyResult<Option<SeriesGuardMut<'a>>> {
    let value_key = ctx.open_key_writable(key);
    match value_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
        Ok(Some(series)) => {
            check_key_permissions(ctx, key, AclPermissions::UPDATE)?;
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
