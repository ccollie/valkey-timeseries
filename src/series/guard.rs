use std::ops::{Deref, DerefMut};
use valkey_module::key::ValkeyKey;
use valkey_module::{Context, ValkeyString};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::TimeSeries;

pub struct SeriesGuard {
    pub(super) key_inner: ValkeyString,
    pub(super) key: ValkeyKey,
}

impl SeriesGuard {
    pub(crate) fn open(ctx: &Context, key: ValkeyString) -> Self {
        let key_ = ValkeyKey::open(ctx.ctx, &key);
        SeriesGuard {
            key: key_,
            key_inner: key,
        }
    }

    pub fn get_series(&self) -> &TimeSeries {
        self.key
            .get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE)
            .expect("Key existence should be checked before deref")
            .unwrap()
    }

    pub fn get_key(&self) -> &ValkeyString {
        &self.key_inner
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
    pub(crate) series: &'a mut TimeSeries,
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