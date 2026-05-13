//! Handle scanning keyspace for time series that were imported because of a cluster migration.
//! Instead of executing individual commands that touch keys directly, the source node forks a child process
//! to snapshot the slot and streams the data directly to the target node using an AOF/replication-like payload.
//! Because this bulk data transfer mimics replica synchronization rather than normal client-driven data
//! modifications, no individual key creation or mutation events are triggered on either side during
//! the bulk transfer phase. In normal operation, we use these events to maintain the inverted index.

//! We therefore need to scan the keyspace, so we can identify any time series that were imported during this period
//! and ensure that they are indexed.

use crate::common::hash::IntMap;
use crate::series::TimeSeries;
use crate::series::index::{next_timeseries_id, with_timeseries_postings_mut};
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use ahash::HashMapExt;
use valkey_module::{Context, KeysCursor, ValkeyString, key::ValkeyKey};

const BATCH_SIZE: usize = 40;

pub(crate) fn reindex_db(ctx: &Context) {
    let mut id_remap: IntMap<u64, u64> = IntMap::with_capacity(256);

    let scan_callback = |ctx: &Context, key_name: ValkeyString, key: Option<&ValkeyKey>| {
        if key.is_none() {
            return;
        }

        let writeable_key = ctx.open_key_writable(&key_name);
        let Ok(Some(series)) = writeable_key.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) else {
            return;
        };

        // _db == None means "imported during migration" in this flow.
        if series._db.is_some() {
            return;
        }

        with_timeseries_postings_mut(ctx, |postings| {
            let id = if postings.has_id(series.id) {
                *id_remap.entry(series.id).or_insert_with(next_timeseries_id)
            } else {
                series.id
            };

            series.id = id;

            postings.index_timeseries(series, key_name.as_slice());
        });
    };

    let cursor = KeysCursor::new();
    while cursor.scan(ctx, &scan_callback) {}
}
