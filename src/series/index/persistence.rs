//! RDB aux-field persistence for the postings index.
//!
//! Design: docs/postings-index-persistence.md (Option A). The whole index snapshot for all dbs is
//! written as a single length-prefixed string via the module type's `aux_save2` callback with
//! `AUX_BEFORE_RDB`, so `aux_load` runs before any key loads and consumes exactly one string from
//! the RDB stream. Any parse failure after that read is soft: the payload is discarded and the
//! index is rebuilt by the existing per-key `loaded` notification path (`index_series_by_key`).
//!
//! Fork safety: `aux_save` runs in the BGSAVE fork child, where a background task holding the
//! postings write lock at fork time would deadlock the child. All db locks are acquired with
//! `try_read`; if any is contended we write nothing at all (`aux_save2` omits the aux field
//! entirely) and the loader falls back to the rebuild path.

use super::postings::{Postings, PostingsBitmap, PostingsIndex};
use super::{TIMESERIES_INDEX, get_db_index};
use crate::common::context::{get_current_db, set_current_db};
use crate::common::encoding::{
    try_read_byte_slice, try_read_u8, try_read_uvarint, write_byte_slice, write_u8, write_uvarint,
};
use crate::common::logging::{log_debug, log_notice, log_warning};
use crate::config::is_index_persist_enabled;
use crate::series::index::IndexKey;
use crate::series::series_data_type::VK_TIME_SERIES_TYPE;
use crate::series::{SeriesRef, TimeSeries};
use croaring::{Bitmap64, Portable};
use std::collections::BTreeMap;
use std::os::raw::c_int;
use std::sync::Mutex;
use valkey_module::{MODULE_CONTEXT, RedisModuleIO, raw};

/// Identifies the aux payload; guards against reading garbage from a foreign/corrupt field.
const INDEX_AUX_MAGIC: &[u8; 4] = b"TSIX";

/// Internal payload format version, independent of `TIMESERIES_TYPE_ENCODING_VERSION`.
/// Any mismatch discards the payload and falls back to the per-key rebuild.
const INDEX_AUX_VERSION: u8 = 1;

/// Databases whose index was preloaded from the aux payload during the current load.
/// Consumed by the post-load reconciliation pass (`LoadingSubevent::Ended`) or discarded
/// on load failure.
static PRELOADED_DBS: Mutex<Vec<i32>> = Mutex::new(Vec::new());

const RECONCILE_BATCH_SIZE: usize = 256;

type PayloadResult<T> = Result<T, String>;

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

fn write_bitmap(buf: &mut Vec<u8>, bitmap: &PostingsBitmap) {
    let size = bitmap.get_serialized_size_in_bytes::<Portable>();
    write_uvarint(buf, size as u64);
    buf.reserve(size);
    let before = buf.len();
    let _ = bitmap.serialize_into_vec::<Portable>(buf);
    debug_assert_eq!(buf.len() - before, size);
}

fn read_bitmap(buf: &mut &[u8]) -> PayloadResult<PostingsBitmap> {
    let blob = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
    Bitmap64::try_deserialize::<Portable>(blob)
        .ok_or_else(|| "corrupt roaring bitmap blob".to_string())
}

/// Appends one per-db section to `buf`.
fn serialize_postings_section(buf: &mut Vec<u8>, db: i32, postings: &Postings) {
    write_uvarint(buf, db as u64);

    // label_index in tree iteration order (sorted): cache-friendly ART rebuild on load.
    write_uvarint(buf, postings.label_index.len() as u64);
    for (key, bitmap) in postings.label_index.iter() {
        // `as_str` strips the NUL sentinel; `IndexKey::from(&[u8])` re-appends it on load.
        write_byte_slice(buf, key.as_str().as_bytes());
        write_bitmap(buf, bitmap);
    }

    // id_to_key in ascending id order: ids share their high epoch bits and increment densely,
    // so varint deltas stay small.
    write_uvarint(buf, postings.id_to_key.len() as u64);
    let mut prev_id: SeriesRef = 0;
    for (id, key) in postings.id_to_key.iter() {
        write_uvarint(buf, id - prev_id);
        prev_id = *id;
        write_byte_slice(buf, key.as_ref());
    }

    write_bitmap(buf, &postings.all_postings);
    write_bitmap(buf, &postings.stale_ids);
}

fn deserialize_postings_section(buf: &mut &[u8]) -> PayloadResult<(i32, Postings)> {
    let db = try_read_uvarint(buf).map_err(|e| e.to_string())?;
    let db = i32::try_from(db).map_err(|_| format!("invalid db number {db}"))?;

    let label_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut label_index = PostingsIndex::new();
    for _ in 0..label_count {
        let key_bytes = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        let key = IndexKey::from(key_bytes);
        let bitmap = read_bitmap(buf)?;
        label_index
            .try_insert(key, bitmap)
            .map_err(|e| format!("label index insert failed: {e}"))?;
    }

    let id_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut id_to_key = BTreeMap::new();
    let mut prev_id: SeriesRef = 0;
    for _ in 0..id_count {
        let delta = try_read_uvarint(buf).map_err(|e| e.to_string())?;
        let id = prev_id
            .checked_add(delta)
            .ok_or_else(|| "series id overflow".to_string())?;
        prev_id = id;
        let key = try_read_byte_slice(buf).map_err(|e| e.to_string())?;
        id_to_key.insert(id, key.to_vec().into_boxed_slice());
    }

    let all_postings = read_bitmap(buf)?;
    let stale_ids = read_bitmap(buf)?;

    Ok((
        db,
        Postings {
            label_index,
            id_to_key,
            stale_ids,
            all_postings,
        },
    ))
}

/// Serializes every non-empty db index into a single payload buffer.
/// Returns `None` if any db's postings lock is contended (possible fork-time writer:
/// skip persisting entirely rather than risk deadlock or write a torn snapshot).
fn build_aux_payload() -> Option<Vec<u8>> {
    let map = TIMESERIES_INDEX.pin();
    let mut dbs: Vec<i32> = map.keys().copied().collect();
    dbs.sort_unstable();

    let mut sections: Vec<u8> = Vec::new();
    let mut section_count: u64 = 0;

    for db in dbs {
        let Some(index) = map.get(&db) else {
            continue;
        };
        let Ok(postings) = index.inner.try_read() else {
            log_warning(format!(
                "Postings lock for db {db} contended at aux save time; skipping index persistence"
            ));
            return None;
        };
        if postings.id_to_key.is_empty() && postings.stale_ids.is_empty() {
            continue;
        }
        serialize_postings_section(&mut sections, db, &postings);
        section_count += 1;
    }

    if section_count == 0 {
        return None;
    }

    let mut buf = Vec::with_capacity(sections.len() + 16);
    buf.extend_from_slice(INDEX_AUX_MAGIC);
    write_u8(&mut buf, INDEX_AUX_VERSION);
    write_uvarint(&mut buf, section_count);
    buf.extend_from_slice(&sections);
    Some(buf)
}

fn parse_aux_payload(mut buf: &[u8]) -> PayloadResult<Vec<(i32, Postings)>> {
    let buf = &mut buf;

    let magic = try_read_byte_slice_exact(buf, INDEX_AUX_MAGIC.len())?;
    if magic != INDEX_AUX_MAGIC {
        return Err("bad magic".to_string());
    }
    let version = try_read_u8(buf).map_err(|e| e.to_string())?;
    if version != INDEX_AUX_VERSION {
        return Err(format!(
            "unsupported index payload version {version} (expected {INDEX_AUX_VERSION})"
        ));
    }

    let section_count = try_read_uvarint(buf).map_err(|e| e.to_string())? as usize;
    let mut sections = Vec::with_capacity(section_count.min(16));
    for _ in 0..section_count {
        sections.push(deserialize_postings_section(buf)?);
    }
    if !buf.is_empty() {
        return Err(format!("{} trailing bytes after payload", buf.len()));
    }
    Ok(sections)
}

fn try_read_byte_slice_exact<'a>(buf: &mut &'a [u8], len: usize) -> PayloadResult<&'a [u8]> {
    if buf.len() < len {
        return Err(format!(
            "not enough bytes: {} available, {len} requested",
            buf.len()
        ));
    }
    let (head, tail) = buf.split_at(len);
    *buf = tail;
    Ok(head)
}

// ---------------------------------------------------------------------------
// RDB aux save / load
// ---------------------------------------------------------------------------

/// `aux_save2` body. Runs in the BGSAVE fork child; must not block on locks.
/// Writing nothing at all makes `aux_save2` omit the aux field from the RDB.
pub(crate) fn save_index_to_rdb(rdb: *mut RedisModuleIO) {
    if !is_index_persist_enabled() {
        return;
    }
    let Some(payload) = build_aux_payload() else {
        return;
    };
    log_debug(format!(
        "Persisting postings index aux payload ({} bytes)",
        payload.len()
    ));
    raw::save_slice(rdb, &payload);
}

/// `aux_load` body. Must consume exactly the string written by `save_index_to_rdb`; only a
/// failure to read it at all is a hard error (the RDB stream would be desynced). Every error
/// after that point discards the payload and falls back to the per-key rebuild.
pub(crate) fn load_index_from_rdb(rdb: *mut RedisModuleIO) -> c_int {
    let Ok(buffer) = raw::load_string_buffer(rdb) else {
        log_warning("Failed to read postings index aux payload from RDB");
        return raw::Status::Err as c_int;
    };

    if !is_index_persist_enabled() {
        log_notice(
            "ts-index-persist is disabled; discarding persisted postings index and rebuilding",
        );
        return raw::Status::Ok as c_int;
    }

    match parse_aux_payload(buffer.as_ref()) {
        Ok(sections) => {
            let mut preloaded = PRELOADED_DBS.lock().unwrap();
            for (db, postings) in sections {
                let series_count = postings.id_to_key.len();
                let index = get_db_index(db);
                *index.inner.write().unwrap() = postings;
                if !preloaded.contains(&db) {
                    preloaded.push(db);
                }
                log_notice(format!(
                    "Preloaded postings index for db {db} ({series_count} series)"
                ));
            }
        }
        Err(e) => {
            log_warning(format!(
                "Discarding persisted postings index ({e}); falling back to per-key rebuild"
            ));
        }
    }
    raw::Status::Ok as c_int
}

// ---------------------------------------------------------------------------
// Post-load reconciliation
// ---------------------------------------------------------------------------

/// Runs after a successful load (`LoadingSubevent::Ended`). A preloaded index can contain
/// "dangling" ids whose key never made it into the keyspace (e.g. its `rdb_load` was skipped);
/// nothing in the query path stale-marks those (`TS.CARD` would over-count and no-date-filter
/// `TS.QUERYINDEX` would emit phantom key names), so sweep them here. Ids for keys that loaded
/// but are missing from the index self-heal via the `has_id == false` path and need no work.
pub(crate) fn reconcile_preloaded_indexes() {
    let dbs: Vec<i32> = {
        let mut guard = PRELOADED_DBS.lock().unwrap();
        std::mem::take(&mut *guard)
    };
    if dbs.is_empty() {
        return;
    }

    // Off the main thread: the sweep opens every indexed key once.
    std::thread::spawn(move || {
        for db in dbs {
            reconcile_db(db);
        }
    });
}

fn reconcile_db(db: i32) {
    let mut cursor: SeriesRef = 0;
    let mut checked = 0usize;
    let mut dangling = 0usize;

    loop {
        // Snapshot a window of (id, key) pairs under the read lock, then drop it before
        // touching the keyspace or taking the write lock.
        let window: Vec<(SeriesRef, Box<[u8]>)> = {
            let index = get_db_index(db);
            let postings = index.inner.read().unwrap();
            postings
                .id_to_key
                .range(cursor..)
                .take(RECONCILE_BATCH_SIZE)
                .map(|(id, key)| (*id, key.clone()))
                .collect()
        };
        let Some(&(last_id, _)) = window.last() else {
            break;
        };
        cursor = last_id + 1;

        let mut missing: Vec<SeriesRef> = Vec::new();
        {
            let ctx = MODULE_CONTEXT.lock();
            let save_db = get_current_db(&ctx);
            set_current_db(&ctx, db);
            for (id, key) in &window {
                let valkey_key = ctx.create_string(key.as_ref());
                let key_handle = ctx.open_key(&valkey_key);
                match key_handle.get_value::<TimeSeries>(&VK_TIME_SERIES_TYPE) {
                    Ok(Some(series)) if series.id == *id => {}
                    _ => missing.push(*id),
                }
            }
            set_current_db(&ctx, save_db);
        }

        checked += window.len();
        if !missing.is_empty() {
            dangling += missing.len();
            let index = get_db_index(db);
            let mut postings = index.inner.write().unwrap();
            postings.mark_ids_as_stale(&missing);
        }
    }

    if dangling > 0 {
        log_notice(format!(
            "Postings index reconciliation for db {db}: {checked} ids checked, {dangling} dangling ids marked stale"
        ));
    } else {
        log_debug(format!(
            "Postings index reconciliation for db {db}: {checked} ids checked, none dangling"
        ));
    }
}

/// Runs when a load fails (`LoadingSubevent::Failed`). The keyspace state after a failed load is
/// engine-dependent (startup aborts; a replica may restore its pre-sync dataset), so a preloaded
/// index cannot be trusted — drop it and let the natural indexing paths rebuild.
pub(crate) fn discard_preloaded_indexes() {
    let dbs: Vec<i32> = {
        let mut guard = PRELOADED_DBS.lock().unwrap();
        std::mem::take(&mut *guard)
    };
    for db in dbs {
        let index = get_db_index(db);
        index.inner.write().unwrap().clear();
        log_warning(format!(
            "Load failed; discarded preloaded postings index for db {db}"
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_postings() -> Postings {
        let mut postings = Postings::default();
        let mut bmp_a = PostingsBitmap::new();
        bmp_a.add_many(&[1, 2, 3]);
        let mut bmp_b = PostingsBitmap::new();
        bmp_b.add_many(&[2, 3]);
        postings
            .label_index
            .try_insert(IndexKey::for_label_value("region", "us-east-1"), bmp_a)
            .unwrap();
        postings
            .label_index
            .try_insert(IndexKey::for_label_value("service", "api"), bmp_b)
            .unwrap();
        for (id, key) in [(1u64, "ts:one"), (2, "ts:two"), (3, "ts:three")] {
            postings
                .id_to_key
                .insert(id, key.as_bytes().to_vec().into_boxed_slice());
            postings.all_postings.add(id);
        }
        postings.stale_ids.add(99);
        postings
    }

    fn assert_postings_eq(a: &Postings, b: &Postings) {
        assert_eq!(a.id_to_key, b.id_to_key);
        assert_eq!(a.all_postings, b.all_postings);
        assert_eq!(a.stale_ids, b.stale_ids);
        assert_eq!(a.label_index.len(), b.label_index.len());
        for ((ka, va), (kb, vb)) in a.label_index.iter().zip(b.label_index.iter()) {
            assert_eq!(ka, kb);
            assert_eq!(va, vb);
        }
    }

    fn build_payload(sections: &[(i32, &Postings)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (db, postings) in sections {
            serialize_postings_section(&mut body, *db, postings);
        }
        let mut buf = Vec::new();
        buf.extend_from_slice(INDEX_AUX_MAGIC);
        write_u8(&mut buf, INDEX_AUX_VERSION);
        write_uvarint(&mut buf, sections.len() as u64);
        buf.extend_from_slice(&body);
        buf
    }

    #[test]
    fn payload_roundtrip_single_db() {
        let postings = sample_postings();
        let payload = build_payload(&[(0, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].0, 0);
        assert_postings_eq(&sections[0].1, &postings);
    }

    #[test]
    fn payload_roundtrip_multiple_dbs() {
        let postings_a = sample_postings();
        let postings_b = Postings::default();
        let payload = build_payload(&[(0, &postings_a), (5, &postings_b)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].0, 0);
        assert_eq!(sections[1].0, 5);
        assert_postings_eq(&sections[0].1, &postings_a);
        assert_postings_eq(&sections[1].1, &postings_b);
    }

    #[test]
    fn payload_roundtrip_empty_postings() {
        let postings = Postings::default();
        let payload = build_payload(&[(2, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].0, 2);
        assert_postings_eq(&sections[0].1, &postings);
    }

    #[test]
    fn rejects_bad_magic() {
        let postings = sample_postings();
        let mut payload = build_payload(&[(0, &postings)]);
        payload[0] ^= 0xFF;
        assert!(parse_aux_payload(&payload).is_err());
    }

    #[test]
    fn rejects_unknown_version() {
        let postings = sample_postings();
        let mut payload = build_payload(&[(0, &postings)]);
        payload[INDEX_AUX_MAGIC.len()] = INDEX_AUX_VERSION + 1;
        assert!(parse_aux_payload(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_payload() {
        let postings = sample_postings();
        let payload = build_payload(&[(0, &postings)]);
        for len in 0..payload.len() {
            assert!(
                parse_aux_payload(&payload[..len]).is_err(),
                "truncation at {len} bytes should fail"
            );
        }
    }

    #[test]
    fn rejects_trailing_garbage() {
        let postings = sample_postings();
        let mut payload = build_payload(&[(0, &postings)]);
        payload.push(0xAB);
        assert!(parse_aux_payload(&payload).is_err());
    }

    #[test]
    fn rejects_corrupt_bitmap_blob() {
        // A blob whose declared length is intact but whose contents are not a valid portable
        // bitmap: the leading u64 bucket count is absurd relative to the remaining bytes.
        // (Bit flips that still decode as *some* valid bitmap are the RDB CRC's job to catch.)
        let mut buf = Vec::new();
        write_byte_slice(&mut buf, &[0xFF; 9]);
        let mut slice = buf.as_slice();
        assert!(read_bitmap(&mut slice).is_err());
    }

    #[test]
    fn id_delta_encoding_handles_epoch_style_ids() {
        // Ids with large shared high bits (epoch) and dense low bits.
        let epoch = 0x0000_ABCD_u64 << 40;
        let mut postings = Postings::default();
        for i in 1..=100u64 {
            let id = epoch | i;
            postings
                .id_to_key
                .insert(id, format!("key:{i}").into_bytes().into_boxed_slice());
            postings.all_postings.add(id);
        }
        let payload = build_payload(&[(0, &postings)]);
        let sections = parse_aux_payload(&payload).unwrap();
        assert_postings_eq(&sections[0].1, &postings);
    }
}
