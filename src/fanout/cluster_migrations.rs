use crate::fanout::cluster_map::NUM_SLOTS;
use crate::fanout::is_clustered;
use range_set_blaze::RangeSetBlaze;
use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::AtomicBool;
use std::sync::{LazyLock, RwLock};
use valkey_module::{
    CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context, Version, raw,
};

const ASM_MINIMUM_VERSION: Version = Version {
    major: 9,
    minor: 0,
    patch: 0,
};

/// Representation of a single slot migration entry returned by
/// `CLUSTER GETSLOTMIGRATIONS`.
#[derive(Debug, Clone, Default)]
pub struct SlotMigration {
    pub name: String,
    pub operation: String,
    pub slot_ranges: RangeSetBlaze<u16>,
    pub target_node: Option<String>,
    pub source_node: Option<String>,
    pub create_time: i64,
    pub last_update_time: i64,
    pub last_ack_time: i64,
    pub state: String,
    pub message: String,
    pub cow_size: i64,
    pub remaining_repl_size: i64,
}

pub fn supports_atomic_slot_migration(ctx: &Context) -> bool {
    if !is_clustered(ctx) {
        return false;
    }
    match ctx.get_server_version() {
        Err(e) => {
            ctx.log_warning(&format!("Error getting server version: {e}"));
            false
        }
        Ok(ver) => {
            ver.major >= ASM_MINIMUM_VERSION.major
                && ver.minor >= ASM_MINIMUM_VERSION.minor
                && ver.patch >= ASM_MINIMUM_VERSION.patch
        }
    }
}

/// Call CLUSTER GETSLOTMIGRATIONS and parse the result into a Vec<SlotMigration>.
pub fn get_slot_migrations(ctx: &Context) -> Result<Vec<SlotMigration>, String> {
    let call_options = CallOptionsBuilder::new()
        .resp(CallOptionResp::Resp3)
        .errors_as_replies()
        .build();

    let res: CallResult =
        ctx.call_ext::<_, CallResult>("CLUSTER", &call_options, &["GETSLOTMIGRATIONS"]);

    let top = match res {
        Err(e) => return Err(format!("Error calling CLUSTER GETSLOTMIGRATIONS: {e}")),
        Ok(CallReply::Array(arr)) => {
            debug_assert!(
                arr.len() % 2 == 0,
                "Expected even number of entries in CLUSTER GETSLOTMIGRATIONS result"
            );
            arr
        }
        _ => return Err("CLUSTER GETSLOTMIGRATIONS did not return an array".to_string()),
    };

    let mut out = Vec::with_capacity(top.len());

    for job_entry in top.iter().flatten() {
        let map_arr = match job_entry {
            CallReply::Array(arr) => arr,
            other => {
                ctx.log_warning(&format!("Unexpected slot migration entry type: {other:?}"));
                continue;
            }
        };

        let mut mig = SlotMigration::default();

        let set_string = |idx: usize, field: &mut String| {
            *field = map_arr.get(idx).and_then(as_str).unwrap_or_default();
        };

        let set_slots = |idx: usize, field: &mut RangeSetBlaze<u16>| {
            let s = map_arr.get(idx).and_then(as_str).unwrap_or_default();
            match parse_slot_ranges(&s) {
                Ok(ranges) => *field = ranges,
                Err(e) => ctx.log_warning(&format!("Error parsing slot ranges '{s}': {e}")),
            }
        };

        let set_i64 = |idx: usize, field: &mut i64| {
            *field = map_arr.get(idx).and_then(as_i64).unwrap_or(0);
        };

        let set_optional_node = |idx: usize, field: &mut Option<String>| {
            let s = map_arr.get(idx).and_then(as_str).unwrap_or_default();
            if !s.is_empty() {
                *field = Some(s);
            }
        };

        let mut i = 0usize;
        while i + 1 < map_arr.len() {
            let key = map_arr.get(i).and_then(as_str).unwrap_or_default();
            let val_idx = i + 1;

            hashify::fnc_map_ignore_case!(key.as_bytes(),
                "name" => set_string(val_idx, &mut mig.name),
                "operation" => set_string(val_idx, &mut mig.operation),
                "slot_ranges" => set_slots(val_idx, &mut mig.slot_ranges),
                "target_node" => set_optional_node(val_idx, &mut mig.target_node),
                "source_node" => set_optional_node(val_idx, &mut mig.source_node),
                "create_time" => set_i64(val_idx, &mut mig.create_time),
                "last_update_time" => set_i64(val_idx, &mut mig.last_update_time),
                "last_ack_time" => set_i64(val_idx, &mut mig.last_ack_time),
                "state" => set_string(val_idx, &mut mig.state),
                "message" => set_string(val_idx, &mut mig.message),
                "cow_size" => set_i64(val_idx, &mut mig.cow_size),
                "remaining_repl_size" => set_i64(val_idx, &mut mig.remaining_repl_size),
                _ => {
                    // Unknown key - ignore
                }
            );

            i += 2;
        }

        out.push(mig);
    }

    Ok(out)
}

fn as_str(r: CallResult) -> Option<String> {
    match r {
        Ok(CallReply::String(v)) => v.to_string(),
        _ => None,
    }
}

fn as_i64(r: CallResult) -> Option<i64> {
    match r {
        Ok(CallReply::I64(v)) => Some(v.to_i64()),
        _ => None,
    }
}

/// Parse a slot ranges string into a vector of inclusive ranges.
///
/// Examples accepted:
/// - "0-100"
/// - "0-100 200-300"
/// - "0-100,200-300"
/// - "5" (single slot)
fn parse_slot_ranges(s: &str) -> Result<RangeSetBlaze<u16>, String> {
    let mut out = RangeSetBlaze::new();
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(out);
    }

    // Accept spaces or commas as separators
    for token in trimmed
        .split(|c: char| c.is_whitespace() || c == ',')
        .filter(|t| !t.is_empty())
    {
        let token = token.trim();
        // Expect either "start-end" or a single number
        if let Some(pos) = token.find('-') {
            let (a, b) = token.split_at(pos);
            let start_str = a.trim();
            let end_str = b[1..].trim(); // skip '-'
            let start: u16 = start_str
                .parse()
                .map_err(|e| format!("Invalid start slot '{start_str}': {e}"))?;
            let end: u16 = end_str
                .parse()
                .map_err(|e| format!("Invalid end slot '{end_str}': {e}"))?;
            if start > end {
                return Err(format!(
                    "Start slot {} greater than end slot {}",
                    start, end
                ));
            }
            if end >= NUM_SLOTS {
                return Err(format!(
                    "End slot {} out of range (must be < {})",
                    end, NUM_SLOTS
                ));
            }
            out.ranges_insert(start..=end);
        } else {
            // single slot
            let v: u16 = token
                .parse()
                .map_err(|e| format!("Invalid slot '{}': {e}", token))?;
            if v >= NUM_SLOTS {
                return Err(format!("Slot {} out of range (must be < {})", v, NUM_SLOTS));
            }
            out.ranges_insert(v..=v);
        }
    }

    Ok(out)
}

// symbolic constants for atomic slot migration subevents (from valkeymodule.h).
// Prefer to read the event id from the `raw` bindings when available; otherwise
// fall back to the numeric constant below.
const VALKEYMODULE_EVENT_ATOMIC_SLOT_MIGRATION: u64 = 19u64;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_STARTED: u64 = 0;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_STARTED: u64 = 1;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_ABORTED: u64 = 2;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_ABORTED: u64 = 3;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_COMPLETED: u64 = 4;
const VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_COMPLETED: u64 = 5;

const VALKEYMODULE_NODE_ID_LEN: usize = 40;

pub enum AtomicSlotMigrationEvent {
    ImportStarted,
    ExportStarted,
    ImportAborted,
    ExportAborted,
    ImportCompleted,
    ExportCompleted,
}

pub type AtomicSlotMigrationEventHandler =
    fn(event: AtomicSlotMigrationEvent, slots: RangeSetBlaze<u16>);
pub type PostMigrationCleanupFn = fn(slots: RangeSetBlaze<u16>);

static IN_SLOT_IMPORT: AtomicBool = AtomicBool::new(false);
static EVENT_HANDLER_FN: LazyLock<RwLock<Option<AtomicSlotMigrationEventHandler>>> =
    LazyLock::new(|| RwLock::new(None));

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ValkeyModuleSlotRange {
    pub start: c_int, // Start slot, inclusive.
    pub end: c_int,   // End slot, inclusive.
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ValkeyModuleAtomicSlotMigrationInfoV1 {
    pub version: u64, // Version of this structure for ABI compat.
    pub job_name: [c_char; VALKEYMODULE_NODE_ID_LEN + 1], // Unique ID for the migration operation.
    pub slot_ranges: *mut ValkeyModuleSlotRange, // Array of slot ranges involved in the migration.
    pub num_slot_ranges: u32, // Number of slot ranges in the array.
}

impl ValkeyModuleAtomicSlotMigrationInfoV1 {
    pub fn job_name_str(&self) -> String {
        let c_str = unsafe { std::ffi::CStr::from_ptr(self.job_name.as_ptr()) };
        c_str.to_string_lossy().into_owned()
    }

    fn convert_slot_ranges(&self) -> RangeSetBlaze<u16> {
        let mut ranges = RangeSetBlaze::new();
        self.extend_slot_ranges(&mut ranges);
        ranges
    }

    fn extend_slot_ranges(&self, dest: &mut RangeSetBlaze<u16>) {
        for i in 0..self.num_slot_ranges {
            unsafe {
                let range = *self.slot_ranges.add(i as usize);
                let start = range.start as u16;
                let end = range.end as u16;
                dest.extend(start..=end);
            }
        }
    }

    fn remove_slots_from(&self, dest: &mut RangeSetBlaze<u16>) {
        for i in 0..self.num_slot_ranges {
            unsafe {
                let range = *self.slot_ranges.add(i as usize);
                let start = range.start as u16;
                let end = range.end as u16;
                dest.retain(|x| !(start..=end).contains(x));
            }
        }
    }
}

pub type ValkeyModuleAtomicSlotMigrationInfo = ValkeyModuleAtomicSlotMigrationInfoV1;

unsafe extern "C" fn on_atomic_slot_migration_event(
    _ctx: *mut raw::RedisModuleCtx,
    _eid: raw::RedisModuleEvent,
    sub_event: u64,
    data: *mut c_void,
) {
    fn raise_event(event: AtomicSlotMigrationEvent, data: *mut c_void) {
        if let Some(handler) = *EVENT_HANDLER_FN.read().unwrap() {
            let info = unsafe { &*(data as *const ValkeyModuleAtomicSlotMigrationInfo) };
            let slots = info.convert_slot_ranges();
            handler(event, slots);
        }
    }

    match sub_event {
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_STARTED => {
            IN_SLOT_IMPORT.store(true, std::sync::atomic::Ordering::SeqCst);
            raise_event(AtomicSlotMigrationEvent::ImportStarted, data);
        }
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_COMPLETED => {
            IN_SLOT_IMPORT.store(false, std::sync::atomic::Ordering::SeqCst);
            raise_event(AtomicSlotMigrationEvent::ImportCompleted, data);
        }
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_IMPORT_ABORTED => {
            IN_SLOT_IMPORT.store(false, std::sync::atomic::Ordering::SeqCst);
            raise_event(AtomicSlotMigrationEvent::ImportAborted, data);
        }
        VALKEYMODULE_SUBEVENT_ATOMIC_SLOT_MIGRATION_EXPORT_COMPLETED => {
            raise_event(AtomicSlotMigrationEvent::ExportCompleted, data);
        }
        _ => {}
    }
}

pub(super) fn register_atomic_slot_migration_event_handler(
    ctx: &Context,
    on_event: Option<AtomicSlotMigrationEventHandler>,
) {
    {
        let mut guard = EVENT_HANDLER_FN.write().unwrap();
        *guard = on_event;
    }
    let res = unsafe {
        raw::RedisModule_SubscribeToServerEvent.unwrap()(
            ctx.ctx,
            raw::RedisModuleEvent {
                id: VALKEYMODULE_EVENT_ATOMIC_SLOT_MIGRATION,
                dataver: 1,
            },
            Some(on_atomic_slot_migration_event),
        )
    };
    if res != raw::REDISMODULE_OK as i32 {
        ctx.log_warning("Failed to subscribe to atomic slot migration events");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_range() {
        let s = "0-100";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(*ranges[0].start(), 0);
        assert_eq!(*ranges[0].end(), 100);
    }

    #[test]
    fn test_parse_multiple_ranges_space() {
        let s = "0-10 20-30";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 2);
        assert_eq!(*ranges[0].start(), 0);
        assert_eq!(*ranges[0].end(), 10);
        assert_eq!(*ranges[1].start(), 20);
        assert_eq!(*ranges[1].end(), 30);
    }

    #[test]
    fn test_parse_multiple_ranges_comma() {
        let s = "0-10,20-30";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 2);
        assert_eq!(*ranges[0].start(), 0);
        assert_eq!(*ranges[0].end(), 10);
        assert_eq!(*ranges[1].start(), 20);
        assert_eq!(*ranges[1].end(), 30);
    }

    #[test]
    fn test_parse_single_slot() {
        let s = "5";
        let r = parse_slot_ranges(s).unwrap();
        let ranges: Vec<_> = r.ranges().collect();
        assert_eq!(ranges.len(), 1);
        assert_eq!(*ranges[0].start(), 5);
        assert_eq!(*ranges[0].end(), 5);
    }
}
