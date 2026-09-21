use crate::commands::BorrowedArgs;
use crate::commands::command_parser::{parse_plain_timestamp, parse_timestamp, parse_value_arg};
use crate::common::block_on_keys::signal_timeseries_ready;
use crate::common::context::notify_module_event;
use crate::common::replies::{reply_error_string, reply_with_array, reply_with_integer};
use crate::common::time::current_time_millis;
use crate::common::{Sample, Timestamp};
use crate::error_consts;
use crate::series::{
    PerSeriesSamples, SampleAddResult, SeriesGuardMut, TimeSeries, apply_group_retention,
    merge_groups, run_group_compactions,
};
use smallvec::SmallVec;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

/// One `key timestamp value` triple of the command, in argument order.
struct Item<'a> {
    key: &'a ValkeyString,
    /// The timestamp argument as given, or `None` for `*`: the replica must then
    /// see the resolved current time (`Parsed::current_ts`) instead.
    raw_timestamp: Option<&'a ValkeyString>,
    raw_value: &'a ValkeyString,
    timestamp: Timestamp,
    value: f64,
    /// The series `key` holds. Null when the item already failed (missing key,
    /// wrong type, unparsable timestamp or value), in which case `res` says why.
    series: *mut TimeSeries,
    /// Index of the first item in the command naming the same series (itself
    /// when it is the first). Items are grouped by the series they resolve to,
    /// not by key bytes, so a duplicate is found by pointer identity.
    first: u32,
    /// Number of items naming this series. Meaningful on the first item only.
    count: u32,
    /// Position of this series' group in `apply`'s groups, or [`NO_GROUP`]
    /// until the group exists. Meaningful on the first item only.
    group: u32,
    /// For a series named once: its last timestamp and sample count before the
    /// add, for compaction and the reader wake-up. Meaningful on that item only.
    prev_last: Option<Timestamp>,
    samples_before: usize,
    /// The item's outcome: a parse-time error, or the write's result.
    res: SampleAddResult,
    /// The item resolved to a usable series and parsed. Replication and the
    /// `ts.add` event follow this, not the write's outcome: an item the series
    /// itself rejected (a duplicate under BLOCK, a sample below retention) is
    /// still announced, as RTS does, and replayed on the replica to the same end.
    reached: bool,
}

const NO_GROUP: u32 = u32::MAX;

acl_categories!(TS_MADD, "ts.madd", "fast write timeseries");

/// The `ValkeyModuleCmdFunc` for `ts.madd`.
///
/// Registered by hand rather than through `#[valkey_module_macros::command]`:
/// the macro's entry point retains and frees every argument to hand the handler
/// a `Vec<ValkeyString>` — two API calls per argument, 770 for a 128-sample
/// batch — while the server already guarantees argv for the duration of the
/// call, so [`BorrowedArgs`] lends them instead. [`ts_madd_command_info`] is
/// what the attribute would have generated; keep the two in step.
extern "C" fn ts_madd_entry(
    ctx: *mut valkey_module::raw::RedisModuleCtx,
    argv: *mut *mut valkey_module::raw::RedisModuleString,
    argc: std::os::raw::c_int,
) -> std::os::raw::c_int {
    let context = Context::new(ctx);
    let args = BorrowedArgs::new(ctx, argv, argc);
    let response = ts_madd(&context, args.as_slice());
    context.reply(response) as std::os::raw::c_int
}

#[linkme::distributed_slice(valkey_module::commands::COMMANDS_LIST)]
fn ts_madd_command_info() -> Result<valkey_module::commands::CommandInfo, ValkeyError> {
    use valkey_module::commands::{BeginSearch, CommandInfo, FindKeys, KeySpec, KeySpecFlags};
    let key_spec = vec![KeySpec::new(
        None,
        KeySpecFlags::READ_WRITE | KeySpecFlags::UPDATE,
        BeginSearch::new_index(1),
        FindKeys::new_range(-1, 3, 0),
    )];
    Ok(CommandInfo::new(
        "ts.madd".to_owned(),
        Some("write deny-oom".to_owned()),
        Some("Append new samples to one or more time series.".to_owned()),
        Some("O(N) where N is the number of samples added.".to_owned()),
        Some("1.0.0".to_owned()),
        None,
        -4,
        key_spec,
        ts_madd_entry,
    ))
}

/// TS.MADD key timestamp value [key timestamp value ...]
///
/// Almost every MADD carries one sample per series, so that is the path kept
/// lean: each key is opened once, in argument order, and its sample is appended
/// directly, with the result written back into the item. Only a series named
/// more than once in the same command collects its samples into a
/// [`PerSeriesSamples`] group and goes through [`merge_groups`], which owns
/// the in-batch ordering and duplicate-policy rules.
///
/// The command runs in three phases across *all* series — every add, then
/// every compaction, then every retention trim (see [`merge_groups`]) — never
/// add-then-compact per series: a compaction writes into another series that
/// the same command may also name, and which items it then accepts depends on
/// the order.
///
/// No module-side ACL check: the command's key spec covers every key, so the
/// server has already refused a denied key before this handler ran.
fn ts_madd(ctx: &Context, args: &[ValkeyString]) -> ValkeyResult {
    let arg_count = args.len() - 1;

    if arg_count < 3 || !arg_count.is_multiple_of(3) {
        return Err(ValkeyError::WrongArity);
    }

    let Parsed {
        mut items,
        has_repeats,
        current_ts,
    } = parse_items(ctx, &args[1..])?;

    apply(ctx, &mut items, has_repeats)?;

    handle_replication(ctx, &items, current_ts.as_ref());

    // Reply straight from the results rather than through a `ValkeyValue` tree: one array
    // header and one integer (or error) per item, no per-item enum and no `Vec` of them.
    reply_with_array(ctx, items.len());
    for item in &items {
        match item.res {
            SampleAddResult::Ok(sample) => reply_with_integer(ctx, sample.timestamp),
            SampleAddResult::Ignored(ts) => reply_with_integer(ctx, ts),
            // Per-item failures must be real RESP error replies inside the array (clients
            // type-check the elements), matching RTS.
            SampleAddResult::Duplicate => reply_error_string(ctx, error_consts::DUPLICATE_SAMPLE),
            SampleAddResult::TooOld => reply_error_string(ctx, error_consts::SAMPLE_TOO_OLD),
            SampleAddResult::Error(e) => reply_error_string(ctx, e),
        };
    }
    Ok(ValkeyValue::NoReply)
}

struct Parsed<'a> {
    items: Vec<Item<'a>>,
    /// Some series is named by more than one item.
    has_repeats: bool,
    /// The current time as an argument string, built when some item used `*`:
    /// the replica must then see the resolved arguments, not the original ones.
    current_ts: Option<ValkeyString>,
}

/// Resolves every item's series and parses its timestamp and value, in argument
/// order. A key that cannot be written is a per-item error and leaves its
/// timestamp and value unparsed, as RTS does.
fn parse_items<'a>(ctx: &'a Context, args: &'a [ValkeyString]) -> ValkeyResult<Parsed<'a>> {
    let sample_count = args.len() / 3;
    let mut items: Vec<Item<'a>> = Vec::with_capacity(sample_count);
    let mut has_repeats = false;
    let mut current_ts: Option<ValkeyString> = None;
    let mut seen = SeriesTable::with_capacity(sample_count);
    // The previous item's key bytes and outcome: a client batching several
    // samples of one series sends its key in consecutive items, and those must
    // not open the key again.
    let mut prev: Option<(&[u8], Result<*mut TimeSeries, SampleAddResult>)> = None;

    for chunk in args.chunks_exact(3) {
        let key = &chunk[0];
        let key_bytes = key.as_slice();
        let raw_timestamp_in = &chunk[1];
        let raw_value = &chunk[2];

        // Normalize replication timestamp first ("*" becomes the concrete current timestamp)
        let timestamp_bytes = raw_timestamp_in.as_slice();
        let plain_timestamp = parse_plain_timestamp(timestamp_bytes);
        let raw_timestamp = if timestamp_bytes == b"*" {
            if current_ts.is_none() {
                let now = current_time_millis();
                current_ts = Some(ctx.create_string(now.to_string()));
            }
            None
        } else {
            Some(raw_timestamp_in)
        };

        let index = items.len() as u32;
        let mut item = Item {
            key,
            raw_timestamp,
            raw_value,
            timestamp: 0,
            value: 0.0,
            series: std::ptr::null_mut(),
            first: index,
            count: 1,
            group: NO_GROUP,
            prev_last: None,
            samples_before: 0,
            res: SampleAddResult::Ok(Sample::default()),
            reached: false,
        };

        let opened = match prev {
            Some((prev_key, outcome)) if prev_key == key_bytes => outcome,
            _ => open_series(ctx, key),
        };
        prev = Some((key_bytes, opened));
        match opened {
            Ok(series) => {
                item.series = series;
                if let Some(first) = seen.insert(series, index, &items) {
                    has_repeats = true;
                    item.first = first;
                    items[first as usize].count += 1;
                }
            }
            Err(res) => item.res = res,
        }

        // Parse timestamp/value only if the series is usable.
        if item.res.is_ok() {
            if let Some(ts) = plain_timestamp {
                item.timestamp = ts;
            } else {
                match parse_timestamp(std::str::from_utf8(timestamp_bytes)?) {
                    Ok(ts) => item.timestamp = ts,
                    Err(ValkeyError::Str(msg)) => item.res = SampleAddResult::Error(msg),
                    Err(_) => item.res = SampleAddResult::Error(error_consts::INVALID_TIMESTAMP),
                }
            }
            match parse_value_arg(raw_value) {
                Ok(v) => item.value = v,
                Err(_) => item.res = SampleAddResult::Error(error_consts::INVALID_VALUE),
            }
            item.reached = item.res.is_ok();
        }

        items.push(item);
    }

    Ok(Parsed {
        items,
        has_repeats,
        current_ts,
    })
}

/// Opens `key` for writing and returns the series it holds. Unlike TS.ADD,
/// TS.MADD does not create the series: a missing key is a per-item error and the
/// keyspace is left alone (RedisTimeSeries parity — a mistyped key in a batch
/// must not silently materialize a series).
fn open_series(ctx: &Context, key: &ValkeyString) -> Result<*mut TimeSeries, SampleAddResult> {
    match SeriesGuardMut::from_key(ctx, key) {
        Ok(guard) => Ok(guard.series),
        Err(ValkeyError::Str(err)) if err == error_consts::KEY_NOT_FOUND => {
            Err(SampleAddResult::Error(error_consts::INVALID_TIMESERIES_KEY))
        }
        Err(ValkeyError::WrongType) => {
            Err(SampleAddResult::Error(error_consts::INVALID_TIMESERIES_KEY))
        }
        Err(ValkeyError::Str(err)) => Err(SampleAddResult::Error(err)),
        Err(_) => Err(SampleAddResult::Error(error_consts::PERMISSION_DENIED)),
    }
}

/// Open-addressing table over series pointers, used to spot a series named more
/// than once in a command. Slots hold `item index + 1`; zero is empty. Sized
/// for the command on the stack for typical batches, so grouping costs no
/// allocation and no hashing of key bytes.
struct SeriesTable {
    slots: SmallVec<[u32; 256]>,
    mask: usize,
}

impl SeriesTable {
    fn with_capacity(items: usize) -> Self {
        let cap = (items * 2).next_power_of_two().max(16);
        Self {
            slots: SmallVec::from_elem(0, cap),
            mask: cap - 1,
        }
    }

    /// Records `series` as named by item `index`; returns the index of the first
    /// item that named it when this is a repeat.
    fn insert(&mut self, series: *mut TimeSeries, index: u32, items: &[Item]) -> Option<u32> {
        // Fibonacci hashing of the address; the low bits are alignment.
        let mut slot =
            ((series as usize >> 4).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) & self.mask;
        loop {
            let entry = self.slots[slot];
            if entry == 0 {
                self.slots[slot] = index + 1;
                return None;
            }
            let first = entry - 1;
            if std::ptr::eq(items[first as usize].series, series) {
                return Some(first);
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

/// Writes the items: phase one adds every item's sample, phase two propagates
/// compactions, phase three trims retention and wakes blocked readers.
///
/// A series named once is written in place, in argument order. A series named
/// more than once is a group, merged by [`merge_groups`] after the single adds
/// — series are independent within a phase, so that order is free.
fn apply(ctx: &Context, items: &mut [Item], has_repeats: bool) -> ValkeyResult<()> {
    let group_count = if has_repeats {
        items.iter().filter(|i| i.count > 1).count()
    } else {
        0
    };
    let mut groups: Vec<PerSeriesSamples> = Vec::with_capacity(group_count);
    // Per group: its first item and the series' sample count before the merge,
    // for the wake-up (the groups' mutable borrows run until they are dropped).
    let mut group_items: SmallVec<[(usize, usize); 8]> = SmallVec::with_capacity(group_count);

    // Phase one: adds.
    for index in 0..items.len() {
        let item = &items[index];
        if !item.res.is_ok() {
            continue;
        }
        let first = item.first as usize;
        if items[first].count == 1 {
            // SAFETY: `series` is non-null for an item whose `res` is ok, and this
            // series is named by no other item, so this is its only reference.
            let series = unsafe { &mut *item.series };
            let (timestamp, value) = (item.timestamp, item.value);
            let item = &mut items[index];
            item.samples_before = series.total_samples;
            item.prev_last = series.last_sample.map(|s| s.timestamp);
            item.res = series.add_deferring_retention(timestamp, value, None);
            continue;
        }
        let sample = Sample::new(item.timestamp, item.value);
        let series = item.series;
        let mut group = items[first].group;
        if group == NO_GROUP {
            // The group's first accepted item (earlier ones may have failed to
            // parse). The series is the same for every item of the group.
            //
            // SAFETY: `series` is non-null for an item whose `res` is ok. The `&mut`
            // is created once per series — here — and later items of the group only
            // reach the series through this group.
            let series = unsafe { &mut *series };
            group = groups.len() as u32;
            group_items.push((first, series.total_samples));
            groups.push(PerSeriesSamples::new(series));
            items[first].group = group;
        }
        groups[group as usize].add_sample(sample, index);
    }
    if !groups.is_empty() {
        for (index, res) in merge_groups(&mut groups)? {
            items[index].res = res;
        }
    }

    // Phase two: compactions, one batch per series.
    for (index, item) in items.iter().enumerate() {
        if let SampleAddResult::Ok(added) = item.res
            && item.first as usize == index
            && item.count == 1
        {
            // SAFETY: as in phase one — the only reference to a series named once.
            let series = unsafe { &mut *item.series };
            if !series.rules.is_empty()
                && let Err(e) =
                    series.batch_compaction(ctx, &[added], item.prev_last, &[added.timestamp])
            {
                let msg = format!("TSDB: error running compaction for key '{}': {e}", item.key);
                ctx.log_warning(&msg);
            }
        }
    }
    run_group_compactions(ctx, &mut groups);

    // Phase three: retention, then the wake-up of blocked `TS.READ` readers on
    // every series that gained a sample.
    for (index, item) in items.iter().enumerate() {
        if item.first as usize == index && item.count == 1 && item.reached {
            // SAFETY: as above.
            let series = unsafe { &mut *item.series };
            series.apply_retention();
            if series.total_samples > item.samples_before {
                signal_timeseries_ready(ctx, item.key);
            }
        }
    }
    apply_group_retention(&mut groups);
    drop(groups);
    for (first, samples_before) in group_items {
        let item = &items[first];
        // SAFETY: the groups are gone; nothing else references the series.
        if unsafe { (*item.series).total_samples } > samples_before {
            signal_timeseries_ready(ctx, item.key);
        }
    }
    Ok(())
}

fn handle_replication(ctx: &Context, items: &[Item], current_ts: Option<&ValkeyString>) {
    let reached_count = items.iter().filter(|i| i.reached).count();
    if reached_count == 0 {
        return;
    }

    if reached_count == items.len() && current_ts.is_none() {
        // Every item reached its series as given: replicate the command as the
        // client sent it instead of re-marshalling every argument.
        ctx.replicate_verbatim();
    } else {
        let mut replication_args: SmallVec<[_; 24]> = SmallVec::new();
        for item in items.iter().filter(|i| i.reached) {
            replication_args.push(item.key);
            // `*` was resolved once for the command; the replica gets that time.
            replication_args.push(item.raw_timestamp.or(current_ts).unwrap());
            replication_args.push(item.raw_value);
        }
        ctx.replicate("TS.MADD", &*replication_args);
    }

    for item in items.iter().filter(|i| i.reached) {
        notify_module_event(ctx, c"ts.add", item.key);
    }
}
