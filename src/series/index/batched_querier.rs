//! Selector reads that give the module lock back between batches of series.
//!
//! [`series_by_selectors`](super::series_by_selectors) plans the query, resolves every key,
//! and opens every series in one lock hold, and its callers then keep the lock while they
//! work through the result. For a selector matching many series that hold stalls the main
//! thread for the whole read. The functions here split it:
//!
//! 1. **Plan** the postings with no lock at all — only the postings read lock. Planning
//!    takes no `Context`, and a regex-heavy selector is the slowest part of a small read.
//! 2. For each batch of up to `batch_size` ids: take the lock, **resolve** the batch's ids to
//!    keys (postings read lock, released before any key is opened, as in
//!    `querier::resolve_series_keys`), **open** them, hand them to the caller, and release
//!    the lock before the next batch. A read whose series vary in cost — a range read copies
//!    every chunk its window touches — also ends a batch once the series opened reach a
//!    weight ([`BatchLimit::weighed`]); the ids after that point go to the next batch.
//!
//! # What a batched read sees
//! It is not a point-in-time view of the database: writes made between batches are visible
//! to later ones, a series created after planning is not seen, and one deleted part-way
//! through is skipped. That is fine for a shard's share of a fan-out read, which has no
//! cross-shard atomicity to begin with. Do not use it where a command must be atomic, or
//! for a mutation.
//!
//! # Why ids are resolved per batch
//! Resolving every id to its key up front and opening the keys later would be cheaper, and
//! wrong: if a key is renamed in between, its old name opens to nothing, the id is taken for
//! stale, and `mark_ids_as_stale` deletes the *live* id → key mapping and drops the id from
//! `all_postings`. Resolving and opening in the same lock hold keeps the two in agreement,
//! exactly as the unbatched read does.

use super::get_db_index;
use super::postings::Postings;
use super::querier::{StaleIds, matches_date_range};
use crate::common::context::{create_key_string, get_current_db};
use crate::fanout::{FanoutContext, FanoutContextGuard};
use crate::labels::filters::SeriesSelector;
use crate::series::acl::KeyAccess;
use crate::series::request_types::MetaDateRangeFilter;
use crate::series::{SeriesGuard, SeriesRef, TimeSeries, try_get_timeseries_as};
use blart::AsBytes;
use std::num::NonZeroUsize;
use std::ops::{ControlFlow, Deref};
use valkey_module::{AclPermissions, Context, ValkeyResult, ValkeyString};

/// Series per lock hold when a caller has no reason to pick another size.
///
/// Opening a series is a hash lookup plus an ACL check, so a batch this size holds the lock
/// for well under a millisecond of opening; what the caller does with each series under the
/// lock comes on top. Each batch pays one lock acquisition, a database select, the ACL
/// identity's resolution, and a postings read lock.
pub const DEFAULT_SERIES_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(512).unwrap();

/// Bytes of chunks a range read may copy under one lock hold, on top of its series count.
///
/// [`DEFAULT_SERIES_BATCH_SIZE`] counts series, but a range read copies every chunk its
/// window touches ([`TimeSeries::range_copy_bytes`]): a chunk or two for a short window, and
/// hundreds per series for a long window over dense series, which at 512 series would hold
/// the lock for the whole copy. 4 MiB is about a thousand full default-size chunks, and an
/// allocation and memcpy each: a fraction of a millisecond. A short-window read stays under
/// it and is batched by series alone.
pub const DEFAULT_BATCH_COPY_BYTES: usize = 4 << 20;

/// How much one batch reads under its lock hold: at most `series` series and, for a read
/// whose series vary in cost, no more once their combined `weigh` reaches `weight`. The
/// series that reaches `weight` is still read, so every batch makes progress, however heavy
/// one series is.
#[derive(Clone, Copy)]
pub struct BatchLimit<W = fn(&TimeSeries) -> usize> {
    series: usize,
    weight: usize,
    weigh: W,
}

impl BatchLimit {
    /// At most `series` series, however heavy each is.
    pub fn series(series: usize) -> Self {
        Self {
            series,
            weight: usize::MAX,
            weigh: |_| 0,
        }
    }
}

impl<W: Fn(&TimeSeries) -> usize> BatchLimit<W> {
    /// At most `series` series, and no more once their `weigh` sums to `weight`.
    pub fn weighed(series: usize, weight: usize, weigh: W) -> Self {
        Self {
            series,
            weight,
            weigh,
        }
    }
}

/// Where a multi-step read gets the module lock from, one batch at a time.
pub trait GilSource {
    /// The lock for one batch; the series opened under it live no longer than it does.
    type Guard<'s>: Deref<Target = Context>
    where
        Self: 's;

    /// Take the lock for one batch.
    fn lock(&self) -> ValkeyResult<Self::Guard<'_>>;

    /// The database the read runs against.
    fn db(&self) -> i32;

    /// Whether the lock can be given back between batches. A caller that already holds it
    /// (the main thread) cannot, and is read in a single batch.
    fn can_release(&self) -> bool {
        true
    }
}

/// A shard-local fan-out share: each lock re-selects the request's database and re-installs
/// its ACL identity. A `FanoutContext` with no user is also how a background thread reads
/// a given database through `MODULE_CONTEXT`.
impl GilSource for FanoutContext {
    type Guard<'s> = FanoutContextGuard;

    fn lock(&self) -> ValkeyResult<FanoutContextGuard> {
        FanoutContext::lock(self)
    }

    fn db(&self) -> i32 {
        FanoutContext::db(self)
    }
}

/// A caller already holding the lock: the read is one batch, as `series_by_selectors` is.
impl GilSource for Context {
    type Guard<'s> = &'s Context;

    fn lock(&self) -> ValkeyResult<&Context> {
        Ok(self)
    }

    fn db(&self) -> i32 {
        get_current_db(self)
    }

    fn can_release(&self) -> bool {
        false
    }
}

/// Call `f` for every series `selectors` match, giving the lock back every `batch_size`
/// series.
///
/// `f` runs under the lock and receives the batch's [`Context`], the series, and its key.
/// Neither the guard nor anything borrowed from it can outlive the call — the `for<'g>`
/// bound enforces that — so `f` must copy out what it needs (a `RangeSnapshot`, the labels,
/// the latest sample) and leave heavy work, such as decoding, for after the read. The key is
/// lent rather than given: freeing a `ValkeyString` needs the lock.
///
/// The ACL (`ACCESS`) check and the optional date-range filter are those of
/// `series_by_selectors`: the read fails on the first matched series the caller may not
/// read. Return [`ControlFlow::Break`] to stop early.
///
/// Must not be called while holding the lock through a [`GilSource`] that releases it: each
/// batch takes it afresh. See the module docs for what a batched read can and cannot see.
pub fn for_each_series<S, F>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    batch_size: NonZeroUsize,
    mut f: F,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    F: for<'g> FnMut(&'g Context, SeriesGuard<'g>, &ValkeyString) -> ValkeyResult<ControlFlow<()>>,
{
    visit_batches(
        src,
        selectors,
        range,
        BatchLimit::series(batch_size.get()),
        |ctx, batch| {
            for (guard, key) in batch.drain(..) {
                if f(ctx, guard, &key)?.is_break() {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        },
        Ok,
    )
}

/// [`for_each_series`] a batch at a time, for a caller that fans each batch out across a
/// pool. The batch is lent as a slice for the same reason the key is lent per series.
pub fn for_each_series_batch<S, F>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    batch_size: NonZeroUsize,
    mut f: F,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    F: for<'g> FnMut(
        &'g Context,
        &[(SeriesGuard<'g>, ValkeyString)],
    ) -> ValkeyResult<ControlFlow<()>>,
{
    visit_batches(
        src,
        selectors,
        range,
        BatchLimit::series(batch_size.get()),
        |ctx, batch| f(ctx, batch),
        Ok,
    )
}

/// [`for_each_series_batch`] in two halves: `copy` runs under the batch's lock and returns
/// what it copied out, and `process` gets that value once the lock is released, before the
/// next batch takes it again.
///
/// For a read whose per-series work outweighs the copy — decoding a `RangeSnapshot`,
/// evaluating it — this bounds what the read holds to one batch's worth of that work's
/// input, rather than the whole match's, and the work itself is the gap that lets the main
/// thread take the lock between batches. `T` cannot borrow the batch: it is chosen by the
/// caller, outside the `for<'g>` bound.
///
/// `limit` sizes each batch; weigh it by what `copy` copies when that varies by series.
pub fn for_each_series_batch_then<S, T, W, F, G>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    limit: BatchLimit<W>,
    mut copy: F,
    process: G,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    W: Fn(&TimeSeries) -> usize,
    F: for<'g> FnMut(&'g Context, &[(SeriesGuard<'g>, ValkeyString)]) -> ValkeyResult<T>,
    G: FnMut(T) -> ValkeyResult<ControlFlow<()>>,
{
    visit_batches(
        src,
        selectors,
        range,
        limit,
        |ctx, batch| copy(ctx, batch),
        process,
    )
}

/// The engine of every entry point. `visit` runs under each batch's lock and `after` gets
/// its result once the lock is released. `visit` gets the batch by `&mut Vec` so the
/// per-series entry point can move each guard out; this is private because that would also
/// let a caller move a `ValkeyString` out from under the lock.
fn visit_batches<S, B, W, F, G>(
    src: &S,
    selectors: &[SeriesSelector],
    range: Option<MetaDateRangeFilter>,
    limit: BatchLimit<W>,
    mut visit: F,
    mut after: G,
) -> ValkeyResult<()>
where
    S: GilSource + ?Sized,
    W: Fn(&TimeSeries) -> usize,
    F: for<'g> FnMut(&'g Context, &mut Vec<(SeriesGuard<'g>, ValkeyString)>) -> ValkeyResult<B>,
    G: FnMut(B) -> ValkeyResult<ControlFlow<()>>,
{
    if selectors.is_empty() {
        return Ok(());
    }
    let mut cursor = SeriesCursor::plan(src.db(), selectors, range)?;
    let limit = if src.can_release() {
        limit
    } else {
        BatchLimit::weighed(usize::MAX, usize::MAX, limit.weigh)
    };
    while !cursor.is_done() {
        let copied = {
            let ctx = src.lock()?;
            cursor.next_batch_within(&ctx, &limit, &mut visit)?
            // The batch's keys were freed inside `next_batch`; the lock goes here.
        };
        // A batch whose every id was gone, filtered, or stale has nothing to process.
        if let Some(copied) = copied
            && after(copied)?.is_break()
        {
            break;
        }
        if !cursor.is_done() {
            // The main thread waiting on the lock is woken by the release above, but this
            // thread can take it straight back before the main thread runs. Step aside.
            std::thread::yield_now();
        }
    }
    Ok(())
}

/// A batched read driven by its caller a batch at a time, for interleaving several reads
/// within each lock hold — the PromQL selector executor shares one hold among the tasks it
/// has queued, so a burst of small selectors pays one lock acquisition, not one each.
///
/// Planned with no lock; each [`Self::next_batch`] then runs under a lock the caller holds.
/// The entry points above are this, driven one lock per batch.
pub struct SeriesCursor {
    db: i32,
    range: Option<MetaDateRangeFilter>,
    ids: Vec<SeriesRef>,
    next: usize,
}

impl SeriesCursor {
    /// Plan a read of `selectors` in database `db`: only the postings read lock is taken.
    pub fn plan(
        db: i32,
        selectors: &[SeriesSelector],
        range: Option<MetaDateRangeFilter>,
    ) -> ValkeyResult<Self> {
        let ids = if selectors.is_empty() {
            Vec::new()
        } else {
            let index = get_db_index(db);
            let postings = index.get_postings();
            postings.postings_for_selectors(selectors)?.iter().collect()
        };
        Ok(Self {
            db,
            range,
            ids,
            next: 0,
        })
    }

    /// The database the read runs against: the caller selects it before each batch.
    pub fn db(&self) -> i32 {
        self.db
    }

    pub fn is_done(&self) -> bool {
        self.next >= self.ids.len()
    }

    /// Abandon the rest of the read: [`Self::is_done`] from here on.
    pub fn finish(&mut self) {
        self.next = self.ids.len();
    }

    /// Ids planned and not yet read: an upper bound on the series still to come.
    pub fn remaining(&self) -> usize {
        self.ids.len() - self.next.min(self.ids.len())
    }

    /// Read up to `max` more of the planned series and visit those that survive, under the
    /// lock `ctx` holds — with [`Self::db`] selected and, for an ACL-checked read, the
    /// caller's identity installed. `None` when none survived. See the module docs for what
    /// is resolved, skipped and marked stale.
    pub fn next_batch<B, F>(
        &mut self,
        ctx: &Context,
        max: usize,
        visit: &mut F,
    ) -> ValkeyResult<Option<B>>
    where
        F: for<'g> FnMut(&'g Context, &mut Vec<(SeriesGuard<'g>, ValkeyString)>) -> ValkeyResult<B>,
    {
        self.next_batch_within(ctx, &BatchLimit::series(max), visit)
    }

    /// [`Self::next_batch`] sized by `limit`: when the series opened reach its weight, the
    /// batch ends there and the ids after it are left for the next one.
    pub fn next_batch_within<B, W, F>(
        &mut self,
        ctx: &Context,
        limit: &BatchLimit<W>,
        visit: &mut F,
    ) -> ValkeyResult<Option<B>>
    where
        W: Fn(&TimeSeries) -> usize,
        F: for<'g> FnMut(&'g Context, &mut Vec<(SeriesGuard<'g>, ValkeyString)>) -> ValkeyResult<B>,
    {
        let start = self.next;
        let end = self
            .ids
            .len()
            .min(start.saturating_add(limit.series.max(1)));
        // Taken in full unless the batch reports otherwise: a failed read stays consumed.
        self.next = end;
        let ids = &self.ids[start..end];
        let (visited, read) = visit_batch(ctx, self.db, ids, self.range.as_ref(), limit, visit)?;
        self.next = start + read;
        Ok(visited)
    }
}

/// One batch, under the lock: resolve, open, filter, visit, and record the stale ids.
/// `None` when no series of the batch survived to be visited. Also returns how many of `ids`
/// the batch took: fewer than all when the series opened reached `limit`'s weight first.
fn visit_batch<'g, B, W, F>(
    ctx: &'g Context,
    db: i32,
    ids: &[SeriesRef],
    range: Option<&MetaDateRangeFilter>,
    limit: &BatchLimit<W>,
    visit: &mut F,
) -> ValkeyResult<(Option<B>, usize)>
where
    W: Fn(&TimeSeries) -> usize,
    F: for<'x> FnMut(&'x Context, &mut Vec<(SeriesGuard<'x>, ValkeyString)>) -> ValkeyResult<B>,
{
    // Looked up per batch rather than held: the guard pins the index map's reclamation.
    let index = get_db_index(db);
    let mut stale = StaleIds::new();
    let resolved = {
        let postings = index.get_postings();
        resolve_live_series_keys(ctx, &postings, ids, &mut stale)
    };

    // Per batch: under a fan-out share, the ACL identity is installed by this lock and only
    // valid while it is held.
    let access = KeyAccess::new(ctx, AclPermissions::ACCESS);
    // Per batch, not reused across them: its guards borrow this lock hold.
    let mut batch: Vec<(SeriesGuard<'g>, ValkeyString)> = Vec::with_capacity(resolved.len());
    let mut opened = Ok(());
    let mut read = ids.len();
    let mut weight = 0usize;
    // The keys left unopened when the weight runs out are dropped with the loop, under the
    // lock. Their ids are resolved afresh by the batch that reads them; any stale id among
    // them was found stale under this lock, and is recorded below either way.
    for (id, key) in resolved {
        match try_get_timeseries_as(ctx, &key, &access) {
            Ok(Some(guard)) => {
                // Resolved and opened in one lock hold, so the key holds this id unless the
                // index is out of step with the keyspace. Another series is not this read's
                // to report, and this id is not stale: it may live on under another key.
                if guard.id != id {
                    continue;
                }
                if let Some(range) = range {
                    let (start, end) = range.range();
                    if !matches_date_range(&guard, start, end, range.is_exclude()) {
                        continue;
                    }
                }
                weight = weight.saturating_add((limit.weigh)(&guard));
                batch.push((guard, key));
                if weight >= limit.weight {
                    read = ids
                        .iter()
                        .position(|&i| i == id)
                        .map_or(ids.len(), |p| p + 1);
                    break;
                }
            }
            Ok(None) => stale.push(id),
            Err(err) => {
                opened = Err(err);
                break;
            }
        }
    }
    let result = match opened {
        Ok(()) if batch.is_empty() => Ok(None),
        Ok(()) => visit(ctx, &mut batch).map(Some),
        Err(err) => Err(err),
    };
    // Whatever `visit` left is dropped here, under the lock: `ValkeyString`s need it.
    drop(batch);
    index.mark_ids_as_stale(&stale);
    result.map(|visited| (visited, read))
}

/// `querier::resolve_series_keys` for a batch planned under an earlier lock hold.
///
/// An id no longer in `all_postings` was removed since planning — its series deleted, the
/// database flushed or swapped, or the id already marked stale — and is skipped, not marked
/// again. Only an id the index still lists with no key is stale, as in the unbatched read.
fn resolve_live_series_keys(
    ctx: &Context,
    postings: &Postings,
    ids: &[SeriesRef],
    stale: &mut StaleIds,
) -> Vec<(SeriesRef, ValkeyString)> {
    let mut resolved = Vec::with_capacity(ids.len());
    for_live_keys(postings, ids, stale, |id, key| {
        resolved.push((id, create_key_string(ctx, key)));
    });
    resolved
}

/// The server-free half of [`resolve_live_series_keys`]: `f` gets each live id's key,
/// `stale` the ids the index lists with no key.
fn for_live_keys(
    postings: &Postings,
    ids: &[SeriesRef],
    stale: &mut StaleIds,
    mut f: impl FnMut(SeriesRef, &[u8]),
) {
    for &id in ids {
        if !postings.all_postings.contains(id) {
            continue;
        }
        match postings.get_key_by_id(id) {
            Some(key) => f(id, key.as_bytes()),
            None => stale.push(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{Label, MetricName};
    use crate::series::TimeSeries;

    fn series(id: SeriesRef, name: &str) -> TimeSeries {
        let mut series = TimeSeries::new();
        series.id = id;
        series.labels = MetricName::new(&[Label::new("__name__", name)]);
        series
    }

    fn live_keys(postings: &Postings, ids: &[SeriesRef]) -> (Vec<(SeriesRef, Vec<u8>)>, StaleIds) {
        let mut stale = StaleIds::new();
        let mut keys = Vec::new();
        for_live_keys(postings, ids, &mut stale, |id, key| {
            keys.push((id, key.to_vec()))
        });
        (keys, stale)
    }

    #[test]
    fn live_ids_resolve_to_their_keys() {
        let mut postings = Postings::default();
        postings.index_timeseries(&series(1, "a"), b"key:a");
        postings.index_timeseries(&series(2, "b"), b"key:b");

        let (keys, stale) = live_keys(&postings, &[1, 2]);

        assert_eq!(keys, vec![(1, b"key:a".to_vec()), (2, b"key:b".to_vec())]);
        assert!(stale.is_empty());
    }

    /// A series deleted between planning and its batch is gone from the index, not dangling:
    /// marking it stale would queue a sweep for nothing.
    #[test]
    fn ids_removed_since_planning_are_skipped_not_stale() {
        let mut postings = Postings::default();
        let gone = series(1, "a");
        postings.index_timeseries(&gone, b"key:a");
        postings.index_timeseries(&series(2, "b"), b"key:b");
        postings.remove_timeseries(&gone);

        let (keys, stale) = live_keys(&postings, &[1, 2]);

        assert_eq!(keys, vec![(2, b"key:b".to_vec())]);
        assert!(stale.is_empty());
    }

    /// A RENAME between planning and the batch: the id resolves to its new key, because the
    /// batch resolves under its own lock hold rather than reusing a name resolved earlier.
    #[test]
    fn a_renamed_series_resolves_to_its_new_key() {
        let mut postings = Postings::default();
        let renamed = series(1, "a");
        postings.index_timeseries(&renamed, b"old");
        assert!(postings.remove_timeseries_for_key(&renamed, b"old"));
        postings.index_timeseries(&renamed, b"new");

        let (keys, stale) = live_keys(&postings, &[1]);

        assert_eq!(keys, vec![(1, b"new".to_vec())]);
        assert!(stale.is_empty());
    }

    /// The case the unbatched read marks stale too: the index lists the id, but no key.
    #[test]
    fn a_listed_id_without_a_key_is_stale() {
        let mut postings = Postings::default();
        postings.index_timeseries(&series(2, "b"), b"key:b");
        // Postings for a label without a key: an index out of step with the keyspace.
        postings.add_posting_for_label_value(3, "__name__", "dangling");

        let (keys, stale) = live_keys(&postings, &[2, 3]);

        assert_eq!(keys, vec![(2, b"key:b".to_vec())]);
        assert_eq!(stale.as_slice(), &[3]);
    }
}
