// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The AUTO_INCREMENT / `_tidb_rowid` counter: Go `pkg/meta/autoid`'s
//! `Allocator` reduced to the one thing a table needs from it, an id source
//! that lives OUTSIDE transaction semantics.
//!
//! Inside: [`AutoIdStore`], WHERE the counter lives; [`AutoIdAllocator`],
//! Go's `allocator.base`/`allocator.end` cache over one of those; and
//! [`AutoIdError`], carrying Go `autoid.ErrAutoincReadFailed` alongside the
//! store failures a shared counter can have and a process-local one cannot.
//!
//! # Why the counter has a HOME rather than being a field
//!
//! Go does not keep this counter in `TableInfo`. It lives in a meta key of
//! its own -- `pkg/meta/meta.go`'s `autoTableIDKey` (`TID:<id>`), bumped with
//! `HInc` -- and `pkg/meta/autoid/autoid.go`'s `Allocator` hands ids out of a
//! range it RESERVED there in a transaction of its OWN, so an id is burned
//! the moment it is issued and is never returned by a rollback.
//!
//! That is one mechanism serving two tiers, and it is why this module has a
//! trait where it used to have an `AtomicU64`. A table that owns every row it
//! will ever serve can keep its counter in memory; a table on shared cluster
//! storage cannot, because a counter that starts at zero on each node
//! re-issues ids that already exist, and a peer `tidb-server` allocates from
//! the same range at the same time. Making the home pluggable rather than
//! special-casing the cluster tier is what lets the allocation RULES below --
//! which ids are issued, which explicit value rebases, when the domain is
//! exhausted -- exist once and be the same rules on both.
//!
//! The callers that drive the allocator -- `KvTable::apply_auto_increment`,
//! `rebase_auto_increment`, `truncate` -- stay with the table in the parent
//! module.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Go `autoid.GetStep()`'s `defaultStep`: how many ids an allocator reserves
/// from a shared store at once.
///
pub const DEFAULT_AUTO_ID_STEP: u64 = 30000;

const MAX_AUTO_ID_STEP: u64 = 2_000_000;
const DEFAULT_CONSUME_TIME: Duration = Duration::from_secs(10);

/// Go `autoid.NextStep`: resize the next reservation by how quickly the
/// current one was consumed, bounded by `minStep` and `maxStep`.
#[must_use]
pub fn next_step(current_step: u64, consume_duration: Duration) -> u64 {
    let consume_rate = DEFAULT_CONSUME_TIME.as_secs_f64() / consume_duration.as_secs_f64();
    ((current_step as f64 * consume_rate) as u64).clamp(DEFAULT_AUTO_ID_STEP, MAX_AUTO_ID_STEP)
}

/// Where an allocator's counter really lives.
///
/// One implementation per storage tier, both holding Go's stored value: the
/// id last reserved by ANYONE, as a raw 64-bit pattern. Ids are handed out
/// above it, so that value is also the high-water mark a rebase raises.
pub trait AutoIdStore: fmt::Debug + Send + Sync {
    /// Go `alloc4Signed`'s inner transaction: `idAcc.Get()` then
    /// `idAcc.Inc(step)`, run in a transaction that is NOT the row's.
    ///
    /// Returns `(base, end)`: `base` is the stored value before the bump, and
    /// the ids in `(base, end]` now belong to this allocator alone. `end` is
    /// clamped to the end of the domain, so it equals `base` when there was
    /// no room left; the caller reads that as exhaustion rather than the
    /// store having to know what the column's limit means.
    ///
    /// `unsigned` is the column's domain, which decides how the stored
    /// pattern is compared and how much room is left above it.
    fn reserve(&self, step: u64, unsigned: bool) -> Result<(u64, u64), AutoIdStoreError>;

    /// Go `Allocator.NextGlobalAutoID`: the first id beyond every range that
    /// has already been reserved from this counter.
    fn next_global(&self) -> Result<u64, AutoIdStoreError>;

    /// Reserve enough space for a concrete source `Allocator.Alloc` request.
    ///
    /// Go reads the shared base and computes the batch span in the same
    /// transaction. Implementations that can do so should override this
    /// method. The default safely reserves the maximum possible span; it may
    /// leave a larger cache window but cannot overlap another allocator.
    fn reserve_batch(
        &self,
        minimum_step: u64,
        n: u64,
        increment: u64,
        _offset: u64,
        unsigned: bool,
    ) -> Result<(u64, u64), AutoIdStoreError> {
        self.reserve(minimum_step.max(n.saturating_mul(increment)), unsigned)
    }

    /// Go `rebase4Signed`/`rebase4Unsigned` with `allocIDs == false`: raise
    /// the stored value to `required` if it is not already past it, so the
    /// next id ANY node allocates exceeds `required`.
    ///
    /// Never lowers, which is what makes an explicit id below the counter a
    /// no-op on every tier.
    fn rebase(&self, required: u64, unsigned: bool) -> Result<(), AutoIdStoreError>;

    /// Go `Allocator.Rebase` with `allocIDs == true`: replace the stored
    /// counter even when that moves it down. `FORCE AUTO_INCREMENT` needs one
    /// atomic store operation; spelling it as reset followed by rebase would
    /// let another allocator reserve an overlapping range between the two.
    fn force_rebase(&self, required: u64, unsigned: bool) -> Result<(), AutoIdStoreError>;

    /// Starts the counter over, so the next id is 1 again.
    ///
    /// Go reaches this by REPLACING the table -- TRUNCATE builds a new table
    /// id, whose counter has simply never been written -- so unlike
    /// [`rebase`](AutoIdStore::rebase) it also moves down.
    fn reset(&self) -> Result<(), AutoIdStoreError>;
}

/// A counter home that could not be read or written.
///
/// Distinct from exhaustion on purpose: a store that is unreachable has not
/// said the column is full, and answering `1467` to a failed meta-key
/// transaction would report a schema fact where there is an availability one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoIdStoreError(pub String);

impl fmt::Display for AutoIdStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why a row got no auto id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutoIdError {
    /// Go `autoid.ErrAutoincReadFailed` (1467): the column's domain is full.
    Exhausted,
    /// Go `types.ErrOverflow` (1690) raised by `setDatumAutoIDAndCast`: the
    /// allocated id does not fit the COLUMN, which is narrower than the
    /// 64-bit domain the allocator counts in. Distinct from
    /// [`Exhausted`](AutoIdError::Exhausted): ids remain, this column just
    /// cannot hold them.
    OutOfRange {
        /// The refused id, printed in the column's own domain.
        value: String,
        /// The column type's name, as Go `types.TypeStr` prints it.
        type_name: String,
    },
    /// The counter's home could not be reached.
    Store(AutoIdStoreError),
}

/// The in-process counter home: an atomic cell, reserved one id at a time.
///
/// Holding the cell behind an `Arc` is what reproduces Go's burn-on-rollback
/// here: a transaction stages a COPY of the catalog, and the copied tables
/// allocate from the very cell the committed ones do, so dropping the staged
/// copy on ROLLBACK discards the rows while keeping the burn. As a plain
/// field, "returned on rollback" would be the normal path and every failure
/// site would need its own fixup.
#[derive(Clone, Debug, Default)]
pub struct LocalAutoIdStore {
    /// The id last handed out, as its 64-bit pattern.
    last: Arc<AtomicU64>,
}

impl LocalAutoIdStore {
    /// A counter that has never been written, so the first id is 1.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl AutoIdStore for LocalAutoIdStore {
    fn reserve(&self, step: u64, unsigned: bool) -> Result<(u64, u64), AutoIdStoreError> {
        let mut base = self.last.load(Ordering::SeqCst);
        loop {
            let end = advance(base, step, unsigned);
            match self
                .last
                .compare_exchange_weak(base, end, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok((base, end)),
                Err(observed) => base = observed,
            }
        }
    }

    fn next_global(&self) -> Result<u64, AutoIdStoreError> {
        Ok(self.last.load(Ordering::SeqCst).wrapping_add(1))
    }

    fn reserve_batch(
        &self,
        minimum_step: u64,
        n: u64,
        increment: u64,
        offset: u64,
        unsigned: bool,
    ) -> Result<(u64, u64), AutoIdStoreError> {
        let mut base = self.last.load(Ordering::SeqCst);
        loop {
            let needed = calc_needed_batch_size(base, n, increment, offset, unsigned);
            let end = advance(base, minimum_step.max(needed), unsigned);
            match self
                .last
                .compare_exchange_weak(base, end, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok((base, end)),
                Err(observed) => base = observed,
            }
        }
    }

    fn rebase(&self, required: u64, unsigned: bool) -> Result<(), AutoIdStoreError> {
        let mut last = self.last.load(Ordering::SeqCst);
        while exceeds(required, last, unsigned) {
            match self.last.compare_exchange_weak(
                last,
                required,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => last = observed,
            }
        }
        Ok(())
    }

    fn force_rebase(&self, required: u64, _unsigned: bool) -> Result<(), AutoIdStoreError> {
        self.last.store(required, Ordering::SeqCst);
        Ok(())
    }

    fn reset(&self) -> Result<(), AutoIdStoreError> {
        self.last.store(0, Ordering::SeqCst);
        Ok(())
    }
}

/// How many ids remain above `last` in the column's domain.
#[must_use]
pub const fn headroom(last: u64, unsigned: bool) -> u128 {
    if unsigned {
        (u64::MAX as u128) - (last as u128)
    } else {
        // Read in the signed domain, where `last` is at most `i64::MAX` and
        // may in principle be negative.
        ((i64::MAX as i128) - (last as i64 as i128)) as u128
    }
}

/// Go `table.getIncrementAndOffset`: resolves `@@auto_increment_increment`
/// and `@@auto_increment_offset` into the pair the allocator seeks with.
///
/// MySQL's documented oddity, which TiDB follows verbatim -- "when the value
/// of auto_increment_offset is greater than that of auto_increment_increment,
/// the value of auto_increment_offset is ignored" -- is the whole of it.
/// Captured from TiDB: `increment = 2, offset = 7` hands out `1, 3, 5`, NOT
/// `7, 9, 11`, so the offset really is replaced by 1 rather than reduced
/// modulo the increment. `increment = 6, offset = 6` is not greater, so it
/// stands and yields `6, 12, 18`.
#[must_use]
pub const fn increment_and_offset(increment: u64, offset: u64) -> (u64, u64) {
    if offset > increment {
        (increment, 1)
    } else {
        (increment, offset)
    }
}

/// Go `SeekToFirstAutoIDSigned` / `SeekToFirstAutoIDUnSigned`: the first id
/// at or after `base + 1` that lies on the `offset + k * increment`
/// progression.
///
/// ```text
/// nr := (base + increment - offset) / increment
/// nr = nr*increment + offset
/// ```
///
/// Go's integer division truncates toward zero and so does Rust's, which is
/// what makes a base BELOW the offset land on the offset itself rather than
/// one step under it. The id is computed in the column's own domain, so an
/// unsigned progression past `i64::MAX` stays on its grid.
///
/// Note this is the ONLY place the step arithmetic lives, and it runs BEFORE
/// the column-width cast in `KvTable::check_auto_increment_fits`. Captured
/// from TiDB: a `TINYINT AUTO_INCREMENT` under `increment = 100` hands out
/// `1`, then `101`, and then fails with `[types:1690]constant 201 overflows
/// tinyint` -- the stepped candidate is produced first and the column bound
/// judges it, so a wider step reaches the column's ceiling in fewer rows
/// rather than being clamped onto it.
#[must_use]
pub fn seek_to_first(base: u64, increment: u64, offset: u64, unsigned: bool) -> u64 {
    if unsigned {
        let nr = base
            .wrapping_add(increment)
            .wrapping_sub(offset)
            .wrapping_div(increment);
        nr.wrapping_mul(increment).wrapping_add(offset)
    } else {
        let (base, increment, offset) = (base as i64, increment as i64, offset as i64);
        let nr = base.wrapping_add(increment).wrapping_sub(offset) / increment;
        nr.wrapping_mul(increment).wrapping_add(offset) as u64
    }
}

/// Go `autoid.CalcNeededBatchSize`: the distance from `base` to the final id
/// of an `n`-value batch on the increment/offset progression.
#[must_use]
pub fn calc_needed_batch_size(
    base: u64,
    n: u64,
    increment: u64,
    offset: u64,
    unsigned: bool,
) -> u64 {
    if n == 0 {
        return 0;
    }
    if increment == 1 {
        return n;
    }
    let first = seek_to_first(base, increment, offset, unsigned);
    first
        .wrapping_sub(base)
        .wrapping_add((n - 1).wrapping_mul(increment))
}

/// True when `value` ranks after `other` in the column's domain.
#[must_use]
pub const fn exceeds(value: u64, other: u64, unsigned: bool) -> bool {
    if unsigned {
        value > other
    } else {
        (value as i64) > (other as i64)
    }
}

/// Go's `tmpStep := min(math.MaxInt64-newBase, nextStep)`: `base` moved up by
/// `step`, never past the end of the domain.
#[must_use]
pub fn advance(base: u64, step: u64, unsigned: bool) -> u64 {
    let taken = u128::from(step).min(headroom(base, unsigned));
    // `taken` is bounded by the room left, so this cannot leave the domain.
    base.wrapping_add(taken as u64)
}

/// Go `autoid.Allocator`: the cached range `(base, end]` over a counter that
/// lives in an [`AutoIdStore`].
///
/// The cache holds Go's `allocator.base` -- the id LAST handed out, not the
/// next one -- as a raw 64-bit PATTERN, with the column's signedness deciding
/// how that pattern is read. Both choices come straight from Go and each one
/// removes a whole class of edge case:
///
/// * Last-allocated is the representation `Rebase` is written against
///   (`rebase4Signed`: `if requiredBase <= alloc.base { return }`), so
///   rebasing to an explicit id is a plain maximum with no `+ 1` to overflow
///   at `i64::MAX`.
/// * The pattern plus `unsigned` is Go's `isUnsigned` split
///   (`rebase4Unsigned`/`alloc4Unsigned` read the same field as `uint64`), so
///   a `BIGINT UNSIGNED` id above `i64::MAX` keeps its own domain end to end
///   instead of turning negative and being ignored by the rebase.
///
/// Cloning shares the cache AND the store, so the staged catalog copy a
/// transaction allocates from is the same allocator the committed table has.
#[derive(Clone, Debug)]
pub(crate) struct AutoIdAllocator {
    /// Go `allocator.base`/`allocator.end`: the ids `(base, end]` reserved
    /// for this allocator and not yet handed out.
    cache: Arc<Mutex<AutoIdRange>>,
    /// Where the counter really lives.
    store: Arc<dyn AutoIdStore>,
    /// Initial reservation size. The default enables Go's dynamic `NextStep`;
    /// any explicitly different value remains a fixed custom step.
    initial_step: u64,
    dynamic_step: bool,
    /// Go `allocator.isUnsigned`: which domain the patterns are read in.
    pub(crate) unsigned: bool,
}

/// Go `allocator.base` and `allocator.end`.
#[derive(Clone, Copy, Debug)]
struct AutoIdRange {
    /// The id last handed out from this cache.
    base: u64,
    /// The highest id this cache may hand out.
    end: u64,
    /// Go `allocator.step`, updated after each dynamic reservation.
    step: u64,
    /// Go `allocator.lastAllocTime`.
    last_reserve_at: Instant,
}

impl AutoIdAllocator {
    /// A fresh in-process allocator, whose first id is 1.
    pub(crate) fn new() -> Self {
        Self::over(Arc::new(LocalAutoIdStore::new()), DEFAULT_AUTO_ID_STEP)
    }

    /// An allocator over `store`, reserving `step` ids at a time.
    pub(crate) fn over(store: Arc<dyn AutoIdStore>, step: u64) -> Self {
        let step = step.max(1);
        AutoIdAllocator {
            // An empty range, so the first allocation reserves.
            cache: Arc::new(Mutex::new(AutoIdRange {
                base: 0,
                end: 0,
                step,
                last_reserve_at: Instant::now(),
            })),
            store,
            initial_step: step,
            dynamic_step: step == DEFAULT_AUTO_ID_STEP,
            unsigned: false,
        }
    }

    /// Whether `other` is a clone of this allocator rather than a second one
    /// over the same store: same cache, so the same reserved range.
    pub(crate) fn shares_cache_with(&self, other: &AutoIdAllocator) -> bool {
        Arc::ptr_eq(&self.cache, &other.cache)
    }

    /// Builds a fresh local range over the same global counter.
    ///
    /// A schema change to `AUTO_ID_CACHE` rebuilds Go's table allocator. The
    /// global high-water mark remains, while the old table object's unused
    /// local range is abandoned.
    pub(crate) fn with_step(&self, step: u64) -> Self {
        let mut allocator = Self::over(self.store.clone(), step);
        allocator.unsigned = self.unsigned;
        allocator
    }

    /// Whether this allocator uses Go's single-point `AUTO_ID_CACHE=1` mode.
    pub(crate) fn is_single_point(&self) -> bool {
        self.initial_step == 1
    }

    /// Go `allocator.isUnsigned`, set from the AUTO_INCREMENT column's type.
    pub(crate) fn set_unsigned(&mut self, unsigned: bool) {
        self.unsigned = unsigned;
    }

    /// Consumes and returns the next id, as its 64-bit pattern.
    ///
    /// Go's `alloc4Signed`/`alloc4Unsigned` both refuse before the counter can
    /// wrap -- `if math.MaxInt64-alloc.base <= n1 { return ErrAutoincReadFailed }`
    /// with `n1 == 1` for a one-row insert -- so the last id an allocator ever
    /// hands out is one BELOW the type's maximum. Captured from TiDB: on a
    /// `BIGINT` at `9223372036854775806` and on a `BIGINT UNSIGNED` at
    /// `18446744073709551614`, the next insert fails with
    /// `[autoid:1467]`. Saturating instead would silently re-issue an id that
    /// already exists.
    ///
    /// The domain is checked BEFORE the store is asked for a range and again
    /// after, which is Go's pair of guards (the local
    /// `math.MaxInt64-alloc.base <= n1` and the in-transaction `tmpStep < n1`)
    /// and is what keeps a refused allocation from burning the last id: Go
    /// returns from inside the reservation without ever calling `Inc`.
    ///
    /// DIVERGENCE (documented): the capture shows one exception -- an `ALTER
    /// TABLE ... AUTO_INCREMENT = 9223372036854775807` followed by an insert
    /// DOES hand out `9223372036854775807` in Go, from the same counter value
    /// that refuses a plain insert. That is Go's batch cache reaching past its
    /// own guard on the ALTER path, not a rule; refusing one id earlier is the
    /// safe side of it, since the id at the very end of the domain is one this
    /// allocator then never issues twice.
    /// `increment` and `offset` are `@@auto_increment_increment` and
    /// `@@auto_increment_offset` as the caller already resolved them (see
    /// [`increment_and_offset`]); the default `1, 1` reduces the seek to
    /// `base + 1`, so the stepped and the ordinary case are the same code.
    pub(crate) fn alloc(&self, increment: u64, offset: u64) -> Result<u64, AutoIdError> {
        self.alloc_batch(1, increment, offset)
            .map(|(_, maximum)| maximum)
    }

    /// Go `Allocator.Alloc`: reserve `n` ids and return the half-open batch
    /// description `(base, maximum]` used by the caller to enumerate them.
    fn alloc_batch(&self, n: u64, increment: u64, offset: u64) -> Result<(u64, u64), AutoIdError> {
        if n == 0 {
            return Ok((0, 0));
        }
        let mut cache = self.cache.lock().expect("auto id cache poisoned");
        // Go `alloc4Signed`/`alloc4Unsigned` first rebases to `offset - 1`
        // when the offset lies beyond the current base. The table caller
        // normally supplies an offset no greater than the increment, but the
        // allocator contract itself permits the full validated offset range:
        // `(base=21, increment=1, offset=30)` must allocate 30, not 22.
        let offset_base = offset.saturating_sub(1);
        if exceeds(offset_base, cache.base, self.unsigned) {
            if exceeds(offset_base, cache.end, self.unsigned) {
                self.store
                    .rebase(offset_base, self.unsigned)
                    .map_err(AutoIdError::Store)?;
                cache.end = offset_base;
            }
            cache.base = offset_base;
        }
        let mut needed = calc_needed_batch_size(cache.base, n, increment, offset, self.unsigned);
        if headroom(cache.base, self.unsigned) <= u128::from(needed) {
            return Err(AutoIdError::Exhausted);
        }
        let mut maximum = cache.base.wrapping_add(needed);
        if exceeds(maximum, cache.end, self.unsigned) {
            // The cached range cannot answer the seek: reserve the next one
            // from the store, which is also where a peer node's allocations
            // become visible. Go re-seeks from the NEW base rather than
            // carrying the old target across (`alloc4Signed`: "CalcNeededBatchSize
            // calculates the total batch size needed on global base"), because
            // the store may have moved the counter further than this node knew.
            let mut next_reservation = cache.step;
            if self.dynamic_step && cache.end != 0 {
                next_reservation = next_step(cache.step, cache.last_reserve_at.elapsed());
            }
            let (base, end) = self
                .store
                .reserve_batch(next_reservation, n, increment, offset, self.unsigned)
                .map_err(AutoIdError::Store)?;
            // The shared store may have advanced since this allocator last
            // observed it. Go recomputes the batch from that new base rather
            // than carrying the old distance across.
            needed = calc_needed_batch_size(base, n, increment, offset, self.unsigned);
            if headroom(base, self.unsigned) <= u128::from(needed) {
                return Err(AutoIdError::Exhausted);
            }
            maximum = base.wrapping_add(needed);
            if exceeds(maximum, end, self.unsigned) {
                return Err(AutoIdError::Exhausted);
            }
            *cache = AutoIdRange {
                base,
                end,
                step: if self.dynamic_step {
                    next_reservation.max(needed)
                } else {
                    cache.step
                },
                last_reserve_at: Instant::now(),
            };
        }
        let base = cache.base;
        cache.base = maximum;
        Ok((base, maximum))
    }

    /// The id the next allocation will return, as a pattern. Meaningful only
    /// while ids remain, which is what `SHOW TABLE STATUS` reports.
    pub(crate) fn next(&self) -> u64 {
        self.cache
            .lock()
            .expect("auto id cache poisoned")
            .base
            .wrapping_add(1)
    }

    /// The next id beyond all ranges reserved in the shared store.
    pub(crate) fn next_global(&self) -> Result<u64, AutoIdStoreError> {
        self.store.next_global()
    }

    /// Advances the shared counter by one for a DDL validation and discards
    /// this node's old reservation. Go performs this directly through the
    /// meta accessor before changing AUTO_RANDOM bits; the next table object
    /// therefore starts above the new shared high-water mark.
    pub(crate) fn advance_global_one(&self) -> Result<u64, AutoIdError> {
        let (base, end) = self
            .store
            .reserve(1, self.unsigned)
            .map_err(AutoIdError::Store)?;
        if end == base {
            return Err(AutoIdError::Exhausted);
        }
        *self.cache.lock().expect("auto id cache poisoned") = AutoIdRange {
            base: end,
            end: 0,
            step: self.initial_step,
            last_reserve_at: Instant::now(),
        };
        Ok(end)
    }

    /// Go `Allocator.Rebase`: moves the counter so the next id exceeds
    /// `value`. A value the counter is already past is ignored, which is why
    /// an explicit id SMALLER than the counter -- and an `ALTER TABLE ...
    /// AUTO_INCREMENT=` that names a smaller number -- changes nothing.
    ///
    /// A value inside the range this allocator already reserved is settled in
    /// the cache alone: those ids are nobody else's to issue, so raising the
    /// shared counter would say nothing new and would cost a transaction. Go
    /// makes the same cut (`rebase4Signed`: "satisfied by alloc.end, need to
    /// update alloc.base" and nothing more).
    pub(crate) fn rebase(&self, value: u64) -> Result<(), AutoIdStoreError> {
        let mut cache = self.cache.lock().expect("auto id cache poisoned");
        if !exceeds(value, cache.base, self.unsigned) {
            return Ok(());
        }
        if exceeds(value, cache.end, self.unsigned) {
            self.store.rebase(value, self.unsigned)?;
            cache.end = value;
        }
        cache.base = value;
        Ok(())
    }

    /// Go's `AUTO_INCREMENT = n` table option: the counter moves so the NEXT
    /// id is `n`, which is a rebase to its predecessor. `n` at the bottom of
    /// the domain has no predecessor and leaves the counter alone.
    pub(crate) fn rebase_to_next(&self, next: u64) -> Result<(), AutoIdStoreError> {
        let last = if self.unsigned {
            next.checked_sub(1)
        } else {
            (next as i64).checked_sub(1).map(|value| value as u64)
        };
        match last {
            Some(last) => self.rebase(last),
            None => Ok(()),
        }
    }

    /// Go `ALTER TABLE ... FORCE AUTO_INCREMENT = n`: discard any local
    /// reservation and replace the global base so the next allocation is
    /// exactly `n`, even when that moves the counter backwards.
    pub(crate) fn force_rebase_to_next(&self, next: u64) -> Result<(), AutoIdError> {
        if next == 0 {
            return Err(AutoIdError::Exhausted);
        }
        let base = if self.unsigned {
            next - 1
        } else {
            (next as i64).checked_sub(1).ok_or(AutoIdError::Exhausted)? as u64
        };
        let mut cache = self.cache.lock().expect("auto id cache poisoned");
        self.store
            .force_rebase(base, self.unsigned)
            .map_err(AutoIdError::Store)?;
        *cache = AutoIdRange {
            base,
            // An empty reservation is deliberately encoded with end == 0,
            // even when `base` is nonzero: the next allocation must take the
            // normal initial-size reservation from the forced global base,
            // not adapt a fresh cache as though it had just consumed a range.
            end: 0,
            step: self.initial_step,
            last_reserve_at: Instant::now(),
        };
        Ok(())
    }

    /// Drops any range this allocator still holds, so the next allocation
    /// reserves afresh from the stored counter.
    ///
    /// Go reaches the same state by rebuilding the table's allocators when a
    /// new `InfoSchema` is built: a counter that some other writer moved --
    /// `ALTER TABLE ... AUTO_INCREMENT` is the one that moves it out from
    /// under a live node -- must be re-read, or this node keeps handing out
    /// the ids it reserved before the change and the statement looks like it
    /// did nothing. The ids in the abandoned range are skipped, which is the
    /// same hole Go leaves.
    pub(crate) fn forget_reservation(&self) {
        let mut cache = self.cache.lock().expect("auto id cache poisoned");
        *cache = AutoIdRange {
            base: 0,
            end: 0,
            step: self.initial_step,
            last_reserve_at: Instant::now(),
        };
    }

    /// Starts the counter over, so the next id is 1 again.
    ///
    /// Go reaches this by replacing the table (TRUNCATE builds a new table id
    /// with a fresh allocator), so unlike `rebase` it also moves DOWN.
    pub(crate) fn reset(&self) -> Result<(), AutoIdStoreError> {
        let mut cache = self.cache.lock().expect("auto id cache poisoned");
        self.store.reset()?;
        *cache = AutoIdRange {
            base: 0,
            end: 0,
            step: self.initial_step,
            last_reserve_at: Instant::now(),
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Barrier;

    fn allocator(unsigned: bool) -> (Arc<LocalAutoIdStore>, AutoIdAllocator) {
        let store = Arc::new(LocalAutoIdStore::new());
        let mut allocator = AutoIdAllocator::over(store.clone(), DEFAULT_AUTO_ID_STEP);
        allocator.set_unsigned(unsigned);
        (store, allocator)
    }

    fn allocator_over(store: &Arc<LocalAutoIdStore>, unsigned: bool) -> AutoIdAllocator {
        let mut allocator = AutoIdAllocator::over(store.clone(), DEFAULT_AUTO_ID_STEP);
        allocator.set_unsigned(unsigned);
        allocator
    }

    fn global_next(store: &LocalAutoIdStore) -> u64 {
        store.last.load(Ordering::SeqCst).wrapping_add(1)
    }

    fn cache_end(allocator: &AutoIdAllocator) -> u64 {
        allocator.cache.lock().expect("auto id cache").end
    }

    /// Source: `pkg/meta/autoid/autoid_test.go::TestNextStep`.
    #[test]
    fn test_next_step() {
        assert_eq!(next_step(2_000_000, Duration::from_nanos(1)), 2_000_000);
        assert_eq!(next_step(678_910, Duration::from_secs(10)), 678_910);
        assert_eq!(next_step(50_000, Duration::from_secs(600)), 30_000);
    }

    #[test]
    fn changing_cache_step_keeps_the_global_counter_and_discards_the_old_range() {
        let store = Arc::new(LocalAutoIdStore::new());
        let allocator = AutoIdAllocator::over(store.clone(), DEFAULT_AUTO_ID_STEP);
        assert_eq!(allocator.alloc(1, 1), Ok(1));
        assert_eq!(allocator.next_global(), Ok(DEFAULT_AUTO_ID_STEP + 1));

        let allocator = allocator.with_step(100);
        assert_eq!(allocator.alloc(1, 1), Ok(DEFAULT_AUTO_ID_STEP + 1));
        assert_eq!(allocator.next_global(), Ok(DEFAULT_AUTO_ID_STEP + 101));
    }

    /// Complete allocator-value translation of
    /// `pkg/meta/autoid/autoid_test.go::TestSignedAutoid`.
    #[test]
    fn test_signed_autoid() {
        let (store, mut alloc) = allocator(false);
        assert_eq!(global_next(&store), 1);
        assert_eq!(alloc.alloc(1, 1), Ok(1));
        assert_eq!(alloc.alloc(1, 1), Ok(2));
        assert_eq!(global_next(&store), DEFAULT_AUTO_ID_STEP + 1);

        alloc.rebase(1).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(3));
        alloc.rebase(3).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(4));
        alloc.rebase(10).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(11));
        alloc.rebase(3010).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(3011));

        alloc = allocator_over(&store, false);
        assert_eq!(alloc.alloc(1, 1), Ok(DEFAULT_AUTO_ID_STEP + 1));

        let (_, table_two) = allocator(false);
        table_two.rebase(1).unwrap();
        assert_eq!(table_two.alloc(1, 1), Ok(2));

        let (table_three_store, first_table_three) = allocator(false);
        first_table_three.rebase(3210).unwrap();
        let table_three = allocator_over(&table_three_store, false);
        table_three.rebase(3000).unwrap();
        assert_eq!(table_three.alloc(1, 1), Ok(3211));
        table_three.rebase(6543).unwrap();
        assert_eq!(table_three.alloc(1, 1), Ok(6544));
        table_three.rebase(i64::MAX as u64 - 1).unwrap();
        assert_eq!(table_three.alloc(1, 1), Err(AutoIdError::Exhausted));
        table_three.rebase(i64::MAX as u64).unwrap();

        let (batch_store, batch) = allocator(false);
        assert_eq!(global_next(&batch_store), 1);
        assert_eq!(batch.alloc_batch(1, 1, 1), Ok((0, 1)));
        assert_eq!(batch.alloc_batch(2, 1, 1), Ok((1, 3)));
        assert_eq!(batch.alloc_batch(100, 1, 1), Ok((3, 103)));
        batch.rebase(1000).unwrap();
        assert_eq!(batch.alloc_batch(3, 1, 1), Ok((1000, 1003)));
        let last_reserved_end = cache_end(&batch);
        batch.rebase(last_reserved_end - 2).unwrap();
        let (minimum, maximum) = batch.alloc_batch(5, 1, 1).unwrap();
        assert_eq!(maximum - minimum, 5);
        assert!(minimum + 1 > last_reserved_end);

        let (_, stepped) = allocator(false);
        assert_eq!(stepped.alloc_batch(1, 2, 100), Ok((99, 100)));
        assert_eq!(stepped.alloc_batch(2, 2, 100), Ok((100, 104)));
        assert_eq!(calc_needed_batch_size(100, 2, 2, 100, false), 4);
        assert_eq!(stepped.alloc_batch(3, 5, 100), Ok((104, 115)));
        assert_eq!(calc_needed_batch_size(104, 3, 5, 100, false), 11);
        assert_eq!(seek_to_first(104, 5, 100, false), 105);
        assert_eq!(stepped.alloc_batch(2, 15, 100), Ok((115, 145)));
        assert_eq!(calc_needed_batch_size(115, 2, 15, 100, false), 30);
        assert_eq!(seek_to_first(115, 15, 100, false), 130);
        assert_eq!(stepped.alloc_batch(2, 15, 200), Ok((199, 215)));
        assert_eq!(calc_needed_batch_size(199, 2, 15, 200, false), 16);
        assert_eq!(seek_to_first(199, 15, 200, false), 200);
    }

    /// Complete allocator-value translation of
    /// `pkg/meta/autoid/autoid_test.go::TestUnsignedAutoid`.
    #[test]
    fn test_unsigned_autoid() {
        let (store, mut alloc) = allocator(true);
        assert_eq!(global_next(&store), 1);
        assert_eq!(alloc.alloc(1, 1), Ok(1));
        assert_eq!(alloc.alloc(1, 1), Ok(2));
        assert_eq!(global_next(&store), DEFAULT_AUTO_ID_STEP + 1);

        alloc.rebase(1).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(3));
        alloc.rebase(3).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(4));
        alloc.rebase(10).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(11));
        alloc.rebase(3010).unwrap();
        assert_eq!(alloc.alloc(1, 1), Ok(3011));

        alloc = allocator_over(&store, true);
        assert_eq!(alloc.alloc(1, 1), Ok(DEFAULT_AUTO_ID_STEP + 1));

        let (_, table_two) = allocator(true);
        table_two.rebase(1).unwrap();
        assert_eq!(table_two.alloc(1, 1), Ok(2));

        let (table_three_store, first_table_three) = allocator(true);
        first_table_three.rebase(3210).unwrap();
        let table_three = allocator_over(&table_three_store, true);
        table_three.rebase(3000).unwrap();
        assert_eq!(table_three.alloc(1, 1), Ok(3211));
        table_three.rebase(6543).unwrap();
        assert_eq!(table_three.alloc(1, 1), Ok(6544));
        table_three.rebase(u64::MAX - 1).unwrap();
        assert_eq!(table_three.alloc(1, 1), Err(AutoIdError::Exhausted));
        table_three.rebase(u64::MAX).unwrap();

        let (batch_store, batch) = allocator(true);
        assert_eq!(global_next(&batch_store), 1);
        assert_eq!(batch.alloc_batch(2, 1, 1), Ok((0, 2)));
        batch.rebase(500).unwrap();
        assert_eq!(batch.alloc_batch(2, 1, 1), Ok((500, 502)));
        let last_reserved_end = cache_end(&batch);
        batch.rebase(last_reserved_end - 2).unwrap();
        let (minimum, maximum) = batch.alloc_batch(5, 1, 1).unwrap();
        assert_eq!(maximum - minimum, 5);
        assert!(minimum + 1 > last_reserved_end);

        let (_, stepped) = allocator(true);
        let offset = u64::MAX - 100;
        assert_eq!(
            stepped.alloc_batch(2, 2, offset),
            Ok((u64::MAX - 101, u64::MAX - 98))
        );
        assert_eq!(
            calc_needed_batch_size(u64::MAX - 101, 2, 2, offset, true),
            3
        );
        assert_eq!(seek_to_first(u64::MAX - 101, 2, offset, true), offset);
    }

    /// Source: `pkg/meta/autoid/autoid_test.go::TestConcurrentAlloc`.
    #[test]
    fn test_concurrent_alloc() {
        let store = Arc::new(LocalAutoIdStore::new());
        let seen = Arc::new(Mutex::new(HashSet::new()));
        let start = Arc::new(Barrier::new(11));
        let mut workers = Vec::new();

        for worker in 0_u64..10 {
            let allocator = AutoIdAllocator::over(store.clone(), 100);
            let seen = seen.clone();
            let start = start.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                for iteration in 0_u64..105 {
                    let id = allocator.alloc(1, 1).unwrap();
                    assert!(seen.lock().unwrap().insert(id), "duplicate id {id}");

                    // The Go test chooses 0..99 randomly. A deterministic
                    // permutation keeps every batch size in play without
                    // making a parity test probabilistic.
                    let n = (worker * 37 + iteration * 17) % 100;
                    let (minimum, maximum) = allocator.alloc_batch(n, 1, 1).unwrap();
                    let mut seen = seen.lock().unwrap();
                    for id in minimum + 1..=maximum {
                        assert!(seen.insert(id), "duplicate id {id}");
                    }
                }
            }));
        }
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[derive(Debug)]
    struct FailingStore;

    impl AutoIdStore for FailingStore {
        fn reserve(&self, _step: u64, _unsigned: bool) -> Result<(u64, u64), AutoIdStoreError> {
            Err(AutoIdStoreError("injected".to_owned()))
        }

        fn next_global(&self) -> Result<u64, AutoIdStoreError> {
            Err(AutoIdStoreError("injected".to_owned()))
        }

        fn rebase(&self, _required: u64, _unsigned: bool) -> Result<(), AutoIdStoreError> {
            Err(AutoIdStoreError("injected".to_owned()))
        }

        fn force_rebase(&self, _required: u64, _unsigned: bool) -> Result<(), AutoIdStoreError> {
            Err(AutoIdStoreError("injected".to_owned()))
        }

        fn reset(&self) -> Result<(), AutoIdStoreError> {
            Err(AutoIdStoreError("injected".to_owned()))
        }
    }

    /// Source: `pkg/meta/autoid/autoid_test.go::TestRollbackAlloc`.
    #[test]
    fn test_rollback_alloc() {
        let allocator = AutoIdAllocator::over(Arc::new(FailingStore), 1);
        assert_eq!(
            allocator.alloc(1, 1),
            Err(AutoIdError::Store(AutoIdStoreError("injected".to_owned())))
        );
        {
            let cache = allocator.cache.lock().unwrap();
            assert_eq!((cache.base, cache.end), (0, 0));
        }

        assert_eq!(
            allocator.rebase(100),
            Err(AutoIdStoreError("injected".to_owned()))
        );
        let cache = allocator.cache.lock().unwrap();
        assert_eq!((cache.base, cache.end), (0, 0));
    }

    #[test]
    fn force_rebase_discards_the_reserved_range() {
        let (store, allocator) = allocator(false);
        assert_eq!(allocator.alloc(1, 1), Ok(1));
        assert_eq!(allocator.alloc(1, 1), Ok(2));
        // The dynamic allocator has already reserved a range past 2. FORCE
        // must replace both that cache and the shared home, or a later insert
        // would keep handing out the stale reserved ids instead of 2.
        allocator.force_rebase_to_next(2).unwrap();
        assert_eq!(allocator.alloc(1, 1), Ok(2));
        assert_eq!(store.last.load(Ordering::SeqCst), DEFAULT_AUTO_ID_STEP + 1);
        assert_eq!(
            allocator.force_rebase_to_next(0),
            Err(AutoIdError::Exhausted)
        );
    }

    /// Source: `pkg/meta/autoid/autoid_test.go::TestIssue40584`.
    #[test]
    fn test_issue40584() {
        let allocator = Arc::new(AutoIdAllocator::new());
        let start = Arc::new(Barrier::new(3));

        let allocating = {
            let allocator = allocator.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..20_000 {
                    allocator.alloc(1, 1).unwrap();
                }
            })
        };
        let reading = {
            let allocator = allocator.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..20_000 {
                    let _ = allocator.next();
                }
            })
        };

        start.wait();
        allocating.join().unwrap();
        reading.join().unwrap();
        assert_eq!(allocator.next(), 20_001);
    }

    /// Complete observable translation of
    /// `pkg/meta/autoid/memid_test.go::TestInMemoryAlloc`.
    ///
    #[test]
    fn test_in_memory_alloc() {
        fn alloc_many(
            allocator: &AutoIdAllocator,
            count: u64,
            increment: u64,
            offset: u64,
        ) -> Result<u64, AutoIdError> {
            allocator
                .alloc_batch(count, increment, offset)
                .map(|(_, maximum)| maximum)
        }

        let allocator = AutoIdAllocator::new();
        assert_eq!(allocator.next(), 1);
        assert_eq!(alloc_many(&allocator, 1, 1, 1), Ok(1));
        assert_eq!(allocator.next(), 2);
        assert_eq!(alloc_many(&allocator, 1, 1, 1), Ok(2));
        assert_eq!(alloc_many(&allocator, 10, 1, 1), Ok(12));
        assert_eq!(alloc_many(&allocator, 1, 10, 1), Ok(21));
        assert_eq!(alloc_many(&allocator, 1, 1, 30), Ok(30));

        allocator.rebase(40).unwrap();
        assert_eq!(alloc_many(&allocator, 1, 1, 1), Ok(41));
        assert_eq!(allocator.next(), 42);
        allocator.rebase(10).unwrap();
        assert_eq!(alloc_many(&allocator, 1, 1, 1), Ok(42));

        allocator.rebase(i64::MAX as u64 - 2).unwrap();
        assert_eq!(alloc_many(&allocator, 1, 1, 1), Ok(i64::MAX as u64 - 1));
        assert_eq!(alloc_many(&allocator, 1, 1, 1), Err(AutoIdError::Exhausted));

        let mut unsigned = AutoIdAllocator::new();
        unsigned.set_unsigned(true);
        let near_unsigned_max = u64::MAX - 2;
        unsigned.rebase(near_unsigned_max).unwrap();
        assert_eq!(unsigned.next(), near_unsigned_max + 1);
        assert_eq!(alloc_many(&unsigned, 1, 1, 1), Ok(near_unsigned_max + 1));
        assert_eq!(alloc_many(&unsigned, 1, 1, 1), Err(AutoIdError::Exhausted));

        let initial_base = AutoIdAllocator::new();
        initial_base.rebase_to_next(100).unwrap();
        assert_eq!(initial_base.next(), 100);
        assert_eq!(alloc_many(&initial_base, 1, 1, 1), Ok(100));
    }
}
