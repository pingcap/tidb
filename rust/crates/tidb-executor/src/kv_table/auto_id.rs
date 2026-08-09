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
//! The in-process home ([`LocalAutoIdStore`]) reserves one id at a time, so it
//! caches nothing and every id comes straight off its cell: exactly the
//! semantics that tier had before the counter had a home at all.
//!
//! The callers that drive the allocator -- `KvTable::apply_auto_increment`,
//! `rebase_auto_increment`, `truncate` -- stay with the table in the parent
//! module.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Go `autoid.GetStep()`'s `defaultStep`: how many ids an allocator reserves
/// from a shared store at once.
///
/// DIVERGENCE (documented, and only about how OFTEN the store is read): Go
/// grows this between reservations from how fast the last one was consumed
/// (`NextStep`, `minStep` 30000 up to `maxStep` 2000000). A fixed step issues
/// the same ids in the same order and differs only in the number of meta-key
/// transactions a long insert run costs.
pub const DEFAULT_AUTO_ID_STEP: u64 = 30000;

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

    /// Go `rebase4Signed`/`rebase4Unsigned` with `allocIDs == false`: raise
    /// the stored value to `required` if it is not already past it, so the
    /// next id ANY node allocates exceeds `required`.
    ///
    /// Never lowers, which is what makes an explicit id below the counter a
    /// no-op on every tier.
    fn rebase(&self, required: u64, unsigned: bool) -> Result<(), AutoIdStoreError>;

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
    /// Go `allocator.step`: how much to reserve when the cache runs dry.
    step: u64,
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
}

impl AutoIdAllocator {
    /// A fresh in-process allocator, whose first id is 1.
    pub(crate) fn new() -> Self {
        Self::over(Arc::new(LocalAutoIdStore::new()), 1)
    }

    /// An allocator over `store`, reserving `step` ids at a time.
    pub(crate) fn over(store: Arc<dyn AutoIdStore>, step: u64) -> Self {
        AutoIdAllocator {
            // An empty range, so the first allocation reserves.
            cache: Arc::new(Mutex::new(AutoIdRange { base: 0, end: 0 })),
            store,
            step: step.max(1),
            unsigned: false,
        }
    }

    /// Whether `other` is a clone of this allocator rather than a second one
    /// over the same store: same cache, so the same reserved range.
    pub(crate) fn shares_cache_with(&self, other: &AutoIdAllocator) -> bool {
        Arc::ptr_eq(&self.cache, &other.cache)
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
        let mut target = seek_to_first(cache.base, increment, offset, self.unsigned);
        if headroom(cache.base, self.unsigned) <= u128::from(target.wrapping_sub(cache.base)) {
            return Err(AutoIdError::Exhausted);
        }
        if exceeds(target, cache.end, self.unsigned) {
            // The cached range cannot answer the seek: reserve the next one
            // from the store, which is also where a peer node's allocations
            // become visible. Go re-seeks from the NEW base rather than
            // carrying the old target across (`alloc4Signed`: "CalcNeededBatchSize
            // calculates the total batch size needed on global base"), because
            // the store may have moved the counter further than this node knew.
            let needed = self.step.max(target.wrapping_sub(cache.base));
            let (base, end) = self
                .store
                .reserve(needed, self.unsigned)
                .map_err(AutoIdError::Store)?;
            *cache = AutoIdRange { base, end };
            target = seek_to_first(base, increment, offset, self.unsigned);
            if headroom(base, self.unsigned) <= u128::from(target.wrapping_sub(base)) {
                return Err(AutoIdError::Exhausted);
            }
        }
        cache.base = target;
        Ok(target)
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

    /// Starts the counter over, so the next id is 1 again.
    ///
    /// Go reaches this by replacing the table (TRUNCATE builds a new table id
    /// with a fresh allocator), so unlike `rebase` it also moves DOWN.
    pub(crate) fn reset(&self) -> Result<(), AutoIdStoreError> {
        let mut cache = self.cache.lock().expect("auto id cache poisoned");
        self.store.reset()?;
        *cache = AutoIdRange { base: 0, end: 0 };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Complete observable translation of
    /// `pkg/meta/autoid/memid_test.go::TestInMemoryAlloc`.
    ///
    /// Rust's table-facing allocator issues one id per call, while Go's
    /// package method can return a batch range. Repeating the one-id call
    /// preserves every value the source test observes: its assertions use
    /// only the returned maximum, never the batch minimum.
    #[test]
    fn test_in_memory_alloc() {
        fn alloc_many(
            allocator: &AutoIdAllocator,
            count: u64,
            increment: u64,
            offset: u64,
        ) -> Result<u64, AutoIdError> {
            let mut last = 0;
            for _ in 0..count {
                last = allocator.alloc(increment, offset)?;
            }
            Ok(last)
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
