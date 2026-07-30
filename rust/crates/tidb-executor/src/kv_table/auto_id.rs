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
//! Inside: [`AutoIdAllocator`], holding Go's `allocator.base` (the id LAST
//! handed out) as a raw 64-bit pattern plus the column's signedness, and
//! [`AutoIncrementExhausted`], Go `autoid.ErrAutoincReadFailed`. The type
//! doc records why both representation choices are Go's and what each one
//! removes.
//!
//! Mirrors Go `pkg/meta/autoid/autoid.go` (`Alloc`, `Rebase`,
//! `rebase4Signed`/`rebase4Unsigned`, `alloc4Unsigned`). The callers that
//! drive it -- `KvTable::apply_auto_increment`, `rebase_auto_increment`,
//! `truncate` -- stay with the table in the parent module.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Go `autoid.Allocator`: the AUTO_INCREMENT counter, which lives OUTSIDE
/// transaction semantics.
///
/// Go allocates auto ids against the meta store in a transaction of their
/// own, so an id is consumed the moment a row asks for one and is never
/// handed back -- not by a statement that fails afterwards, and not by a
/// transaction that rolls back. Holding the counter in a SHARED cell rather
/// than as a plain field is what reproduces that here: a transaction stages a
/// COPY of the catalog, and the copied tables allocate from the very counter
/// the committed ones do, so dropping the staged copy on ROLLBACK discards
/// the rows while keeping the burn. As a plain field, "returned on rollback"
/// would be the normal path and every failure site would need its own burn
/// fixup.
///
/// The counter holds Go's `allocator.base` -- the id LAST handed out, not the
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
#[derive(Clone, Debug)]
pub(crate) struct AutoIdAllocator {
    /// Go `allocator.base`: the id last handed out, as its 64-bit pattern.
    last: Arc<AtomicU64>,
    /// Go `allocator.isUnsigned`: which domain the pattern is read in.
    pub(crate) unsigned: bool,
}

/// Go `autoid.ErrAutoincReadFailed` (1467): the allocator has no id left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoIncrementExhausted;

impl AutoIdAllocator {
    /// A fresh signed allocator, whose first id is 1.
    pub(crate) fn new() -> Self {
        AutoIdAllocator {
            last: Arc::new(AtomicU64::new(0)),
            unsigned: false,
        }
    }

    /// Go `allocator.isUnsigned`, set from the AUTO_INCREMENT column's type.
    pub(crate) fn set_unsigned(&mut self, unsigned: bool) {
        self.unsigned = unsigned;
    }

    /// How many ids remain above `last` in the column's domain.
    const fn headroom(&self, last: u64) -> u128 {
        if self.unsigned {
            (u64::MAX as u128) - (last as u128)
        } else {
            // Read in the signed domain, where `last` is at most `i64::MAX`
            // and may in principle be negative.
            ((i64::MAX as i128) - (last as i64 as i128)) as u128
        }
    }

    /// True when `value` ranks after `other` in the column's domain.
    const fn exceeds(&self, value: u64, other: u64) -> bool {
        if self.unsigned {
            value > other
        } else {
            (value as i64) > (other as i64)
        }
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
    /// DIVERGENCE (documented): the capture shows one exception -- an `ALTER
    /// TABLE ... AUTO_INCREMENT = 9223372036854775807` followed by an insert
    /// DOES hand out `9223372036854775807` in Go, from the same counter value
    /// that refuses a plain insert. That is Go's batch cache reaching past its
    /// own guard on the ALTER path, not a rule; refusing one id earlier is the
    /// safe side of it, since the id at the very end of the domain is one this
    /// allocator then never issues twice.
    pub(crate) fn alloc(&self) -> Result<u64, AutoIncrementExhausted> {
        let mut last = self.last.load(Ordering::SeqCst);
        loop {
            if self.headroom(last) <= 1 {
                return Err(AutoIncrementExhausted);
            }
            let next = last.wrapping_add(1);
            match self
                .last
                .compare_exchange_weak(last, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok(next),
                Err(observed) => last = observed,
            }
        }
    }

    /// The id the next allocation will return, as a pattern. Meaningful only
    /// while ids remain, which is what `SHOW TABLE STATUS` reports.
    pub(crate) fn next(&self) -> u64 {
        self.last.load(Ordering::SeqCst).wrapping_add(1)
    }

    /// Go `Allocator.Rebase`: moves the counter so the next id exceeds
    /// `value`. A value the counter is already past is ignored, which is why
    /// an explicit id SMALLER than the counter -- and an `ALTER TABLE ...
    /// AUTO_INCREMENT=` that names a smaller number -- changes nothing.
    pub(crate) fn rebase(&self, value: u64) {
        let mut last = self.last.load(Ordering::SeqCst);
        while self.exceeds(value, last) {
            match self
                .last
                .compare_exchange_weak(last, value, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return,
                Err(observed) => last = observed,
            }
        }
    }

    /// Go's `AUTO_INCREMENT = n` table option: the counter moves so the NEXT
    /// id is `n`, which is a rebase to its predecessor. `n` at the bottom of
    /// the domain has no predecessor and leaves the counter alone.
    pub(crate) fn rebase_to_next(&self, next: u64) {
        let last = if self.unsigned {
            next.checked_sub(1)
        } else {
            (next as i64).checked_sub(1).map(|value| value as u64)
        };
        if let Some(last) = last {
            self.rebase(last);
        }
    }

    /// Starts the counter over, so the next id is 1 again.
    ///
    /// Go reaches this by replacing the table (TRUNCATE builds a new table id
    /// with a fresh allocator), so unlike `rebase` it also moves DOWN.
    pub(crate) fn reset(&self) {
        self.last.store(0, Ordering::SeqCst);
    }
}
