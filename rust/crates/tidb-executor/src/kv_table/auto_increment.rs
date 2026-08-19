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

//! The table's `AUTO_INCREMENT` machinery, split out of `kv_table.rs`.
//!
//! Everything about where a row's auto-increment id comes from lives here:
//! the shared allocator handle a node keeps across catalog rebuilds
//! ([`TableAutoId`]), the three origins Go distinguishes ([`AutoIncrement`]),
//! and the [`KvTable`] methods that mark the column, rebase the counter, draw
//! an id for a row, and refuse an id the column's own width cannot hold.
//!
//! The counting itself is one level down in [`super::auto_id`]; this module
//! is the seam between that counter and a table's columns.

use super::auto_id::{self, exceeds, AutoIdAllocator, AutoIdError, AutoIdStore, AutoIdStoreError};
use super::KvTable;
use std::sync::Arc;
use tidb_datatype::{
    integer_signed_upper_bound, integer_unsigned_upper_bound, type_str, Datum, FieldTypeCode,
};

/// One table's live auto-increment allocator, held by whoever owns the
/// table's lifetime rather than by the table itself.
///
/// Cloning shares the reserved range and the counter's home, which is what
/// lets a node keep one allocator per table across catalog rebuilds and hand
/// each rebuilt [`KvTable`] a clone of it -- Go's arrangement, where the
/// allocator sits on the domain's table cache and the `TableInfo` carries no
/// counter at all.
#[derive(Clone, Debug)]
pub struct TableAutoId(pub(super) AutoIdAllocator);

impl TableAutoId {
    /// Drops any reserved range, so the next id comes from a fresh read of
    /// the stored counter. See `AutoIdAllocator::forget_reservation`.
    pub fn forget_reservation(&self) {
        self.0.forget_reservation();
    }
}

impl TableAutoId {
    /// An allocator over `store`, reserving `step` ids at a time.
    #[must_use]
    pub fn over(store: Arc<dyn AutoIdStore>, step: u64) -> Self {
        TableAutoId(AutoIdAllocator::over(store, step))
    }

    /// Whether both handles drive the same allocator, and so the same
    /// reserved range.
    ///
    /// The registry that hands these out must answer the same table with
    /// clones of one allocator; this is how that invariant is asserted rather
    /// than assumed.
    #[must_use]
    pub fn same_allocator_as(&self, other: &TableAutoId) -> bool {
        self.0.shares_cache_with(&other.0)
    }

    /// A fresh cached range over the same persistent counter.
    #[must_use]
    pub fn with_step(&self, step: u64) -> Self {
        TableAutoId(self.0.with_step(step))
    }
}

/// Where a row's `AUTO_INCREMENT` value came from.
///
/// Go splits the same three ways and treats them differently, so collapsing
/// any pair loses a rule. `Given` is the arm that `continue`s before the retry
/// cursor (`insert_common.go:894-903`) and never touches `lastInsertID`;
/// `Reused` is the consume loop (`insert_common.go:909-921`), which likewise
/// leaves `lastInsertID` alone so a replay cannot move the value a client
/// already read; only `Allocated` -- the `AllocBatchAutoIncrementValue` arm --
/// sets it (`insert_common.go:936-938`).
///
/// All three still get RECORDED for the next attempt, which is the one rule
/// that is uniform, and so the recording is unconditional at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoIncrement {
    /// The table has no `AUTO_INCREMENT` column; nothing was placed.
    Absent,
    /// The row supplied its own non-zero id, carried as its 64-bit pattern.
    /// The counter was rebased past it.
    Given(u64),
    /// The id came back from the losing attempt's list rather than the counter.
    Reused(i64),
    /// The id was drawn from the counter.
    Allocated(i64),
}

impl AutoIncrement {
    /// The id placed in the row, or `None` when there was no column to place
    /// one in. Every placed id is recorded for the next attempt, whichever arm
    /// produced it.
    #[must_use]
    pub fn placed(self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Given(id) => Some(id),
            Self::Reused(id) | Self::Allocated(id) => Some(id.max(0) as u64),
        }
    }
}

impl KvTable {
    /// Marks the AUTO_INCREMENT column.
    ///
    /// The column's signedness travels to the allocator here, because it is
    /// what decides the domain every later id is compared and counted in
    /// (Go's `isUnsigned`, taken from the same `ColumnInfo` flag).
    pub fn set_auto_increment_offset(&mut self, offset: usize) {
        self.auto_increment_offset = Some(offset);
        let unsigned = self
            .columns
            .get(offset)
            .is_some_and(|column| column.field_type.is_unsigned());
        self.auto_id.set_unsigned(unsigned);
    }

    /// Unmarks the AUTO_INCREMENT column, which `ALTER TABLE ... MODIFY
    /// COLUMN` does when the new definition drops the option and
    /// `@@tidb_allow_remove_auto_inc` allows it. The allocator itself stays,
    /// as Go's does: nothing hands out ids while no column claims them, and
    /// re-adding the option is refused anyway.
    pub fn clear_auto_increment_offset(&mut self) {
        self.auto_increment_offset = None;
    }

    /// Gives the table an allocator built elsewhere and kept alive across
    /// catalog rebuilds.
    ///
    /// This is the seam the cluster tier uses to give the counter the meta-key
    /// home Go gives it, and the sharing is as load-bearing as the home. Go's
    /// allocator lives on the domain's table cache, not on the `TableInfo`, so
    /// it outlives every schema reload; this tier rebuilds its `KvTable`s
    /// whenever the schema version or the stats snapshot moves, and an
    /// allocator rebuilt with them would throw away its reserved range and
    /// reserve a fresh one -- leaving a visible hole in the ids on a table
    /// nothing was wrong with. Handing in a [`TableAutoId`] the node already
    /// holds is what keeps the range across the rebuild.
    ///
    /// The column's signedness is re-derived rather than carried, since it is
    /// a fact about this table's column and
    /// [`set_auto_increment_offset`](Self::set_auto_increment_offset) may run
    /// on either side of this call.
    pub fn set_auto_id(&mut self, shared: TableAutoId) {
        self.auto_id = shared.0;
        if let Some(offset) = self.auto_increment_offset {
            self.set_auto_increment_offset(offset);
        }
    }

    /// The AUTO_INCREMENT column's offset, if any.
    #[must_use]
    pub fn auto_increment_offset(&self) -> Option<usize> {
        self.auto_increment_offset
    }

    /// The AUTO_INCREMENT column's name.
    #[must_use]
    fn auto_increment_column_name(&self) -> Option<&str> {
        self.auto_increment_offset
            .and_then(|offset| self.columns.get(offset))
            .map(|column| column.name.as_str())
    }

    /// Rows produced by Go's `ShowNextRowIDExec` for this table.
    pub fn next_global_row_ids(
        &self,
    ) -> Result<Vec<(String, i64, &'static str)>, AutoIdStoreError> {
        let mut rows = Vec::new();
        let has_implicit_row_id =
            self.pk_handle_offset.is_none() && self.common_handle_offsets.is_empty();
        if has_implicit_row_id || self.auto_increment_offset.is_some() {
            let column = if self.pk_handle_offset.is_some() {
                self.auto_increment_column_name().unwrap_or_default()
            } else {
                "_tidb_rowid"
            };
            rows.push((
                column.to_owned(),
                self.auto_id.next_global()? as i64,
                "_TIDB_ROWID",
            ));
        }
        if let Some(spec) = self.auto_random {
            let column = self
                .columns
                .get(spec.offset)
                .map_or("", |column| column.name.as_str());
            rows.push((
                column.to_owned(),
                self.auto_random_id.next_global()? as i64,
                "AUTO_RANDOM",
            ));
        }
        Ok(rows)
    }

    /// Rebuilds the allocator after `ALTER TABLE ... AUTO_ID_CACHE=n` while
    /// retaining the counter's global high-water mark.
    pub fn set_auto_id_cache(&mut self, cache: u64) -> Result<(), &'static str> {
        let single_point = cache == 1;
        if single_point != self.auto_id.is_single_point() {
            return Err(
                "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different",
            );
        }
        let step = if cache == 0 {
            auto_id::DEFAULT_AUTO_ID_STEP
        } else {
            cache
        };
        self.auto_id = self.auto_id.with_step(step);
        self.auto_random_id = self.auto_random_id.with_step(step);
        Ok(())
    }

    /// Go's `AUTO_INCREMENT=n` table option: the first id the table hands out.
    ///
    /// Go seeds the allocator so the next id is `n`, so `AUTO_INCREMENT=100`
    /// at CREATE makes the first row land on 100. On an existing table
    /// (`ALTER TABLE ... AUTO_INCREMENT=n`) it is a Rebase, which only ever
    /// moves the counter UP -- naming a smaller number leaves it alone.
    ///
    /// `next_id` carries the option's 64-bit PATTERN (Go's
    /// `int64(opt.UintValue)`), and `rebase_to_next` reads it in the auto
    /// column's own domain -- Go's `adjustNewBaseToNextGlobalID`, which is
    /// why `ALTER TABLE ... AUTO_INCREMENT = 18446744073709551615` really does
    /// move a `BIGINT UNSIGNED` counter to the top of its range while the same
    /// number on a signed column is a negative base and moves nothing.
    /// CREATE does NOT share that domain-aware read; see
    /// `auto_increment_option`'s caller.
    pub fn rebase_auto_increment(&mut self, next_id: i64) -> Result<(), AutoIdStoreError> {
        self.auto_id.rebase_to_next(next_id as u64)
    }

    /// Go `ALTER TABLE ... FORCE AUTO_INCREMENT = n`: unlike the ordinary
    /// table option, this deliberately moves the shared counter down and
    /// drops any allocator range already reserved by this table.
    pub fn force_rebase_auto_increment(&mut self, next_id: i64) -> Result<(), AutoIdError> {
        self.auto_id.force_rebase_to_next(next_id as u64)
    }

    /// Go `adjustAutoIncrementDatum`: fills the auto-increment column.
    ///
    /// An omitted, NULL or zero value takes the next allocated id; an explicit
    /// non-zero value is kept and REBASES the allocator so later rows exceed
    /// it. Returns the id allocated for this row, which the statement reports
    /// as `LAST_INSERT_ID` for the first such row. Fails with Go's
    /// `ErrAutoincReadFailed` (1467) once the column's domain is exhausted.
    ///
    /// The explicit value is carried as its 64-bit PATTERN rather than as an
    /// `i64`, so a `BIGINT UNSIGNED` id above `i64::MAX` rebases the allocator
    /// in its own domain. Reading it as a signed integer made it negative, the
    /// rebase then ignored it, and the allocator went on to hand out ids the
    /// table already held (captured: `INSERT ... VALUES
    /// (18446744073709551615)` leaves the next insert with no id at all,
    /// `[autoid:1467]`, never with a duplicate).
    ///
    /// The INSERT path does not call this for a supplied zero when
    /// `NO_AUTO_VALUE_ON_ZERO` is set; every row that reaches this helper
    /// follows the normal allocation-or-rebase rules.
    ///
    /// `reuse` is Go's `RetryInfo` arm: a statement being RUN AGAIN after a
    /// write conflict is handed the id its losing attempt already assigned to
    /// this row, so `LAST_INSERT_ID()` and the stored row still agree. It
    /// enters HERE rather than at the call site so the reused id goes through
    /// the same domain check and the same column-typed placement an allocated
    /// one does -- a reused id that no longer fits its column must fail the way
    /// a fresh one would, not slip past untested.
    ///
    /// It is a CLOSURE because the cursor it reads must advance only on the
    /// rows that actually take an id from it. Go's batch arm
    /// (`lazyAdjustAutoIncrementDatum`, `insert_common.go:894-903`) hits
    /// `continue` on an explicitly-supplied id and so never reaches the
    /// consume loop below it (`insert_common.go:909-921`); a call site that
    /// drew from the cursor unconditionally shifted every later row's id by
    /// one per explicit id in the batch. Measured against TiDB -- see the
    /// receipt on `RetryAutoIds`.
    pub fn apply_auto_increment(
        &mut self,
        row: &mut [Datum],
        step: (u64, u64),
        reuse: impl FnOnce() -> Option<u64>,
    ) -> Result<AutoIncrement, AutoIdError> {
        let Some(offset) = self.auto_increment_offset else {
            return Ok(AutoIncrement::Absent);
        };
        let current = match row.get(offset) {
            Some(Datum::Int(value)) => *value as u64,
            Some(Datum::UInt(value)) => *value,
            _ => 0,
        };
        if current != 0 {
            // Go rebases so the next allocation is past the explicit value,
            // and a value the counter is already past changes nothing.
            self.auto_id.rebase(current).map_err(AutoIdError::Store)?;
            return Ok(AutoIncrement::Given(current));
        }
        let (increment, step_offset) = auto_id::increment_and_offset(step.0, step.1);
        // A replay does not draw from the counter at all: the id it is
        // rewriting was already drawn, and drawing again is exactly the gap
        // that moves `LAST_INSERT_ID()` off the row it names.
        let (id, reused) = match reuse() {
            Some(id) => (id, true),
            None => (self.auto_id.alloc(increment, step_offset)?, false),
        };
        self.check_auto_increment_fits(offset, id)?;
        // The allocated id skips the per-column cast the written values went
        // through, so it is placed in the column's own domain here.
        row[offset] = if self.auto_id.unsigned {
            Datum::UInt(id)
        } else {
            Datum::Int(id as i64)
        };
        Ok(if reused {
            AutoIncrement::Reused(id as i64)
        } else {
            AutoIncrement::Allocated(id as i64)
        })
    }

    /// Go `setDatumAutoIDAndCast`: the id the allocator handed out is CAST to
    /// the column's own type before it is written, so a column narrower than
    /// `BIGINT` refuses the id that does not fit it.
    ///
    /// The allocator counts in the full 64-bit domain -- Go's `autoid`
    /// package knows only signedness, never the column's width -- so a
    /// `TINYINT AUTO_INCREMENT` sitting at `127` still gets `128` handed to
    /// it, and it is this cast that turns that into `[types:1690]constant 128
    /// overflows tinyint`. Without it the row is written with a value the
    /// column cannot hold. Captured across every width and both
    /// signednesses: the id AT the type's maximum is accepted (`127`, `255`,
    /// `32767`, `65535`, `8388607`, `16777215`, `2147483647`, `4294967295`)
    /// and the next one is refused.
    ///
    /// The bound is carried as a 64-bit PATTERN read in the column's domain,
    /// which is what keeps `BIGINT UNSIGNED` correct: its maximum is above
    /// `i64::MAX`, so a bound computed as a signed integer would truncate and
    /// refuse ids the column holds perfectly well. At `BIGINT` width the
    /// bound IS the domain end, so this check can never fire there and the
    /// allocator's own exhaustion rule (`1467`, one id earlier) stays the
    /// only limit -- the two rules do not overlap.
    fn check_auto_increment_fits(&self, offset: usize, allocated: u64) -> Result<(), AutoIdError> {
        let Some(column) = self.columns.get(offset) else {
            return Ok(());
        };
        let code = column.field_type.code();
        let unsigned = self.auto_id.unsigned;
        // Go allows AUTO_INCREMENT on FLOAT/DOUBLE too, whose cast is not an
        // integer range check; only the integer widths are bounded here.
        let limit = match code {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                if unsigned {
                    integer_unsigned_upper_bound(code)
                } else {
                    integer_signed_upper_bound(code) as u64
                }
            }
            _ => return Ok(()),
        };
        if exceeds(allocated, limit, unsigned) {
            let value = if unsigned {
                allocated.to_string()
            } else {
                (allocated as i64).to_string()
            };
            return Err(AutoIdError::OutOfRange {
                value,
                type_name: type_str(code).to_owned(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_table::KvColumn;
    use tidb_datatype::FieldType;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    /// A table whose only column is an AUTO_INCREMENT `BIGINT`, signed or not.
    fn auto_increment_table(unsigned: bool) -> KvTable {
        let mut table = KvTable::new(
            7,
            vec![KvColumn {
                name: "id".to_owned(),
                id: 1,
                field_type: long().with_unsigned(unsigned),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                default_value: None,
                origin_default: None,
                generated: None,
            }],
        );
        table.set_auto_increment_offset(0);
        table
    }

    /// An explicit id at the domain's end leaves the allocator with nothing to
    /// hand out, and it says so instead of wrapping or repeating.
    ///
    /// Captured from TiDB: `BIGINT` at `9223372036854775807` and `BIGINT
    /// UNSIGNED` at both `18446744073709551614` and `18446744073709551615` all
    /// answer the next insert with `[autoid:1467]`. The unsigned pair is here
    /// rather than in the session tests because a literal above `i64::MAX` is
    /// not yet expressible in this tier's SQL.
    #[test]
    fn the_allocator_refuses_at_the_end_of_the_columns_domain() {
        for (unsigned, explicit, pattern) in [
            (false, Datum::Int(i64::MAX), i64::MAX as u64),
            (true, Datum::UInt(u64::MAX), u64::MAX),
            (true, Datum::UInt(u64::MAX - 1), u64::MAX - 1),
        ] {
            let mut table = auto_increment_table(unsigned);
            let mut row = [explicit];
            assert_eq!(
                table.apply_auto_increment(&mut row, (1, 1), || None),
                Ok(AutoIncrement::Given(pattern))
            );
            let mut row = [Datum::Null];
            assert_eq!(
                table.apply_auto_increment(&mut row, (1, 1), || None),
                Err(AutoIdError::Exhausted),
                "unsigned={unsigned}"
            );
        }
    }

    /// An explicit UNSIGNED id above `i64::MAX` rebases the allocator in its
    /// own domain, and the id that follows it is the next unsigned integer --
    /// not the low id a signed reading of the counter would have re-issued on
    /// top of the row just written.
    #[test]
    fn an_unsigned_explicit_id_rebases_in_the_unsigned_domain() {
        let mut table = auto_increment_table(true);
        let mut row = [Datum::UInt(1 << 63)];
        assert_eq!(
            table.apply_auto_increment(&mut row, (1, 1), || None),
            Ok(AutoIncrement::Given(1 << 63))
        );
        let mut row = [Datum::Null];
        table
            .apply_auto_increment(&mut row, (1, 1), || None)
            .unwrap();
        assert_eq!(row[0], Datum::UInt((1 << 63) + 1));
    }

    /// The same explicit id on a SIGNED column is a value the counter is
    /// already past (Go's rebase only ever moves up), so allocation carries on
    /// from where it was.
    #[test]
    fn a_signed_explicit_id_below_the_counter_does_not_move_it() {
        let mut table = auto_increment_table(false);
        let mut row = [Datum::Int(-5)];
        assert_eq!(
            table.apply_auto_increment(&mut row, (1, 1), || None),
            Ok(AutoIncrement::Given(-5_i64 as u64))
        );
        let mut row = [Datum::Null];
        table
            .apply_auto_increment(&mut row, (1, 1), || None)
            .unwrap();
        assert_eq!(row[0], Datum::Int(1));
    }
}
