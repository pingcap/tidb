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

//! The narrowed access paths as streaming source executors: Go's
//! `PointGet`/`Batch_Point_Get` and `IndexRangeScan` + row lookup.
//!
//! # Why these are executors and not a `Vec`
//!
//! Each of these paths reads a *subset* of a table rather than all of it, but
//! a subset is not a bound: `WHERE b > 4` over a ten-million-row index is an
//! index range, and materializing it into a `Vec<Vec<Datum>>` to hand to a
//! `MemTableSourceExec` costs the whole relation in decoded form before the
//! first row leaves the source. These executors hold the cursor instead
//! ([`TableCommon::row_cursor_with_context`], [`TableCommon::index_range_cursor`]) and decode one
//! row per pull, so the decoded rows alive at once are one chunk's worth
//! regardless of how many rows the range covers, and a pushed `LIMIT` never
//! decodes or looks up a row past its cap.
//!
//! # How far down the streaming reaches today
//!
//! The executors are fully pull-based, and so is the row decoding above the
//! [`TableStorage`](tidb_kv::storage::TableStorage) seam. Below it, neither
//! backend's `iter` is lazy yet: `MemStorage::iter` copies the range's
//! key/value bytes into a `Vec`, and `ClusterTableStorage::iter` scans the
//! snapshot range and merges the staged buffer into another one. So a scan
//! today holds the range's *packed bytes*, not its decoded rows -- which is
//! the smaller of the two by a wide margin (no per-row `Vec<Datum>`, no
//! per-value allocation), and on the index path the row lookups are avoided
//! outright: a capped scan performs `cap` point reads instead of one per
//! entry in the range.
//!
//! This is the seam, not a workaround: the executors pull one row at a time
//! through `StorageIterator`, so the day a backend's `iter` returns a real
//! lazy cursor (a TiKV region iterator, or a borrowing `MemIterator`) the
//! whole path streams end to end with no change above the seam.
//!
//! # `actRows`
//!
//! Both sources expose a live produced-row counter, which the plan trace
//! reads for the access operator's `actRows`. Without a pushed limit that
//! count is exactly the row count a materializing path would have reported;
//! with one it reports the truncation, as Go's does.

#[allow(unused_imports)]
use crate::RowDecodeContextExt;

use tidb_datatype::Datum;
use tidb_driver_error::ExecError;

use tidb_ranger::types::Range;

/// A locking point read's lock (Go `PointGetExecutor.lock` with its
/// `LockWaitTime`), taken inside `Next`.
#[derive(Clone, Debug)]
pub(crate) struct PointLock {
    /// How long the lock request waits on a held key.
    pub(crate) wait: tidb_txnkv::transaction::LockWaitTime,
    /// Go `IsPessimisticReadConsistency`: lock only existing keys, after
    /// (batch) or while (`lockKeyIfExists`) reading them; repeatable read
    /// locks every named key before reading it.
    pub(crate) read_committed: bool,
    /// The transaction-state publisher, when the session has one.
    pub(crate) lock_state: Option<tidb_sessionctx::select_lock::LockStateObserver>,
    /// Go `TxnCtx.TemporaryTables`, whose keys `doLockKeys` skips.
    pub(crate) temporary_tables: tidb_tblctx::temporary_table::TemporaryTables,
}

impl PointLock {
    /// Go `doLockKeys` for a point read, publishing the lock wait around it.
    pub(crate) fn lock(
        &self,
        store: &mut dyn tidb_kv::storage::TableStorage,
        keys: &[tidb_txnkv::Key],
        only_if_exists: bool,
    ) -> Result<(), ExecError> {
        // Go `doLockKeys`: "Skip the temporary table keys."
        let keys = tidb_tblctx::temporary_table::filter_temporary_table_keys(&self.temporary_tables, keys.to_vec());
        let keys = keys.as_slice();
        if keys.is_empty() {
            return Ok(());
        }
        if let Some(state) = &self.lock_state {
            state.set(true);
        }
        let locked = store
            .lock_keys_for_read(keys, self.wait, only_if_exists)
            .map_err(tidb_table::tables::KvTableError::from);
        if let Some(state) = &self.lock_state {
            state.set(false);
        }
        locked.map_err(ExecError::from)
    }
}


/// What a statement's AST tells the session about its read before planning:
/// a locking read, or an autocommit write whose timestamped transaction can
/// open while it plans. Go's plan-time answer (`AdviseOptimizeWithPlan`'s
/// `MaxUint64` point get) is the optimistic provider's, after planning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementReadShape {
    /// A locking SELECT needs a current statement snapshot in a pessimistic
    /// transaction, even when an older snapshot would find no rows to lock.
    LockingRead,
    /// Nothing is claimed.
    #[default]
    Unknown,
    /// An autocommit UPDATE or DELETE whose first storage read can overlap
    /// opening its ordinary timestamped snapshot with AST planning; the read
    /// and any eventual prewrite use the same real `start_ts`.
    AutocommitWrite,
}

/// Whether a set operation's terms take a `FOR UPDATE` lock (Go
/// `buildSelectLock` under a `PhysicalUnionAll`).
fn set_opr_has_update_lock(set_opr: &tidb_ast::SetOprStmt) -> bool {
    set_opr.terms.iter().any(|term| match &term.body {
        tidb_ast::SetOprTermBody::Select(select) => select
            .lock
            .as_ref()
            .is_some_and(|lock| lock.kind == tidb_ast::LockKind::Update),
        tidb_ast::SetOprTermBody::Nested(nested) => set_opr_has_update_lock(nested),
    })
}

/// The [`StatementReadShape`] of `stmt`.
#[must_use]
pub fn statement_read_shape(stmt: &tidb_ast::Stmt) -> StatementReadShape {
    if matches!(
        stmt,
        tidb_ast::Stmt::Dml(dml)
            if matches!(
                dml.as_ref(),
                tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_)
            )
    ) {
        return StatementReadShape::AutocommitWrite;
    }
    let tidb_ast::Stmt::Query(query) = stmt else {
        return StatementReadShape::Unknown;
    };
    // A `FOR UPDATE` term makes a set operation a locking read: Go
    // `ExecStmt.isSelectForUpdate` is `b.hasLock`, set by any
    // `buildSelectLock` in the tree.
    let locking = match &**query {
        tidb_ast::QueryStmt::Select(select) => select
            .lock
            .as_ref()
            .is_some_and(|lock| lock.kind == tidb_ast::LockKind::Update),
        tidb_ast::QueryStmt::SetOpr(set_opr) => set_opr_has_update_lock(set_opr),
    };
    if locking {
        StatementReadShape::LockingRead
    } else {
        StatementReadShape::Unknown
    }
}

#[cfg(test)]
mod statement_read_shape_tests {
    use super::*;

    /// DML gets its own ordinary-snapshot shape so the session can start the
    /// PD-backed open before the write read path is built.
    #[test]
    fn update_and_delete_use_the_prefetchable_write_shape() {
        for sql in [
            "UPDATE t SET v = 1 WHERE k = 'x'",
            "DELETE FROM t WHERE k = 'x'",
        ] {
            let stmt = tidb_parser::parse(sql).unwrap();
            assert_eq!(
                statement_read_shape(&stmt),
                StatementReadShape::AutocommitWrite,
                "{sql} should overlap its normal snapshot open with planning"
            );
        }
        let insert = tidb_parser::parse("INSERT INTO t VALUES (1, 2)").unwrap();
        assert_eq!(
            statement_read_shape(&insert),
            StatementReadShape::Unknown,
            "INSERT has no guaranteed first read to prefetch"
        );
    }
}



/// Go `rebuildIndexRanges` / `PhysicalTableScan.ResolveCorrelatedColumns`:
/// recomputes a scan's ranges from access conditions over correlated columns
/// each time an Apply reopens it for a new outer row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandleOutputColumn {
    /// One stored table-column offset.
    Stored(usize),
    /// Go's synthetic `_tidb_rowid`, read from the record key.
    ExtraHandle,
    /// The physical record keyspace retained alongside each handle.
    PhysicalTableId,
    /// Go's `ExtraRowChecksumID` column of a fast point plan
    /// (`fillRowChecksum`), computed from the encoded row value.
    RowChecksum,
}

pub(crate) type RangeRebuilder =
    std::sync::Arc<dyn Fn() -> Result<Vec<Range>, ExecError> + Send + Sync>;


pub(crate) fn calculate_lookup_batch_size_with_cap(
    estimated_rows: Option<f64>,
    initial_batch_size: usize,
    index_paging: bool,
    max_batch_size: usize,
) -> usize {
    let max_batch_size = max_batch_size.max(1);
    let mut batch_size = initial_batch_size.clamp(1, max_batch_size);
    if index_paging {
        return batch_size;
    }
    let estimated_rows = estimated_rows
        .filter(|rows| rows.is_finite() && *rows > 0.0)
        .map_or(0, |rows| rows as usize);
    if estimated_rows >= max_batch_size {
        return max_batch_size;
    }
    while batch_size < estimated_rows {
        batch_size = batch_size.saturating_mul(2);
        if batch_size >= max_batch_size {
            return max_batch_size;
        }
    }
    batch_size
}

/// One object-key component of an index-join probe.
///
/// Go's ranger can combine `eq(inner_col, outer_col)` with constant access
/// conditions on earlier key columns. `Dynamic` indexes the join-key tuple
/// supplied by `JoinExec`; `Constant` is copied into every probe.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LookupProbePart {
    Dynamic(usize),
    Constant(Datum),
    // Values at this column in each ranger template. Alternative columns
    // share the same ordinal, preserving correlated static range tuples.
    Alternatives(Vec<Datum>),
}
