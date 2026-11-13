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

use crate::KvTableReader;
#[allow(unused_imports)]
use crate::RowDecodeContextExt;
use std::collections::BTreeSet;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_driver_error::ExecError;
use tidb_hack::GoCaseCompare;

use tidb_table::tables::TableHandle;
use tidb_table::tables::TableCommon;
use tidb_ranger::types::Range;
use tidb_distsql::remote_scan::PushdownStatementContext;
use tidb_distsql::remote_scan::PushdownRowStream;

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


/// What a statement declares about its whole read, before its first read
/// happens.
///
/// Go's counterpart is the answer `IsPointGetWithPKOrUniqueKeyByAutoCommit`
/// gives `AdviseOptimizeWithPlan` once per statement, with the statement's
/// ROOT plan. It is a statement-level fact, not a per-read one, and the
/// difference is the whole safety argument: `MaxUint64` reads the latest
/// committed version at the moment of the read, so two reads of one statement
/// at `MaxUint64` are two different snapshots.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementReadShape {
    /// A locking SELECT needs a current statement snapshot in a pessimistic
    /// transaction, even when an older snapshot would find no rows to lock.
    LockingRead,
    /// Nothing is claimed. The snapshot spends a timestamp at its first read,
    /// which is every statement's answer unless it earned the other one.
    #[default]
    Unknown,
    /// The statement's whole read is one point get on the clustered handle,
    /// reading one row once, with no second read of any kind.
    AutocommitPointGet,
    /// An autocommit UPDATE or DELETE whose first storage read can overlap
    /// opening its ordinary timestamped snapshot with AST planning.  This is
    /// not a max-ts claim: the read and any eventual prewrite still use the
    /// same real `start_ts`, so write-conflict detection is unchanged.
    AutocommitWrite,
}

/// Whether `stmt` is a statement whose WHOLE read is one point get on the
/// clustered handle -- the plan-shape half of Go's
/// `IsPointGetWithPKOrUniqueKeyByAutoCommit`
/// (`pkg/planner/core/common_plans.go`).
///
/// # What this decides, and what it deliberately does not
///
/// Go asks the question of the statement's ROOT plan, and its `switch` is what
/// makes the answer safe: only a `PhysicalProjection` over a reader, a bare
/// reader, or a `PointGetPlan` qualifies, and everything else falls to
/// `default: return false`. A `Selection`, `Apply`, `Limit`, `Sort`,
/// aggregation or join at the root is therefore already a refusal in Go, and
/// so is every statement that is not a query at all. This function is that
/// `switch`, expressed over the statement this tier plans from.
///
/// The two conditions it owns beyond the shape, both of them refusals Go makes
/// elsewhere:
///
/// * The unique-index arm is REFUSED outright. Go admits `PointGetPlan` on a
///   unique index only when `IndexInfo == nil || (Primary && IsCommonHandle)`
///   -- its `noSecondRead` -- because an index point get is a DOUBLE read, and
///   `MaxUint64` on a double read can pair an index entry with a row from a
///   different version. This tier's unique-index arm
///   (`driver::access::try_point_get`) is exactly that double read, and it
///   READS the index while deciding, which a declaration made before the first
///   read cannot do anyway. Refusing it is both halves at once.
/// * A retained `LIMIT` operator is refused. Go folds a positive, zero-offset
///   limit into `tryPointGetPlan`; literal bounds permit the same proof here
///   without evaluating an expression. The complete handle-key proof still
///   runs below, so a bounded range never gains MaxTS merely from its limit.
///
/// The conditions this function does NOT own, because the caller owns them
/// structurally -- see `ClusterSnapshot::declare_autocommit_point_get`:
///
/// * `IsAutoCommitTxn`: autocommit set and no open transaction. The snapshot
///   an explicit transaction binds refuses the declaration by inheriting the
///   trait's fail-closed default, so being inside `BEGIN` is not a check here
///   at all -- there is nothing to declare to.
/// * "The timestamp is not already spent." The deferred snapshot refuses a
///   declaration once it has opened, which is Go's `p.txn != nil` arm.
///
/// # Why an `UPDATE`'s read-before-write cannot reach the max-ts answer
///
/// An UPDATE/DELETE is classified as [`StatementReadShape::AutocommitWrite`]
/// before this query-only proof. That shape permits only overlapping its
/// ordinary timestamp allocation with planning; it never selects max-ts. This
/// matters because at the storage seam an UPDATE's read-before-write and a
/// SELECT's point get are the SAME `get` on the same key: the difference lives
/// only in the statement, which is why both declarations are made from the
/// statement and never inferred from a read.
#[must_use]
pub fn statement_read_shape(
    stmt: &tidb_ast::Stmt,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    current_db: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> StatementReadShape {
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
    // A set operation reads every term, so it is several reads by
    // construction; Go's root plan for it is a `PhysicalUnionAll`, which its
    // `switch` refuses.
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        return StatementReadShape::Unknown;
    };
    if select
        .lock
        .as_ref()
        .is_some_and(|lock| lock.kind == tidb_ast::LockKind::Update)
    {
        return StatementReadShape::LockingRead;
    }
    // Go's tryPointGetPlan permits a positive LIMIT with zero offset, but
    // IsPointGetWithPKOrUniqueKeyByAutoCommit still requires a complete point
    // key. A row-count bound alone does not prove one snapshot-consistent read.
    if !select_is_bare_read(select, true)
        || select.limit.as_ref().is_some_and(|limit| {
            !matches!(&limit.count, tidb_ast::Expr::Int(value)
                if value.parse::<u64>().is_ok_and(|count| count > 0))
                || limit.offset.as_ref().is_some_and(|offset| {
                    !matches!(offset, tidb_ast::Expr::Int(value)
                        if value.parse::<u64>() == Ok(0))
                })
        })
    {
        return StatementReadShape::Unknown;
    }
    let Some(table_ref) = tidb_planner::point_get_plan::single_table_ref(&select.from) else {
        return StatementReadShape::Unknown;
    };
    let (database, table_name) = match table_ref.name.as_slice() {
        [name] if !current_db.is_empty() => (current_db, name.as_str()),
        [database, name] => (database.as_str(), name.as_str()),
        _ => return StatementReadShape::Unknown,
    };
    let Some(tidb_infoschema::TableEntry::Kv(table)) = catalog.get_in(database, table_name) else {
        return StatementReadShape::Unknown;
    };
    // A partitioned table's point get routes to a partition, which this
    // predicate does not resolve; a refusal costs one timestamp.
    if table.partition().is_some() {
        return StatementReadShape::Unknown;
    }
    let columns: Vec<(String, FieldType)> = table
        .visible_columns()
        .into_iter()
        .map(|(_, column)| (column.read().name.original().to_owned(), column.read().field_type.clone()))
        .collect();
    let Some(where_clause) = select.where_clause.as_ref() else {
        return StatementReadShape::Unknown;
    };
    // The rest is `try_point_get`'s handle arm verbatim, through the same two
    // helpers, so a statement this predicate accepts is a statement that arm
    // accepts. It reads nothing: both helpers walk the AST only.
    let mut pairs = Vec::new();
    if !tidb_planner::point_get_plan::get_name_value_pairs(where_clause, &mut pairs, zone) {
        return StatementReadShape::Unknown;
    }
    if !tidb_planner::point_get_plan::convert_pairs_to_column_domain(&mut pairs, &columns) {
        return StatementReadShape::Unknown;
    }
    if let Some(handle_offset) = table.pk_handle_offset() {
        if pairs.len() != 1 {
            return StatementReadShape::Unknown;
        }
        let handle_column = table.columns()[handle_offset].read().name.original().to_owned();
        if !pairs[0].column().go_lower_eq(&handle_column) {
            return StatementReadShape::Unknown;
        }
        return match pairs[0].value() {
            Datum::Int(_) | Datum::UInt(_) => StatementReadShape::AutocommitPointGet,
            _ => StatementReadShape::Unknown,
        };
    }

    // A clustered common handle is a primary-key tuple encoded directly in
    // the record key.  Go's `IsPointGetWithPKOrUniqueKeyByAutoCommit` admits
    // this `PointGetPlan` arm as well as the signed-BIGINT handle arm; unlike
    // a secondary unique index it is still one storage read, so the
    // MaxUint64 shortcut is safe for a bare point projection.  Require every
    // handle column exactly once so a duplicate equality or a non-key
    // residual cannot accidentally claim the shortcut.
    let common_offsets = table.common_handle_offsets();
    if common_offsets.is_empty() || pairs.len() != common_offsets.len() {
        return StatementReadShape::Unknown;
    }
    let complete = common_offsets.iter().all(|offset| {
        table.columns().get(*offset).is_some_and(|column| {
            let name = column.read().name.original().to_owned();
            pairs
                .iter()
                .filter(|pair| pair.column().go_lower_eq(&name))
                .count()
                == 1
        })
    });
    complete
        .then_some(StatementReadShape::AutocommitPointGet)
        .unwrap_or(StatementReadShape::Unknown)
}

#[cfg(test)]
mod common_handle_shape_tests {
    use super::*;

    // Keep the common-handle guard's value-domain branch exercised without
    // requiring a storage backend: this is the shape decision made before
    // the first read, not a transport test.
    #[test]
    fn common_handle_point_read_is_admitted() {
        let mut table = tidb_infoschema::SessionExtendedInfoSchema::mock();
        let mut kv = tidb_table::tables::TableCommon::new(
            1,
            vec![
                tidb_table::column::Column {
                    info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                        id: 1,
                        name: tidb_ast::CiString::new("k"),
                        field_type: FieldType::new(tidb_datatype::FieldTypeCode::VarString),
                        version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::ColumnInfo::default()
                    }),
                    generated: None,
                    default_value: None,
                    origin_default: None,
                },
                tidb_table::column::Column {
                    info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                        id: 2,
                        name: tidb_ast::CiString::new("v"),
                        field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::ColumnInfo::default()
                    }),
                    generated: None,
                    default_value: None,
                    origin_default: None,
                },
            ],
        );
        kv.set_name("t");
        kv.set_common_handle_offsets(vec![0]);
        table.register_kv_in("test", "t", kv).unwrap();
        let stmt = tidb_parser::parse("SELECT v FROM t WHERE k = 'x'").unwrap();
        assert_eq!(
            statement_read_shape(
                &stmt,
                &table,
                "test",
                &tidb_datatype::SessionTimeZone::utc(),
            ),
            StatementReadShape::AutocommitPointGet
        );
    }

    #[test]
    fn a_limit_bound_does_not_replace_the_complete_point_key_proof() {
        let mut table = tidb_infoschema::SessionExtendedInfoSchema::mock();
        let mut kv = tidb_table::tables::TableCommon::new(
            1,
            vec![
                tidb_table::column::Column {
                    info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                        id: 1,
                        name: tidb_ast::CiString::new("k"),
                        field_type: FieldType::new(tidb_datatype::FieldTypeCode::VarString),
                        version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::ColumnInfo::default()
                    }),
                    generated: None,
                    default_value: None,
                    origin_default: None,
                },
                tidb_table::column::Column {
                    info: tidb_model::GoShared::new(tidb_model::ColumnInfo {
                        id: 2,
                        name: tidb_ast::CiString::new("v"),
                        field_type: FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::ColumnInfo::default()
                    }),
                    generated: None,
                    default_value: None,
                    origin_default: None,
                },
            ],
        );
        kv.set_name("t");
        kv.set_common_handle_offsets(vec![0]);
        table.register_kv_in("test", "t", kv).unwrap();
        for (sql, expected) in [
            (
                "SELECT v FROM t WHERE k >= 'x' LIMIT 1",
                StatementReadShape::Unknown,
            ),
            ("SELECT v FROM t LIMIT 1", StatementReadShape::Unknown),
            (
                "SELECT v FROM t WHERE v = 1 LIMIT 1",
                StatementReadShape::Unknown,
            ),
            (
                "SELECT v FROM t WHERE k = 'x' LIMIT 1",
                StatementReadShape::AutocommitPointGet,
            ),
            (
                "SELECT v FROM t WHERE k = 'x' LIMIT 2",
                StatementReadShape::AutocommitPointGet,
            ),
            (
                "SELECT v FROM t WHERE k = 'x' LIMIT 1 OFFSET 0",
                StatementReadShape::AutocommitPointGet,
            ),
            (
                "SELECT v FROM t WHERE k = 'x' LIMIT 0",
                StatementReadShape::Unknown,
            ),
            (
                "SELECT v FROM t WHERE k = 'x' LIMIT 1 OFFSET 1",
                StatementReadShape::Unknown,
            ),
        ] {
            let stmt = tidb_parser::parse(sql).unwrap();
            assert_eq!(
                statement_read_shape(
                    &stmt,
                    &table,
                    "test",
                    &tidb_datatype::SessionTimeZone::utc(),
                ),
                expected,
                "{sql}",
            );
        }
    }

    /// DML gets its own ordinary-snapshot shape so the session can start the
    /// PD-backed open before the write read path is built.  It must never be
    /// confused with the max-ts point-get shortcut used by SELECT.
    #[test]
    fn update_and_delete_use_the_prefetchable_write_shape() {
        let catalog = tidb_infoschema::SessionExtendedInfoSchema::mock();
        for sql in [
            "UPDATE t SET v = 1 WHERE k = 'x'",
            "DELETE FROM t WHERE k = 'x'",
        ] {
            let stmt = tidb_parser::parse(sql).unwrap();
            assert_eq!(
                statement_read_shape(
                    &stmt,
                    &catalog,
                    "test",
                    &tidb_datatype::SessionTimeZone::utc(),
                ),
                StatementReadShape::AutocommitWrite,
                "{sql} should overlap its normal snapshot open with planning"
            );
        }
        let insert = tidb_parser::parse("INSERT INTO t VALUES (1, 2)").unwrap();
        assert_eq!(
            statement_read_shape(
                &insert,
                &catalog,
                "test",
                &tidb_datatype::SessionTimeZone::utc(),
            ),
            StatementReadShape::Unknown,
            "INSERT has no guaranteed first read to prefetch"
        );
    }
}

/// Whether the query block carries nothing above the table read: Go's
/// `switch` admitting only a projection over a reader.
///
/// Every clause listed here would put an operator above the reader in Go's
/// root plan and so land in its `default` arm. The select list is held to
/// plain column references and wildcards for the same reason -- a scalar
/// subquery in it is a second read, and an aggregate is a root `HashAgg`.
pub(crate) fn select_is_bare_point_read(select: &tidb_ast::SelectStmt) -> bool {
    select_is_bare_read(select, false)
}

fn select_is_bare_read(select: &tidb_ast::SelectStmt, allow_limit: bool) -> bool {
    use tidb_ast::{SelectField, SelectStatementKind};

    select.kind == SelectStatementKind::Select
        && !select.is_in_braces
        && select.with.is_none()
        && select.values.is_empty()
        && !select.distinct
        && select.group_by.is_empty()
        && !select.rollup
        && select.having.is_none()
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && (allow_limit || select.limit.is_none())
        // `FOR UPDATE` locks the row, which needs a real timestamp to lock at.
        && select.lock.is_none()
        && select.into_outfile.is_none()
        && !select.calc_found_rows
        && select.fields.fields().iter().all(|field| match field {
            SelectField::Wildcard(_) => true,
            SelectField::Expr { expr, .. } => matches!(expr, tidb_ast::Expr::Column(_)),
        })
}

/// The visible columns of a decoded table row, the ones a source emits.
///
/// `TableCommon::get_row_by_handle` decodes and materializes EVERY column of the
/// table, hidden ones included -- the hidden column an expression index was
/// rewritten into has to be computed so an index entry can be written from
/// the same row. The default output schema of these sources is the visible
/// one; an explicit physical output mapping may also request hidden columns
/// and uses the full row instead. A hidden column sits wherever Go's DDL put
/// it in `TableInfo.Columns`, so a table that has one gathers the rest.
///
/// `TableScanExec` states the same rule as its `keep` list; this is that rule
/// for the two narrowed sources, which read their rows by handle instead.
fn visible_of<'a>(
    table: &TableCommon,
    row: &'a [tidb_datatype::Datum],
) -> std::borrow::Cow<'a, [tidb_datatype::Datum]> {
    if table.visible_column_count() == table.columns().len() {
        return std::borrow::Cow::Borrowed(&row[..row.len().min(table.columns().len())]);
    }
    std::borrow::Cow::Owned(
        (0..row.len())
            .filter(|offset| !table.is_hidden(*offset))
            .map(|offset| row[offset].clone())
            .collect(),
    )
}


/// Go `rebuildIndexRanges` / `PhysicalTableScan.ResolveCorrelatedColumns`:
/// recomputes a scan's ranges from access conditions over correlated columns
/// each time an Apply reopens it for a new outer row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandleOutputColumn {
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

/// Reads rows for an already-known handle list, one per pull: the source
/// behind Go's `PointGet` (one handle) and `Batch_Point_Get` (several).
///
/// The handle list is bounded by the statement text (`a IN (1, 2, 3)`), so it
/// is materialized; the *rows* are not. Go likewise leaves a `Limit` at the
/// root above a `Batch_Point_Get` rather than pushing into it (captured:
/// `Limit_10 | root` over `Batch_Point_Get_12`), which is why this source
/// takes no row cap.
pub struct HandleSourceExec {
    meta: ExecutorMeta,
    table: TableCommon,
    /// The snapshot this reader reads, apart from the table metadata (Go's
    /// executor reads through the session transaction, never the table).
    store: Box<dyn tidb_kv::storage::TableStorage>,
    handles: Vec<TableHandle>,
    /// The next handle to read.
    cursor: usize,
    /// Rows produced so far, which the trace reads as this node's `actRows`.
    produced: tidb_executor_internal_exec::RowCount,
    /// The statement-class flags for an origin default plus the session zone
    /// a stored `TIMESTAMP` is read back into, captured where the statement's
    /// context is (`Executor` has none).
    decode_context: tidb_table::tables::RowDecodeContext,
    /// Source-row offsets this complete point plan emits, in result order.
    /// `None` keeps the ordinary visible-row schema for residual root work.
    output_columns: Option<Vec<HandleOutputColumn>>,
    /// Encoded values fetched once by the first Next, then decoded only to
    /// fill the requested chunk, like Go's BatchPointGetExec.values.
    preloaded: Option<tidb_table::tables::PointReadValues>,
    /// Set only for a table task; PointGet/BatchPointGet retain native gets.
    table_statement: Option<PushdownStatementContext>,
    remote_rows: Option<std::collections::VecDeque<(usize, TableHandle, Vec<Datum>)>>,
    decoder: Option<tidb_table::tables::PointRowDecoder>,
    initialized: bool,
    /// Go's `PointGetExecutor` performs one direct record-key `Get`; the
    /// batch executor uses `BatchGet` even when its retained list happens to
    /// contain one handle.
    single_point_get: bool,
    /// One physical partition per handle, retained by the fast batch plan.
    partition_ids: Option<Vec<i64>>,
    /// Where `_tidb_rowid` sits in the source row, when the schema carries
    /// it. Go's extra handle column reports the record HANDLE rather than any
    /// stored value, so nothing in the decoded row fills this slot and the
    /// source has to write it -- the same contract
    /// [`crate::kv_table::table_scan`] has for a scan.
    extra_handle_slot: Option<usize>,
    /// Go `PointGetExecutor.lock` / `BatchPointGetExec.lock` in a
    /// repeatable-read pessimistic transaction: the record keys are locked
    /// with this wait before the first read, inside `Next`.
    lock: Option<PointLock>,
    /// The unique-index key each handle was found under, aligned with
    /// `handles`, which a batch read locks together with its record keys
    /// (Go `BatchPointGetExec.initialize`'s `indexKeys`).
    lock_index_keys: Vec<Option<tidb_txnkv::Key>>,
    /// Index keys that named no row, which repeatable read locks with the
    /// rest before reading (Go `BatchPointGetExec`: "lock all keys in
    /// repeatable read isolation").
    lock_missing_index_keys: Vec<tidb_txnkv::Key>,
}

impl HandleSourceExec {
    /// Builds a source over `handles` with an explicit row-decode context.
    #[must_use]
    pub fn new_with_context(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handles: Vec<TableHandle>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        HandleSourceExec {
            meta,
            store,
            table,
            handles,
            cursor: 0,
            preloaded: None,
            table_statement: None,
            remote_rows: None,
            decoder: None,
            initialized: false,
            produced: tidb_executor_internal_exec::RowCount::default(),
            decode_context,
            output_columns: None,
            extra_handle_slot: None,
            single_point_get: false,
            partition_ids: None,
            lock: None,
            lock_index_keys: Vec::new(),
            lock_missing_index_keys: Vec::new(),
        }
    }

    /// Locks the record keys before reading them (see
    /// [`HandleSourceExec::lock`]).
    pub(crate) fn with_lock_before_read(
        mut self,
        lock: Option<PointLock>,
    ) -> Self {
        self.lock = lock;
        self
    }

    /// The unique-index keys the handles were found under (see
    /// [`HandleSourceExec::lock_index_keys`]).
    pub(crate) fn with_lock_index_keys(
        mut self,
        keys: Vec<Option<tidb_txnkv::Key>>,
        missing: Vec<tidb_txnkv::Key>,
    ) -> Self {
        self.lock_index_keys = keys;
        self.lock_missing_index_keys = missing;
        self
    }

    /// The record and index keys of the handles at `positions`.
    fn lock_keys_at(
        &self,
        record_keys: &[tidb_txnkv::Key],
        positions: &[usize],
    ) -> Vec<tidb_txnkv::Key> {
        let mut keys = positions
            .iter()
            .filter_map(|position| record_keys.get(*position).cloned())
            .collect::<Vec<_>>();
        keys.extend(
            positions
                .iter()
                .filter_map(|position| self.lock_index_keys.get(*position).cloned().flatten()),
        );
        keys
    }

    /// Builds the final table reader for a retained physical plan whose
    /// output can mix stored columns and Go's synthetic `_tidb_rowid`.
    #[must_use]
    pub(crate) fn new_mapped_with_context(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handles: Vec<TableHandle>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self {
            output_columns: Some(output_columns),
            ..Self::new_with_context(meta, table, store, handles, decode_context)
        }
    }

    /// Builds Go's `PointGetExecutor`: one retained handle, one direct row
    /// `Get`, and the physical plan's exact output projection.
    #[must_use]
    pub(crate) fn new_point_mapped_with_context(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        handle: TableHandle,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
    ) -> Self {
        Self {
            single_point_get: true,
            ..Self::new_mapped_with_context(meta, table, store, vec![handle], output_columns, decode_context)
        }
    }

    pub(crate) fn with_partition_ids(mut self, ids: Option<Vec<i64>>) -> Self {
        self.partition_ids = ids;
        self
    }

    /// The live count of rows this source produced.
    #[must_use]
    pub fn produced_rows(&self) -> tidb_executor_internal_exec::RowCount {
        self.produced.clone()
    }

    fn initialize(&mut self) -> Result<(), ExecError> {
        // Go `PointGetExecutor.getAndLock` / `BatchPointGetExec.initialize`:
        // "Lock the key before get in RR isolation, then get will get the
        // value from the cache"; read committed's single point get locks the
        // key only if it exists (`lockKeyIfExists`) and reads the answer.
        let record_keys = self.lock.as_ref().and_then(|_| {
            self.table
                .record_lock_keys(&self.handles, self.partition_ids.as_deref())
        });
        let mut locked_before_read = false;
        if let (Some(lock), Some(record_keys)) = (self.lock.clone(), record_keys.as_ref()) {
            if !lock.read_committed {
                let positions = (0..self.handles.len()).collect::<Vec<_>>();
                let mut keys = self.lock_keys_at(record_keys, &positions);
                keys.extend(self.lock_missing_index_keys.iter().cloned());
                lock.lock(&mut *self.store, &keys, false)?;
                locked_before_read = true;
            } else if self.single_point_get {
                lock.lock(&mut *self.store, record_keys, true)?;
                locked_before_read = true;
            }
        }
        if let Some(statement) = &self.table_statement {
            let keep = self.output_columns.as_ref().map_or_else(
                || self.table.visible_offsets(),
                |columns| {
                    columns
                        .iter()
                        .filter_map(|column| match column {
                            HandleOutputColumn::Stored(offset) => Some(*offset),
                            _ => None,
                        })
                        .collect()
                },
            );
            if let Some(staged) = self
                .table
                .build_table_reader_from_routed_handles(
                    &mut *self.store,
                    &self.handles,
                    self.partition_ids.as_deref(),
                    &keep,
                    &[],
                    &self.decode_context,
                    statement,
                    self.meta.max_chunk_size(),
                )
                .map_err(ExecError::from)?
            {
                if let Some((rows, _, _, indices)) =
                    TableCommon::finish_routed_rows_by_handles(&self.handles, staged)
                        .map_err(ExecError::from)?
                {
                    self.remote_rows = Some(
                        indices
                            .into_iter()
                            .zip(rows)
                            .map(|(index, (handle, row))| (index, handle, row))
                            .collect(),
                    );
                    return Ok(());
                }
            }
        }
        // Go's `BatchPointGetExec.initialize` reads every handle with ONE
        // `BatchGet` on the first `Next`. Prefetch all physical keys
        // together so the storage layer can schedule regions concurrently.
        let rows = if self.single_point_get {
            let handle = self
                .handles
                .first()
                .ok_or_else(|| ExecError::unsupported("a point get lost its retained handle"))?;
            tidb_table::tables::PointReadValues::Single(
                self.table
                    .stored_record(&mut *self.store, handle)
                    .map_err(ExecError::from)?
                    .map(|(_, bytes)| bytes),
            )
        } else {
            if self
                .partition_ids
                .as_ref()
                .is_some_and(|ids| ids.len() != self.handles.len())
            {
                return Err(ExecError::unsupported(
                    "batch point partition routes lost alignment",
                ));
            }
            self.table
                .stored_records_batched(&mut *self.store, &self.handles, self.partition_ids.as_deref())
                .map_err(ExecError::from)?
        };
        // Counted only on an IndexMerge reader's table side, whose copy
        // carries the statement budget; a point get's copy does not.
        let found = match &rows {
            tidb_table::tables::PointReadValues::Single(row) => usize::from(row.is_some()),
            tidb_table::tables::PointReadValues::Batch { values, .. } => values.len(),
        };
        self.table
            .note_keys_read(found as u64)
            .map_err(ExecError::from)?;
        // Go `BatchPointGetExec.initialize` under read committed: "Lock
        // exists keys only", the found rows and their index keys. A
        // partitioned read whose handles were not routed to one partition
        // could name no key before reading and locks what it found.
        if let Some(lock) = self.lock.clone().filter(|_| !locked_before_read) {
            let (record_keys, positions): (Vec<tidb_txnkv::Key>, Vec<usize>) = match &rows {
                tidb_table::tables::PointReadValues::Single(Some(_)) => {
                    match record_keys.as_ref().and_then(|keys| keys.first()) {
                        Some(key) => (vec![key.clone()], vec![0]),
                        None => (Vec::new(), Vec::new()),
                    }
                }
                tidb_table::tables::PointReadValues::Single(None) => (Vec::new(), Vec::new()),
                tidb_table::tables::PointReadValues::Batch { keys, .. } => {
                    let mut found_keys = vec![tidb_txnkv::Key::default(); keys.len()];
                    let mut positions = Vec::new();
                    for (position, key) in keys.iter().enumerate() {
                        if let Some(key) = key {
                            found_keys[position] = key.clone();
                            positions.push(position);
                        }
                    }
                    (found_keys, positions)
                }
            };
            let keys = self.lock_keys_at(&record_keys, &positions);
            lock.lock(&mut *self.store, &keys, false)?;
        }
        self.preloaded = Some(rows);
        self.decoder = Some(tidb_table::tables::PointRowDecoder::new(&self.table));
        Ok(())
    }
}

impl HandleSourceExec {
    fn append_handle_row(
        &self,
        req: &mut Chunk,
        index: usize,
        handle: &TableHandle,
        row: &[Datum],
        stored_value: Option<&[u8]>,
        remote: bool,
    ) -> Result<(), ExecError> {
        let visible = visible_of(&self.table, row);
        if let Some(columns) = &self.output_columns {
            let mut remote_offset = 0;
            for (output, source) in columns.iter().copied().enumerate() {
                match source {
                    HandleOutputColumn::Stored(source) => {
                        // The physical schema may request a hidden
                        // generated column for an UPDATE/index key.
                        // Stored offsets address the full decoded row,
                        // not the wildcard's visible-column prefix.
                        let offset = if remote {
                            let position = remote_offset;
                            remote_offset += 1;
                            Some(position)
                        } else {
                            Some(source)
                        };
                        let value = offset.and_then(|offset| row.get(offset)).ok_or_else(|| {
                            ExecError::unsupported("point-get output column is outside the row")
                        })?;
                        req.append_datum(output, value);
                    }
                    HandleOutputColumn::ExtraHandle => match handle {
                        TableHandle::Int(value) => {
                            req.append_datum(output, &Datum::Int(*value));
                        }
                        _ => {
                            return Err(ExecError::unsupported(
                                "an extra row handle is not an integer handle",
                            ));
                        }
                    },
                    HandleOutputColumn::PhysicalTableId => {
                        let physical_id = self
                            .partition_ids
                            .as_ref()
                            .and_then(|ids| ids.get(index))
                            .copied()
                            .or_else(|| match self.table.record_physical_ids().as_slice() {
                                [id] => Some(*id),
                                _ => None,
                            })
                            .ok_or_else(|| {
                                ExecError::internal(
                                    "physical-table ID output lost its record partition",
                                )
                            })?;
                        req.append_int64(output, physical_id);
                    }
                    HandleOutputColumn::RowChecksum => {
                        let stored_value = stored_value.ok_or_else(|| {
                            ExecError::unsupported(
                                "a row checksum needs the encoded row value of a point get",
                            )
                        })?;
                        let checksum = tidb_table::tables::fill_row_checksum(
                            &self.table,
                            handle,
                            stored_value,
                            &self.decode_context,
                        )
                        .map_err(ExecError::from)?;
                        req.append_datum(output, &checksum);
                    }
                }
            }
        } else {
            let owned;
            let visible: &[tidb_datatype::Datum] = match self.extra_handle_slot {
                Some(slot) => {
                    owned = crate::kv_table::insert_extra_handle(visible.to_vec(), slot, handle);
                    &owned
                }
                None => &visible,
            };
            for (column, value) in visible.iter().enumerate() {
                req.append_datum(column, value);
            }
        }
        Ok(())
    }
}

impl Executor for HandleSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.cursor = 0;
        self.produced.set(0);
        self.preloaded = None;
        self.remote_rows = None;
        self.decoder = None;
        self.initialized = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !std::mem::replace(&mut self.initialized, true) {
            self.initialize()?;
        }
        while !req.is_full() {
            if let Some(rows) = self.remote_rows.as_mut() {
                let Some((index, handle, row)) = rows.pop_front() else {
                    return Ok(());
                };
                self.append_handle_row(req, index, &handle, &row, None, true)?;
                self.cursor += 1;
                self.produced.set(self.produced.get() + 1);
                continue;
            }
            if self.cursor >= self.handles.len() {
                return Ok(());
            };
            let index = self.cursor;
            let handle = self.handles.get(index);
            // PointGet marks done before its read; BatchPointGet advances
            // only after successfully decoding the current stored value.
            if self.single_point_get {
                self.cursor += 1;
            }
            // A handle with no row is Go's point get that finds nothing: the
            // plan is right, the row is simply absent.
            let value = self.preloaded.as_ref().and_then(|values| values.get(index));
            if let Some(value) = value {
                let row = self
                    .decoder
                    .as_mut()
                    .expect("initialized point reader")
                    .decode(
                        &self.table,
                        handle.expect("cursor indexes handles"),
                        value,
                        &self.decode_context,
                    )
                    .map_err(ExecError::from)?;
                self.append_handle_row(
                    req,
                    index,
                    handle.expect("cursor indexes handles"),
                    &row,
                    Some(value),
                    false,
                )?;
                self.produced.set(self.produced.get() + 1);
            }
            if !self.single_point_get {
                self.cursor += 1;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.preloaded = None;
        self.remote_rows = None;
        self.decoder = None;
        self.initialized = true;
        self.cursor = self.handles.len();
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }
}

/// This source refuses every offer, on purpose, by taking the fail-closed
/// defaults of [`tidb_executor_internal_exec::table_access`] wholesale.
///
/// It is listed as an access path rather than left unnegotiable so the refusal
/// is a stated position instead of an omission: the handle list is fixed by the
/// statement text, so a cap would duplicate the `LimitExec` above (Go likewise
/// keeps `Limit_10 | root` over `Batch_Point_Get_12`), the `WHERE` that pinned
/// the handles is still applied above by design (`TryFastPlan` narrows the
/// source, it does not replace the filter), and there is no scanned-row count
/// to report because nothing is scanned.
impl tidb_executor_internal_exec::table_access::TableAccess for HandleSourceExec {}

/// Go's secondary-unique PointGet/BatchPointGet first resolves retained index
/// keys to record handles, then delegates the record reads to
/// [`HandleSourceExec`]. The single form uses two direct Gets; the batch form
/// uses one index BatchGet followed by one record BatchGet.
pub(crate) struct UniqueIndexPointSourceExec {
    meta: ExecutorMeta,
    table: TableCommon,
    /// The snapshot this reader reads, apart from the table metadata (Go's
    /// executor reads through the session transaction, never the table).
    store: Box<dyn tidb_kv::storage::TableStorage>,
    index_id: i64,
    index_values: Vec<Vec<Datum>>,
    output_columns: Vec<HandleOutputColumn>,
    decode_context: tidb_table::tables::RowDecodeContext,
    single_point: bool,
    /// Go `PartitionNames` resolved to physical ids: a global index entry
    /// whose row lives elsewhere answers nothing. `None` restricts nothing.
    allowed_partitions: Option<Vec<i64>>,
    /// Go `PointGetExecutor.lock` / `BatchPointGetExec.lock`.
    lock: Option<PointLock>,
    source: Option<HandleSourceExec>,
    initialized: bool,
}

impl UniqueIndexPointSourceExec {
    /// Restricts a global index's rows to the PARTITION clause's ids.
    #[must_use]
    pub(crate) fn with_allowed_partitions(mut self, ids: Option<Vec<i64>>) -> Self {
        self.allowed_partitions = ids;
        self
    }

    /// Locks the index and record keys this read names inside `Next`.
    #[must_use]
    pub(crate) fn with_lock(mut self, lock: Option<PointLock>) -> Self {
        self.lock = lock;
        self
    }

    #[must_use]
    pub(crate) fn new(
        meta: ExecutorMeta,
        table: TableCommon,
        store: Box<dyn tidb_kv::storage::TableStorage>,
        index_id: i64,
        index_values: Vec<Vec<Datum>>,
        output_columns: Vec<HandleOutputColumn>,
        decode_context: tidb_table::tables::RowDecodeContext,
        single_point: bool,
    ) -> Self {
        Self {
            meta,
            store,
            table,
            index_id,
            index_values,
            output_columns,
            decode_context,
            single_point,
            allowed_partitions: None,
            lock: None,
            source: None,
            initialized: false,
        }
    }
}

impl UniqueIndexPointSourceExec {
    /// Go `matchPartitionNames(pid, partitionNames, pi)` for a global index
    /// entry; a local entry carries no partition and is already confined.
    fn partition_allowed(&self, partition: Option<i64>) -> bool {
        match (&self.allowed_partitions, partition) {
            (Some(allowed), Some(id)) => allowed.contains(&id),
            _ => true,
        }
    }

    fn initialize(&mut self) -> Result<(), ExecError> {
        // Go isCommonHandleRead: clustered PRIMARY has metadata but no
        // separate index entries. Its values directly encode record handles.
        let common_primary = !self.table.common_handle_offsets().is_empty()
            && self
                .table
                .plan_indexes()
                .any(|index| index.read().id == self.index_id && index.clustered_primary);
        let mut routed_partitions: Option<Vec<i64>> = None;
        let mut index_keys: Vec<Option<tidb_txnkv::Key>> = Vec::new();
        let mut missing_index_keys: Vec<tidb_txnkv::Key> = Vec::new();
        let mut handles: Vec<TableHandle> = if common_primary {
            self.index_values
                .iter()
                .filter(|values| !values.iter().any(Datum::is_null))
                .map(|values| {
                    self.table
                        .common_handle_from_values(values, self.decode_context.zone())
                        .map_err(|error| {
                            ExecError::unsupported(format!(
                                "common primary key encoding failed: {error:?}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else if self.single_point {
            let [values] = self.index_values.as_slice() else {
                return Err(ExecError::unsupported(
                    "a unique-index PointGet does not retain exactly one key",
                ));
            };
            // Go `PointGetExecutor.Next`: repeatable read locks the index
            // key whether or not it exists, then reads it from the lock
            // cache; read committed locks it only if it exists
            // (`lockKeyIfExists`).
            if let Some(lock) = self.lock.clone() {
                let keys = self
                    .table
                    .encode_unique_index_key(self.index_id, values, self.decode_context.zone())
                    .map_err(|error| {
                        ExecError::unsupported(format!(
                            "unique index key encoding failed: {error:?}"
                        ))
                    })?;
                lock.lock(&mut *self.store, &keys, lock.read_committed)?;
            }
            self.table
                .lookup_unique_with_partition(
                    &mut *self.store,
                    self.index_id,
                    values,
                    self.decode_context.zone(),
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("unique index lookup failed: {error:?}"))
                })?
                .into_iter()
                .filter(|(_, partition)| self.partition_allowed(*partition))
                .map(|(handle, _)| handle)
                .collect()
        } else {
            let keys = self
                .table
                .unique_batch_index_keys(
                    self.index_id,
                    &self.index_values,
                    self.decode_context.zone(),
                    Some(self.decode_context.expression()),
                )
                .map_err(|error| {
                    ExecError::unsupported(format!("unique index batch lookup failed: {error:?}"))
                })?;
            let looked_up = self
                .table
                .lookup_unique_index_keys(&mut *self.store, &keys)
                .map_err(|error| {
                    ExecError::unsupported(format!("unique index batch lookup failed: {error:?}"))
                })?;
            let mut found = Vec::new();
            for (key, entry) in keys.into_iter().zip(looked_up) {
                let key = key.map(|(key, _)| key);
                match entry {
                    Some((handle, partition)) if self.partition_allowed(partition) => {
                        found.push((handle, partition));
                        index_keys.push(key);
                    }
                    _ => missing_index_keys.extend(key),
                }
            }
            // Each row reads from the partition its key routed to (or its
            // global entry recorded), as Go's second BatchGet does.
            if self.table.partition().is_some()
                && found.iter().all(|(_, partition)| partition.is_some())
            {
                routed_partitions = Some(found.iter().filter_map(|(_, id)| *id).collect());
            }
            found.into_iter().map(|(handle, _)| handle).collect()
        };
        let Some(first) = handles.first().cloned() else {
            // Go `BatchPointGetExec.initialize` with no row found still
            // locks every named index key under repeatable read.
            if let Some(lock) = self.lock.clone().filter(|lock| !lock.read_committed) {
                lock.lock(&mut *self.store, &missing_index_keys, false)?;
            }
            return Ok(());
        };
        let meta = ExecutorMeta::new(
            self.meta.schema().clone(),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        let mut source = if self.single_point {
            HandleSourceExec::new_point_mapped_with_context(
                meta,
                self.table.clone(),
                self.store.clone(),
                first,
                self.output_columns.clone(),
                self.decode_context.clone(),
            )
            .with_lock_before_read(self.lock.clone())
        } else {
            HandleSourceExec::new_mapped_with_context(
                meta,
                self.table.clone(),
                self.store.clone(),
                std::mem::take(&mut handles),
                self.output_columns.clone(),
                self.decode_context.clone(),
            )
            .with_partition_ids(routed_partitions)
            .with_lock_before_read(self.lock.clone())
            .with_lock_index_keys(index_keys, missing_index_keys)
        };
        source.open()?;
        self.source = Some(source);
        Ok(())
    }
}

impl Executor for UniqueIndexPointSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.source = None;
        self.initialized = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if !std::mem::replace(&mut self.initialized, true) {
            self.initialize()?;
        }
        match self.source.as_mut() {
            Some(source) => source.next(req),
            None => Ok(()),
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.initialized = true;
        if let Some(source) = self.source.as_mut() {
            source.close()?;
        }
        self.source = None;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
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

pub(crate) fn expression_column_offsets(expressions: &[Expression]) -> Vec<usize> {
    fn collect(expression: &Expression, offsets: &mut BTreeSet<usize>) {
        match expression {
            Expression::Column(column) => {
                if let Ok(offset) = usize::try_from(column.index) {
                    offsets.insert(offset);
                }
            }
            Expression::ScalarFunction(function) => {
                for argument in &function.args {
                    collect(argument, offsets);
                }
            }
            Expression::Constant(_)
            | Expression::CorrelatedColumn(_)
            | Expression::ScalarSubQuery(_) => {}
        }
    }

    let mut offsets = BTreeSet::new();
    for expression in expressions {
        collect(expression, &mut offsets);
    }
    offsets.into_iter().collect()
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
