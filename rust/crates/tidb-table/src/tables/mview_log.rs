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

//! Go `pkg/table/tables/mview_log.go`: a base table's DML appends its
//! materialized view log (MLog) rows. Go wraps the base `table.Table`; here
//! the statement holds the [`MLogTable`] beside the base table and calls it
//! after each base `AddRecord` / `UpdateRecord` / `RemoveRecord`, as the
//! wrapper's overrides do after delegating.

use tidb_datatype::Datum;
use tidb_kv::storage::TableStorage;
use tidb_model::table::{MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME, MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME};

use super::{KvTableError, TableCommon};

/// Go `MLogSourceStmt`: the statement kind writing the MLog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MLogSourceStmt {
    /// Go `MLogSourceInsert`.
    Insert,
    /// Go `MLogSourceUpdate`.
    Update,
    /// Go `MLogSourceDelete`.
    Delete,
    /// Go `MLogSourceReplace`.
    Replace,
    /// Go `MLogSourceLoadData`.
    LoadData,
    /// Go `MLogSourceRefresh`.
    Refresh,
}

/// Go `MLogDMLType`: the `_MLOG$_DML_TYPE` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MLogDMLType {
    /// Go `MLogDMLTypeInsert`.
    Insert,
    /// Go `MLogDMLTypeUpdate`.
    Update,
    /// Go `MLogDMLTypeDelete`.
    Delete,
}

impl MLogDMLType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "I",
            Self::Update => "U",
            Self::Delete => "D",
        }
    }
}

/// Go `mlogOldRowMarker`.
const MLOG_OLD_ROW_MARKER: i64 = -1;
/// Go `mlogNewRowMarker`.
const MLOG_NEW_ROW_MARKER: i64 = 1;

fn error(message: String) -> KvTableError {
    KvTableError::Encode(message)
}

/// Go `validateMLogMetaColumn`.
fn validate_mlog_meta_column(mlog: &tidb_model::TableInfo) -> Result<(), KvTableError> {
    let cols: Vec<tidb_model::GoShared<tidb_model::ColumnInfo>> = mlog
        .columns
        .iter_deref()
        .filter(|column| column.read().state == tidb_model::SchemaState::PUBLIC)
        .collect();
    let info = mlog.materialized_view_log.as_ref().expect("checked by the caller").read().clone();
    let tracked: Vec<&tidb_ast::CiString> = info.columns.iter().collect();
    let expected_len = tracked.len() + 2;
    if cols.len() != expected_len {
        return Err(error(format!("invalid mlog meta columns, expect {expected_len}, got {}", cols.len())));
    }
    for (i, tracked_col) in tracked.iter().enumerate() {
        if cols[i].read().name.lowercase() != tracked_col.lowercase() {
            return Err(error(format!(
                "invalid mlog tracked columns order at position {i}, expect {}, got {}",
                tracked_col.original(),
                cols[i].read().name.original()
            )));
        }
    }
    let (dml_type, old_new) = (cols[cols.len() - 2].read(), cols[cols.len() - 1].read());
    if dml_type.name.lowercase() != MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME.to_lowercase()
        || old_new.name.lowercase() != MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME.to_lowercase()
    {
        return Err(error(format!(
            "invalid mlog meta columns, expect {MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME},{MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME}, got {},{}",
            dml_type.name.original(),
            old_new.name.original()
        )));
    }
    Ok(())
}

/// Go `mlogTable`'s MLog side: the statement-scoped MLog table, the source
/// statement and the base-table offsets of the tracked columns.
pub struct MLogTable {
    mlog: TableCommon,
    source_stmt: MLogSourceStmt,
    tracked_base_offsets: Vec<usize>,
    removed_conflict: bool,
}

/// Go `WrapTableWithMaterializedViewLog`: the MLog writer of `base`'s
/// statement over `mlog`, after Go's metadata checks.
///
/// # Errors
///
/// The base or MLog metadata does not pair up, or the MLog's columns are not
/// the tracked ones followed by the DML-type and old/new columns.
pub fn wrap_table_with_materialized_view_log(
    base: &TableCommon,
    mlog: TableCommon,
    source_stmt: MLogSourceStmt,
) -> Result<MLogTable, KvTableError> {
    let base_meta = base.meta().read().clone();
    let mlog_meta = mlog.meta().read().clone();
    let mlog_id = base_meta.materialized_view_base.as_ref().map_or(0, |info| info.read().mlog_id);
    if mlog_id == 0 {
        return Err(error("wrap table with mlog: base table has no mlog info".to_owned()));
    }
    if mlog_id != mlog_meta.id {
        return Err(error(format!(
            "wrap table with mlog: mlog id mismatch, base mlog id={mlog_id}, mlog table id={}",
            mlog_meta.id
        )));
    }
    let Some(log_info) = mlog_meta.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return Err(error("wrap table with mlog: mlog table has no MaterializedViewLog info".to_owned()));
    };
    if log_info.base_table_id != base_meta.id {
        return Err(error(format!(
            "wrap table with mlog: base table id mismatch, mlog base id={}, base id={}",
            log_info.base_table_id, base_meta.id
        )));
    }
    validate_mlog_meta_column(&mlog_meta).map_err(|e| error(format!("wrap table with mlog: {e:?}")))?;
    let mut tracked_base_offsets = Vec::with_capacity(log_info.columns.len());
    for tracked in log_info.columns.iter() {
        let offset = base_meta
            .columns
            .iter_deref()
            .find(|column| column.read().name.lowercase() == tracked.lowercase())
            .map(|column| column.read().offset)
            .ok_or_else(|| error(format!("wrap table with mlog: base column {} not found", tracked.original())))?;
        tracked_base_offsets.push(usize::try_from(offset).unwrap_or(usize::MAX));
    }
    Ok(MLogTable { mlog, source_stmt, tracked_base_offsets, removed_conflict: false })
}

impl MLogTable {
    /// Go `mlogTable.AddRecord`'s first step, before the base `AddRecord`:
    /// "Consume the marker before calling the base table so an error cannot
    /// leak it to a later row."
    pub fn take_removed_conflict(&mut self) -> bool {
        std::mem::take(&mut self.removed_conflict)
    }

    /// Go `mlogTable.AddRecord` after the base `AddRecord` succeeded:
    /// `is_update` is Go's `AddRecordOpt.IsUpdate`, the insert half of a
    /// handle-changing update; `had_removed_conflict` is what
    /// [`Self::take_removed_conflict`] took before the base add.
    ///
    /// # Errors
    ///
    /// The MLog row could not be written.
    pub fn add_record(
        &mut self,
        store: &mut dyn TableStorage,
        row: &[Datum],
        is_update: bool,
        had_removed_conflict: bool,
        lazy_dup_check: bool,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        let dml_type = match self.source_stmt {
            MLogSourceStmt::Refresh => MLogDMLType::Insert,
            MLogSourceStmt::Update => MLogDMLType::Update,
            MLogSourceStmt::Insert | MLogSourceStmt::Replace | MLogSourceStmt::LoadData => {
                if is_update || had_removed_conflict {
                    MLogDMLType::Update
                } else {
                    MLogDMLType::Insert
                }
            }
            MLogSourceStmt::Delete => MLogDMLType::Insert,
        };
        self.write_mlog_row(store, row, dml_type, MLOG_NEW_ROW_MARKER, lazy_dup_check, ctx)
    }

    /// Go `mlogTable.UpdateRecord` after the base `UpdateRecord` (the
    /// handle-unchanged path) succeeded: the old and new rows, when a tracked
    /// column is among the `touched` offsets.
    ///
    /// # Errors
    ///
    /// An MLog row could not be written.
    pub fn update_record(
        &mut self,
        store: &mut dyn TableStorage,
        old: &[Datum],
        new: &[Datum],
        touched: &dyn Fn(usize) -> bool,
        lazy_dup_check: bool,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        if !self.tracked_base_offsets.iter().any(|offset| touched(*offset)) {
            return Ok(());
        }
        self.write_mlog_row(store, old, MLogDMLType::Update, MLOG_OLD_ROW_MARKER, lazy_dup_check, ctx)?;
        self.write_mlog_row(store, new, MLogDMLType::Update, MLOG_NEW_ROW_MARKER, lazy_dup_check, ctx)
    }

    /// Go `mlogTable.RemoveRecord` after the base `RemoveRecord` succeeded.
    ///
    /// # Errors
    ///
    /// The MLog row could not be written.
    pub fn remove_record(
        &mut self,
        store: &mut dyn TableStorage,
        row: &[Datum],
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        if matches!(self.source_stmt, MLogSourceStmt::Insert | MLogSourceStmt::Replace | MLogSourceStmt::LoadData) {
            self.removed_conflict = true;
        }
        let dml_type = if matches!(self.source_stmt, MLogSourceStmt::Delete | MLogSourceStmt::Refresh) {
            MLogDMLType::Delete
        } else {
            // Under INSERT/UPDATE/REPLACE/LOAD DATA, RemoveRecord is the old
            // side of an update.
            MLogDMLType::Update
        };
        self.write_mlog_row(store, row, dml_type, MLOG_OLD_ROW_MARKER, false, ctx)
    }

    /// Go `mlogTable.writeMLogRow`: the tracked columns of `base_row`, the
    /// DML type and the old/new marker, appended to the MLog with no
    /// duplicate check and the MLog's own row ids.
    fn write_mlog_row(
        &mut self,
        store: &mut dyn TableStorage,
        base_row: &[Datum],
        dml_type: MLogDMLType,
        old_new: i64,
        lazy_dup_check: bool,
        ctx: &dyn tidb_tblctx::MutateContext,
    ) -> Result<(), KvTableError> {
        if let Some(message) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/table/tables/mockMLogWriteError") {
            // Go: a string value is the error's message.
            return Err(error(message.as_string().unwrap_or("mock materialized view log write error").to_owned()));
        }
        let mut mlog_row = Vec::with_capacity(self.tracked_base_offsets.len() + 2);
        for offset in &self.tracked_base_offsets {
            let Some(value) = base_row.get(*offset) else {
                return Err(error(format!(
                    "write mlog row: column at offset {offset} is missing from the base row (len {})",
                    base_row.len()
                )));
            };
            mlog_row.push(value.clone());
        }
        mlog_row.push(Datum::new_string_datum(dml_type.as_str()));
        mlog_row.push(Datum::Int(old_new));
        // Go: "ReservedRowIDAlloc only tracks one table. Use a separate
        // allocation range for the MLog insert, then restore the base-table
        // range for the caller."
        let reserved = ctx.get_reserved_row_id_alloc();
        let saved = reserved.map(|alloc| {
            let mut alloc = alloc.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = alloc.current();
            alloc.reset(0, 0);
            current
        });
        let added = self
            .mlog
            .insert_row_with_row_id_checked_without_primary_duplicate_check(store, &mlog_row, None, 0, ctx, lazy_dup_check)
            .map(|_| ());
        if let (Some(alloc), Some((base, max))) = (reserved, saved) {
            alloc
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reset(base, max);
        }
        added
    }
}
