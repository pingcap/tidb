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

//! Go `pkg/executor/check_table_index.go`'s `CheckTableExec`: the
//! non-fast `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX`, which compares the
//! table's and each index's row counts (`admin.CheckIndicesCount`) and runs
//! one checking `IndexLookUpExecutor` per index (`checkIndexValue`), falling
//! back to `admin.CheckRecordAndIndex` where the table has more rows.

use tidb_executor_internal_exec::Executor;
use tidb_model::{GoShared, IndexInfo};

use crate::index_lookup::IndexLookUpExecutor;
use crate::DriverError;

/// Go `admin.TblCntGreater` / `admin.IdxCntGreater`: which side of a count
/// mismatch has more rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CountGreater {
    /// Go `TblCntGreater`.
    TblCntGreater,
    /// Go `IdxCntGreater`.
    IdxCntGreater,
}

/// Go `admin.CheckIndicesCount`: the table's row count against each named
/// index's, read by `count` (Go `getCount`); the first mismatch answers
/// which side is greater, the index's offset and Go's `ErrAdminCheckTable`.
///
/// # Errors
/// A count that does not run.
pub(crate) fn check_indices_count(
    db_name: &str,
    table_name: &str,
    indices: &[String],
    count: &mut dyn FnMut(&str) -> Result<i64, DriverError>,
) -> Result<(), (Option<CountGreater>, usize, DriverError)> {
    let sql = |format: &str, arguments: &[tidb_sqlescape::SqlArg<'_>]| {
        tidb_sqlescape::escape_sql_text(format, arguments)
            .map_err(|error| DriverError::unsupported(format!("{error:?}")))
    };
    let tbl_cnt = sql(
        "SELECT COUNT(*) FROM %n.%n USE INDEX()",
        &[tidb_sqlescape::SqlArg::from(db_name), tidb_sqlescape::SqlArg::from(table_name)],
    )
    .and_then(|sql| count(&sql))
    .map_err(|error| (None, 0, error))?;
    for (i, idx) in indices.iter().enumerate() {
        let idx_cnt = sql(
            "SELECT COUNT(*) FROM %n.%n USE INDEX(%n)",
            &[
                tidb_sqlescape::SqlArg::from(db_name),
                tidb_sqlescape::SqlArg::from(table_name),
                tidb_sqlescape::SqlArg::from(idx.as_str()),
            ],
        )
        .and_then(|sql| count(&sql))
        .map_err(|error| (None, i, error))?;
        if tbl_cnt == idx_cnt {
            continue;
        }
        let ret = if tbl_cnt > idx_cnt { CountGreater::TblCntGreater } else { CountGreater::IdxCntGreater };
        return Err((
            Some(ret),
            i,
            DriverError::from(crate::admin_errors::ERR_ADMIN_CHECK_TABLE.gen_with_stack(format!(
                "table count {tbl_cnt} != index({idx}) count {idx_cnt}"
            ))),
        ));
    }
    Ok(())
}

/// What `CheckTableExec.Next` asks of its session: Go `getCount`
/// (`CheckIndicesCount`'s restricted SQL) and `checkTableRecord`
/// (`CheckRecordAndIndex` over each physical table) for one index.
pub(crate) trait CheckTableSession {
    /// Go `getCount(exec, snapshot, sql)`.
    fn count(&mut self, sql: &str) -> Result<i64, DriverError>;
    /// Go `checkTableRecord(ctx, idxOffset)`.
    fn check_table_record(&mut self, index: &IndexInfo) -> Result<(), DriverError>;
}

/// Go `CheckTableExec`.
pub(crate) struct CheckTableExec {
    /// Go `dbName`.
    pub(crate) db_name: String,
    /// Go `table.Meta().Name.O`.
    pub(crate) table_name: String,
    /// Go `indexInfos`.
    pub(crate) index_infos: Vec<GoShared<IndexInfo>>,
    /// Go `srcs`.
    pub(crate) srcs: Vec<IndexLookUpExecutor>,
    /// Go `done`.
    done: bool,
    /// Go `checkIndex`.
    pub(crate) check_index: bool,
}

impl CheckTableExec {
    /// Go `buildCheckTable`'s `CheckTableExec`.
    pub(crate) fn new(
        db_name: String,
        table_name: String,
        index_infos: Vec<GoShared<IndexInfo>>,
        srcs: Vec<IndexLookUpExecutor>,
        check_index: bool,
    ) -> Self {
        Self { db_name, table_name, index_infos, srcs, done: false, check_index }
    }

    /// Go `Open`: every lookup opened.
    pub(crate) fn open(&mut self) -> Result<(), DriverError> {
        for src in &mut self.srcs {
            src.open().map_err(DriverError::from)?;
        }
        self.done = false;
        Ok(())
    }

    /// Go `Close`: every lookup closed, answering the first failure.
    pub(crate) fn close(&mut self) -> Result<(), DriverError> {
        let mut first_err = None;
        for src in &mut self.srcs {
            if let Err(error) = src.close() {
                first_err.get_or_insert(DriverError::from(error));
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Go `checkIndexHandle`: the lookup drained, its table worker comparing
    /// every row with its index entry.
    fn check_index_handle(src: &mut IndexLookUpExecutor) -> Result<(), DriverError> {
        let mut chk = src.new_chunk();
        loop {
            src.next(&mut chk).map_err(DriverError::from)?;
            if chk.num_rows() == 0 {
                return Ok(());
            }
        }
    }

    /// Go `checkTableIndexHandle`: the lookups of `idx_info`.
    fn check_table_index_handle(&mut self, idx_info: &IndexInfo) -> Result<(), DriverError> {
        let name = idx_info.name.lowercase().to_owned();
        for src in &mut self.srcs {
            if src.index().name.lowercase() == name {
                Self::check_index_handle(src)?;
            }
        }
        Ok(())
    }

    /// Go `Next`.
    ///
    /// # Errors
    /// Go's: a partial index, a count mismatch under `ADMIN CHECK INDEX`, or
    /// the first inconsistency a check finds.
    pub(crate) fn next(&mut self, session: &mut dyn CheckTableSession) -> Result<(), DriverError> {
        if self.done || self.srcs.is_empty() {
            return Ok(());
        }
        let result = self.next_inner(session);
        self.done = true;
        result
    }

    fn next_inner(&mut self, session: &mut dyn CheckTableSession) -> Result<(), DriverError> {
        let mut idx_names = Vec::with_capacity(self.index_infos.len());
        for idx in &self.index_infos {
            let idx = idx.read();
            if idx.mv_index || idx.is_columnar_index() {
                continue;
            }
            if idx.has_condition() {
                return Err(DriverError::from(
                    crate::executor_errors::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK.gen_with_stack_by_args(&[]),
                ));
            }
            idx_names.push(idx.name.original().to_owned());
        }
        if let Err((greater, idx_offset, err)) =
            check_indices_count(&self.db_name, &self.table_name, &idx_names, &mut |sql| session.count(sql))
        {
            if self.check_index {
                return Err(err);
            }
            let idx_info = self.index_infos[idx_offset].read().clone();
            return match greater {
                Some(CountGreater::IdxCntGreater) => self.check_table_index_handle(&idx_info),
                Some(CountGreater::TblCntGreater) => session.check_table_record(&idx_info),
                // Go returns `err` unchanged when the count itself failed.
                None => Err(err),
            };
        }

        if self.srcs.len() == 1 {
            Self::check_index_handle(&mut self.srcs[0])?;
            if self.srcs[0].index().mv_index {
                session.check_table_record(&self.srcs[0].index().clone())?;
            }
        }
        // Go hands the lookups to `min(3, len(srcs))` workers, which stop at
        // the first failure; only a lookup's own failure reaches `retCh`,
        // a multi-valued index's `checkTableRecord` failure stops the
        // workers without one.
        for src in &mut self.srcs {
            Self::check_index_handle(src)?;
            if src.index().mv_index {
                let index = src.index().clone();
                if session.check_table_record(&index).is_err() {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
