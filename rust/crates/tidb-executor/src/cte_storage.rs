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

//! Spill-backed storage for common table expressions.
//!
//! This is the Rust ownership equivalent of Go `pkg/util/cteutil.StorageRC`.
//! A [`CteStorage`] owns one [`RowContainer`]; [`Arc`] replaces the explicit
//! `OpenAndRef`/`DerefAndClose` counter, and the last owner closes the spill
//! file and detaches its trackers. The stored rows, iteration marker, done
//! flag, producer error, reopen, and data-swap behavior remain explicit.

use std::fmt;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row_container::{RowContainer, RowContainerChunk};
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;
use tidb_util::memory::{ArcAction, Tracker, LABEL_FOR_CTE_STORAGE};

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::predicate_pushdown::{PushedScanFilter, ScanFilterProbe};
use crate::table_access::TableAccess;
use crate::StatementMemory;

/// One open row-container and the accounting authority that owns it.
struct CteData {
    field_types: Vec<FieldType>,
    chunk_size: usize,
    rows: RowContainer,
    memory: StatementMemory,
    mem_parent: Arc<Tracker>,
    disk_parent: Arc<Tracker>,
    registered_action: Option<ArcAction>,
}

impl CteData {
    fn new(field_types: Vec<FieldType>, chunk_size: usize, memory: StatementMemory) -> Self {
        let chunk_size = chunk_size.max(1);
        let mem_parent = memory.operator_tracker(LABEL_FOR_CTE_STORAGE);
        let disk_parent = memory.operator_disk_tracker(LABEL_FOR_CTE_STORAGE);
        let mut rows = RowContainer::new(&field_types, chunk_size, memory.spill_storage());
        rows.mem_tracker().set_label(LABEL_FOR_CTE_STORAGE);
        rows.mem_tracker().attach_to(&mem_parent);
        rows.disk_tracker().set_label(LABEL_FOR_CTE_STORAGE);
        rows.disk_tracker().attach_to(&disk_parent);

        let registered_action = if memory.tmp_storage_on_oom() {
            let action: ArcAction = rows.action_spill();
            memory
                .session_tracker()
                .fallback_old_and_set_new_action(Arc::clone(&action));
            Some(action)
        } else {
            None
        };

        Self {
            field_types,
            chunk_size,
            rows,
            memory,
            mem_parent,
            disk_parent,
            registered_action,
        }
    }

    fn close(&mut self) {
        if let Some(action) = self.registered_action.take() {
            self.memory
                .session_tracker()
                .unbind_action_from_hard_limit(&action);
        }
        self.rows.close();
        self.mem_parent.detach();
        self.disk_parent.detach();
    }
}

impl Drop for CteData {
    fn drop(&mut self) {
        self.close();
    }
}

/// Go `StorageRC`, expressed with Rust ownership instead of a manual refcount.
pub struct CteStorage {
    data: Option<CteData>,
    done: bool,
    error: Option<String>,
    iter: usize,
}

impl fmt::Debug for CteStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CteStorage")
            .field("open", &self.data.is_some())
            .field("rows", &self.num_rows())
            .field("chunks", &self.num_chunks())
            .field("done", &self.done)
            .field("error", &self.error)
            .field("iter", &self.iter)
            .finish()
    }
}

impl CteStorage {
    /// Opens an empty spill-backed storage.
    #[must_use]
    pub fn new(field_types: Vec<FieldType>, chunk_size: usize, memory: StatementMemory) -> Self {
        Self {
            data: Some(CteData::new(field_types, chunk_size, memory)),
            done: false,
            error: None,
            iter: 0,
        }
    }

    fn data(&self) -> Result<&CteData, ExecError> {
        self.data
            .as_ref()
            .ok_or_else(|| ExecError::internal("CTE storage is not open"))
    }

    fn data_mut(&mut self) -> Result<&mut CteData, ExecError> {
        self.data
            .as_mut()
            .ok_or_else(|| ExecError::internal("CTE storage is not open"))
    }

    /// Adds one already-columnar batch. Empty batches are a no-op.
    pub fn add_chunk(&mut self, chunk: Chunk) -> Result<(), ExecError> {
        if chunk.num_rows() == 0 {
            return Ok(());
        }
        let data = self.data_mut()?;
        data.rows
            .add(chunk)
            .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
        data.memory.check()
    }

    /// Adds row values in source order, batching them by this storage's chunk
    /// size. The iterator is consumed, so no second full row matrix is kept.
    pub fn add_rows(
        &mut self,
        rows: impl IntoIterator<Item = Vec<Datum>>,
    ) -> Result<(), ExecError> {
        let (field_types, chunk_size) = {
            let data = self.data()?;
            (data.field_types.clone(), data.chunk_size)
        };
        let mut chunk = Chunk::new_with_capacity(&field_types, chunk_size);
        for row in rows {
            if row.len() != field_types.len() {
                return Err(ExecError::internal(format!(
                    "CTE row width {} does not match schema width {}",
                    row.len(),
                    field_types.len()
                )));
            }
            if field_types.is_empty() {
                chunk.set_num_virtual_rows(chunk.num_rows() + 1);
            } else {
                for (column, value) in row.iter().enumerate() {
                    chunk.append_datum(column, value);
                }
            }
            if chunk.num_rows() == chunk_size {
                let full = std::mem::replace(
                    &mut chunk,
                    Chunk::new_with_capacity(&field_types, chunk_size),
                );
                self.add_chunk(full)?;
            }
        }
        self.add_chunk(chunk)
    }

    /// Replaces the row container with a fresh empty one while retaining the
    /// storage's schema and statement authority.
    pub fn reopen(&mut self) -> Result<(), ExecError> {
        let data = self.data()?;
        let replacement = CteData::new(
            data.field_types.clone(),
            data.chunk_size,
            data.memory.clone(),
        );
        self.data = Some(replacement);
        self.done = false;
        self.error = None;
        self.iter = 0;
        Ok(())
    }

    /// Swaps only stored data and its row schema. Producer state (`done`,
    /// error and iteration) remains on each storage, as in Go `SwapData`.
    pub fn swap_data(&mut self, other: &mut Self) -> Result<(), ExecError> {
        self.data()?;
        other.data()?;
        std::mem::swap(&mut self.data, &mut other.data);
        Ok(())
    }

    /// Explicitly closes the storage. Dropping the final owner does the same.
    pub fn close(&mut self) {
        self.data.take();
        self.done = false;
        self.error = None;
        self.iter = 0;
    }

    /// Returns one stored chunk, reading it back from disk after a spill.
    pub fn get_chunk(&self, index: usize) -> Result<RowContainerChunk<'_>, ExecError> {
        self.data()?
            .rows
            .get_chunk(index)
            .map_err(|error| ExecError::SpillFailed(error.to_string()))
    }

    /// Materializes one addressed row. This is the Rust value-returning
    /// equivalent of Go `GetRow`'s borrowed chunk row.
    pub fn get_row(&self, chunk_index: usize, row_index: usize) -> Result<Vec<Datum>, ExecError> {
        let field_types = self.field_types()?;
        let chunk = self.get_chunk(chunk_index)?;
        Ok(chunk.get_row(row_index).get_datum_row(field_types))
    }

    /// Materializes every row. Used by boundaries that still require a value
    /// matrix; CTE table scans use [`CteTableSourceExec`] and do not call it.
    pub fn to_rows(&self) -> Result<Vec<Vec<Datum>>, ExecError> {
        let mut rows = Vec::with_capacity(self.num_rows());
        for chunk_index in 0..self.num_chunks() {
            let chunk = self.get_chunk(chunk_index)?;
            for row_index in 0..chunk.num_rows() {
                rows.push(chunk.get_row(row_index).get_datum_row(self.field_types()?));
            }
        }
        Ok(rows)
    }

    /// Configured row field types.
    pub fn field_types(&self) -> Result<&[FieldType], ExecError> {
        Ok(&self.data()?.field_types)
    }

    /// Configured maximum rows per stored chunk.
    pub fn chunk_size(&self) -> Result<usize, ExecError> {
        Ok(self.data()?.chunk_size)
    }

    /// Number of stored chunks.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.data.as_ref().map_or(0, |data| data.rows.num_chunks())
    }

    /// Number of stored rows.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.data.as_ref().map_or(0, |data| data.rows.num_row())
    }

    /// Number of rows in one stored chunk.
    #[must_use]
    pub fn num_rows_of_chunk(&self, chunk_index: usize) -> usize {
        self.data
            .as_ref()
            .map_or(0, |data| data.rows.num_rows_of_chunk(chunk_index))
    }

    /// Whether the row container has spilled.
    #[must_use]
    pub fn already_spilled(&self) -> bool {
        self.data
            .as_ref()
            .is_some_and(|data| data.rows.already_spilled())
    }

    /// Bytes retained in memory by the row container.
    #[must_use]
    pub fn mem_bytes(&self) -> i64 {
        self.data
            .as_ref()
            .map_or(0, |data| data.rows.mem_tracker().bytes_consumed())
    }

    /// Bytes retained on disk by the row container.
    #[must_use]
    pub fn disk_bytes(&self) -> i64 {
        self.data
            .as_ref()
            .map_or(0, |data| data.rows.disk_tracker().bytes_consumed())
    }

    /// Marks producer completion.
    pub fn set_done(&mut self) {
        self.done = true;
    }

    /// Whether the producer completed.
    #[must_use]
    pub fn done(&self) -> bool {
        self.done
    }

    /// Stores the producer error for a later reader.
    pub fn set_error(&mut self, error: impl Into<String>) {
        self.error = Some(error.into());
    }

    /// Stored producer error.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Sets the producer iteration counter.
    pub fn set_iter(&mut self, iter: usize) {
        self.iter = iter;
    }

    /// Producer iteration counter.
    #[must_use]
    pub fn iter(&self) -> usize {
        self.iter
    }
}

/// A catalog-visible CTE relation. A recursive definition can expose a LIMIT
/// window without copying its spill-backed result.
#[derive(Clone, Debug)]
pub struct CteTable {
    columns: Vec<(String, FieldType)>,
    storage: Arc<CteStorage>,
    row_offset: usize,
    row_count: usize,
}

impl CteTable {
    /// Exposes every row in `storage`.
    #[must_use]
    pub fn new(columns: Vec<(String, FieldType)>, storage: Arc<CteStorage>) -> Self {
        let row_count = storage.num_rows();
        Self {
            columns,
            storage,
            row_offset: 0,
            row_count,
        }
    }

    /// Exposes one logical LIMIT window over `storage` without copying rows.
    #[must_use]
    pub fn window(
        columns: Vec<(String, FieldType)>,
        storage: Arc<CteStorage>,
        row_offset: usize,
        row_count: usize,
    ) -> Self {
        let available = storage.num_rows().saturating_sub(row_offset);
        Self {
            columns,
            storage,
            row_offset,
            row_count: row_count.min(available),
        }
    }

    /// Result columns in row order.
    #[must_use]
    pub fn columns(&self) -> &[(String, FieldType)] {
        &self.columns
    }

    /// Number of visible rows after windowing.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Materializes this relation's visible window for a boundary that still
    /// consumes value matrices (currently multi-table DML source staging).
    pub fn to_rows(&self) -> Result<Vec<Vec<Datum>>, ExecError> {
        Ok(self
            .storage
            .to_rows()?
            .into_iter()
            .skip(self.row_offset)
            .take(self.row_count)
            .collect())
    }
}

/// Pull-based scan over a spill-backed CTE relation.
pub(crate) struct CteTableSourceExec {
    meta: ExecutorMeta,
    table: CteTable,
    chunk_index: usize,
    row_index: usize,
    remaining: usize,
    filter: Option<ScanFilterProbe>,
}

impl CteTableSourceExec {
    #[must_use]
    pub(crate) fn new(meta: ExecutorMeta, table: CteTable) -> Self {
        Self {
            meta,
            table,
            chunk_index: 0,
            row_index: 0,
            remaining: 0,
            filter: None,
        }
    }

    fn seek_to_window(&mut self) {
        self.chunk_index = 0;
        self.row_index = 0;
        let mut skip = self.table.row_offset;
        while self.chunk_index < self.table.storage.num_chunks() {
            let rows = self.table.storage.num_rows_of_chunk(self.chunk_index);
            if skip < rows {
                self.row_index = skip;
                break;
            }
            skip -= rows;
            self.chunk_index += 1;
        }
        self.remaining = self.table.row_count;
    }
}

impl Executor for CteTableSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.seek_to_window();
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if let Some(error) = self.table.storage.error() {
            return Err(ExecError::SpillFailed(error.to_owned()));
        }
        let target = req
            .required_rows()
            .min(self.meta.max_chunk_size())
            .min(self.remaining);
        while req.num_rows() < target && self.remaining > 0 {
            let chunk = self.table.storage.get_chunk(self.chunk_index)?;
            while self.row_index < chunk.num_rows() && req.num_rows() < target && self.remaining > 0
            {
                let row = chunk.get_row(self.row_index);
                self.row_index += 1;
                self.remaining -= 1;
                if let Some(filter) = self.filter.as_mut() {
                    let values = row.get_datum_row(self.meta.ret_field_types());
                    if !filter.admits(&values)? {
                        continue;
                    }
                    for (column, value) in values.iter().enumerate() {
                        req.append_datum(column, value);
                    }
                } else {
                    req.append_row(row);
                }
            }
            if self.row_index == chunk.num_rows() {
                self.chunk_index += 1;
                self.row_index = 0;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
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

    fn table_access(&mut self) -> Option<&mut dyn TableAccess> {
        Some(self)
    }
}

impl TableAccess for CteTableSourceExec {
    fn accept_scan_filter(&mut self, filter: &PushedScanFilter, ctx: &crate::StmtContext) -> bool {
        if filter.is_empty() {
            return false;
        }
        self.filter = Some(ScanFilterProbe::new(
            filter.clone(),
            ctx.clone(),
            self.meta.new_chunk(),
        ));
        true
    }
}
