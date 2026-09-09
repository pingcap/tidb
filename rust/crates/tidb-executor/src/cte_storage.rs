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
//! The explicit `OpenAndRef`/`DerefAndClose` lifecycle, stored rows, iteration
//! marker, done flag, producer error, reopen, and data-swap behavior match the
//! Go storage contract.

use std::fmt;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::row_container::{RowContainer, RowContainerChunk};
use tidb_datatype::{Datum, FieldType};
use tidb_util::memory::{ArcAction, Tracker, LABEL_FOR_CTE_STORAGE};

use crate::executor::ExecError;
use crate::StatementMemory;

/// One open row-container and the accounting authority that owns it.
struct CteData {
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

/// Go `StorageRC` with the same explicit lifecycle and state.
pub struct CteStorage {
    field_types: Vec<FieldType>,
    chunk_size: usize,
    memory: StatementMemory,
    data: Option<CteData>,
    ref_count: isize,
    done: bool,
    error: Option<String>,
    iter: usize,
}

impl fmt::Debug for CteStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (rows, chunks) = self
            .data
            .as_ref()
            .map_or((0, 0), |data| (data.rows.num_row(), data.rows.num_chunks()));
        formatter
            .debug_struct("CteStorage")
            .field("open", &self.data.is_some())
            .field("rows", &rows)
            .field("chunks", &chunks)
            .field("done", &self.done)
            .field("error", &self.error)
            .field("iter", &self.iter)
            .finish()
    }
}

impl CteStorage {
    /// Creates a closed storage. [`Self::open_and_ref`] opens its row container.
    #[must_use]
    pub fn new(field_types: Vec<FieldType>, chunk_size: usize, memory: StatementMemory) -> Self {
        Self {
            field_types,
            chunk_size,
            memory,
            data: None,
            ref_count: 0,
            done: false,
            error: None,
            iter: 0,
        }
    }

    /// Opens the underlying row container on the first reference, then
    /// increments the explicit storage reference count.
    pub fn open_and_ref(&mut self) -> Result<(), ExecError> {
        if self.ref_count <= 0 || self.data.is_none() {
            self.data = Some(CteData::new(
                self.field_types.clone(),
                self.chunk_size,
                self.memory.clone(),
            ));
            self.ref_count = 1;
            self.iter = 0;
        } else {
            self.ref_count += 1;
        }
        Ok(())
    }

    /// Drops one explicit reference and closes the row container at zero.
    pub fn deref_and_close(&mut self) -> Result<(), ExecError> {
        if self.ref_count <= 0 || self.data.is_none() {
            return Err(ExecError::internal("Storage not opend yet"));
        }
        self.ref_count -= 1;
        if self.ref_count < 0 {
            return Err(ExecError::internal("Storage ref count is less than zero"));
        }
        if self.ref_count == 0 {
            self.ref_count = -1;
            self.done = false;
            self.error = None;
            self.iter = 0;
            self.data.take();
        }
        Ok(())
    }

    fn data(&self) -> Result<&CteData, ExecError> {
        self.data
            .as_ref()
            .ok_or_else(|| ExecError::internal("Storage is not valid"))
    }

    fn data_mut(&mut self) -> Result<&mut CteData, ExecError> {
        self.data
            .as_mut()
            .ok_or_else(|| ExecError::internal("Storage is not valid"))
    }

    /// Adds one already-columnar batch. Empty batches are a no-op while open.
    pub fn add_chunk(&mut self, chunk: Chunk) -> Result<(), ExecError> {
        let data = self.data_mut()?;
        if chunk.num_rows() == 0 {
            return Ok(());
        }
        data.rows
            .add(chunk)
            .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
        data.memory.check()
    }

    /// Replaces the row container with a fresh empty one while retaining the
    /// storage's schema and statement authority.
    pub fn reopen(&mut self) -> Result<(), ExecError> {
        self.data.as_ref().expect("CTE storage is not open");
        let replacement = CteData::new(
            self.field_types.clone(),
            self.chunk_size,
            self.memory.clone(),
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
        std::mem::swap(&mut self.field_types, &mut other.field_types);
        std::mem::swap(&mut self.chunk_size, &mut other.chunk_size);
        std::mem::swap(&mut self.data, &mut other.data);
        Ok(())
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
        self.data()?;
        let chunk = self.get_chunk(chunk_index)?;
        Ok(chunk.get_row(row_index).get_datum_row(&self.field_types))
    }

    /// Number of stored chunks.
    #[must_use]
    pub fn num_chunks(&self) -> usize {
        self.data
            .as_ref()
            .expect("CTE storage is not open")
            .rows
            .num_chunks()
    }

    /// Number of stored rows.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.data
            .as_ref()
            .expect("CTE storage is not open")
            .rows
            .num_row()
    }

    /// Whether the row container has spilled.
    #[must_use]
    pub fn already_spilled(&self) -> bool {
        self.data
            .as_ref()
            .expect("CTE storage is not open")
            .rows
            .already_spilled()
    }

    /// Bytes retained in memory by the row container.
    #[must_use]
    pub fn mem_bytes(&self) -> i64 {
        self.data
            .as_ref()
            .expect("CTE storage is not open")
            .rows
            .mem_tracker()
            .bytes_consumed()
    }

    /// Bytes retained on disk by the row container.
    #[must_use]
    pub fn disk_bytes(&self) -> i64 {
        self.data
            .as_ref()
            .expect("CTE storage is not open")
            .rows
            .disk_tracker()
            .bytes_consumed()
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
