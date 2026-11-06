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

//! Go `pkg/executor/staticrecordset` (`recordset.go`, `cursorrecordset.go`):
//! the record set a detached executor tree keeps running in once its session
//! has finished the statement, and the wrapper that ties it to the cursor the
//! session tracks.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_session_cursor::CursorHandle;

use crate::{DriverError, Executor, StatementMemory};

/// Go `staticRecordSet`: a record set over an executor tree that no longer
/// depends on the session context.
///
/// Go's `Next` recovers an executor panic into an error; the operators this
/// tier builds already convert their Open/Next/Close panics to executor
/// errors at each operator boundary, so nothing is left to recover here.
pub struct StaticRecordSet {
    /// Go `fields`, as the result columns and the inputs of
    /// `colNames2ResultFields`.
    columns: Vec<(String, FieldType)>,
    output_names: Vec<tidb_datatype::FieldNameMetadata>,
    current_db: String,
    /// Go `executor`; `None` once closed (Go sets it to nil).
    executor: Option<Box<dyn Executor>>,
    memory: StatementMemory,
    init_cap: usize,
    max_chunk_size: usize,
}

impl StaticRecordSet {
    /// Go `staticrecordset.New`.
    #[must_use]
    pub(crate) fn new(
        columns: Vec<(String, FieldType)>,
        output_names: Vec<tidb_datatype::FieldNameMetadata>,
        current_db: String,
        executor: Box<dyn Executor>,
        memory: StatementMemory,
    ) -> Self {
        Self {
            init_cap: executor.init_cap(),
            max_chunk_size: executor.max_chunk_size(),
            columns,
            output_names,
            current_db,
            executor: Some(executor),
            memory,
        }
    }

    /// Go `Fields`: the result columns.
    #[must_use]
    pub fn columns(&self) -> &[(String, FieldType)] {
        &self.columns
    }

    /// Go `Fields`: the output names and the database the statement opened
    /// under, from which the result fields derive.
    #[must_use]
    pub fn output_names(&self) -> (&[tidb_datatype::FieldNameMetadata], &str) {
        (&self.output_names, &self.current_db)
    }

    /// Go `Next`: `exec.Next(ctx, s.executor, req)`, under the statement's
    /// memory quota as every executor Next is.
    pub fn next(&mut self, req: &mut Chunk) -> Result<(), DriverError> {
        self.memory.check()?;
        let executor = self.executor.as_mut().ok_or_else(|| {
            DriverError::Mysql(crate::MysqlError::from_terror(
                &tidb_exeerrors::ERR_QUERY_INTERRUPTED.gen_with_stack_by_args(&[]),
            ))
        })?;
        executor.next(req)?;
        self.memory.check()?;
        Ok(())
    }

    /// Go `NewChunk(nil)`: `exec.NewFirstChunk(s.executor)`.
    #[must_use]
    pub fn new_chunk(&self) -> Chunk {
        if let Some(executor) = &self.executor {
            // Go `staticRecordSet.NewChunk`: `exec.NewFirstChunk(s.executor)`.
            return tidb_executor_internal_exec::new_first_chunk(executor.as_ref());
        }
        let types: Vec<_> = self.columns.iter().map(|(_, ty)| ty.clone()).collect();
        Chunk::new(&types, self.init_cap, self.max_chunk_size)
    }

    /// Go `Close`: `exec.Close(s.executor)`, then drops the executor.
    pub fn close(&mut self) -> Result<(), DriverError> {
        match self.executor.take() {
            Some(mut executor) => executor.close().map_err(DriverError::from),
            None => Ok(()),
        }
    }
}

impl Drop for StaticRecordSet {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Go `cursorRecordSet`: a detached record set and the session cursor that
/// keeps its start timestamp visible to `ReportMinStartTS` until it closes.
pub struct CursorRecordSet {
    cursor: CursorHandle,
    record_set: StaticRecordSet,
}

impl CursorRecordSet {
    /// Go `Fields`.
    #[must_use]
    pub fn columns(&self) -> &[(String, FieldType)] {
        self.record_set.columns()
    }

    /// Go `Fields`; see [`StaticRecordSet::output_names`].
    #[must_use]
    pub fn output_names(&self) -> (&[tidb_datatype::FieldNameMetadata], &str) {
        self.record_set.output_names()
    }

    /// Go `Next`.
    pub fn next(&mut self, req: &mut Chunk) -> Result<(), DriverError> {
        self.record_set.next(req)
    }

    /// Go `NewChunk`.
    #[must_use]
    pub fn new_chunk(&self) -> Chunk {
        self.record_set.new_chunk()
    }

    /// Go `Close`: closes the cursor, then the record set.
    pub fn close(&mut self) -> Result<(), DriverError> {
        self.cursor.close();
        self.record_set.close()
    }
}

impl Drop for CursorRecordSet {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Go `WrapRecordSetWithCursor`.
#[must_use]
pub fn wrap_record_set_with_cursor(
    cursor: CursorHandle,
    record_set: StaticRecordSet,
) -> CursorRecordSet {
    CursorRecordSet { cursor, record_set }
}
