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

//! `pkg/executor` `TableDualExec`: a source that emits a fixed number (0 or 1)
//! of rows carrying no real column data -- the source for a `FROM`-less
//! `SELECT`.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

/// Go `TableDualExec`: yields `num_dual_rows` (0 or 1) rows, then EOF.
pub struct TableDualExec {
    meta: ExecutorMeta,
    /// Go `numDualRows` (0 or 1).
    num_dual_rows: usize,
    /// Go `numReturned`.
    num_returned: usize,
}

impl TableDualExec {
    /// Builds a table-dual source that will emit `num_dual_rows` rows.
    #[must_use]
    pub fn new(meta: ExecutorMeta, num_dual_rows: usize) -> Self {
        TableDualExec {
            meta,
            num_dual_rows,
            num_returned: 0,
        }
    }
}

impl Executor for TableDualExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.num_returned = 0;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.num_returned >= self.num_dual_rows {
            return Ok(());
        }
        if self.meta.schema().is_empty() {
            // No columns: record the row count virtually.
            req.set_num_virtual_rows(1);
        } else {
            for i in 0..self.meta.schema().len() {
                req.append_null(i);
            }
        }
        self.num_returned = self.num_dual_rows;
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
}
