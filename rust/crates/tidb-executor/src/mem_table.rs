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

//! An in-memory table source: emits rows from a provided value matrix.
//!
//! This is the mock table source that lets `SELECT ... FROM t` run before the
//! storage-backed readers exist -- the seam where Go's `TableReaderExec`
//! (distsql/tikv, via tablecodec) will plug in. It is deliberately NOT a port
//! of a specific Go executor; it stands in for one, and is documented as such.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

/// A source that emits the given rows once, then EOF.
pub struct MemTableSourceExec {
    meta: ExecutorMeta,
    rows: Vec<Vec<Datum>>,
    emitted: bool,
}

impl MemTableSourceExec {
    /// Builds a source over `rows` (each row one `Datum` per schema column).
    #[must_use]
    pub fn new(meta: ExecutorMeta, rows: Vec<Vec<Datum>>) -> Self {
        MemTableSourceExec {
            meta,
            rows,
            emitted: false,
        }
    }
}

impl Executor for MemTableSourceExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.emitted = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        for row in &self.rows {
            for (c, value) in row.iter().enumerate() {
                req.append_datum(c, value);
            }
        }
        self.emitted = true;
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
