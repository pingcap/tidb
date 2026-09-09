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

//! Go `pkg/executor/select.go`: zero input rows produce a NULL row;
//! more than one input row produces `ErrSubqueryMoreThan1Row`.

use crate::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

/// The scalar-subquery cardinality operator.
pub struct MaxOneRowExec {
    meta: ExecutorMeta,
    child: Box<dyn Executor>,
    evaluated: bool,
}

impl MaxOneRowExec {
    /// Construct fresh cardinality state for one execution.
    pub fn new(meta: ExecutorMeta, child: Box<dyn Executor>) -> Self {
        Self {
            meta,
            child,
            evaluated: false,
        }
    }
}

impl Executor for MaxOneRowExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.evaluated = false;
        Ok(())
    }
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.evaluated {
            return Ok(());
        }
        self.evaluated = true;
        self.child.next(req)?;
        match req.num_rows() {
            0 => {
                for column in 0..self.schema().len() {
                    req.append_null(column);
                }
                return Ok(());
            }
            1 => {}
            _ => return Err(ExecError::SubqueryReturnsMoreThanOneRow),
        }
        let mut next = self.child.new_chunk();
        self.child.next(&mut next)?;
        if next.num_rows() != 0 {
            return Err(ExecError::SubqueryReturnsMoreThanOneRow);
        }
        Ok(())
    }
    fn close(&mut self) -> Result<(), ExecError> {
        self.child.close()
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
