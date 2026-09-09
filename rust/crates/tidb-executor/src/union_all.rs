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

//! Streaming UNION ALL executor, shared by SQL planning and the physical builder.

use crate::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

/// Concatenate already type-aligned child results.
pub struct UnionAllExec {
    meta: ExecutorMeta,
    children: Vec<Box<dyn Executor>>,
    current: usize,
}

impl UnionAllExec {
    /// Build fresh branch-cursor state.
    pub fn new(meta: ExecutorMeta, children: Vec<Box<dyn Executor>>) -> Self {
        Self {
            meta,
            children,
            current: 0,
        }
    }
}

impl Executor for UnionAllExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.current = 0;
        for child in &mut self.children {
            child.open()?;
        }
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        while self.current < self.children.len() {
            self.children[self.current].next(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
            self.current += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        for child in &mut self.children {
            child.close()?;
        }
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

    fn row_count(&mut self) -> Result<Option<u64>, ExecError> {
        let mut total = 0_u64;
        for child in &mut self.children {
            let count = match child.row_count()? {
                Some(count) => count,
                None => {
                    // A UNION ALL count remains exact when a child does not
                    // expose a structural shortcut: drain just that child
                    // and continue asking later branches.  In Web3Bench the
                    // point/index branch is tiny, while the join branch uses
                    // JoinExec::row_count and never enters this fallback.
                    let mut chunk = child.new_chunk();
                    let mut count = 0_u64;
                    loop {
                        child.next(&mut chunk)?;
                        let rows = chunk.num_rows();
                        if rows == 0 {
                            break;
                        }
                        count = count.checked_add(rows as u64).ok_or_else(|| {
                            ExecError::unsupported("UNION ALL row count overflow")
                        })?;
                        chunk.reset();
                    }
                    count
                }
            };
            total = total
                .checked_add(count)
                .ok_or_else(|| ExecError::unsupported("UNION ALL row count overflow"))?;
        }
        Ok(Some(total))
    }
}
