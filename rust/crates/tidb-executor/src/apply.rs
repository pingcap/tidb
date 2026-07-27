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

//! Apply: the operator a CORRELATED subquery becomes.
//!
//! Go's `NestedLoopApplyExec` re-runs the inner plan once per outer row, after
//! writing that row's values into the correlated columns the inner plan
//! references (`for _, col := range e.OuterSchema { *col.Data = ... }`). This
//! is that loop: the outer child streams rows, each row is handed to a
//! `run_inner` callback that produces the inner result for those bindings, and
//! the output row is the outer row plus one appended column carrying it.
//!
//! Appending exactly one column is what lets the outer query keep referring to
//! the subquery by an ordinary column reference, which is how Go's plan reads
//! after `handleScalarSubquery` builds an Apply: the subquery expression is
//! replaced by the Apply's last schema column.
//!
//! NOT MODELLED (documented): Go's apply cache (`applycache`), which skips
//! re-running the inner plan when consecutive outer rows share correlated
//! values, its parallel variant, and the decorrelation rewrites the optimizer
//! applies before falling back to Apply. Those change cost, not results.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

/// Produces the inner result for one outer row's bindings.
///
/// The values are the outer row's cells, in outer-schema order; the callback
/// binds the correlated columns from them and runs the inner query.
pub type InnerRunner = Box<dyn FnMut(&[Datum]) -> Result<Datum, ExecError>>;

/// Go `NestedLoopApplyExec`, restricted to the one-appended-column shape a
/// scalar or `EXISTS` correlated subquery needs.
pub struct ApplyExec {
    meta: ExecutorMeta,
    outer: Box<dyn Executor>,
    run_inner: InnerRunner,
    emitted: bool,
}

impl ApplyExec {
    /// Builds an apply over `outer`, appending the column `run_inner` yields.
    ///
    /// The callback owns whatever it reads, because an executor is a `'static`
    /// trait object here; the driver therefore hands it an owned catalog
    /// snapshot. That copy is the price of this seed's ownership shape, not a
    /// semantic choice -- the inner plan only reads, and Go likewise runs it
    /// against one fixed snapshot for the whole statement.
    #[must_use]
    pub fn new(meta: ExecutorMeta, outer: Box<dyn Executor>, run_inner: InnerRunner) -> Self {
        ApplyExec {
            meta,
            outer,
            run_inner,
            emitted: false,
        }
    }
}

impl Executor for ApplyExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.outer.open()?;
        self.emitted = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        let outer_types: Vec<FieldType> = self.outer.ret_field_types().to_vec();
        let mut outer_chunk = self.outer.new_chunk();
        loop {
            self.outer.next(&mut outer_chunk)?;
            let rows = outer_chunk.num_rows();
            if rows == 0 {
                break;
            }
            for r in 0..rows {
                let row = outer_chunk.get_row(r);
                let values: Vec<Datum> = outer_types
                    .iter()
                    .enumerate()
                    .map(|(c, ft)| row.get_datum(c, ft))
                    .collect();
                // One inner run per outer row, as Go's apply loop does.
                let inner = (self.run_inner)(&values)?;
                for (c, value) in values.iter().enumerate() {
                    req.append_datum(c, value);
                }
                req.append_datum(values.len(), &inner);
            }
        }
        self.emitted = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.outer.close()
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
