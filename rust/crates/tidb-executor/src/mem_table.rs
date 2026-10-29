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

//! The row source used by Go's `executorBuilder.buildMemTable` path after a
//! `PhysicalMemTable` resolves its virtual-table rows.

use tidb_executor_internal_exec::{Executor, ExecutorMeta};
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::predicate_pushdown::{PushedScanFilter, ScanFilterProbe};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::schema::Schema;

/// Builds a memory-table scan's rows when it is first read (Go's
/// `MemTableReaderExec.Next` calling `retriever.retrieve`).
pub type MemTableRowsFn = Box<dyn FnOnce() -> Result<Vec<Vec<Datum>>, ExecError> + Send>;

enum MemTableRows {
    Ready(Vec<Vec<Datum>>),
    /// Retrieved at the first `next`; taken then.
    Pending(Option<MemTableRowsFn>),
}

/// A source that emits the given rows once, then EOF.
pub struct MemTableSourceExec {
    meta: ExecutorMeta,
    rows: MemTableRows,
    emitted: bool,
    /// Conjuncts this source took over from the `Selection` above it; every
    /// row it emits has passed all of them.
    filter: Option<ScanFilterProbe>,
}

impl MemTableSourceExec {
    /// Builds a source over `rows` (each row one `Datum` per schema column).
    #[must_use]
    pub fn new(meta: ExecutorMeta, rows: Vec<Vec<Datum>>) -> Self {
        MemTableSourceExec {
            meta,
            rows: MemTableRows::Ready(rows),
            emitted: false,
            filter: None,
        }
    }

    /// Builds a source whose rows `retrieve` produces when the scan is first
    /// read, as Go's `MemTableReaderExec` does: a scan never read retrieves
    /// nothing.
    #[must_use]
    pub fn lazy(meta: ExecutorMeta, retrieve: MemTableRowsFn) -> Self {
        MemTableSourceExec {
            meta,
            rows: MemTableRows::Pending(Some(retrieve)),
            emitted: false,
            filter: None,
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
        if let MemTableRows::Pending(retrieve) = &mut self.rows {
            let retrieve = retrieve
                .take()
                .ok_or_else(|| ExecError::Internal("a memory-table scan retrieved twice".into()))?;
            self.rows = MemTableRows::Ready(retrieve()?);
        }
        let MemTableRows::Ready(rows) = &self.rows else {
            unreachable!("the rows were retrieved just above")
        };
        for row in rows {
            if let Some(filter) = self.filter.as_mut() {
                if !filter.admits(row)? {
                    continue;
                }
            }
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

    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        Some(self)
    }
}

impl tidb_executor_internal_exec::table_access::TableAccess for MemTableSourceExec {
    /// Every row this source can emit is in `rows`, and each one is tested,
    /// so the promise `accept_scan_filter` makes holds unconditionally --
    /// there is no second, unfiltered half of the stream to lose.
    fn accept_scan_filter(&mut self, filter: &PushedScanFilter, ctx: &crate::StmtContext) -> bool {
        if filter.is_empty() {
            return false;
        }
        match self.filter.as_mut() {
            Some(existing) => existing.conjoin(filter),
            None => {
                self.filter = Some(ScanFilterProbe::new(
                    filter.clone(),
                    ctx.clone(),
                    self.meta.new_chunk(),
                ));
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::column::Column;

    fn meta() -> ExecutorMeta {
        let mut column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        column.index = 0;
        ExecutorMeta::new(Schema::new(vec![column]), 0, 4, 1024)
    }

    /// Go's `MemTableReaderExec.Open` retrieves nothing and the first `Next`
    /// retrieves once: a scan that is never read costs no retrieval, and a
    /// failed retrieval is that `Next`'s error.
    #[test]
    fn a_lazy_scan_retrieves_at_its_first_next() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let mut scan = MemTableSourceExec::lazy(
            meta(),
            Box::new(move || {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(vec![vec![Datum::Int(7)]])
            }),
        );
        scan.open().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut chunk = scan.new_chunk();
        scan.next(&mut chunk).unwrap();
        assert_eq!((calls.load(Ordering::SeqCst), chunk.num_rows()), (1, 1));
        scan.next(&mut chunk).unwrap();
        assert_eq!((calls.load(Ordering::SeqCst), chunk.num_rows()), (1, 0));

        let mut failing = MemTableSourceExec::lazy(
            meta(),
            Box::new(|| Err(ExecError::Internal("retrieval failed".into()))),
        );
        failing.open().unwrap();
        let mut chunk = failing.new_chunk();
        assert!(failing.next(&mut chunk).is_err());
    }
}
