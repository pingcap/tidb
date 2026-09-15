// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;

use super::{DriverError, SelectMeta};
use crate::{Executor, StatementMemory};

/// Go `recordSet`: an opened query executor whose results remain in chunks.
/// The caller retains the statement and snapshot until `finish`.
pub struct QueryRecordSet {
    executor: Option<Box<dyn Executor>>,
    columns: Vec<(String, FieldType)>,
    memory: StatementMemory,
    init_cap: usize,
    max_chunk_size: usize,
}

impl QueryRecordSet {
    pub(super) fn open(
        mut executor: Box<dyn Executor>,
        columns: Vec<(String, FieldType)>,
        memory: StatementMemory,
    ) -> Result<Self, DriverError> {
        if let Err(error) = executor.open() {
            // Go ExecStmt.Exec closes even a partially opened tree and
            // preserves the Open error if cleanup fails too.
            let _ = executor.close();
            return Err(error.into());
        }
        Ok(Self {
            init_cap: executor.init_cap(),
            max_chunk_size: executor.max_chunk_size(),
            executor: Some(executor),
            columns,
            memory,
        })
    }

    /// Result field names and types, including after `finish`.
    pub fn columns(&self) -> &[(String, FieldType)] {
        &self.columns
    }

    /// Go `recordSet.NewChunk`, retaining schema after the executor closes.
    pub fn new_chunk(&self) -> Chunk {
        if let Some(executor) = &self.executor {
            return executor.new_chunk();
        }
        let types: Vec<_> = self.columns.iter().map(|(_, ty)| ty.clone()).collect();
        Chunk::new(&types, self.init_cap, self.max_chunk_size)
    }

    /// Fills a caller-owned chunk; an empty chunk is EOF, not implicit close.
    pub fn next(&mut self, req: &mut Chunk) -> Result<(), DriverError> {
        self.memory.check()?;
        let executor = self.executor.as_mut().ok_or_else(|| {
            DriverError::Mysql(crate::MysqlError::new(
                tidb_error::mysql::errcode::ErrQueryInterrupted,
                "Query execution was interrupted",
            ))
        })?;
        executor.next(req)?;
        self.memory.check()?;
        Ok(())
    }

    /// Releases the executor once, including when its Close reports an error.
    pub fn finish(&mut self) -> Result<(), DriverError> {
        if let Some(mut executor) = self.executor.take() {
            executor.close()?;
        }
        Ok(())
    }

    /// Materializes for consumers that explicitly require owned values,
    /// using the same chunk-producing execution as streaming consumers.
    pub fn collect(mut self) -> Result<SelectMeta, DriverError> {
        let types: Vec<_> = self.columns.iter().map(|(_, ty)| ty.clone()).collect();
        let mut req = self.new_chunk();
        let mut rows = Vec::new();
        loop {
            self.next(&mut req)?;
            if req.num_rows() == 0 {
                break;
            }
            for index in 0..req.num_rows() {
                rows.push(req.get_row(index).get_datum_row(&types));
            }
        }
        self.finish()?;
        Ok((std::mem::take(&mut self.columns), rows))
    }
}

impl Drop for QueryRecordSet {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened_record_set() -> QueryRecordSet {
        let ctx = crate::StmtContext::for_query();
        let statement = ctx.parse("SELECT 1").unwrap();
        let tidb_ast::Stmt::Query(query) = &statement else {
            unreachable!()
        };
        super::super::open_query_meta_stmt_with_physical(
            query,
            None,
            &super::super::Catalog::default(),
            "test",
            &ctx,
        )
        .unwrap()
    }

    // Go pkg/executor/adapter_internal_test.go::TestRecordSetNewChunkAfterFinish.
    #[test]
    fn test_record_set_new_chunk_after_finish() {
        let mut rs = opened_record_set();
        rs.finish().unwrap();
        assert_eq!(rs.new_chunk().num_cols(), 1);
        assert_eq!(rs.columns().len(), 1);
    }

    // Go pkg/executor/adapter_internal_test.go::TestRecordSetNextAfterFinish.
    #[test]
    fn test_record_set_next_after_finish() {
        let mut rs = opened_record_set();
        rs.finish().unwrap();
        let mut req = rs.new_chunk();
        let error = rs.next(&mut req).unwrap_err().to_mysql_error();
        assert_eq!(error.code, tidb_error::mysql::errcode::ErrQueryInterrupted);
    }
}
