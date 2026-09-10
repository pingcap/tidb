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

//! Server-facing result-set source contracts.

use tidb_datatype::Datum;
use tidb_exec::distsql_recordset::{DistSqlRecordSet, TextResultBatch};
use tidb_protocol::ColumnInfo;

/// Lazy source consumed by the connection result-set writer.
pub trait ResultSetSource {
    /// Pulls a bounded row batch.
    fn next_batch(&mut self, max_rows: usize)
        -> Result<Vec<Vec<Datum>>, tidb_executor::MysqlError>;

    /// Whether this source can retain a typed chunk while the text writer
    /// formats rows directly from borrowed cells. Row-oriented sources keep
    /// the default `false` and use [`Self::next_batch`].
    fn supports_text_batch(&self) -> bool {
        false
    }

    /// Pulls one typed chunk for the Go-shaped text writer. `None` means the
    /// source is exhausted; unsupported sources must leave this method at its
    /// default and report `supports_text_batch() == false`.
    fn next_text_batch(
        &mut self,
        _max_rows: usize,
    ) -> Result<Option<Box<dyn TextResultBatch>>, tidb_executor::MysqlError> {
        Ok(None)
    }

    /// Returns metadata after the first pull has established dynamic schema.
    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError>;

    /// Finishes statement execution once rows are drained.
    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError>;

    /// Releases the record-set resource, finishing it first when needed.
    fn close(&mut self) -> Result<(), tidb_executor::MysqlError>;
}

impl ResultSetSource for DistSqlRecordSet {
    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, tidb_executor::MysqlError> {
        DistSqlRecordSet::next_batch(self, max_rows).map_err(tidb_executor::MysqlError::from)
    }

    fn supports_text_batch(&self) -> bool {
        true
    }

    fn next_text_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Option<Box<dyn TextResultBatch>>, tidb_executor::MysqlError> {
        DistSqlRecordSet::next_text_batch(self, max_rows).map_err(tidb_executor::MysqlError::from)
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError> {
        Ok(DistSqlRecordSet::columns(self).to_vec())
    }

    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        DistSqlRecordSet::finish(self).map_err(tidb_executor::MysqlError::from)
    }

    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        DistSqlRecordSet::close(self).map_err(tidb_executor::MysqlError::from)
    }
}
