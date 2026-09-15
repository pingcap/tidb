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

/// Statement values read at the metadata/terminal boundary, not before Next.
pub struct StatementStatus<'a> {
    /// Warnings produced so far, including during Next/Finish.
    pub warnings: u16,
    /// Current transaction/autocommit state.
    pub status: crate::wire_status::WireStatus,
    /// Affected-row count for an OK-as-EOF packet.
    pub affected_rows: u64,
    /// Statement's insert identifier.
    pub last_insert_id: u64,
    /// Statement informational text.
    pub info: &'a [u8],
}

impl StatementStatus<'_> {
    pub(crate) fn apply(&self, options: &mut tidb_protocol::ResultSetOptions) {
        // Keep command-owned flags (more results/cursor) alongside live session bits.
        const SESSION_BITS: u16 = crate::wire_status::SERVER_STATUS_IN_TRANS
            | crate::wire_status::SERVER_STATUS_AUTOCOMMIT;
        options.status_flags = (options.status_flags & !SESSION_BITS) | self.status.bits();
        options.warnings = self.warnings;
        options.affected_rows = self.affected_rows;
        options.last_insert_id = self.last_insert_id;
        options.info.clear();
        options.info.extend_from_slice(self.info);
    }
}

/// Lazy source consumed by the connection result-set writer.
pub trait ResultSetSource {
    /// Live statement state when execution belongs to this source.
    fn statement_status(&self) -> Option<StatementStatus<'_>> {
        None
    }

    /// Native chunk shape, or None for a row-oriented compatibility source.
    fn new_chunk(&self) -> Option<tidb_chunk::chunk::Chunk> {
        None
    }

    /// Fills the source's reusable chunk.
    fn next_chunk(
        &mut self,
        _chunk: &mut tidb_chunk::chunk::Chunk,
    ) -> Result<(), tidb_executor::MysqlError> {
        Err("result source does not produce chunks".into())
    }

    /// Types of native chunk cells.
    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        &[]
    }

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
