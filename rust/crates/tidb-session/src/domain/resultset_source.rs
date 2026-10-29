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
use tidb_executor::adapter::ResultFieldMetadata;

/// Statement values read at the metadata/terminal boundary, not before Next.
pub struct StatementStatus<'a> {
    /// Warnings produced so far, including during Next/Finish.
    pub warnings: u16,
    /// Current transaction/autocommit state.
    pub status: crate::domain::wire_status::WireStatus,
    /// Affected-row count for an OK-as-EOF packet.
    pub affected_rows: u64,
    /// Statement's insert identifier.
    pub last_insert_id: u64,
    /// Statement informational text.
    pub info: &'a [u8],
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
    ) -> Result<(), tidb_terror::MysqlError> {
        Err("result source does not produce chunks".into())
    }

    /// Types of native chunk cells.
    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        &[]
    }

    /// Pulls a bounded row batch.
    fn next_batch(&mut self, max_rows: usize)
        -> Result<Vec<Vec<Datum>>, tidb_terror::MysqlError>;

    /// Go `RecordSet.Fields()`: the result fields, read after the first pull
    /// has established a dynamic schema.
    fn fields(&mut self) -> Result<Vec<ResultFieldMetadata>, tidb_terror::MysqlError>;

    /// Finishes statement execution once rows are drained.
    fn finish(&mut self) -> Result<(), tidb_terror::MysqlError>;

    /// Releases the record-set resource, finishing it first when needed.
    fn close(&mut self) -> Result<(), tidb_terror::MysqlError>;

    /// Go `StmtExecDetails.WriteSQLRespDuration += ...`: hands the
    /// statement the time the connection spent writing its result. Sources
    /// with no statement of their own ignore it.
    fn add_write_sql_resp_duration(&mut self, _duration: std::time::Duration) {}

    /// Go `writeChunks`' deferred `RUV2Metrics.AddResultChunkCells`: hands
    /// the statement the rows times columns written for it.
    fn add_result_chunk_cells(&mut self, _cells: i64) {}
}

/// Times the pulls a result-set writer makes on `inner`, so the writer's
/// remaining time -- column info, row encoding, packet writes, the terminal
/// packet and the flush -- is Go `writeChunks`' `WriteSQLRespDuration`, which
/// leaves `rs.Next` and `rs.Finish` out.
pub struct PullTimedResultSetSource<'a, S: ResultSetSource + ?Sized> {
    inner: &'a mut S,
    pulled: std::time::Duration,
    /// Go `writeChunks`' `columnCount`, once the writer has the columns.
    column_count: usize,
}

impl<'a, S: ResultSetSource + ?Sized> PullTimedResultSetSource<'a, S> {
    pub fn new(inner: &'a mut S) -> Self {
        Self {
            inner,
            pulled: std::time::Duration::ZERO,
            column_count: 0,
        }
    }

    /// The number of result columns the writer read.
    pub fn column_count(&self) -> usize {
        self.column_count
    }

    /// The time spent inside `inner`'s pulls so far.
    pub fn pulled(&self) -> std::time::Duration {
        self.pulled
    }

    fn timed<T>(&mut self, pull: impl FnOnce(&mut S) -> T) -> T {
        let started = std::time::Instant::now();
        let result = pull(self.inner);
        self.pulled += started.elapsed();
        result
    }
}

impl<S: ResultSetSource + ?Sized> ResultSetSource for PullTimedResultSetSource<'_, S> {
    fn statement_status(&self) -> Option<StatementStatus<'_>> {
        self.inner.statement_status()
    }

    fn new_chunk(&self) -> Option<tidb_chunk::chunk::Chunk> {
        self.inner.new_chunk()
    }

    fn next_chunk(
        &mut self,
        chunk: &mut tidb_chunk::chunk::Chunk,
    ) -> Result<(), tidb_terror::MysqlError> {
        self.timed(|inner| inner.next_chunk(chunk))
    }

    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        self.inner.field_types()
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, tidb_terror::MysqlError> {
        self.timed(|inner| inner.next_batch(max_rows))
    }

    fn fields(&mut self) -> Result<Vec<ResultFieldMetadata>, tidb_terror::MysqlError> {
        let fields = self.timed(|inner| inner.fields());
        if let Ok(fields) = &fields {
            self.column_count = fields.len();
        }
        fields
    }

    fn finish(&mut self) -> Result<(), tidb_terror::MysqlError> {
        self.timed(|inner| inner.finish())
    }

    fn close(&mut self) -> Result<(), tidb_terror::MysqlError> {
        self.timed(|inner| inner.close())
    }

    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        self.inner.add_write_sql_resp_duration(duration);
    }

    fn add_result_chunk_cells(&mut self, cells: i64) {
        self.inner.add_result_chunk_cells(cells);
    }
}
