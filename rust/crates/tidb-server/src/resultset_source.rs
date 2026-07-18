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
use tidb_exec::distsql_recordset::DistSqlRecordSet;
use tidb_protocol::ColumnInfo;

/// Lazy source consumed by the connection result-set writer.
pub trait ResultSetSource {
    /// Pulls a bounded row batch.
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String>;

    /// Returns metadata after the first pull has established dynamic schema.
    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String>;

    /// Finishes statement execution once rows are drained.
    fn finish(&mut self) -> Result<(), String>;

    /// Releases the record-set resource, finishing it first when needed.
    fn close(&mut self) -> Result<(), String>;
}

impl ResultSetSource for DistSqlRecordSet {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<Datum>>, String> {
        DistSqlRecordSet::next_batch(self, max_rows).map_err(|error| error.to_string())
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, String> {
        Ok(DistSqlRecordSet::columns(self).to_vec())
    }

    fn finish(&mut self) -> Result<(), String> {
        DistSqlRecordSet::finish(self).map_err(|error| error.to_string())
    }

    fn close(&mut self) -> Result<(), String> {
        DistSqlRecordSet::close(self).map_err(|error| error.to_string())
    }
}
