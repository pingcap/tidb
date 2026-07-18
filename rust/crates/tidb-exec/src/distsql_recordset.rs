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

//! DistSQL-backed implementation of the executor RecordSet lifecycle.
//!
//! This adapter consumes the existing decoded `SelectResponseIter`; it does
//! not construct requests or invent a TiKV transport. Raw datum rows are
//! exposed only as they are pulled, preserving the source's bounded lifecycle.

use tidb_datatype::Datum;
use tidb_distsql::{ResponseChannelError, SelectResponseIter, SelectResultRuntimeStats};
use tidb_protocol::ColumnInfo;

use crate::recordset_lifecycle::RecordSetLifecycle;

/// Error returned while consuming or closing a DistSQL record set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DistSqlRecordSetError {
    /// The checked DistSQL response iterator failed.
    Source(String),
}

impl std::fmt::Display for DistSqlRecordSetError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for DistSqlRecordSetError {}

/// Lazy RecordSet over one already-injected select response iterator.
pub struct DistSqlRecordSet {
    iter: SelectResponseIter,
    columns: Vec<ColumnInfo>,
    lifecycle: RecordSetLifecycle,
}

impl DistSqlRecordSet {
    /// Binds resolved result metadata to an existing decoded response stream.
    #[must_use]
    pub fn new(iter: SelectResponseIter, columns: Vec<ColumnInfo>) -> Self {
        Self {
            iter,
            columns,
            lifecycle: RecordSetLifecycle::default(),
        }
    }

    /// Returns source-derived metadata. The server controls when this is read.
    #[must_use]
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Borrows runtime statistics accumulated while the source iterator drains.
    #[must_use]
    pub fn runtime_stats(&self) -> &SelectResultRuntimeStats {
        self.iter.runtime_stats()
    }

    /// Pulls at most `max_rows` rows without reading the remainder.
    pub fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, DistSqlRecordSetError> {
        self.lifecycle.mark_advanced();
        let mut rows = Vec::with_capacity(max_rows);
        while rows.len() < max_rows {
            let next = self.iter.next_row().map_err(map_source_error)?;
            let Some(row) = next else {
                break;
            };
            rows.push(row.row);
        }
        Ok(rows)
    }

    /// Runs statement finish once. Resource close remains a separate phase.
    pub fn finish(&mut self) -> Result<(), DistSqlRecordSetError> {
        self.lifecycle.begin_finish();
        Ok(())
    }

    /// Closes the injected response iterator exactly once.
    pub fn close(&mut self) -> Result<(), DistSqlRecordSetError> {
        if self.lifecycle.begin_close() {
            // Go recordSet.Close always enters Finish first, including error
            // paths where the server never reached terminal EOF. Keep that
            // cleanup invariant structural instead of relying on every caller
            // to remember a separate finish call.
            self.finish()?;
            self.iter.close();
        }
        Ok(())
    }

    /// Exposes lifecycle state for connection adapters and focused tests.
    #[must_use]
    pub const fn lifecycle(&self) -> &RecordSetLifecycle {
        &self.lifecycle
    }
}

fn map_source_error(error: ResponseChannelError) -> DistSqlRecordSetError {
    DistSqlRecordSetError::Source(error.to_string())
}
