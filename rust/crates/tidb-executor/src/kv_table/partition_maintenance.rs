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

use super::*;

/// The per-partition row reads Go's admin check runs over a [`KvTable`].
pub trait KvTablePartitionMaintenance {
    /// The rows of each physical table, with their handles and the physical
    /// id they are stored under: Go's per-partition reorg and check workers.
    /// Two partitions may hold the same `_tidb_rowid` once a partition has
    /// been exchanged, so a handle alone does not name a row.
    fn scan_physical_rows_with_handles(
        &mut self,
        decode_context: &RowDecodeContext,
    ) -> Result<Vec<(i64, TableHandle, Vec<Datum>)>, KvTableError>;

    /// The rows stored under one physical table, with their handles.
    fn rows_of_physical_table(
        &mut self,
        physical_id: i64,
        decode_context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError>;
}

impl KvTablePartitionMaintenance for KvTable {
    fn scan_physical_rows_with_handles(
        &mut self,
        decode_context: &RowDecodeContext,
    ) -> Result<Vec<(i64, TableHandle, Vec<Datum>)>, KvTableError> {
        let mut rows = Vec::new();
        for physical_id in self.record_physical_ids() {
            rows.extend(
                self.rows_of_physical_table(physical_id, decode_context)?
                    .into_iter()
                    .map(|(handle, row)| (physical_id, handle, row)),
            );
        }
        Ok(rows)
    }

    fn rows_of_physical_table(
        &mut self,
        physical_id: i64,
        decode_context: &RowDecodeContext,
    ) -> Result<Vec<(TableHandle, Vec<Datum>)>, KvTableError> {
        let previous_read_partitions = self
            .partition()
            .is_some()
            .then(|| self.read_partitions_mut().replace(vec![physical_id]));
        let rows = self.scan_rows_with_handles_recomputed(decode_context);
        if let Some(previous) = previous_read_partitions {
            *self.read_partitions_mut() = previous;
        }
        rows
    }

}

