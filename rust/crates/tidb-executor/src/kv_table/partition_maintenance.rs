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

impl KvTable {
    /// Replaces selected physical partitions with empty ones while preserving
    /// the logical table and every unselected partition.
    pub(crate) fn truncate_partitions(
        &mut self,
        ordinals: &[usize],
        replacement_ids: &[i64],
        ctx: &crate::StmtContext,
    ) -> Result<(), KvTableError> {
        debug_assert_eq!(ordinals.len(), replacement_ids.len());
        let old_ids = {
            let partition = self.partition.as_ref().expect("validated by DDL");
            ordinals
                .iter()
                .map(|ordinal| partition.definitions[*ordinal].id)
                .collect::<Vec<_>>()
        };

        let previous_read_partitions = self.read_partitions.replace(old_ids.clone());
        let rows = self.scan_rows_with_handles_recomputed(&RowDecodeContext::for_write(ctx));
        self.read_partitions = previous_read_partitions;
        let rows = rows?;
        let zone = ctx.session_zone();
        for (handle, row) in rows {
            self.delete_index_entries(&row, &handle, &zone)?;
        }

        for physical_id in old_ids.iter().copied() {
            let (low, high) = get_table_handle_key_range(physical_id);
            let mut upper = high;
            upper.push(0);
            let mut iterator = self
                .store
                .iter(Some(&Key::from_bytes(low)), Some(&Key::from_bytes(upper)))
                .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            let mut keys = Vec::new();
            while iterator.valid() {
                keys.push(iterator.key().clone());
                iterator
                    .next()
                    .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            }
            iterator.close();
            for key in keys {
                self.store
                    .delete(key)
                    .map_err(|error| KvTableError::Storage(format!("{error:?}")))?;
            }
        }

        let partition = self.partition.as_mut().expect("validated by DDL");
        for (ordinal, replacement_id) in ordinals.iter().zip(replacement_ids) {
            partition.definitions[*ordinal].id = *replacement_id;
        }
        // Catalog-owned tables normally have no read restriction, but keeping
        // a restriction coherent costs nothing and prevents a narrowed clone
        // from retaining retired physical IDs if this operation is reused.
        self.read_partitions = self.read_partitions.take().map(|ids| {
            ids.into_iter()
                .map(|id| {
                    old_ids
                        .iter()
                        .position(|old_id| *old_id == id)
                        .map_or(id, |index| replacement_ids[index])
                })
                .collect()
        });
        self.dirty_content = true;
        Ok(())
    }
}
